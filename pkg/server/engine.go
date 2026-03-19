package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/lynxbase/lynxdb/pkg/bufmgr"
	"github.com/lynxbase/lynxdb/pkg/bufmgr/consumers"
	"github.com/lynxbase/lynxdb/pkg/cache"
	ingestcluster "github.com/lynxbase/lynxdb/pkg/cluster/ingest"
	querycluster "github.com/lynxbase/lynxdb/pkg/cluster/query"
	"github.com/lynxbase/lynxdb/pkg/config"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	ingestpipeline "github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/storage/compaction"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
	"github.com/lynxbase/lynxdb/pkg/storage/sources"
	"github.com/lynxbase/lynxdb/pkg/storage/tiering"
	"github.com/lynxbase/lynxdb/pkg/storage/views"
	"golang.org/x/sync/singleflight"
)

// ErrShuttingDown is returned when ingest is called after PrepareShutdown.
var ErrShuttingDown = errors.New("engine: shutting down")

// Engine owns all storage state, background goroutines, and query execution.
type Engine struct {
	indexes      map[string]model.IndexConfig
	currentEpoch atomic.Pointer[segmentEpoch] // immutable segment snapshot; see epoch.go
	dataDir      string
	retention    time.Duration // data retention period for partition-based retention
	storageCfg   config.StorageConfig
	logger       *slog.Logger
	mu           sync.RWMutex
	startTime    time.Time

	closing        atomic.Bool  // set by PrepareShutdown to reject new ingests
	lastIngestTime atomic.Int64 // UnixNano of last Ingest() call

	activeJobs atomic.Int64
	maxConcur  atomic.Int32
	jobs       sync.Map // jobID (string) -> *SearchJob
	queryCfg   config.QueryConfig

	// ingestGen is a monotonically increasing generation counter, bumped on every
	// ingest and flush. Used as part of the query cache key to ensure queries
	// after new data always miss the cache (E3: cache invalidation on ingest).
	ingestGen atomic.Int64

	// External IndexStore for full SPL2 queries (CTEs, multi-index, etc.)
	indexStore *spl2.IndexStore

	// Query result cache.
	cache *cache.Store

	// Decoded column projection cache (C4): caches decoded column data to
	// avoid repeated LZ4 decompression across queries hitting the same segments.
	projectionCache *cache.ProjectionCache

	// Event bus for live query subscriptions.
	eventBus *storage.EventBus

	// Observability metrics.
	metrics *storage.Metrics

	// Disk persistence components (nil when dataDir=="").
	compactor       *compaction.Compactor
	tierMgr         *tiering.Manager
	objStore        objstore.ObjectStore
	remoteLoadGroup singleflight.Group // deduplicates concurrent S3 fetches for the same segment

	// Data directory layout (nil when dataDir=="").
	layout *storage.Layout

	// Part-based storage (direct-to-disk model). Nil when dataDir=="".
	partLayout   *part.Layout
	partRegistry *part.Registry
	partWriter   *part.Writer // shared writer for batcher and compaction output
	batcher      *part.AsyncBatcher
	retentionMgr *part.RetentionManager

	// Materialized views.
	viewRegistry *views.ViewRegistry
	mvDispatcher *views.Dispatcher
	backfillJobs sync.Map // viewName (string) → *SearchJob — tracks active backfill jobs for progress reporting

	// Source registry for multi-source/wildcard query resolution.
	sourceRegistry *sources.Registry

	// Spill file lifecycle manager for sort/aggregate disk spill.
	spillMgr *enginepipeline.SpillManager

	// Memory management v2: Governor + BufferManager.
	governor         memgov.Governor                  // process-wide memory budget with class-based accounting
	bufMgr           bufmgr.Manager                   // frame-based buffer manager with state machine
	bufMgrCancel     context.CancelFunc               // cancels the bufMgr scheduler goroutine on shutdown
	segCacheConsumer *consumers.SegmentCacheConsumer   // segment cache backed by bufMgr (nil when bufMgr is nil)

	// Ingest dedup stage (nil when ingest.dedup_enabled is false).
	ingestDedup *ingestpipeline.DedupStage

	// Compaction consecutive failure tracker (per-index).
	compactionFailures *compactionFailureTracker

	// Deletion pacer: rate-limits file deletion to avoid SSD TRIM spikes (nil when dataDir=="").
	deletionPacer       *compaction.DeletionPacer
	deletionPacerCancel context.CancelFunc

	// Compaction scheduler: priority queue + worker pool + rate limiter (nil when dataDir=="").
	compactionSched *compaction.Scheduler
	adaptiveCtrl    *compaction.AdaptiveController

	// Server, ingest, views, and cluster config.
	serverCfg  config.ServerConfig
	ingestCfg  config.IngestConfig
	viewsCfg   config.ViewsConfig
	bufMgrCfg  config.BufferManagerConfig
	clusterCfg config.ClusterConfig

	// Cluster ingest components (all nil in single-node mode).
	// These are initialized by InitCluster() when cluster mode is enabled
	// and this node has the ingest role.
	clusterRouter    *ingestcluster.Router
	clusterCatalog   *ingestcluster.PartCatalog
	clusterNotifier  *ingestcluster.PartNotifier
	clusterSequencer *ingestcluster.BatchSequencer
	clusterMetaLoss  *ingestcluster.MetaLossDetector

	// Cluster query coordinator (nil in single-node mode).
	// Initialized by InitClusterQuery() when cluster mode is enabled
	// and this node has the query role.
	clusterCoordinator *querycluster.Coordinator

	// OnQueryComplete is an optional callback invoked after every query completes
	// (success or error). Used by the REST layer to record Prometheus metrics
	// without introducing a prometheus dependency in the server package.
	onQueryComplete func(*SearchStats)
}

// Config configures the Engine.
type Config struct {
	DataDir   string        // Root directory for all data (segments, parts, indexes). Empty = in-memory only.
	Retention time.Duration // Data retention period. 0 = use default (90 days).

	Storage       config.StorageConfig
	Logger        *slog.Logger
	Query         config.QueryConfig
	Ingest        config.IngestConfig
	Server        config.ServerConfig
	Views         config.ViewsConfig
	BufferManager config.BufferManagerConfig
	Cluster       config.ClusterConfig
}

// NewEngine creates a new Engine.
func NewEngine(cfg Config) *Engine {
	storageCfg := cfg.Storage
	if storageCfg.FlushThreshold == 0 {
		storageCfg = config.DefaultConfig().Storage
	}

	cacheDir := ""
	if cfg.DataDir != "" {
		cacheDir = filepath.Join(cfg.DataDir, "query-cache")
	}

	queryCfg := cfg.Query
	if queryCfg.MaxConcurrent == 0 {
		queryCfg = config.DefaultConfig().Query
	}

	// Create spill manager for sort/aggregate disk spill with quota enforcement.
	spillMgr, spillErr := enginepipeline.NewSpillManagerWithQuota(
		queryCfg.SpillDir, int64(queryCfg.MaxTempDirSizeBytes), cfg.Logger,
	)
	if spillErr != nil {
		// Non-fatal: log warning and proceed without spill support.
		cfg.Logger.Warn("failed to create spill manager, disk spill disabled", "error", spillErr)
	}

	// Build the cache first — we may need its EvictBytes as the evictor callback.
	queryCache := cache.NewStore(cacheDir, 256<<20, 30*time.Second)

	// Projection cache (C4): caches decoded column data to avoid repeated
	// LZ4 decompression across queries hitting the same segments.
	// Size: 256MB or 10% of total memory pool, whichever is smaller.
	projCacheBytes := int64(256 << 20) // 256MB default
	projCache := cache.NewProjectionCache(projCacheBytes)

	// Resolve unified memory pool: when server.total_memory_pool_bytes is set
	// (or auto-detected), create a Governor for elastic sharing between
	// query execution and segment cache. Otherwise, fall back to the legacy
	// separate-pool model.
	totalPoolBytes := int64(cfg.Server.TotalMemoryPoolBytes)
	if totalPoolBytes == 0 {
		// Auto-detect: 40% of system RAM, clamped [512MB, 128GB].
		if sysMem := stats.TotalSystemMemory(); sysMem > 0 {
			totalPoolBytes = sysMem * 40 / 100

			const minTotal = 512 << 20
			const maxTotal = 128 << 30

			if totalPoolBytes < minTotal {
				totalPoolBytes = minTotal
			}
			if totalPoolBytes > maxTotal {
				totalPoolBytes = maxTotal
			}
		}
	}

	// Initialize memory governance v2: Governor + BufferManager.
	bmCfg := cfg.BufferManager
	// The governor provides class-based memory accounting with pressure callbacks.
	// The buffer manager provides frame-level state machine with batched eviction.
	gov := memgov.NewGovernor(memgov.GovernorConfig{
		TotalLimit: totalPoolBytes,
	})

	// Register pressure callbacks so the governor can reclaim memory under pressure.
	// PageCache: evict query cache entries to free memory for higher-priority classes.
	gov.OnPressure(memgov.ClassPageCache, func(target int64) int64 {
		return queryCache.EvictBytes(target)
	})

	var bufMgr bufmgr.Manager
	if bmCfg.Enabled {
		var bmErr error
		bufMgr, bmErr = bufmgr.NewManager(bufmgr.ManagerConfig{
			MaxMemoryBytes: int64(bmCfg.MaxMemoryBytes),
			FrameSize:      bmCfg.PageSize,
			EnableOffHeap:  bmCfg.EnableOffHeap,
			Governor:       gov,
			Logger:         cfg.Logger,
		})
		if bmErr != nil {
			cfg.Logger.Warn("failed to create buffer manager v2, running without",
				"error", bmErr)
			bufMgr = nil
		}
	}

	// Register page cache pressure callback: evict segment cache frames from
	// the buffer manager to free memory for higher-priority allocations.
	if bufMgr != nil {
		bm := bufMgr // capture for closure
		frameSize := int64(bmCfg.PageSize)
		if frameSize <= 0 {
			frameSize = int64(bufmgr.FrameSize64KB)
		}
		gov.OnPressure(memgov.ClassPageCache, func(target int64) int64 {
			framesToEvict := int((target + frameSize - 1) / frameSize)
			if framesToEvict < 1 {
				framesToEvict = 1
			}
			evicted := bm.EvictBatch(framesToEvict, bufmgr.OwnerSegCache)
			return int64(evicted) * frameSize
		})
	}

	// Create segment cache consumer when buffer manager v2 is available.
	var segCacheConsumer *consumers.SegmentCacheConsumer
	if bufMgr != nil {
		segCacheConsumer = consumers.NewSegmentCacheConsumer(bufMgr)
	}

	// Initialize ingest dedup stage when enabled.
	var dedupStage *ingestpipeline.DedupStage
	if cfg.Ingest.DedupEnabled {
		dedupCap := cfg.Ingest.DedupCapacity
		if dedupCap == 0 {
			dedupCap = 100_000
		}

		dedupStage = ingestpipeline.NewDedupStage(dedupCap)
		cfg.Logger.Info("ingest dedup enabled", "capacity", dedupCap)
	}

	e := &Engine{
		indexes:            make(map[string]model.IndexConfig),
		dataDir:            cfg.DataDir,
		retention:          cfg.Retention,
		storageCfg:         storageCfg,
		logger:             cfg.Logger,
		eventBus:           storage.NewEventBus(0),
		cache:              queryCache,
		projectionCache:    projCache,
		startTime:          time.Now(),
		metrics:            storage.NewMetrics(),
		queryCfg:           queryCfg,
		spillMgr:           spillMgr,
		ingestDedup:        dedupStage,
		compactionFailures: newCompactionFailureTracker(),
		governor:           gov,
		bufMgr:             bufMgr,
		segCacheConsumer:   segCacheConsumer,
		serverCfg:          cfg.Server,
		ingestCfg:          cfg.Ingest,
		viewsCfg:           cfg.Views,
		bufMgrCfg:          bmCfg,
		clusterCfg:         cfg.Cluster,
		sourceRegistry:     sources.NewRegistry(),
	}
	e.currentEpoch.Store(&segmentEpoch{
		done: make(chan struct{}),
	})

	e.maxConcur.Store(int32(queryCfg.MaxConcurrent))

	// Wire cache memory into the governor so inserts/evictions are tracked.
	e.cache.SetPool(cache.NewGovernorPoolAdapter(gov))

	// Create default index.
	e.indexes[DefaultIndexName] = model.DefaultIndexConfig(DefaultIndexName)

	return e
}

// Start initializes persistence and starts background goroutines.
// It also starts listening on the provided listener address if ln is non-nil,
// but typically the HTTP server handles listening. Call this to initialize
// the engine's background processes.
func (e *Engine) Start(ctx context.Context) error {
	// Clean up orphan spill files from previous crashes.
	// Must happen BEFORE NewSpillManagerWithQuota() creates the current PID's
	// directory, to avoid deleting our own freshly-created spill directory.
	spillDir := e.queryCfg.SpillDir
	if spillDir == "" {
		spillDir = os.TempDir()
	}
	enginepipeline.CleanupOrphans(spillDir, e.logger)

	if e.dataDir != "" {
		if err := e.initDataDir(); err != nil {
			return fmt.Errorf("engine: init data dir: %w", err)
		}
		if err := e.initDiskPersistence(ctx); err != nil {
			return fmt.Errorf("engine: init persistence: %w", err)
		}
		e.logger.Info("data directory initialized", "path", e.dataDir)
	} else {
		e.logger.Info("running in-memory mode (no data directory configured)")
	}

	if e.bufMgr != nil {
		bmCtx, bmCancel := context.WithCancel(ctx)
		e.bufMgrCancel = bmCancel
		e.bufMgr.Start(bmCtx)
		ms := e.bufMgr.Stats()
		e.logger.Info("buffer manager v2 initialized",
			"frames", ms.TotalFrames,
			"frame_size", e.bufMgr.FrameSize(),
		)
	}

	go e.startJobGC(ctx)

	if e.dataDir != "" {
		// Start deletion pacer for rate-limited file removal.
		e.deletionPacer = compaction.NewDeletionPacer(0)
		dpCtx, dpCancel := context.WithCancel(ctx)
		e.deletionPacerCancel = dpCancel
		go e.deletionPacer.DrainLoop(dpCtx)

		e.startCompaction(ctx)
		e.startTiering(ctx)
	}

	return nil
}

// PrepareShutdown signals the engine to stop accepting new ingests.
// Existing in-flight operations will complete normally. Call Shutdown() after
// draining HTTP connections to finalize cleanup (flush batcher, close mmaps).
func (e *Engine) PrepareShutdown() {
	e.closing.Store(true)
}

// Shutdown performs graceful cleanup: flush batcher, close registry, close mmaps.
// Returns an aggregated error if any step fails.
func (e *Engine) Shutdown(_ time.Duration) error {
	var errs []error

	// Flush remaining buffered events via the async batcher.
	if e.batcher != nil {
		if err := e.batcher.Close(); err != nil {
			e.logger.Error("shutdown batcher close failed", "error", err)
			errs = append(errs, fmt.Errorf("close batcher: %w", err))
		}
	}

	// Flush MV dispatcher.
	if e.mvDispatcher != nil {
		if err := e.mvDispatcher.Stop(); err != nil {
			e.logger.Error("shutdown MV dispatcher failed", "error", err)
			errs = append(errs, fmt.Errorf("stop MV dispatcher: %w", err))
		}
	}

	// Retire all segments via epoch advance so background readers can drain.
	e.mu.Lock()
	old := e.currentEpoch.Load()
	retired := make([]*segmentHandle, len(old.segments))
	copy(retired, old.segments)
	e.advanceEpoch(make([]*segmentHandle, 0), retired)
	e.mu.Unlock()

	// Wait for active queries to drain with timeout.
	select {
	case <-old.done:
	case <-time.After(30 * time.Second):
		e.logger.Warn("shutdown: epoch drain timeout",
			"remaining_readers", old.readers.Load())
	}
	// Close all mmaps (idempotent — drainAndClose may have already closed them).
	for _, sh := range retired {
		if sh.mmap != nil {
			sh.mmap.Close()
		}
	}

	// Stop deletion pacer (flushes remaining deletions synchronously).
	if e.deletionPacerCancel != nil {
		e.deletionPacerCancel()
	}

	// Clean up any remaining spill files.
	e.spillMgr.CleanupAll()

	// Close buffer manager v2: cancel the scheduler context first so the
	// background goroutine exits, then close the manager itself.
	if e.bufMgr != nil {
		if e.bufMgrCancel != nil {
			e.bufMgrCancel()
		}
		if err := e.bufMgr.Close(); err != nil {
			e.logger.Error("shutdown buffer manager v2 close failed", "error", err)
			errs = append(errs, fmt.Errorf("close buffer manager v2: %w", err))
		}
	}

	return errors.Join(errs...)
}

// SetIndexStore sets an external IndexStore for full SPL2 queries.
func (e *Engine) SetIndexStore(store *spl2.IndexStore) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.indexStore = store
}

// SetOnQueryComplete registers a callback invoked after every query completes.
// Used by the REST layer to record Prometheus histogram observations without
// introducing a prometheus dependency in the server package.
func (e *Engine) SetOnQueryComplete(fn func(*SearchStats)) {
	e.onQueryComplete = fn
}

// Logger returns the engine's logger.
func (e *Engine) Logger() *slog.Logger {
	return e.logger
}

// MaxConcurrent returns the current max concurrent queries limit.
func (e *Engine) MaxConcurrent() int32 {
	return e.maxConcur.Load()
}

// SetMaxConcurrent atomically updates the max concurrent queries limit.
func (e *Engine) SetMaxConcurrent(n int32) {
	e.maxConcur.Store(n)
}

// QueryCfg returns the engine's query configuration.
func (e *Engine) QueryCfg() config.QueryConfig {
	return e.queryCfg
}

// SourceRegistry returns the source registry for multi-source query resolution.
func (e *Engine) SourceRegistry() *sources.Registry {
	return e.sourceRegistry
}

// SourceCount returns the number of registered sources (index names) in the registry.
// Implements usecases.QueryEngine.
func (e *Engine) SourceCount() int {
	return e.sourceRegistry.Count()
}

// resolveSourceScope resolves glob patterns and populates SourceScopeSources
// from the source registry. Called before segment filtering in query execution.
// Returns either a new QueryHints with resolved scope or the original hints
// unchanged, plus any warnings about the resolution result.
func (e *Engine) resolveSourceScope(hints *spl2.QueryHints) (*spl2.QueryHints, []string) {
	var warnings []string

	// Resolve optimizer-set glob patterns against the source registry.
	if hints.SourceScopeType == spl2.SourceScopeGlob && hints.SourceScopePattern != "" {
		matched := e.sourceRegistry.Match(hints.SourceScopePattern)
		if len(matched) > 0 {
			h := *hints
			h.SourceScopeType = spl2.SourceScopeList
			h.SourceScopeSources = matched

			if len(matched) > 20 {
				warnings = append(warnings, fmt.Sprintf(
					"Pattern %q matched %d sources — this may be slow.",
					hints.SourceScopePattern, len(matched)))
			}

			return &h, warnings
		}
		// No match: generate helpful warning.
		if e.sourceRegistry.Count() > 0 {
			available := e.sourceRegistry.List()
			if len(available) > 10 {
				available = available[:10]
			}
			warnings = append(warnings, fmt.Sprintf(
				"No sources match pattern %q. Available sources: %s",
				hints.SourceScopePattern, strings.Join(available, ", ")))
		}

		return hints, warnings
	}

	// Also resolve parser-level SourceGlob when no optimizer scope is set.
	if hints.SourceGlob != "" && hints.SourceGlob != "*" && hints.SourceScopeType == "" {
		matched := e.sourceRegistry.Match(hints.SourceGlob)
		if len(matched) > 0 {
			h := *hints
			h.SourceScopeType = spl2.SourceScopeList
			h.SourceScopeSources = matched

			return &h, warnings
		}
	}

	return hints, warnings
}

// checkSourceWarnings generates warnings when queries reference non-existent
// sources. Uses fuzzy matching (Levenshtein distance) to suggest close matches.
func (e *Engine) checkSourceWarnings(hints *spl2.QueryHints) []string {
	if hints.SourceScopeType != spl2.SourceScopeSingle {
		return nil
	}
	if len(hints.SourceScopeSources) == 0 {
		return nil
	}
	if e.sourceRegistry.Count() == 0 {
		return nil
	}

	name := hints.SourceScopeSources[0]
	if e.sourceRegistry.Contains(name) {
		return nil
	}

	available := e.sourceRegistry.List()

	// Try fuzzy match first.
	if match := spl2.ClosestMatch(name, available, 3); match != "" {
		return []string{fmt.Sprintf("Source %q does not exist. Did you mean %q?", name, match)}
	}

	// No close match — list available sources.
	if len(available) > 10 {
		available = available[:10]
	}

	return []string{fmt.Sprintf("Source %q does not exist. Available sources: %s",
		name, strings.Join(available, ", "))}
}

// ReloadConfig applies hot-reloadable configuration fields.
// Fields that require restart are logged as warnings.
func (e *Engine) ReloadConfig(cfg *config.Config) {
	if int32(cfg.Query.MaxConcurrent) != e.maxConcur.Load() {
		old := e.maxConcur.Load()
		e.maxConcur.Store(int32(cfg.Query.MaxConcurrent))
		e.logger.Info("reloaded query.max_concurrent", "old", old, "new", cfg.Query.MaxConcurrent)
	}
	e.queryCfg = cfg.Query
}

// ActiveJobs returns the number of currently running queries.
func (e *Engine) ActiveJobs() int64 {
	return e.activeJobs.Load()
}

// Governor returns the memory governor v2 for process-wide memory management.
func (e *Engine) Governor() memgov.Governor {
	return e.governor
}

// BufMgr returns the buffer manager v2, or nil if not enabled.
func (e *Engine) BufMgr() bufmgr.Manager {
	return e.bufMgr
}

// SegCacheConsumer returns the segment cache consumer backed by the buffer
// manager v2, or nil if the buffer manager is not enabled.
func (e *Engine) SegCacheConsumer() *consumers.SegmentCacheConsumer {
	return e.segCacheConsumer
}

// pinEpoch returns the current epoch with its reader count incremented.
// The caller MUST call ep.unpin() when done reading segment data.
// Pin is O(1) — a single atomic load + atomic increment.
//
// Lock-free: uses atomic.Pointer.Load() with a load-pin-verify-retry pattern
// instead of acquiring e.mu.RLock(). This reduces contention under high QPS
// from ~25ns (RLock) to ~1ns (atomic Load) per query.
func (e *Engine) pinEpoch() *segmentEpoch {
	for {
		ep := e.currentEpoch.Load()
		ep.pin()
		// Verify the epoch hasn't been swapped between Load and pin.
		// If it was, our pin is on a stale epoch — undo and retry.
		if e.currentEpoch.Load() == ep {
			return ep
		}
		ep.unpin()
	}
}

// advanceEpoch atomically creates a new epoch with the given segment list,
// attaches retired segments to the old epoch, and starts background cleanup.
// MUST be called under e.mu.Lock().
//
// Each segment in newSegments gets an incRef() so the new epoch holds a
// reference. When the old epoch drains (all pinned readers finish), it calls
// decRef() on ALL of its segments. A segment's mmap is closed only when its
// refcount reaches 0 — meaning no epoch references it anymore. This prevents
// the use-after-unmap SIGSEGV where intermediate epochs with 0 readers would
// drain immediately and close mmaps still in use by a long-running query
// pinned to an earlier epoch.
func (e *Engine) advanceEpoch(newSegments, retired []*segmentHandle) {
	old := e.currentEpoch.Load()
	old.retired = retired

	// incRef all segments in the NEW epoch before making it current.
	// This ensures segments shared between old and new epochs have refs >= 2
	// (one from old epoch, one from new) and won't be closed when old drains.
	for _, sh := range newSegments {
		sh.incRef()
	}

	e.currentEpoch.Store(&segmentEpoch{
		id:       old.id + 1,
		segments: newSegments,
		done:     make(chan struct{}),
	})

	// If no readers are pinning the old epoch, signal immediately.
	if old.readers.Load() == 0 {
		old.signalDone()
	}

	old.drainAndClose(e.logger)
}

// BufferedEventCount returns the number of events buffered in the async batcher.
func (e *Engine) BufferedEventCount() int64 {
	if e.batcher != nil {
		return e.batcher.BufferedEvents()
	}

	return 0
}

// BuildEventStoreFromHints returns raw events grouped by index, filtered by the given hints.
// Test helper — uses a bounded context to avoid hanging indefinitely.
func (e *Engine) BuildEventStoreFromHints(hints *spl2.QueryHints) map[string][]*event.Event {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, _, _ := e.buildEventStore(ctx, hints, nil)

	return store
}

// SegmentCount returns the number of segments.
func (e *Engine) SegmentCount() int {
	return len(e.currentEpoch.Load().segments)
}

// Segments returns segment metadata for inspection (test helper).
func (e *Engine) Segments() []model.SegmentMeta {
	ep := e.pinEpoch()
	defer ep.unpin()
	metas := make([]model.SegmentMeta, len(ep.segments))
	for i, sh := range ep.segments {
		metas[i] = sh.meta
	}

	return metas
}

// PartCount returns the number of parts in the part registry (0 if not initialized).
func (e *Engine) PartCount() int {
	if e.partRegistry == nil {
		return 0
	}

	return e.partRegistry.Count()
}

// PartLayout returns the part layout, or nil if not initialized.
func (e *Engine) PartLayout() *part.Layout {
	return e.partLayout
}

// HasBloomFilter returns true if any segment has a cached bloom filter (test helper).
func (e *Engine) HasBloomFilter() bool {
	for _, sh := range e.currentEpoch.Load().segments {
		if sh.bloom != nil {
			return true
		}
	}

	return false
}

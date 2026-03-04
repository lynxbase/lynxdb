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
	"github.com/lynxbase/lynxdb/pkg/buffer"
	"github.com/lynxbase/lynxdb/pkg/cache"
	"github.com/lynxbase/lynxdb/pkg/config"
	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	ingestpipeline "github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/storage/compaction"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
	"github.com/lynxbase/lynxdb/pkg/storage/sources"
	"github.com/lynxbase/lynxdb/pkg/storage/tiering"
	"github.com/lynxbase/lynxdb/pkg/storage/views"
)

// ErrShuttingDown is returned when ingest is called after PrepareShutdown.
var ErrShuttingDown = errors.New("engine: shutting down")

// Engine owns all storage state, background goroutines, and query execution.
type Engine struct {
	indexes      map[string]model.IndexConfig
	currentEpoch *segmentEpoch // immutable segment snapshot; see epoch.go
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

	// Event bus for live query subscriptions.
	eventBus *storage.EventBus

	// Observability metrics.
	metrics *storage.Metrics

	// Disk persistence components (nil when dataDir=="").
	compactor *compaction.Compactor
	tierMgr   *tiering.Manager
	objStore  objstore.ObjectStore

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

	// Global query memory pool (coordinates across concurrent queries).
	rootMonitor *stats.RootMonitor

	// Unified memory pool for elastic sharing between queries and cache.
	// Nil when server.total_memory_pool_bytes is not set (legacy mode).
	unifiedPool *stats.UnifiedPool

	// Spill file lifecycle manager for sort/aggregate disk spill.
	spillMgr *enginepipeline.SpillManager

	// Unified buffer manager. Nil when buffer_manager.enabled is false.
	bufferPool *buffer.Pool

	// Ingest dedup stage (nil when ingest.dedup_enabled is false).
	ingestDedup *ingestpipeline.DedupStage

	// Compaction consecutive failure tracker (per-index).
	compactionFailures *compactionFailureTracker

	// Server, ingest, and views config for memory pool, fsync, and backfill budget.
	serverCfg config.ServerConfig
	ingestCfg config.IngestConfig
	viewsCfg  config.ViewsConfig
	bufMgrCfg config.BufferManagerConfig

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

	// Resolve global query pool size: explicit config > auto-detect (25% system RAM).
	poolBytes := int64(queryCfg.GlobalQueryPoolBytes)
	if poolBytes == 0 {
		if sysMem := stats.TotalSystemMemory(); sysMem > 0 {
			poolBytes = sysMem / 4
			// Clamp: min 256MB, max 64GB.
			const minPool = 256 << 20
			const maxPool = 64 << 30
			if poolBytes < minPool {
				poolBytes = minPool
			}
			if poolBytes > maxPool {
				poolBytes = maxPool
			}
		}
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

	// Resolve unified memory pool: when server.total_memory_pool_bytes is set
	// (or auto-detected), create a UnifiedPool for elastic sharing between
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

	cacheReservePercent := cfg.Server.CacheReservePercent
	if cacheReservePercent == 0 {
		cacheReservePercent = 20 // Default: 20% cache floor.
	}

	var unifiedPool *stats.UnifiedPool
	var rootMonitor *stats.RootMonitor

	if totalPoolBytes > 0 {
		// Elastic sharing: UnifiedPool manages queries + cache as one pool.
		unifiedPool = stats.NewUnifiedPool(totalPoolBytes, cacheReservePercent, queryCache.EvictBytes)
		rootMonitor = stats.NewRootMonitorWithPool("query-pool", poolBytes, unifiedPool)
		queryCache.SetPool(unifiedPool)
	} else {
		// Legacy mode: separate pools for queries and cache.
		rootMonitor = stats.NewRootMonitor("query-pool", poolBytes)
	}

	// Initialize buffer manager when enabled.
	var bufPool *buffer.Pool
	bmCfg := cfg.BufferManager
	if bmCfg.Enabled {
		poolCfg := buffer.PoolConfig{
			MaxMemoryBytes:         int64(bmCfg.MaxMemoryBytes),
			PageSize:               bmCfg.PageSize,
			EnableOffHeap:          bmCfg.EnableOffHeap,
			CacheTargetPercent:     bmCfg.CacheTargetPercent,
			QueryTargetPercent:     bmCfg.QueryTargetPercent,
			BatcherTargetPercent:   bmCfg.BatcherTargetPercent,
			MaxPinnedPagesPerQuery: bmCfg.MaxPinnedPagesPerQuery,
			Logger:                 cfg.Logger,
		}
		var bufErr error
		bufPool, bufErr = buffer.NewPool(poolCfg)
		if bufErr != nil {
			cfg.Logger.Warn("failed to create buffer pool, running without buffer manager",
				"error", bufErr)
			bufPool = nil
		}
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
		eventBus:           storage.NewEventBus(),
		cache:              queryCache,
		startTime:          time.Now(),
		metrics:            storage.NewMetrics(),
		queryCfg:           queryCfg,
		rootMonitor:        rootMonitor,
		unifiedPool:        unifiedPool,
		spillMgr:           spillMgr,
		bufferPool:         bufPool,
		ingestDedup:        dedupStage,
		compactionFailures: newCompactionFailureTracker(),
		serverCfg:          cfg.Server,
		ingestCfg:          cfg.Ingest,
		viewsCfg:           cfg.Views,
		bufMgrCfg:          bmCfg,
		sourceRegistry:     sources.NewRegistry(),
	}
	e.currentEpoch = &segmentEpoch{
		done: make(chan struct{}),
	}

	e.maxConcur.Store(int32(queryCfg.MaxConcurrent))

	// Create default index.
	e.indexes[DefaultIndexName] = model.DefaultIndexConfig(DefaultIndexName)

	return e
}

// UnifiedPool returns the unified memory pool, or nil if running in legacy mode.
func (e *Engine) UnifiedPool() *stats.UnifiedPool {
	return e.unifiedPool
}

// BufferPool returns the unified buffer manager pool, or nil if buffer_manager.enabled is false.
func (e *Engine) BufferPool() *buffer.Pool {
	return e.bufferPool
}

// BufferManagerEnabled reports whether the unified buffer manager is active.
func (e *Engine) BufferManagerEnabled() bool {
	return e.bufferPool != nil
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

	if e.bufferPool != nil {
		ps := e.bufferPool.Stats()
		e.logger.Info("unified buffer manager initialized",
			"pages", ps.TotalPages,
			"page_size", ps.PageSizeBytes,
			"total_bytes", ps.TotalBytes,
			"total_mb", ps.TotalBytes/(1<<20),
			"off_heap", ps.OffHeap,
			"cache_target_pct", e.bufMgrCfg.CacheTargetPercent,
			"query_target_pct", e.bufMgrCfg.QueryTargetPercent,
			"batcher_target_pct", e.bufMgrCfg.BatcherTargetPercent)
	}

	if e.unifiedPool != nil {
		ps := e.unifiedPool.Stats()
		e.logger.Info("unified memory pool initialized",
			"total_bytes", ps.TotalLimitBytes,
			"total_mb", ps.TotalLimitBytes/(1<<20),
			"cache_reserve_floor_bytes", ps.CacheReserveFloorBytes)
	} else if poolLimit := e.rootMonitor.Limit(); poolLimit > 0 {
		e.logger.Info("query memory pool initialized",
			"pool_bytes", poolLimit,
			"pool_mb", poolLimit/(1<<20))
	}

	go e.startJobGC(ctx)

	if e.dataDir != "" {
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
	old := e.currentEpoch
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

	// Clean up any remaining spill files.
	e.spillMgr.CleanupAll()

	// Close buffer pool (releases mmap'd pages).
	if e.bufferPool != nil {
		if err := e.bufferPool.Close(); err != nil {
			e.logger.Error("shutdown buffer pool close failed", "error", err)
			errs = append(errs, fmt.Errorf("close buffer pool: %w", err))
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

// RootMonitor returns the global query memory pool monitor.
func (e *Engine) RootMonitor() *stats.RootMonitor {
	return e.rootMonitor
}

// pinEpoch returns the current epoch with its reader count incremented.
// The caller MUST call ep.unpin() when done reading segment data.
// Pin is O(1) — a single atomic increment.
func (e *Engine) pinEpoch() *segmentEpoch {
	e.mu.RLock()
	ep := e.currentEpoch
	ep.pin()
	e.mu.RUnlock()

	return ep
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
	old := e.currentEpoch
	old.retired = retired

	// incRef all segments in the NEW epoch before making it current.
	// This ensures segments shared between old and new epochs have refs >= 2
	// (one from old epoch, one from new) and won't be closed when old drains.
	for _, sh := range newSegments {
		sh.incRef()
	}

	e.currentEpoch = &segmentEpoch{
		id:       old.id + 1,
		segments: newSegments,
		done:     make(chan struct{}),
	}

	// If no readers are pinning the old epoch, signal immediately.
	if old.readers.Load() == 0 {
		old.signalDone()
	}

	old.drainAndClose(e.logger)
}

// Test helpers

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
	store, _, _ := e.buildEventStore(ctx, hints, nil, nil)

	return store
}

// SegmentCount returns the number of segments.
func (e *Engine) SegmentCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return len(e.currentEpoch.segments)
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, sh := range e.currentEpoch.segments {
		if sh.bloom != nil {
			return true
		}
	}

	return false
}

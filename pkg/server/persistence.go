package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/storage/compaction"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
	"github.com/lynxbase/lynxdb/pkg/storage/tiering"
	"github.com/lynxbase/lynxdb/pkg/storage/views"
)

// initDataDir creates the data directory structure using the Layout manager.
func (e *Engine) initDataDir() error {
	e.layout = storage.NewLayout(e.dataDir)

	// Collect index names for per-index segment directories.
	indexNames := make([]string, 0, len(e.indexes))
	for name := range e.indexes {
		indexNames = append(indexNames, name)
	}

	if err := e.layout.EnsureDirs(indexNames...); err != nil {
		return err
	}

	// Query-cache dir is not managed by Layout; create if absent.
	queryCacheDir := filepath.Join(e.dataDir, "query-cache")
	if err := os.MkdirAll(queryCacheDir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", queryCacheDir, err)
	}

	return nil
}

// initDiskPersistence sets up part layout, registry, batcher, compactor,
// tiering, and loads existing segments from disk.
func (e *Engine) initDiskPersistence(ctx context.Context) error {
	// Init part layout and registry (filesystem is source of truth).
	granularity := part.ParseGranularity(e.storageCfg.PartitionBy)
	e.partLayout = part.NewLayoutWithGranularity(e.dataDir, granularity)
	e.partRegistry = part.NewRegistry(e.logger)

	if err := e.partRegistry.ScanDir(e.partLayout); err != nil {
		return fmt.Errorf("scan parts: %w", err)
	}

	e.logger.Info("part registry loaded", "parts", e.partRegistry.Count())

	// Init compactor.
	e.compactor = compaction.NewCompactor(e.logger)

	// Initialize compaction manifest store for crash recovery.
	manifestStore, err := compaction.NewManifestStore(e.dataDir)
	if err != nil {
		return fmt.Errorf("init manifest store: %w", err)
	}
	e.manifestStore = manifestStore

	// Recover from interrupted compactions.
	pending, err := manifestStore.LoadPending()
	if err != nil {
		e.logger.Warn("failed to load pending compaction manifests", "error", err)
	} else if len(pending) > 0 {
		e.logger.Info("found interrupted compactions, cleaning up",
			"count", len(pending))
		cleaned := manifestStore.CleanupInterrupted(pending, func(id string) bool {
			return e.partRegistry.Get(id) != nil
		})
		e.logger.Info("compaction manifest cleanup complete",
			"cleaned", len(cleaned))
	}

	// Load existing parts as segment handles for query path.
	for _, meta := range e.partRegistry.All() {
		if err := e.loadPartAsSegment(meta); err != nil {
			e.logger.Warn("failed to load part, skipping", "id", meta.ID, "error", err)
		}
	}

	// Register index/source names from existing parts.
	for _, meta := range e.partRegistry.All() {
		if meta.Index != "" {
			if _, exists := e.indexes[meta.Index]; !exists {
				e.indexes[meta.Index] = model.DefaultIndexConfig(meta.Index)
			}

			e.sourceRegistry.Register(meta.Index)
		}
	}

	// Init object store.
	if e.storageCfg.S3Bucket != "" {
		s3Opts := objstore.S3Options{
			Endpoint:       e.storageCfg.S3Endpoint,
			ForcePathStyle: e.storageCfg.S3ForcePathStyle,
		}

		store, err := objstore.NewS3StoreWithOptions(ctx, e.storageCfg.S3Bucket, e.storageCfg.S3Region, e.storageCfg.S3Prefix, s3Opts)
		if err != nil {
			return fmt.Errorf("init S3 store: %w", err)
		}

		e.objStore = store
	} else {
		e.objStore = objstore.NewMemStore()
	}

	// Init tiering manager.
	e.tierMgr = tiering.NewManager(e.objStore, e.logger)

	// Pre-create segment-cache directory for remote segment downloads.
	// Done once here to avoid os.MkdirAll syscalls on every remote load.
	segCacheDir := filepath.Join(e.dataDir, "segment-cache")
	if err := os.MkdirAll(segCacheDir, 0o755); err != nil {
		return fmt.Errorf("create segment-cache dir: %w", err)
	}

	// Create part writer (shared by batcher and compaction).
	compression := segment.CompressionLZ4
	fsyncEnabled := true // safe default
	if e.ingestCfg.FSync != nil {
		fsyncEnabled = *e.ingestCfg.FSync
	}
	var writerOpts []part.WriterOption
	writerOpts = append(writerOpts, part.WithFSync(fsyncEnabled))
	writerOpts = append(writerOpts, part.WithLogger(e.logger))
	if e.storageCfg.MaxColumnsPerPart > 0 {
		writerOpts = append(writerOpts, part.WithMaxColumns(e.storageCfg.MaxColumnsPerPart))
	}
	e.partWriter = part.NewWriter(e.partLayout, compression, part.DefaultRowGroupSize, writerOpts...)

	// Create async batcher.
	batcherCfg := part.BatcherConfig{
		MaxEvents: 50_000,
		MaxBytes:  64 * 1024 * 1024,
		MaxWait:   200 * time.Millisecond,
	}

	e.batcher = part.NewAsyncBatcher(e.partWriter, e.partRegistry, batcherCfg, e.logger)
	e.batcher.SetOnCommit(func(meta *part.Meta) {
		// Open mmap'd reader and add to query-visible segments.
		if loadErr := e.loadPartAsSegment(meta); loadErr != nil {
			e.logger.Error("failed to load committed part", "id", meta.ID, "error", loadErr)

			return
		}

		// Bump ingest generation for cache invalidation.
		e.ingestGen.Add(1)

		// Update flush metrics.
		e.metrics.PartFlushes.Add(1)
		e.metrics.PartFlushBytes.Add(meta.SizeBytes)

		// Reactive compaction trigger: check if L0 parts for this index
		// exceed the threshold. When ingest bursts produce many L0 parts
		// within a single compaction tick interval, this ensures compaction
		// responds immediately instead of waiting up to 30 seconds.
		e.maybeCompactAfterFlush(ctx, meta.Index, meta.Partition)

		if e.clusterCatalog != nil && e.objStore != nil {
			go func() {
				publishCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				e.uploadAndCatalog(publishCtx, meta, e.objStore, e.logger.With("component", "cluster-publish"))
			}()
		}
	})
	e.batcher.Start(ctx)

	// Initialize partition-based retention manager.
	retentionCfg := part.RetentionConfig{
		MaxAge:   e.retention,
		Interval: part.DefaultRetentionInterval,
	}
	e.retentionMgr = part.NewRetentionManager(e.partLayout, e.partRegistry, retentionCfg, e.logger)
	e.retentionMgr.SetOnDelete(func(index, partition string, removedIDs []string, partitionDir string) {
		e.onPartitionDeleted(removedIDs, partitionDir)
	})
	e.retentionMgr.Start(ctx)

	// Initialize materialized views.
	viewReg, err := views.Open(e.layout.ViewsDir())
	if err != nil {
		return fmt.Errorf("open view registry: %w", err)
	}

	e.viewRegistry = viewReg
	e.mvDispatcher = views.NewDispatcher(viewReg, e.layout, e.logger, e.viewsCfg.DispatchBatchSize, e.viewsCfg.DispatchBatchDelay.Duration())

	if err := e.mvDispatcher.Start(ctx); err != nil {
		return fmt.Errorf("start MV dispatcher: %w", err)
	}

	return nil
}

// loadPartAsSegment opens a part file via mmap and adds it to the query-visible segment list.
func (e *Engine) loadPartAsSegment(meta *part.Meta) error {
	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		return err
	}

	var bf *index.BloomFilter
	if b, err := ms.Reader().BloomFilter(); err == nil {
		bf = b
	}

	var ii *index.SerializedIndex
	if i, err := ms.Reader().InvertedIndex(); err == nil {
		ii = i
	}

	// Enable decoded column caching on this reader to avoid repeated
	// decompression across queries hitting the same segment.
	if e.projectionCache != nil {
		ms.Reader().SetColumnCache(e.projectionCache, meta.ID)
	}

	sh := &segmentHandle{
		reader: ms.Reader(),
		mmap:   ms,
		meta: model.SegmentMeta{
			ID:           meta.ID,
			Index:        meta.Index,
			Partition:    meta.Partition,
			MinTime:      meta.MinTime,
			MaxTime:      meta.MaxTime,
			EventCount:   meta.EventCount,
			SizeBytes:    meta.SizeBytes,
			Level:        meta.Level,
			Path:         meta.Path,
			CreatedAt:    meta.CreatedAt,
			Columns:      meta.Columns,
			Tier:         meta.Tier,
			BloomVersion: 2,
		},
		index:       meta.Index,
		bloom:       bf,
		invertedIdx: ii,
	}

	e.mu.Lock()
	cur := e.currentEpoch.Load().segments
	combined := make([]*segmentHandle, len(cur)+1)
	copy(combined, cur)
	combined[len(combined)-1] = sh
	e.advanceEpoch(combined, nil)
	e.mu.Unlock()

	// Register with compactor using path-based access (avoids holding
	// a reference to the mmap bytes for compaction tracking).
	if e.compactor != nil {
		e.compactor.AddSegment(&compaction.SegmentInfo{
			Meta: sh.meta,
			Path: meta.Path,
		})
	}

	return nil
}

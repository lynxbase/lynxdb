package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/pkg/storage/compaction"
	storageformat "github.com/lynxbase/lynxdb/pkg/storage/format"
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

	formatMajor, err := e.validateStorageFormat()
	if err != nil {
		return err
	}
	e.formatMajor = formatMajor

	if err := e.partRegistry.ScanDir(e.partLayout); err != nil {
		return fmt.Errorf("scan parts: %w", err)
	}

	e.logger.Info("part registry loaded", "parts", e.partRegistry.Count())

	// Init compactor.
	e.compactor = compaction.NewCompactor(e.logger)

	// Initialize compaction manifest store for crash recovery.
	manifestStore, err := compaction.NewManifestStoreWithFormatVersion(e.dataDir, int(e.formatMajor))
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

	batcherCfg := batcherConfigFromStorageConfig(e.storageCfg)

	e.batcher = part.NewAsyncBatcher(e.partWriter, e.partRegistry, batcherCfg, e.logger)
	e.batcher.SetOnCommit(func(meta *part.Meta) error {
		// Open mmap'd reader and add to query-visible segments.
		if loadErr := e.loadPartAsSegment(meta); loadErr != nil {
			e.logger.Error("failed to load committed part", "id", meta.ID, "error", loadErr)

			return loadErr
		}

		// Bump ingest generation for cache invalidation.
		e.ingestGen.Add(1)

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

		return nil
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

	viewReg, err := views.OpenWithFormatVersion(e.layout.ViewsDir(), int(e.formatMajor))
	if err != nil {
		return fmt.Errorf("open view registry: %w", err)
	}

	e.viewRegistry = viewReg
	e.mvDispatcher = views.NewDispatcherWithBudget(
		viewReg,
		e.layout,
		e.logger,
		e.viewsCfg.DispatchBatchSize,
		e.viewsCfg.DispatchBatchDelay.Duration(),
		e.governor,
		int64(e.viewsCfg.InsertMaxMemoryBytes),
	)

	if err := e.mvDispatcher.Start(ctx); err != nil {
		return fmt.Errorf("start MV dispatcher: %w", err)
	}

	return nil
}

func (e *Engine) validateStorageFormat() (uint16, error) {
	segmentPaths, err := listSegmentFiles(e.dataDir)
	if err != nil {
		return 0, err
	}

	markerValue, err := storageformat.ReadMarker([]string{e.dataDir})
	if err != nil {
		if errors.Is(err, storageformat.ErrMissingMarker) {
			if len(segmentPaths) > 0 {
				return 0, fmt.Errorf("%w: data dir contains %d segment(s) but no FORMAT marker; refusing to write one automatically",
					storageformat.ErrMissingMarker, len(segmentPaths))
			}
			if writeErr := storageformat.WriteMarker([]string{e.dataDir}, segment.LSG_BINARY_MAX_MAJOR); writeErr != nil {
				return 0, fmt.Errorf("write FORMAT marker: %w", writeErr)
			}
			markerValue = segment.LSG_BINARY_MAX_MAJOR
		} else {
			return 0, err
		}
	}

	if markerValue > segment.LSG_BINARY_MAX_MAJOR {
		return 0, fmt.Errorf("%w: data dir was written by a future LynxDB (FORMAT v%d, this binary supports up to v%d)",
			storageformat.ErrFutureFormat, markerValue, segment.LSG_BINARY_MAX_MAJOR)
	}
	if markerValue < segment.LSG_BINARY_MIN_MAJOR {
		return 0, fmt.Errorf("%w: data dir format v%d is below this binary's minimum (v%d); use an older LynxDB to compact forward",
			storageformat.ErrAncientFormat, markerValue, segment.LSG_BINARY_MIN_MAJOR)
	}

	var versionErrs []error
	for _, path := range segmentPaths {
		err := validateSegmentHeaderFile(path)
		if err == nil {
			continue
		}
		if isVersionFormatError(err) {
			e.logger.Error("segment format validation failed", "path", path, "error", err)
			versionErrs = append(versionErrs, fmt.Errorf("%s: %w", path, err))
			continue
		}
		e.logger.Warn("segment physical corruption detected during boot scan", "path", path, "error", err)
	}
	if len(versionErrs) > 0 {
		return 0, fmt.Errorf("segment format validation failed: %w", errors.Join(versionErrs...))
	}

	return markerValue, nil
}

func listSegmentFiles(dataDir string) ([]string, error) {
	root := filepath.Join(dataDir, "segments")
	var paths []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".lsg" {
			return nil
		}
		name := filepath.Base(path)
		if part.IsTempFile(name) || part.IsDeletedFile(name) {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return paths, nil
}

func validateSegmentHeaderFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	header := make([]byte, segment.LSG_HEADER_SIZE)
	n, err := io.ReadFull(f, header)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return segment.ValidateSegmentHeader(header[:n], info.Size())
		}
		return err
	}
	return segment.ValidateSegmentHeader(header, info.Size())
}

func isVersionFormatError(err error) bool {
	return errors.Is(err, segment.ErrUnsupportedMajor) ||
		errors.Is(err, segment.ErrUnsupportedCapability) ||
		errors.Is(err, segment.ErrInvalidMagic) ||
		errors.Is(err, segment.ErrUnsupportedHeaderRev)
}

// openPartSegmentHandle opens a part file via mmap and builds a query-visible
// segment handle for it without publishing it into the current epoch.
func (e *Engine) openPartSegmentHandle(meta *part.Meta) (*segmentHandle, error) {
	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		return nil, err
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

	return &segmentHandle{
		reader: ms.Reader(),
		mmap:   ms,
		meta: model.SegmentMeta{
			ID:         meta.ID,
			Index:      meta.Index,
			Partition:  meta.Partition,
			MinTime:    meta.MinTime,
			MaxTime:    meta.MaxTime,
			EventCount: meta.EventCount,
			SizeBytes:  meta.SizeBytes,
			Level:      meta.Level,
			Path:       meta.Path,
			CreatedAt:  meta.CreatedAt,
			Columns:    meta.Columns,
			Tier:       meta.Tier,
		},
		index:       meta.Index,
		bloom:       bf,
		invertedIdx: ii,
	}, nil
}

// loadPartAsSegment opens a part file via mmap and adds it to the query-visible segment list.
func (e *Engine) loadPartAsSegment(meta *part.Meta) error {
	sh, err := e.openPartSegmentHandle(meta)
	if err != nil {
		return err
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

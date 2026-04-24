package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/storage/compaction"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
)

const compactionEscalateThreshold = 5

// compactionFailureTracker tracks consecutive compaction failures per (index, partition).
type compactionFailureTracker struct {
	mu       sync.Mutex
	counters map[string]int
}

func newCompactionFailureTracker() *compactionFailureTracker {
	return &compactionFailureTracker{counters: make(map[string]int)}
}

func compactionTrackerKey(index, partition string) string {
	return index + "\x00" + partition
}

func (t *compactionFailureTracker) record(index, partition string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := compactionTrackerKey(index, partition)
	t.counters[k]++

	return t.counters[k]
}

func (t *compactionFailureTracker) reset(index, partition string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.counters, compactionTrackerKey(index, partition))
}

// startCompaction initializes the compaction scheduler with priority queue,
// worker pool, and rate limiter — then spawns a plan-submission goroutine
// that scans indexes every tick and submits compaction jobs.
func (e *Engine) startCompaction(ctx context.Context) {
	interval := e.storageCfg.CompactionInterval
	if interval == 0 {
		interval = 15 * time.Second
	}

	acCfg := compaction.AdaptiveConfig{
		Logger: e.logger,
	}
	if e.storageCfg.CompactionRateLimitMB > 0 {
		acCfg.MaxRate = int64(e.storageCfg.CompactionRateLimitMB) << 20
	}
	e.adaptiveCtrl = compaction.NewAdaptiveController(acCfg)

	// Wire query completion callback to feed latency samples.
	prevOnQueryComplete := e.onQueryComplete
	e.onQueryComplete = func(stats *SearchStats) {
		if stats != nil {
			e.adaptiveCtrl.RecordLatency(time.Duration(stats.ElapsedMS * float64(time.Millisecond)))
		}
		if prevOnQueryComplete != nil {
			prevOnQueryComplete(stats)
		}
	}

	// Create scheduler with custom executor that uses the existing
	// executeCompactionPlan path (epoch advance, cache invalidation, etc.).
	e.compactionSched = compaction.NewScheduler(e.compactor, compaction.SchedulerConfig{
		Workers:         2,
		RateBytesPerSec: e.adaptiveCtrl.Rate(),
	}, e.logger)

	// Wire adaptive controller into the scheduler for pause/resume checks.
	e.compactionSched.SetAdaptiveController(e.adaptiveCtrl)

	e.compactionSched.SetExecutor(func(ctx context.Context, job *compaction.Job) error {
		e.executeCompactionPlan(ctx, job.Index, job.Partition, job.Plan)
		return nil
	})

	e.compactionSched.Start(ctx)

	e.logger.Debug("compaction scheduler started",
		"interval", interval,
		"workers", 2,
		"rate_bytes_per_sec", e.adaptiveCtrl.Rate(),
	)

	// Plan-submission goroutine: scans all indexes, produces jobs, submits to scheduler.
	e.compactionWG.Add(1)
	go func() {
		defer e.compactionWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				e.compactionSched.Stop()
				return
			case <-ticker.C:
				// Adjust compaction rate based on query latency.
				newRate := e.adaptiveCtrl.Adjust()
				e.compactionSched.Limiter().SetRate(newRate)

				e.submitCompactionJobs()
			}
		}
	}()
}

// submitCompactionJobs scans all indexes for compaction opportunities
// and submits jobs to the scheduler.
func (e *Engine) submitCompactionJobs() {
	e.mu.RLock()
	indexNames := make([]string, 0, len(e.indexes))
	for name := range e.indexes {
		indexNames = append(indexNames, name)
	}
	e.mu.RUnlock()

	for _, idx := range indexNames {
		jobs := e.compactor.PlanAllCompactions(idx)
		if len(jobs) > 0 {
			e.compactionSched.SubmitAll(jobs)
		}
	}

	e.logger.Debug("compaction scan complete",
		"indexes_scanned", len(indexNames),
		"queue_depth", e.compactionSched.QueueLen(),
	)

	e.metrics.CompactionQueueDepth.Store(int64(e.compactionSched.QueueLen()))
}

// executeCompactionPlan runs a single compaction plan: merge input segments,
// write the output via streaming part writer (atomic rename), and swap handles.
//
// Uses StreamingMerge to emit events in bounded batches (StreamingBatchSize)
// directly to a PartStreamWriter on disk, bounding memory to O(1 batch)
// instead of O(all events). Each batch is written as a row group via
// segment.StreamWriter, then the file is finalized with inverted index +
// footer, fsynced, and atomically renamed.
func (e *Engine) executeCompactionPlan(ctx context.Context, idx, partition string, plan *compaction.Plan) {
	planStart := time.Now()
	e.logger.Debug("compaction plan execution started",
		"index", idx,
		"partition", partition,
		"input_count", len(plan.InputSegments),
		"output_level", plan.OutputLevel,
		"trivial_move", plan.TrivialMove,
	)

	// Write compaction manifest for crash recovery.
	var manifest *compaction.Manifest
	if e.manifestStore != nil {
		inputIDs := make([]string, len(plan.InputSegments))
		for i, seg := range plan.InputSegments {
			inputIDs[i] = seg.Meta.ID
		}
		manifestID := fmt.Sprintf("compact-%s-%s-%d", idx, partition, time.Now().UnixNano())
		manifest = &compaction.Manifest{
			ID:          manifestID,
			Index:       idx,
			Partition:   partition,
			InputIDs:    inputIDs,
			OutputLevel: plan.OutputLevel,
			StartedAt:   time.Now(),
		}
		if err := e.manifestStore.Write(manifest); err != nil {
			e.logger.Warn("failed to write compaction manifest", "error", err)
		}
		// Defer removal for error paths. The success path calls Complete()
		// which moves the manifest to history and removes it from pending.
		defer func() {
			if manifest != nil && manifest.CompletedAt.IsZero() {
				if removeErr := e.manifestStore.Remove(manifestID); removeErr != nil {
					e.logger.Warn("failed to remove compaction manifest", "id", manifestID, "error", removeErr)
				}
			}
		}()
	}

	// Handle trivial moves: promote the segment's level without merge.
	if plan.TrivialMove && len(plan.InputSegments) == 1 {
		e.executeTrivialMove(ctx, idx, partition, plan)
		return
	}

	// Stream merged events directly to disk via PartStreamWriter, bounding
	// memory to O(StreamingBatchSize) instead of O(all events).
	var rateLimiter *compaction.TokenBucket
	if e.compactionSched != nil {
		rateLimiter = e.compactionSched.Limiter()
	}

	// Build writer options from the shared part writer config.
	var writerOpts []part.WriterOption
	writerOpts = append(writerOpts, part.WithFSync(e.partWriter != nil))
	writerOpts = append(writerOpts, part.WithLogger(e.logger))
	if e.storageCfg.MaxColumnsPerPart > 0 {
		writerOpts = append(writerOpts, part.WithMaxColumns(e.storageCfg.MaxColumnsPerPart))
	}

	streamWriter, err := part.NewPartStreamWriter(e.partLayout, idx, plan.OutputLevel, writerOpts...)
	if err != nil {
		e.compactionFailures.record(idx, partition)
		e.metrics.CompactionErrors.Add(1)
		e.logger.Error("compaction stream writer init failed", "index", idx, "partition", partition, "error", err)
		return
	}

	result, err := e.compactor.StreamingMerge(ctx, plan, compaction.MergeWriterFunc(func(batch []*event.Event) error {
		return streamWriter.WriteRowGroup(ctx, batch)
	}), rateLimiter)
	if err != nil {
		streamWriter.Abort()
		consecutive := e.compactionFailures.record(idx, partition)
		e.metrics.CompactionErrors.Add(1)
		if consecutive >= compactionEscalateThreshold {
			e.logger.Error("CRITICAL: persistent compaction failure — L0 growth unbounded",
				"index", idx, "partition", partition, "consecutive_failures", consecutive, "error", err)
		} else {
			e.logger.Error("compaction merge failed", "index", idx, "partition", partition, "error", err)
		}

		return
	}

	e.metrics.CompactionRuns.Add(1)

	mergeElapsed := time.Since(planStart)
	e.logger.Debug("compaction merge phase complete",
		"index", idx,
		"partition", partition,
		"events", result.EventCount,
		"merge_ms", mergeElapsed.Milliseconds(),
	)

	// Finalize the streaming writer: writes inverted index + footer, fsyncs,
	// and performs the atomic rename (tmp_ → final path).
	outputMeta, err := streamWriter.Finalize(ctx)
	if err != nil {
		streamWriter.Abort()
		consecutive := e.compactionFailures.record(idx, partition)
		e.metrics.CompactionErrors.Add(1)
		if consecutive >= compactionEscalateThreshold {
			e.logger.Error("CRITICAL: persistent compaction write failure",
				"index", idx, "partition", partition, "consecutive_failures", consecutive, "error", err)
		} else {
			e.logger.Error("compaction write failed", "index", idx, "partition", partition, "error", err)
		}

		return
	}

	// Compaction succeeded — reset failure counter.
	e.compactionFailures.reset(idx, partition)

	// Register the new part in the part registry.
	e.partRegistry.Add(outputMeta)

	e.logger.Debug("compaction output registered",
		"id", outputMeta.ID,
		"level", outputMeta.Level,
		"size", outputMeta.SizeBytes,
	)

	// Load the new part as a query-visible segment handle.
	if err := e.loadPartAsSegment(outputMeta); err != nil {
		e.logger.Error("compaction load failed", "id", outputMeta.ID, "error", err)

		return
	}

	// Atomic epoch advance under write lock — remove input handles,
	// wire up tiering for the new segment. Retired handles are cleaned up
	// by drainAndClose when all pinned readers finish (epoch-based safety).
	e.mu.Lock()

	removeIDs := make(map[string]bool, len(plan.InputSegments))
	for _, seg := range plan.InputSegments {
		removeIDs[seg.Meta.ID] = true
	}

	var oldHandles []*segmentHandle
	newSegments := make([]*segmentHandle, 0, len(e.currentEpoch.Load().segments))
	for _, sh := range e.currentEpoch.Load().segments {
		if removeIDs[sh.meta.ID] {
			oldHandles = append(oldHandles, sh)
		} else {
			newSegments = append(newSegments, sh)
		}
	}

	e.tierMgr.AddSegment(partMetaToSegmentMeta(outputMeta))

	// Remove old segments from subsystems and defer file deletion until
	// mmap close (refs reaches 0). This prevents SIGSEGV on macOS arm64
	// where the kernel can revoke page protections for unlinked mappings.
	//
	// Rename each old part to .deleted so ScanDir won't reload it on restart.
	// os.Rename is safe with mmap on POSIX (modifies directory entry, not inode).
	for _, old := range oldHandles {
		e.compactor.RemoveSegment(old.meta.ID)
		e.tierMgr.RemoveSegment(old.meta.ID)
		if old.meta.Path != "" {
			deletedPath := old.meta.Path + ".deleted"
			if err := os.Rename(old.meta.Path, deletedPath); err != nil {
				e.logger.Warn("compaction: rename to .deleted failed, deferring",
					"path", old.meta.Path, "error", err)
				old.pendingDelete = []string{old.meta.Path}
			} else {
				old.pendingDelete = []string{deletedPath}
			}
		}
		if e.deletionPacer != nil {
			old.deleteFunc = e.deletionPacer.Enqueue
		}
	}

	e.advanceEpoch(newSegments, oldHandles) // schedules background mmap cleanup
	e.mu.Unlock()

	e.logger.Debug("compaction epoch advanced",
		"removed", len(oldHandles),
		"added", 1,
	)

	// Cache invalidation and registry cleanup (outside lock).
	removedIDs := make([]string, 0, len(oldHandles))
	for _, old := range oldHandles {
		removedIDs = append(removedIDs, old.meta.ID)
	}

	e.cache.OnCompaction(removedIDs, []string{outputMeta.ID})

	e.logger.Debug("compaction cache invalidation",
		"removed_entries", len(removedIDs),
		"added_id", outputMeta.ID,
	)

	// Invalidate projection cache entries for compacted-away segments.
	if e.projectionCache != nil {
		for _, id := range removedIDs {
			e.projectionCache.InvalidateSegment(id)
		}
	}

	for _, old := range oldHandles {
		e.partRegistry.Remove(old.meta.ID)
	}

	var inputBytes int64
	for _, seg := range plan.InputSegments {
		inputBytes += seg.Meta.SizeBytes
	}

	e.metrics.CompactionInputBytes.Add(inputBytes)
	e.metrics.CompactionOutputBytes.Add(outputMeta.SizeBytes)

	// Per-level compaction metrics.
	switch plan.OutputLevel {
	case compaction.L0:
		e.metrics.CompactionIntraL0Runs.Add(1)
	case compaction.L1:
		e.metrics.CompactionL0ToL1Runs.Add(1)
		e.metrics.CompactionL0ToL1Bytes.Add(outputMeta.SizeBytes)
		e.metrics.CompactionL0ToL1InputBytes.Add(inputBytes)
	case compaction.L2:
		e.metrics.CompactionL1ToL2Runs.Add(1)
		e.metrics.CompactionL1ToL2Bytes.Add(outputMeta.SizeBytes)
		e.metrics.CompactionL1ToL2InputBytes.Add(inputBytes)
	case compaction.L3:
		e.metrics.CompactionL2ToL3Runs.Add(1)
		e.metrics.CompactionL2ToL3Bytes.Add(outputMeta.SizeBytes)
		e.metrics.CompactionL2ToL3InputBytes.Add(inputBytes)
	}

	e.metrics.CompactionDurationNs.Add(time.Since(planStart).Nanoseconds())

	// Move manifest from pending to history with lineage info.
	if manifest != nil && e.manifestStore != nil {
		manifest.OutputSegmentID = outputMeta.ID
		manifest.CompletedAt = time.Now()
		if err := e.manifestStore.Complete(manifest); err != nil {
			e.logger.Warn("failed to complete compaction manifest", "id", manifest.ID, "error", err)
		}
	}

	e.logger.Info("compaction complete",
		"index", idx,
		"partition", partition,
		"input_count", len(plan.InputSegments),
		"output_id", outputMeta.ID,
		"output_level", outputMeta.Level,
		"output_size", outputMeta.SizeBytes,
	)
}

// executeTrivialMove promotes a single segment to a higher compaction level
// without performing a merge. The physical file stays the same; only the
// metadata level changes. This avoids the entire merge + write + re-index
// path for segments that are already non-overlapping and can be promoted
// directly (e.g., a single L0 segment that doesn't overlap with any L1).
func (e *Engine) executeTrivialMove(_ context.Context, idx, partition string, plan *compaction.Plan) {
	seg := plan.InputSegments[0]

	e.logger.Info("trivial move: promoting segment",
		"index", idx,
		"partition", partition,
		"segment", seg.Meta.ID,
		"from_level", seg.Meta.Level,
		"to_level", plan.OutputLevel,
	)

	// Write manifest for trivial move crash recovery.
	var trivialManifest *compaction.Manifest
	if e.manifestStore != nil {
		manifestID := fmt.Sprintf("trivial-%s-%s-%d", idx, partition, time.Now().UnixNano())
		trivialManifest = &compaction.Manifest{
			ID:          manifestID,
			Index:       idx,
			Partition:   partition,
			InputIDs:    []string{seg.Meta.ID},
			OutputLevel: plan.OutputLevel,
			TrivialMove: true,
			StartedAt:   time.Now(),
		}
		if err := e.manifestStore.Write(trivialManifest); err != nil {
			e.logger.Warn("failed to write trivial move manifest", "error", err)
		}
		defer func() {
			if trivialManifest != nil && trivialManifest.CompletedAt.IsZero() {
				if removeErr := e.manifestStore.Remove(manifestID); removeErr != nil {
					e.logger.Warn("failed to remove trivial move manifest", "id", manifestID, "error", removeErr)
				}
			}
		}()
	}

	oldMeta := segmentMetaToPartMeta(seg.Meta)
	renamedMeta := *oldMeta
	renamedMeta.Level = plan.OutputLevel
	renamedMeta.CreatedAt = time.Now()
	renamedMeta.ID = part.ID(oldMeta.Index, plan.OutputLevel, renamedMeta.CreatedAt)
	renamedMeta.Path = filepath.Join(filepath.Dir(oldMeta.Path),
		part.Filename(oldMeta.Index, plan.OutputLevel, renamedMeta.CreatedAt))

	if err := os.Rename(oldMeta.Path, renamedMeta.Path); err != nil {
		e.logger.Error("trivial move rename failed",
			"index", idx,
			"partition", partition,
			"segment", seg.Meta.ID,
			"from", oldMeta.Path,
			"to", renamedMeta.Path,
			"error", err,
		)

		return
	}

	newHandle, err := e.openPartSegmentHandle(&renamedMeta)
	if err != nil {
		e.logger.Error("trivial move reopen failed",
			"index", idx,
			"partition", partition,
			"segment", seg.Meta.ID,
			"path", renamedMeta.Path,
			"error", err,
		)
		if renameErr := os.Rename(renamedMeta.Path, oldMeta.Path); renameErr != nil {
			e.logger.Error("trivial move rollback failed",
				"from", renamedMeta.Path,
				"to", oldMeta.Path,
				"error", renameErr,
			)
		}

		return
	}

	var oldHandle *segmentHandle
	e.mu.Lock()
	current := e.currentEpoch.Load().segments
	newSegments := make([]*segmentHandle, len(current))
	for i, sh := range current {
		if sh.meta.ID != seg.Meta.ID {
			newSegments[i] = sh
			continue
		}

		oldHandle = sh
		newSegments[i] = newHandle
	}
	if oldHandle == nil {
		e.mu.Unlock()
		if closeErr := newHandle.mmap.Close(); closeErr != nil {
			e.logger.Warn("trivial move cleanup close failed", "path", renamedMeta.Path, "error", closeErr)
		}
		if renameErr := os.Rename(renamedMeta.Path, oldMeta.Path); renameErr != nil {
			e.logger.Error("trivial move rollback failed",
				"from", renamedMeta.Path,
				"to", oldMeta.Path,
				"error", renameErr,
			)
		}

		return
	}
	e.advanceEpoch(newSegments, []*segmentHandle{oldHandle})
	e.mu.Unlock()

	// Update compactor tracking: remove at old level, re-add at new level.
	e.compactor.RemoveSegment(seg.Meta.ID)
	e.compactor.AddSegment(&compaction.SegmentInfo{
		Meta: newHandle.meta,
		Path: renamedMeta.Path,
	})
	if e.tierMgr != nil {
		e.tierMgr.RemoveSegment(seg.Meta.ID)
		e.tierMgr.AddSegment(newHandle.meta)
	}
	if e.partRegistry != nil {
		e.partRegistry.Remove(seg.Meta.ID)
		e.partRegistry.Add(&renamedMeta)
	}
	e.cache.OnCompaction([]string{seg.Meta.ID}, []string{renamedMeta.ID})
	if e.projectionCache != nil {
		e.projectionCache.InvalidateSegment(seg.Meta.ID)
	}

	e.metrics.CompactionRuns.Add(1)
	e.metrics.CompactionTrivialMoveCount.Add(1)
	e.metrics.CompactionTrivialMoveBytes.Add(seg.Meta.SizeBytes)

	// Per-level compaction metrics (trivial moves still count as level transitions).
	switch plan.OutputLevel {
	case compaction.L1:
		e.metrics.CompactionL0ToL1Runs.Add(1)
	case compaction.L2:
		e.metrics.CompactionL1ToL2Runs.Add(1)
	case compaction.L3:
		e.metrics.CompactionL2ToL3Runs.Add(1)
	}

	// Move manifest from pending to history with lineage info.
	if trivialManifest != nil && e.manifestStore != nil {
		trivialManifest.OutputSegmentID = renamedMeta.ID
		trivialManifest.CompletedAt = time.Now()
		if err := e.manifestStore.Complete(trivialManifest); err != nil {
			e.logger.Warn("failed to complete trivial move manifest", "id", trivialManifest.ID, "error", err)
		}
	}

	e.logger.Info("trivial move complete",
		"index", idx,
		"partition", partition,
		"segment", renamedMeta.ID,
		"new_level", plan.OutputLevel,
	)
}

// maybeCompactAfterFlush checks if the L0 part count for the given (index, partition)
// exceeds the compaction threshold and, if so, submits compaction jobs to
// the scheduler. This is the reactive merge trigger that complements the
// periodic ticker: when ingest bursts produce many L0 parts within one tick
// interval, compaction responds without delay.
func (e *Engine) maybeCompactAfterFlush(_ context.Context, index, partition string) {
	if e.compactor == nil {
		return
	}

	l0Count := len(e.compactor.SegmentsByLevelPartition(index, partition, 0))

	e.logger.Debug("reactive compaction check",
		"index", index,
		"partition", partition,
		"l0_count", l0Count,
		"threshold", compaction.L0CompactionThreshold,
	)

	if l0Count < compaction.L0CompactionThreshold {
		return
	}

	jobs := e.compactor.PlanAllCompactions(index)
	if len(jobs) == 0 {
		return
	}

	e.logger.Debug("reactive compaction triggered",
		"index", index,
		"partition", partition,
		"l0_count", l0Count,
		"jobs", len(jobs),
	)

	if e.compactionSched != nil {
		e.compactionSched.SubmitAll(jobs)
	} else {
		// Fallback for tests or in-memory mode without scheduler.
		for _, job := range jobs {
			e.executeCompactionPlan(context.Background(), job.Index, job.Partition, job.Plan)
		}
	}
}

// onPartitionDeleted handles cleanup when the retention manager deletes a partition.
// It closes mmap handles and removes segment handles for the deleted parts.
// File deletion is deferred to decRef (when refs reaches 0) to prevent SIGSEGV
// from deleting mmap'd files while readers are still active.
func (e *Engine) onPartitionDeleted(removedIDs []string, partitionDir string) {
	if len(removedIDs) == 0 {
		return
	}

	e.logger.Debug("partition deletion started",
		"removed_ids", len(removedIDs),
		"partition_dir", partitionDir,
	)

	removeSet := make(map[string]bool, len(removedIDs))
	for _, id := range removedIDs {
		removeSet[id] = true
	}

	e.mu.Lock()

	var oldHandles []*segmentHandle
	newSegments := make([]*segmentHandle, 0, len(e.currentEpoch.Load().segments))
	for _, sh := range e.currentEpoch.Load().segments {
		if removeSet[sh.meta.ID] {
			oldHandles = append(oldHandles, sh)
		} else {
			newSegments = append(newSegments, sh)
		}
	}

	// Remove from subsystems and defer file deletion until mmap close.
	// Rename to .deleted so ScanDir won't reload on restart.
	for _, old := range oldHandles {
		e.compactor.RemoveSegment(old.meta.ID)
		e.tierMgr.RemoveSegment(old.meta.ID)
		if old.meta.Path != "" {
			deletedPath := old.meta.Path + ".deleted"
			if err := os.Rename(old.meta.Path, deletedPath); err != nil {
				e.logger.Warn("retention: rename to .deleted failed, deferring",
					"path", old.meta.Path, "error", err)
				old.pendingDelete = []string{old.meta.Path}
			} else {
				old.pendingDelete = []string{deletedPath}
			}
		}
		if e.deletionPacer != nil {
			old.deleteFunc = e.deletionPacer.Enqueue
		}
		e.logger.Debug("retention: segment marked for deletion",
			"id", old.meta.ID,
			"path", old.meta.Path,
		)
	}

	e.advanceEpoch(newSegments, oldHandles) // schedules background mmap cleanup
	e.mu.Unlock()

	// Invalidate cache entries for removed segments.
	e.cache.OnCompaction(removedIDs, nil)

	// Invalidate projection cache entries for removed segments.
	if e.projectionCache != nil {
		for _, id := range removedIDs {
			e.projectionCache.InvalidateSegment(id)
		}
	}

	e.logger.Info("retention: cleaned up segment handles",
		"removed_count", len(oldHandles),
	)
}

// partMetaToSegmentMeta converts a part.Meta to a model.SegmentMeta for
// subsystems (tiering) that still expect model.SegmentMeta.
func partMetaToSegmentMeta(pm *part.Meta) model.SegmentMeta {
	return model.SegmentMeta{
		ID:           pm.ID,
		Index:        pm.Index,
		Partition:    pm.Partition,
		MinTime:      pm.MinTime,
		MaxTime:      pm.MaxTime,
		EventCount:   pm.EventCount,
		SizeBytes:    pm.SizeBytes,
		Level:        pm.Level,
		Path:         pm.Path,
		CreatedAt:    pm.CreatedAt,
		Columns:      pm.Columns,
		Tier:         pm.Tier,
		BloomVersion: 2,
	}
}

func segmentMetaToPartMeta(sm model.SegmentMeta) *part.Meta {
	return &part.Meta{
		ID:         sm.ID,
		Index:      sm.Index,
		Partition:  sm.Partition,
		MinTime:    sm.MinTime,
		MaxTime:    sm.MaxTime,
		EventCount: sm.EventCount,
		SizeBytes:  sm.SizeBytes,
		Level:      sm.Level,
		Path:       sm.Path,
		CreatedAt:  sm.CreatedAt,
		Columns:    sm.Columns,
		Tier:       sm.Tier,
	}
}

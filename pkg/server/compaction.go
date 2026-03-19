package server

import (
	"context"
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

	// Create adaptive controller for latency-based throttling.
	e.adaptiveCtrl = compaction.NewAdaptiveController(compaction.AdaptiveConfig{})

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

	// Plan-submission goroutine: scans all indexes, produces jobs, submits to scheduler.
	go func() {
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

	// Update queue depth metric.
	e.metrics.CompactionQueueDepth.Store(int64(e.compactionSched.QueueLen()))
}

// executeCompactionPlan runs a single compaction plan: merge input segments,
// write the output via part.Writer (atomic rename), and swap handles.
//
// Uses StreamingMerge to emit events in bounded batches (StreamingBatchSize)
// with periodic CPU yields, which is better for GC and CPU sharing than
// collecting all events in a single allocation. The events are still
// collected in memory because part.Writer.Write expects a full event slice;
// true streaming-to-disk requires changes to part.Writer (future refactor).
func (e *Engine) executeCompactionPlan(ctx context.Context, idx, partition string, plan *compaction.Plan) {
	// Handle trivial moves: promote the segment's level without merge.
	if plan.TrivialMove && len(plan.InputSegments) == 1 {
		e.executeTrivialMove(ctx, idx, partition, plan)
		return
	}

	// Collect events via streaming merge. Even though we still collect all
	// events, StreamingMerge emits in bounded batches with yield points,
	// which is immediately better for GC pressure and CPU sharing.
	// The rate limiter is threaded through to apply per-batch I/O pacing.
	var rateLimiter *compaction.TokenBucket
	if e.compactionSched != nil {
		rateLimiter = e.compactionSched.Limiter()
	}
	var allEvents []*event.Event
	result, err := e.compactor.StreamingMerge(ctx, plan, compaction.MergeWriterFunc(func(batch []*event.Event) error {
		allEvents = append(allEvents, batch...)
		return nil
	}), rateLimiter)
	if err != nil {
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

	// Write merged events to disk via part.Writer (atomic tmp_ → rename).
	outputMeta, err := e.partWriter.Write(ctx, idx, allEvents, result.Level)
	if err != nil {
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
	for _, old := range oldHandles {
		e.compactor.RemoveSegment(old.meta.ID)
		e.tierMgr.RemoveSegment(old.meta.ID)
		if old.meta.Path != "" {
			old.pendingDelete = []string{old.meta.Path}
		}
		if e.deletionPacer != nil {
			old.deleteFunc = e.deletionPacer.Enqueue
		}
	}

	e.advanceEpoch(newSegments, oldHandles) // schedules background mmap cleanup
	e.mu.Unlock()

	// Cache invalidation and registry cleanup (outside lock).
	removedIDs := make([]string, 0, len(oldHandles))
	for _, old := range oldHandles {
		removedIDs = append(removedIDs, old.meta.ID)
	}

	e.cache.OnCompaction(removedIDs, []string{outputMeta.ID})

	// Invalidate projection cache entries for compacted-away segments.
	if e.projectionCache != nil {
		for _, id := range removedIDs {
			e.projectionCache.InvalidateSegment(id)
		}
	}

	for _, old := range oldHandles {
		e.partRegistry.Remove(old.meta.ID)
	}

	// Update compaction IO metrics.
	var inputBytes int64
	for _, seg := range plan.InputSegments {
		inputBytes += seg.Meta.SizeBytes
	}

	e.metrics.CompactionInputBytes.Add(inputBytes)
	e.metrics.CompactionOutputBytes.Add(outputMeta.SizeBytes)

	// Per-level compaction metrics.
	switch plan.OutputLevel {
	case compaction.L1:
		e.metrics.CompactionL0ToL1Runs.Add(1)
		e.metrics.CompactionL0ToL1Bytes.Add(outputMeta.SizeBytes)
	case compaction.L2:
		e.metrics.CompactionL1ToL2Runs.Add(1)
		e.metrics.CompactionL1ToL2Bytes.Add(outputMeta.SizeBytes)
	case compaction.L3:
		e.metrics.CompactionL2ToL3Runs.Add(1)
		e.metrics.CompactionL2ToL3Bytes.Add(outputMeta.SizeBytes)
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

	e.mu.Lock()
	// Find the segment handle and update its level metadata.
	for _, sh := range e.currentEpoch.Load().segments {
		if sh.meta.ID == seg.Meta.ID {
			sh.meta.Level = plan.OutputLevel
			break
		}
	}
	e.mu.Unlock()

	// Update compactor tracking: remove at old level, re-add at new level.
	e.compactor.RemoveSegment(seg.Meta.ID)
	updatedMeta := seg.Meta
	updatedMeta.Level = plan.OutputLevel
	e.compactor.AddSegment(&compaction.SegmentInfo{
		Meta: updatedMeta,
		Path: seg.Path,
	})

	e.metrics.CompactionRuns.Add(1)

	// Per-level compaction metrics (trivial moves still count as level transitions).
	switch plan.OutputLevel {
	case compaction.L1:
		e.metrics.CompactionL0ToL1Runs.Add(1)
	case compaction.L2:
		e.metrics.CompactionL1ToL2Runs.Add(1)
	case compaction.L3:
		e.metrics.CompactionL2ToL3Runs.Add(1)
	}

	e.logger.Info("trivial move complete",
		"index", idx,
		"partition", partition,
		"segment", seg.Meta.ID,
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
	for _, old := range oldHandles {
		e.compactor.RemoveSegment(old.meta.ID)
		e.tierMgr.RemoveSegment(old.meta.ID)
		if old.meta.Path != "" {
			old.pendingDelete = []string{old.meta.Path}
		}
		if e.deletionPacer != nil {
			old.deleteFunc = e.deletionPacer.Enqueue
		}
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

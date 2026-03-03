package server

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/compaction"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/part"
)

const compactionEscalateThreshold = 5

// compactionFailureTracker tracks consecutive compaction failures per index.
type compactionFailureTracker struct {
	mu       sync.Mutex
	counters map[string]int
}

func newCompactionFailureTracker() *compactionFailureTracker {
	return &compactionFailureTracker{counters: make(map[string]int)}
}

func (t *compactionFailureTracker) record(index string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counters[index]++

	return t.counters[index]
}

func (t *compactionFailureTracker) reset(index string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.counters, index)
}

// startCompaction runs background compaction when dataDir is set.
func (e *Engine) startCompaction(ctx context.Context) {
	interval := e.storageCfg.CompactionInterval
	if interval == 0 {
		interval = 30 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.runCompactionCycle(ctx)
			}
		}
	}()
}

// runCompactionCycle checks each index for compaction opportunities.
// It uses Merge() for the k-way merge and part.Writer for atomic output
// into the time-partitioned directory structure.
//
// Tracks consecutive failures per index. After compactionEscalateThreshold
// consecutive failures, logs at ERROR level with a CRITICAL prefix so
// operators can detect persistent compaction stalls (which cause unbounded
// L0 growth and eventual write stalls).
func (e *Engine) runCompactionCycle(ctx context.Context) {
	e.mu.RLock()
	indexNames := make([]string, 0, len(e.indexes))
	for name := range e.indexes {
		indexNames = append(indexNames, name)
	}
	e.mu.RUnlock()

	for _, idx := range indexNames {
		plan := e.compactor.PlanCompaction(idx)
		if plan == nil {
			// Success (no work needed) resets the failure counter.
			e.compactionFailures.reset(idx)

			continue
		}

		e.executeCompactionPlan(ctx, idx, plan)
	}
}

// executeCompactionPlan runs a single compaction plan: merge input segments,
// write the output via part.Writer (atomic rename), and swap handles.
func (e *Engine) executeCompactionPlan(ctx context.Context, idx string, plan *compaction.Plan) {
	// K-way merge of input segments.
	result, err := e.compactor.Merge(ctx, plan)
	if err != nil {
		consecutive := e.compactionFailures.record(idx)
		e.metrics.CompactionErrors.Add(1)
		if consecutive >= compactionEscalateThreshold {
			e.logger.Error("CRITICAL: persistent compaction failure — L0 growth unbounded",
				"index", idx, "consecutive_failures", consecutive, "error", err)
		} else {
			e.logger.Error("compaction merge failed", "index", idx, "error", err)
		}

		return
	}

	e.metrics.CompactionRuns.Add(1)

	// Write merged events to disk via part.Writer (atomic tmp_ → rename).
	outputMeta, err := e.partWriter.Write(ctx, idx, result.Events, result.Level)
	if err != nil {
		consecutive := e.compactionFailures.record(idx)
		e.metrics.CompactionErrors.Add(1)
		if consecutive >= compactionEscalateThreshold {
			e.logger.Error("CRITICAL: persistent compaction write failure",
				"index", idx, "consecutive_failures", consecutive, "error", err)
		} else {
			e.logger.Error("compaction write failed", "index", idx, "error", err)
		}

		return
	}

	// Compaction succeeded — reset failure counter.
	e.compactionFailures.reset(idx)

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
	newSegments := make([]*segmentHandle, 0, len(e.currentEpoch.segments))
	for _, sh := range e.currentEpoch.segments {
		if removeIDs[sh.meta.ID] {
			oldHandles = append(oldHandles, sh)
		} else {
			newSegments = append(newSegments, sh)
		}
	}

	e.tierMgr.AddSegment(partMetaToSegmentMeta(outputMeta))

	// Remove old segments from subsystems while under lock.
	for _, old := range oldHandles {
		e.compactor.RemoveSegment(old.meta.ID)
		e.tierMgr.RemoveSegment(old.meta.ID)
	}

	e.advanceEpoch(newSegments, oldHandles) // schedules background mmap cleanup
	e.mu.Unlock()

	// Cache invalidation (outside lock).
	removedIDs := make([]string, 0, len(oldHandles))
	for _, old := range oldHandles {
		removedIDs = append(removedIDs, old.meta.ID)
	}

	e.cache.OnCompaction(removedIDs, []string{outputMeta.ID})

	// Delete old part files and remove from registry. unlink on mmap'd
	// files is safe on Linux/macOS (pages remain valid until munmap).
	// mmap.Close() is deferred to drainAndClose in the retired epoch.
	for _, old := range oldHandles {
		if old.meta.Path != "" {
			os.Remove(old.meta.Path)
		}

		e.partRegistry.Remove(old.meta.ID)
	}

	// Update compaction IO metrics.
	var inputBytes int64
	for _, seg := range plan.InputSegments {
		inputBytes += seg.Meta.SizeBytes
	}

	e.metrics.CompactionInputBytes.Add(inputBytes)
	e.metrics.CompactionOutputBytes.Add(outputMeta.SizeBytes)

	e.logger.Info("compaction complete",
		"index", idx,
		"input_count", len(plan.InputSegments),
		"output_id", outputMeta.ID,
		"output_level", outputMeta.Level,
		"output_size", outputMeta.SizeBytes,
	)
}

// maybeCompactAfterFlush checks if the L0 part count for the given index
// exceeds the compaction threshold and, if so, runs a compaction cycle for
// that index immediately. This is the reactive merge trigger that complements
// the periodic 30-second ticker: when ingest bursts produce many L0 parts
// within one tick interval, compaction responds without delay.
func (e *Engine) maybeCompactAfterFlush(ctx context.Context, index string) {
	if e.compactor == nil {
		return
	}

	l0Count := len(e.compactor.SegmentsByLevel(index, 0))
	if l0Count < compaction.L0CompactionThreshold {
		return
	}

	plan := e.compactor.PlanCompaction(index)
	if plan == nil {
		return
	}

	e.logger.Debug("reactive compaction triggered",
		"index", index,
		"l0_count", l0Count,
	)

	e.executeCompactionPlan(ctx, index, plan)
}

// onPartitionDeleted handles cleanup when the retention manager deletes a partition.
// It closes mmap handles and removes segment handles for the deleted parts.
func (e *Engine) onPartitionDeleted(removedIDs []string) {
	if len(removedIDs) == 0 {
		return
	}

	removeSet := make(map[string]bool, len(removedIDs))
	for _, id := range removedIDs {
		removeSet[id] = true
	}

	e.mu.Lock()

	var oldHandles []*segmentHandle
	newSegments := make([]*segmentHandle, 0, len(e.currentEpoch.segments))
	for _, sh := range e.currentEpoch.segments {
		if removeSet[sh.meta.ID] {
			oldHandles = append(oldHandles, sh)
		} else {
			newSegments = append(newSegments, sh)
		}
	}

	// Remove from subsystems.
	for _, old := range oldHandles {
		e.compactor.RemoveSegment(old.meta.ID)
		e.tierMgr.RemoveSegment(old.meta.ID)
	}

	e.advanceEpoch(newSegments, oldHandles) // schedules background mmap cleanup
	e.mu.Unlock()

	// Invalidate cache entries for removed segments.
	e.cache.OnCompaction(removedIDs, nil)

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

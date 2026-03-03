package server

import (
	"context"
	"sort"

	enginepipeline "github.com/OrlovEvgeny/Lynxdb/pkg/engine/pipeline"
	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage"
)

// StreamingStats holds pre-pipeline scan statistics.
type StreamingStats struct {
	RowsScanned         int64
	ProcessedBytes      int64
	IndexesUsed         []string
	SegmentsTotal       int
	SegmentsScanned     int
	SegmentsSkippedTime int
	SegmentsSkippedBF   int
	BufferedEvents      int
}

// BuildStreamingPipeline builds the query pipeline and returns the raw Iterator
// instead of collecting all results. The caller MUST call iter.Close().
// The returned iterator holds a reference to a per-query BudgetMonitor backed
// by the global query pool. The monitor is released when the caller closes the
// iterator; callers that abandon the iterator will leak pool reservations.
func (e *Engine) BuildStreamingPipeline(ctx context.Context, prog *spl2.Program,
	externalTimeBounds *spl2.TimeBounds) (enginepipeline.Iterator, StreamingStats, error) {
	hints := spl2.ExtractQueryHints(prog)
	if externalTimeBounds != nil {
		if hints.TimeBounds == nil {
			hints.TimeBounds = externalTimeBounds
		} else {
			if !externalTimeBounds.Earliest.IsZero() &&
				(hints.TimeBounds.Earliest.IsZero() || externalTimeBounds.Earliest.After(hints.TimeBounds.Earliest)) {
				hints.TimeBounds.Earliest = externalTimeBounds.Earliest
			}
			if !externalTimeBounds.Latest.IsZero() &&
				(hints.TimeBounds.Latest.IsZero() || externalTimeBounds.Latest.Before(hints.TimeBounds.Latest)) {
				hints.TimeBounds.Latest = externalTimeBounds.Latest
			}
		}
	}

	monitor := stats.NewBudgetMonitorWithParent("stream", int64(e.queryCfg.MaxQueryMemory), e.rootMonitor)

	eventStore, ss, memErr := e.buildEventStore(ctx, hints, nil, monitor)
	if memErr != nil {
		monitor.Close()

		return nil, StreamingStats{}, memErr
	}
	var stats StreamingStats
	for name, idxEvents := range eventStore {
		stats.RowsScanned += int64(len(idxEvents))
		stats.IndexesUsed = append(stats.IndexesUsed, name)
	}
	sort.Strings(stats.IndexesUsed)
	stats.SegmentsTotal = ss.SegmentsTotal
	stats.SegmentsScanned = ss.SegmentsScanned
	stats.SegmentsSkippedTime = ss.SegmentsSkippedTime
	stats.SegmentsSkippedBF = ss.SegmentsSkippedBF
	stats.BufferedEvents = ss.BufferedEvents
	stats.ProcessedBytes = ss.TotalBytesRead

	pipeStore := &enginepipeline.ServerIndexStore{Events: eventStore}
	iter, err := enginepipeline.BuildProgramWithViews(ctx, prog, pipeStore, e, e, 0)
	if err != nil {
		return nil, stats, err
	}

	if err := iter.Init(ctx); err != nil {
		iter.Close()

		return nil, stats, err
	}

	return iter, stats, nil
}

// EventBus returns the engine's event bus for live subscriptions.
func (e *Engine) EventBus() *storage.EventBus {
	return e.eventBus
}

package server

import (
	"context"
	"sort"
	"time"

	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/logical"
	"github.com/lynxbase/lynxdb/pkg/logical/physical"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/storage"
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
//
// When hints is non-nil the streaming path uses them for segment pruning and
// source-scope resolution, mirroring the runStreamingPipeline execution path.
// When hints is nil, empty hints are used (no pruning).
//
// The returned iterator holds references to the pinned segment epoch and an
// optional per-query memory budget. Both are released when the caller closes
// the iterator; callers that abandon the iterator will leak the epoch pin.
func (e *Engine) BuildStreamingPipeline(ctx context.Context, prog *logical.Plan,
	hints *model.QueryHints,
	externalTimeBounds *model.TimeBounds) (enginepipeline.Iterator, StreamingStats, error) {

	if hints == nil {
		hints = &model.QueryHints{}
	}

	// Merge external time bounds into hints (intersect — tightest wins).
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

	return e.buildStreamingPipelineReal(ctx, prog, hints)
}

// buildStreamingPipelineReal mirrors runStreamingPipeline but returns the
// raw Iterator instead of draining it. The epoch is pinned and unpinned
// when the returned iterator is closed.
func (e *Engine) buildStreamingPipelineReal(ctx context.Context, prog *logical.Plan,
	hints *model.QueryHints) (enginepipeline.Iterator, StreamingStats, error) {

	// Resolve glob patterns against the source registry.
	hints, _ = e.resolveSourceScope(hints)

	memEvents := e.bufferedEventsForQuery()

	// Pin the current epoch. The returned iterator's Close() will unpin it.
	ep := e.pinEpoch()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)

	// Sort segments newest first.
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].meta.MaxTime.After(segs[j].meta.MaxTime)
	})

	var ss storeStats
	ss.SegmentsTotal = len(segs)
	ss.BufferedEvents = len(memEvents)
	sources := e.buildSegmentSources(ctx, segs, hints, &ss)

	streamHints := buildStreamHints(hints, e.queryCfg.Load().BitmapSelectivityThreshold)

	store := &StreamingServerStore{
		segments:     sources,
		allMemEvents: memEvents,
		baseHints:    streamHints,
		batchSize:    0, // default
		gov:          e.governor,
	}

	source := indexStoreToSource(store, DefaultIndexName)
	iter, err := physical.Build(prog, physical.BuildOptions{
		Source: source,
		Now:    time.Now(),
	})
	if err != nil {
		ep.unpin()
		return nil, StreamingStats{}, err
	}

	if err := iter.Init(ctx); err != nil {
		_ = iter.Close()
		ep.unpin()
		return nil, StreamingStats{}, err
	}

	// Populate pre-drain stats.
	streamStats := StreamingStats{
		SegmentsTotal:       ss.SegmentsTotal,
		SegmentsSkippedTime: ss.SegmentsSkippedTime,
		SegmentsSkippedBF:   ss.SegmentsSkippedBF,
		BufferedEvents:      ss.BufferedEvents,
	}

	// Wrap: Close() unpins epoch + releases governor budget.
	wrapped := &epochClosingIterator{
		Iterator: iter,
		epoch:    ep,
		store:    store,
		metrics:  e.metrics,
	}

	return wrapped, streamStats, nil
}

// epochClosingIterator wraps an Iterator and unpins the segment epoch when
// the iterator is closed. This ensures the epoch stays pinned for the entire
// lifetime of the streaming iterator (which outlives the BuildStreamingPipeline
// call), and segments remain mmapped until all reads complete.
type epochClosingIterator struct {
	enginepipeline.Iterator
	epoch   *segmentEpoch
	store   *StreamingServerStore
	metrics *storage.Metrics
	closed  bool
}

// ScanStats returns the aggregated scan statistics from ALL streaming
// iterators created by the underlying store. Must be called after the
// iterator is fully drained.
func (e *epochClosingIterator) ScanStats() *enginepipeline.SegmentStreamStats {
	if e.store == nil {
		return nil
	}
	return e.store.AggregatedStats()
}

func (e *epochClosingIterator) Close() error {
	err := e.Iterator.Close()
	if !e.closed {
		if e.metrics != nil && e.store != nil {
			if st := e.store.AggregatedStats(); st != nil {
				e.metrics.SegmentReads.Add(int64(st.SegmentsScanned))
			}
		}
		if e.epoch != nil {
			e.epoch.unpin()
		}
		e.closed = true
	}
	return err
}

// govClosingIterator wraps an Iterator and closes the governor BudgetAdapter
// when the iterator is closed, ensuring governor reservations are released.
type govClosingIterator struct {
	enginepipeline.Iterator
	budget *memgov.BudgetAdapter
	closed bool
}

func (g *govClosingIterator) Close() error {
	err := g.Iterator.Close()
	if !g.closed {
		if g.budget != nil {
			g.budget.Close()
		}
		g.closed = true
	}
	return err
}

// EventBus returns the engine's event bus for live subscriptions.
func (e *Engine) EventBus() *storage.EventBus {
	return e.eventBus
}

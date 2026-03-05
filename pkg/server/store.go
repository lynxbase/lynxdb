package server

import (
	"context"
	"log/slog"
	"path"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"

	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

// progressAggregator maintains per-iterator progress slots and sums them to
// produce a single SearchProgress. This solves the APPEND/MULTISEARCH
// flickering bug where independent iterators' progress callbacks would
// overwrite each other via atomic.Store.
type progressAggregator struct {
	mu           sync.Mutex
	iterProgress []enginepipeline.SegmentStreamProgress // one slot per iterator
	baseSS       storeStats                             // pre-filter skip stats
	memEvents    int
	startTime    time.Time
	onReport     func(*SearchProgress)
}

func (a *progressAggregator) ensureSlot(idx int) {
	for len(a.iterProgress) <= idx {
		a.iterProgress = append(a.iterProgress, enginepipeline.SegmentStreamProgress{})
	}
}

func (a *progressAggregator) update(idx int, p enginepipeline.SegmentStreamProgress) {
	a.mu.Lock()
	a.ensureSlot(idx)
	a.iterProgress[idx] = p
	// Sum across ALL iterators.
	var agg enginepipeline.SegmentStreamProgress
	for _, ip := range a.iterProgress {
		agg.SegmentsTotal += ip.SegmentsTotal
		agg.SegmentsScanned += ip.SegmentsScanned
		agg.SegmentsSkipped += ip.SegmentsSkipped
		agg.SegmentsSkippedTime += ip.SegmentsSkippedTime
		agg.SegmentsSkippedBloom += ip.SegmentsSkippedBloom
		agg.EventsScanned += ip.EventsScanned
		agg.EventsMatched += ip.EventsMatched
		agg.BytesRead += ip.BytesRead
	}
	a.mu.Unlock()

	// For APPEND/MULTISEARCH: each iterator independently reports its
	// SegmentsTotal (= len(sources) it received). Sum across iterators
	// gives effective total work. Add baseSS pre-skips (segments removed
	// by buildSegmentSources before any iterator was created).
	preSkipped := a.baseSS.SegmentsSkippedIdx + a.baseSS.SegmentsSkippedTime +
		a.baseSS.SegmentsSkippedStat + a.baseSS.SegmentsSkippedBF +
		a.baseSS.SegmentsSkippedRange
	effectiveTotal := a.baseSS.SegmentsTotal // fallback before any iterator reports
	if agg.SegmentsTotal > 0 {
		effectiveTotal = agg.SegmentsTotal + preSkipped
	}

	sp := &SearchProgress{
		Phase:               PhaseScanningSegments,
		SegmentsTotal:       effectiveTotal,
		SegmentsScanned:     a.baseSS.SegmentsScanned + agg.SegmentsScanned,
		SegmentsSkippedTime: a.baseSS.SegmentsSkippedTime + agg.SegmentsSkippedTime,
		SegmentsSkippedBF:   a.baseSS.SegmentsSkippedBF + agg.SegmentsSkippedBloom,
		SegmentsSkippedIdx:  a.baseSS.SegmentsSkippedIdx,
		SegmentsSkippedStat: a.baseSS.SegmentsSkippedStat,
		BufferedEvents:      a.memEvents,
		RowsReadSoFar:       agg.EventsScanned,
		ElapsedMS:           float64(time.Since(a.startTime).Milliseconds()),
	}
	a.onReport(sp)
}

// StreamingServerStore implements both IndexStore and StreamingIndexStore for
// server-mode queries. When the streaming path is active, GetEventIterator
// returns a SegmentStreamIterator that reads row groups on-demand. GetEvents
// is preserved as a fallback for subsearches and CTEs — it drains a streaming
// iterator to collect all events.
//
// The progressAggregator maintains per-iterator progress slots and sums them,
// enabling correct progress reporting for APPEND/JOIN/MULTISEARCH queries.
// After pipeline execution, call AggregatedStats() to retrieve accumulated
// scan statistics from all iterators (APPEND/JOIN/MULTISEARCH create multiple).
//
// For multi-index queries (APPEND, JOIN, MULTISEARCH), baseHints has IndexName=""
// and each GetEventIterator call creates per-call hints with the requested index.
// The allMemEvents slice contains events from ALL indexes; GetEventIterator filters
// for the requested index before passing them to the iterator.
type StreamingServerStore struct {
	segments     []*enginepipeline.SegmentSource
	allMemEvents []*event.Event
	baseHints    *enginepipeline.SegmentStreamHints // hints WITHOUT IndexName pre-set for multi-index
	batchSize    int
	monitor      *stats.BudgetMonitor

	// aggregator sums progress across all iterators (replaces per-iterator onProgress).
	// Set before calling BuildProgramWithBudget.
	aggregator *progressAggregator

	// mu protects allIters for concurrent access from progress callbacks.
	mu       sync.Mutex
	allIters []*enginepipeline.SegmentStreamIterator
}

// GetEventIterator returns a streaming iterator for the given index.
// Implements enginepipeline.StreamingIndexStore.
//
// For multi-index queries (APPEND, JOIN, MULTISEARCH), each call creates
// per-call hints with the requested IndexName and filters buffered events
// for that index. Progress callbacks include a cumulative offset from
// previously created iterators so "Rows read" never resets to 0.
func (s *StreamingServerStore) GetEventIterator(index string) enginepipeline.Iterator {
	// Create per-call hints with the requested index name.
	perCallHints := *s.baseHints // shallow copy
	perCallHints.IndexName = index

	// Filter buffered events for this specific index.
	// For multi-source queries (source scope already resolved at server layer),
	// all buffered events were pre-filtered during collection — include all.
	// For single-source, filter by the requested index name.
	isMultiSrc := s.baseHints.SourceScopeType == "list" ||
		s.baseHints.SourceScopeType == "glob" ||
		s.baseHints.SourceScopeType == "all" ||
		len(s.baseHints.SourceIndices) > 0 ||
		s.baseHints.SourceGlob != ""
	var filteredMem []*event.Event
	for _, ev := range s.allMemEvents {
		if isMultiSrc {
			// Multi-source: already filtered at collection time, include all.
			filteredMem = append(filteredMem, ev)
		} else {
			idx := ev.Index
			if idx == "" {
				idx = DefaultIndexName
			}
			if index != "" && idx != index {
				continue
			}
			filteredMem = append(filteredMem, ev)
		}
	}

	acct := s.monitor.NewAccount("stream-scan")
	iter := enginepipeline.NewSegmentStreamIterator(
		s.segments, filteredMem, &perCallHints, s.batchSize, acct,
	)

	// Wire progress through the aggregator which sums across all iterators,
	// fixing the APPEND/MULTISEARCH flickering bug where independent iterators'
	// callbacks would overwrite each other.
	s.mu.Lock()
	iterIdx := len(s.allIters)
	s.allIters = append(s.allIters, iter)
	if s.aggregator != nil {
		s.aggregator.ensureSlot(iterIdx)
		idx := iterIdx // capture for closure
		agg := s.aggregator
		iter.SetOnProgress(func(p enginepipeline.SegmentStreamProgress) {
			agg.update(idx, p)
		})
	}
	s.mu.Unlock()

	return iter
}

// AggregatedStats returns the accumulated scan statistics from ALL streaming
// iterators created by this store. For single-index queries there is one
// iterator; for APPEND/JOIN/MULTISEARCH there are multiple.
// Returns nil if no iterator was created (e.g., external store path).
// Must be called after pipeline execution completes.
func (s *StreamingServerStore) AggregatedStats() *enginepipeline.SegmentStreamStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.allIters) == 0 {
		return nil
	}

	var agg enginepipeline.SegmentStreamStats
	for _, iter := range s.allIters {
		st := iter.Stats()
		agg.SegmentsScanned += st.SegmentsScanned
		agg.SegmentsSkipped += st.SegmentsSkipped
		agg.EventsScanned += st.EventsScanned
		agg.EventsMatched += st.EventsMatched
		agg.BytesRead += st.BytesRead
		agg.BitmapHits += st.BitmapHits
		agg.BloomSkips += st.BloomSkips
		agg.TimeSkips += st.TimeSkips
		agg.SegmentBloomSkips += st.SegmentBloomSkips
		agg.SegmentTimeSkips += st.SegmentTimeSkips
		agg.ScopeSkips += st.ScopeSkips
		agg.EmptyBitmapSkips += st.EmptyBitmapSkips
		agg.RGBloomsChecked += st.RGBloomsChecked
		if st.PeakMemoryBytes > agg.PeakMemoryBytes {
			agg.PeakMemoryBytes = st.PeakMemoryBytes
		}
	}

	return &agg
}

// GetEvents implements enginepipeline.IndexStore for backward compatibility.
// Used by subsearches and CTEs that require materialization.
// This does NOT return nil — it materializes by draining a SegmentStreamIterator
// to collect events. This is the fallback path: slower but correct.
func (s *StreamingServerStore) GetEvents(index string) []*event.Event {
	iter := s.GetEventIterator(index)
	defer iter.Close()

	_ = iter.Init(context.Background())

	var events []*event.Event
	for {
		batch, err := iter.Next(context.Background())
		if err != nil || batch == nil {
			break
		}
		// Convert batch rows back to events.
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			ev := &event.Event{
				Fields: make(map[string]event.Value, len(row)),
			}
			for k, v := range row {
				switch k {
				case "_time":
					if t, ok := v.TryAsTimestamp(); ok {
						ev.Time = t
					}
				case "_raw":
					if str, ok := v.TryAsString(); ok {
						ev.Raw = str
					}
				case "_source", "source":
					if str, ok := v.TryAsString(); ok {
						ev.Source = str
					}
				case "_sourcetype", "sourcetype":
					if str, ok := v.TryAsString(); ok {
						ev.SourceType = str
					}
				case "host":
					if str, ok := v.TryAsString(); ok {
						ev.Host = str
					}
				case "index":
					if str, ok := v.TryAsString(); ok {
						ev.Index = str
					}
				default:
					ev.SetField(k, v)
				}
			}
			events = append(events, ev)
		}
	}

	return events
}

// buildStreamHints converts spl2.QueryHints to SegmentStreamHints.
// BitmapThreshold is the bitmap selectivity threshold from query config (0.0-1.0).
func buildStreamHints(hints *spl2.QueryHints, bitmapThreshold float64) *enginepipeline.SegmentStreamHints {
	if hints == nil {
		return &enginepipeline.SegmentStreamHints{
			BitmapSelectivityThreshold: bitmapThreshold,
		}
	}

	sh := &enginepipeline.SegmentStreamHints{
		IndexName:                  hints.IndexName,
		TimeBounds:                 hints.TimeBounds,
		SearchTerms:                hints.SearchTerms,
		SearchTermTree:             hints.SearchTermTree,
		RequiredCols:               hints.RequiredCols,
		Limit:                      hints.Limit,
		SortAscending:              hints.ReverseScan,
		BitmapSelectivityThreshold: bitmapThreshold,
	}

	sh.FieldPreds = append(sh.FieldPreds, hints.FieldPredicates...)
	sh.RangePreds = append(sh.RangePreds, hints.RangePredicates...)
	sh.InPreds = append(sh.InPreds, hints.InPredicates...)
	sh.InvertedPreds = append(sh.InvertedPreds, hints.InvertedIndexPredicates...)

	// Multi-source fields for wildcard/list queries.
	sh.SourceIndices = hints.SourceIndices
	sh.SourceGlob = hints.SourceGlob
	sh.SourceScopeType = hints.SourceScopeType
	sh.SourceScopeSources = hints.SourceScopeSources
	sh.SourceScopePattern = hints.SourceScopePattern

	return sh
}

// buildSegmentSources converts internal segmentHandle slice to pipeline SegmentSource slice.
// Applies segment-level skip logic (index name, time bounds, bloom filter, stats)
// and returns both the sources and updated storeStats.
func (e *Engine) buildSegmentSources(
	segs []*segmentHandle,
	hints *spl2.QueryHints,
	ss *storeStats,
) []*enginepipeline.SegmentSource {
	var sources []*enginepipeline.SegmentSource
	for _, seg := range segs {
		if seg.reader == nil && e.dataDir == "" {
			continue
		}
		if shouldSkipSegment(seg, hints, ss) {
			continue
		}

		// Load remote segment if needed.
		reader := seg.reader
		if reader == nil {
			reader = e.loadRemoteSegment(seg)
		}
		if reader == nil {
			continue
		}

		sources = append(sources, &enginepipeline.SegmentSource{
			Reader:      reader,
			Index:       seg.index,
			InvertedIdx: seg.invertedIdx,
			Bloom:       seg.bloom,
			Meta: enginepipeline.SegmentMeta{
				ID:           seg.meta.ID,
				MinTime:      seg.meta.MinTime,
				MaxTime:      seg.meta.MaxTime,
				EventCount:   seg.meta.EventCount,
				SizeBytes:    seg.meta.SizeBytes,
				BloomVersion: seg.meta.BloomVersion,
			},
		})
	}

	return sources
}

// buildEventStore builds an event store from the engine's internal data.
// This avoids the EventsToRows conversion (1 map alloc/event) and copyRows overhead,
// enabling the pipeline engine to work directly with event.Event pointers.
//
// When monitor is non-nil, a BoundAccount is created to track scan memory against
// the shared per-query budget. When nil, standalone enforcement uses queryCfg.MaxQueryMemory.
func (e *Engine) buildEventStore(ctx context.Context, hints *spl2.QueryHints, onProgress func(*SearchProgress), monitor *stats.BudgetMonitor, traceSegments ...bool) (map[string][]*event.Event, storeStats, error) {
	trace := len(traceSegments) > 0 && traceSegments[0]
	_ = trace

	// Flush buffered events so they are visible to the query scan.
	// BufferedEvents() is a single lock+counter check — negligible overhead.
	// Flush() only runs when there are actual buffered events.
	if e.batcher != nil && e.batcher.BufferedEvents() > 0 {
		if err := e.batcher.Flush(); err != nil {
			e.logger.Warn("pre-query batcher flush failed", "error", err)
		}
	}

	// Pin the current epoch to prevent retired segments from being munmap'd
	// while workers read from them. Unpin after all workers finish.
	ep := e.pinEpoch()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)
	dataDir := e.dataDir

	e.mu.RLock()
	indexStore := e.indexStore
	e.mu.RUnlock()

	// If an external IndexStore was injected (test path), convert ResultRows to events.
	if indexStore != nil {
		ep.unpin() // no segment I/O needed for IndexStore path
		store := make(map[string][]*event.Event)
		for name, rows := range indexStore.Indexes {
			events := make([]*event.Event, len(rows))
			for i, row := range rows {
				ev := &event.Event{
					Fields: make(map[string]event.Value, len(row.Fields)),
				}
				for k, v := range row.Fields {
					ev.Fields[k] = event.ValueFromInterface(v)
				}
				if raw, ok := row.Fields["_raw"].(string); ok {
					ev.Raw = raw
				}
				if src, ok := row.Fields["_source"].(string); ok {
					ev.Source = src
				}
				if st, ok := row.Fields["_sourcetype"].(string); ok {
					ev.SourceType = st
				}
				if h, ok := row.Fields["host"].(string); ok {
					ev.Host = h
				}
				if idx, ok := row.Fields["index"].(string); ok {
					ev.Index = idx
				}
				events[i] = ev
			}
			store[name] = events
		}

		return store, storeStats{}, nil
	}

	store := make(map[string][]*event.Event)
	var ss storeStats

	// No separate buffer to scan — all data is in segments (parts).

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:          PhaseFilteringSegments,
			BufferedEvents: 0,
		})
	}

	// Sort segments by MaxTime descending (newest first).
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].meta.MaxTime.After(segs[j].meta.MaxTime)
	})
	_ = dataDir // used below in skip logic

	ss.SegmentsTotal = len(segs)

	requiredCols := hints.RequiredCols
	if len(requiredCols) == 0 {
		requiredCols = nil
	}

	type segResult struct {
		events    []*event.Event
		index     string
		readStats segReadStats
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(segs) {
		workers = len(segs)
	}
	if workers < 1 {
		workers = 1
	}

	type segJob struct {
		seg        *segmentHandle
		timeBounds *spl2.TimeBounds
	}
	jobCh := make(chan segJob, len(segs))
	resultCh := make(chan segResult, len(segs))

	// ctx from caller — enables query cancellation to stop segment workers.
	var segErrors atomic.Int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if ctx.Err() != nil {
					continue // drain channel on cancellation
				}

				seg := job.seg
				reader := seg.reader
				if reader == nil {
					reader = e.loadRemoteSegment(seg)
				}
				if reader == nil {
					continue
				}

				idx := seg.index
				if idx == "" {
					idx = DefaultIndexName
				}

				events, rs, err := readSegmentEvents(seg, reader, hints, requiredCols, e.logger, e.queryCfg.BitmapSelectivityThreshold)
				if err != nil {
					e.logger.Warn("segment read error",
						"segment", seg.meta.ID, "error", err)
					segErrors.Add(1)

					continue
				}
				if events == nil {
					continue
				}

				events = filterEventsByTime(events, job.timeBounds)
				e.metrics.SegmentReads.Add(1)
				resultCh <- segResult{events: events, index: idx, readStats: rs}
			}
		}()
	}

	dispatched := 0
	for _, seg := range segs {
		if seg.reader == nil && dataDir == "" {
			continue
		}
		if shouldSkipSegment(seg, hints, &ss) {
			continue
		}
		ss.SegmentsScanned++
		jobCh <- segJob{seg: seg, timeBounds: hints.TimeBounds}
		dispatched++
	}
	close(jobCh)

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:                PhaseScanningSegments,
			SegmentsTotal:        ss.SegmentsTotal,
			SegmentsDispatched:   dispatched,
			SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
			SegmentsSkippedTime:  ss.SegmentsSkippedTime,
			SegmentsSkippedStat:  ss.SegmentsSkippedStat,
			SegmentsSkippedBF:    ss.SegmentsSkippedBF,
			SegmentsSkippedRange: ss.SegmentsSkippedRange,
			BufferedEvents:       ss.BufferedEvents,
		})
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Approximate per-event memory overhead: pointer (8) + Event struct (~200 bytes avg).
	const estimatedEventBytes int64 = 208

	// Create scan account from the shared monitor, or fall back to standalone enforcement.
	// When monitor is non-nil the scan shares a budget with pipeline operators.
	// When nil, a local monitor enforces queryCfg.MaxQueryMemory for backward compat.
	var scanAcct *stats.BoundAccount
	if monitor != nil {
		scanAcct = monitor.NewAccount("scan")
	} else if e.queryCfg.MaxQueryMemory > 0 {
		localMon := stats.NewBudgetMonitor("scan", int64(e.queryCfg.MaxQueryMemory))
		scanAcct = localMon.NewAccount("scan")
	}
	defer scanAcct.Close()

	// Effective early-termination limit: head uses hints.Limit, tail reverse scan uses hints.TailLimit.
	earlyLimit := hints.Limit
	if earlyLimit == 0 && hints.TailLimit > 0 && hints.ReverseScan {
		earlyLimit = hints.TailLimit
	}

	scannedSoFar := 0
	totalEvents := 0
	for res := range resultCh {
		store[res.index] = append(store[res.index], res.events...)
		totalEvents += len(res.events)
		// Accumulate per-segment read feature flags.
		if res.readStats.usedInvertedIndex {
			ss.InvertedIndexHits++
		}
		if res.readStats.usedPrefetch {
			ss.PrefetchUsed = true
		}
		if res.readStats.usedDictFilter {
			ss.DictFilterUsed = true
		}
		// Always accumulate total bytes for ClickHouse-style throughput display.
		ss.TotalBytesRead += res.readStats.bytesRead
		// Accumulate per-segment I/O details for trace-level profiling.
		if trace {
			ss.SegmentDetails = append(ss.SegmentDetails, SegmentDetailStat{
				SegmentID:        res.readStats.segmentID,
				Source:           res.readStats.source,
				Rows:             res.readStats.rowsRead,
				RowsAfterFilter:  res.readStats.rowsAfterFilter,
				BloomHit:         res.readStats.usedInvertedIndex,
				InvertedUsed:     res.readStats.usedInvertedIndex,
				ReadDurationNS:   res.readStats.readDurationNS,
				ColumnsProjected: res.readStats.columnsProjected,
			})
		}
		if err := scanAcct.Grow(int64(len(res.events)) * estimatedEventBytes); err != nil {
			wg.Wait()
			ep.unpin()
			ss.SegmentsErrored = int(segErrors.Load())

			return nil, ss, err
		}
		// Early termination: stop collecting once we have enough events for the limit.
		// Works for both head (hints.Limit) and tail reverse scan (hints.TailLimit + ReverseScan).
		if earlyLimit > 0 && totalEvents >= earlyLimit {
			break
		}
		scannedSoFar++
		if onProgress != nil {
			onProgress(&SearchProgress{
				Phase:                PhaseScanningSegments,
				SegmentsTotal:        ss.SegmentsTotal,
				SegmentsScanned:      scannedSoFar,
				SegmentsDispatched:   dispatched,
				SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
				SegmentsSkippedTime:  ss.SegmentsSkippedTime,
				SegmentsSkippedStat:  ss.SegmentsSkippedStat,
				SegmentsSkippedBF:    ss.SegmentsSkippedBF,
				SegmentsSkippedRange: ss.SegmentsSkippedRange,
				BufferedEvents:       ss.BufferedEvents,
				RowsReadSoFar:        int64(totalEvents),
			})
		}
	}

	wg.Wait()
	ep.unpin()

	ss.SegmentsErrored = int(segErrors.Load())

	return store, ss, nil
}

// filterEventsByTime filters events in-place, keeping only those within the time bounds.
func filterEventsByTime(events []*event.Event, tb *spl2.TimeBounds) []*event.Event {
	if tb == nil {
		return events
	}
	earliest := tb.Earliest
	latest := tb.Latest
	if earliest.IsZero() && latest.IsZero() {
		return events
	}
	filtered := events[:0]
	for _, ev := range events {
		if !earliest.IsZero() && ev.Time.Before(earliest) {
			continue
		}
		if !latest.IsZero() && ev.Time.After(latest) {
			continue
		}
		filtered = append(filtered, ev)
	}

	return filtered
}

// canSkipByRange checks if a segment can be skipped based on range predicates.
// Each RangePredicate has a field, min, and max. If the segment's column stats
// show no overlap with the range, the segment can be skipped.
func canSkipByRange(reader *segment.Reader, preds []spl2.RangePredicate) bool {
	for _, pred := range preds {
		stats := reader.StatsByName(pred.Field)
		if stats == nil || (stats.MinValue == "" && stats.MaxValue == "") {
			continue
		}
		// If pred.Min is set and segment's max < pred.Min, skip.
		if pred.Min != "" && stats.MaxValue != "" && stats.MaxValue < pred.Min {
			return true
		}
		// If pred.Max is set and segment's min > pred.Max, skip.
		if pred.Max != "" && stats.MinValue != "" && stats.MinValue > pred.Max {
			return true
		}
	}

	return false
}

// canSkipByStats checks if a segment can be skipped based on column stats
// and the field predicates from the query.
func canSkipByStats(reader *segment.Reader, preds []spl2.FieldPredicate) bool {
	for _, pred := range preds {
		stats := reader.StatsByName(pred.Field)
		if stats == nil {
			continue
		}
		if stats.MinValue == "" && stats.MaxValue == "" {
			continue
		}

		// Try numeric comparison first.
		predVal, predErr := strconv.ParseFloat(pred.Value, 64)
		minVal, minErr := strconv.ParseFloat(stats.MinValue, 64)
		maxVal, maxErr := strconv.ParseFloat(stats.MaxValue, 64)

		if predErr == nil && minErr == nil && maxErr == nil {
			switch pred.Op {
			case "=", "==":
				if predVal < minVal || predVal > maxVal {
					return true
				}
			case ">":
				if maxVal <= predVal {
					return true
				}
			case ">=":
				if maxVal < predVal {
					return true
				}
			case "<":
				if minVal >= predVal {
					return true
				}
			case "<=":
				if minVal > predVal {
					return true
				}
			}
		} else {
			// Fall back to string comparison.
			switch pred.Op {
			case "=", "==":
				if pred.Value < stats.MinValue || pred.Value > stats.MaxValue {
					return true
				}
			case ">":
				if stats.MaxValue <= pred.Value {
					return true
				}
			case ">=":
				if stats.MaxValue < pred.Value {
					return true
				}
			case "<":
				if stats.MinValue >= pred.Value {
					return true
				}
			case "<=":
				if stats.MinValue > pred.Value {
					return true
				}
			}
		}
	}

	return false
}

// shouldSkipSegment checks whether a segment should be skipped based on
// index name, time bounds, column stats, range predicates, and bloom filter.
// It updates ss with skip reason counters.
func shouldSkipSegment(seg *segmentHandle, hints *spl2.QueryHints, ss *storeStats) bool {
	// Source scope filtering: check multi-source scope first, then fall back
	// to single IndexName exact match.
	if !matchesSourceScope(seg.index, hints) {
		ss.SegmentsSkippedIdx++

		return true
	}

	if hints.TimeBounds != nil {
		earliest := hints.TimeBounds.Earliest
		latest := hints.TimeBounds.Latest
		if !earliest.IsZero() && !latest.IsZero() {
			if !seg.meta.Overlaps(earliest, latest) {
				ss.SegmentsSkippedTime++

				return true
			}
		} else if !earliest.IsZero() {
			if seg.meta.MaxTime.Before(earliest) {
				ss.SegmentsSkippedTime++

				return true
			}
		} else if !latest.IsZero() {
			if seg.meta.MinTime.After(latest) {
				ss.SegmentsSkippedTime++

				return true
			}
		}
	}

	if seg.reader != nil && canSkipByStats(seg.reader, hints.FieldPredicates) {
		ss.SegmentsSkippedStat++

		return true
	}

	if seg.reader != nil && len(hints.RangePredicates) > 0 && canSkipByRange(seg.reader, hints.RangePredicates) {
		ss.SegmentsSkippedRange++

		return true
	}

	if len(hints.SearchTerms) > 0 && seg.bloom != nil && seg.meta.BloomVersion >= 2 {
		if !seg.bloom.MayContainAll(hints.SearchTerms) {
			ss.SegmentsSkippedBF++

			return true
		}
	}

	return false
}

// matchesSourceScope checks if a segment's index matches the query's source scope.
// Returns true if the segment should be scanned (i.e., it matches).
// Checks optimizer-resolved SourceScopeType first, then falls back to parser-level
// SourceIndices/SourceGlob, then single IndexName.
func matchesSourceScope(segIndex string, hints *spl2.QueryHints) bool {
	// If SourceScopeType is set by optimizer, use it as the authoritative source.
	switch hints.SourceScopeType {
	case "all":
		return true
	case "single":
		if len(hints.SourceScopeSources) > 0 {
			return segIndex == hints.SourceScopeSources[0]
		}
	case "list":
		if set := hints.SourceIndexSet(); set != nil {
			_, ok := set[segIndex]

			return ok
		}
		for _, src := range hints.SourceScopeSources {
			if segIndex == src {
				return true
			}
		}

		return false
	case "glob":
		if hints.SourceScopePattern != "" {
			matched, _ := path.Match(hints.SourceScopePattern, segIndex)

			return matched
		}
	}

	// Fallback: use SourceIndices (from parser, multi-source FROM a,b,c).
	if len(hints.SourceIndices) > 0 {
		for _, idx := range hints.SourceIndices {
			if segIndex == idx {
				return true
			}
		}

		return false
	}

	// Fallback: use SourceGlob (from parser, FROM logs*).
	if hints.SourceGlob != "" {
		if hints.SourceGlob == "*" {
			return true
		}
		matched, _ := path.Match(hints.SourceGlob, segIndex)

		return matched
	}

	// Fallback: single IndexName exact match (existing behavior).
	if hints.IndexName != "" {
		return segIndex == hints.IndexName
	}

	// No source filter — scan everything.
	return true
}

// segReadStats tracks which read features were used for a single segment read.
type segReadStats struct {
	usedInvertedIndex bool
	usedDictFilter    bool
	usedPrefetch      bool
	bitmapOverridden  bool // true when bitmap was discarded by selectivity gate
	columnsProjected  int  // number of projected columns; 0 = all columns read
	// Trace-level fields — always populated (cheap: two clock reads + counter).
	readDurationNS  int64
	rowsRead        int64
	rowsAfterFilter int64
	segmentID       string
	source          string // "disk", "cache", "s3"
	// bytesRead is the on-disk segment size (from segment metadata).
	// Always populated for ClickHouse-style throughput metrics.
	bytesRead int64
}

// shouldOverrideBitmap returns true if the bitmap selectivity exceeds the threshold.
// When the bitmap covers more than threshold fraction of total rows, per-row roaring
// bitmap lookups are more expensive than a sequential scan with column projection.
func shouldOverrideBitmap(bitmapCard uint64, totalRows int64, threshold float64) bool {
	if threshold <= 0 || threshold >= 1.0 || totalRows <= 0 {
		return false
	}

	return float64(bitmapCard)/float64(totalRows) > threshold
}

// readSegmentEvents reads events from a segment using the best available method:
// inverted index bitmap search, field predicate filtering, prefetch reader, or full scan.
// Returns nil, zero stats, nil if the inverted index eliminates the segment entirely.
// The bitmapSelectivityThreshold controls when a non-selective bitmap is discarded in favor
// of sequential scan with projection (0.9 = discard when >90% of rows match).
func readSegmentEvents(
	seg *segmentHandle,
	reader *segment.Reader,
	hints *spl2.QueryHints,
	requiredCols []string,
	logger *slog.Logger,
	bitmapSelectivityThreshold float64,
) ([]*event.Event, segReadStats, error) {
	var rs segReadStats
	rs.segmentID = seg.meta.ID
	rs.source = "disk"
	rs.rowsRead = seg.meta.EventCount
	rs.bytesRead = seg.meta.SizeBytes

	readStart := time.Now()

	var searchBitmap *roaring.Bitmap
	if len(hints.SearchTerms) > 0 && seg.invertedIdx != nil && seg.meta.BloomVersion >= 2 {
		for i, term := range hints.SearchTerms {
			bm, err := seg.invertedIdx.Search(term)
			if err != nil {
				logger.Warn("inverted index search error",
					"segment", seg.meta.ID, "term", term, "error", err)

				continue
			}
			rs.usedInvertedIndex = true
			if i == 0 {
				searchBitmap = bm
			} else {
				searchBitmap.And(bm)
			}
			if searchBitmap.GetCardinality() == 0 {
				break
			}
		}
		if searchBitmap != nil && searchBitmap.GetCardinality() == 0 {
			rs.readDurationNS = time.Since(readStart).Nanoseconds()
			rs.rowsAfterFilter = 0

			return nil, rs, nil
		}
	}

	// Inverted index field=value lookups.
	if len(hints.InvertedIndexPredicates) > 0 && seg.invertedIdx != nil {
		for _, iip := range hints.InvertedIndexPredicates {
			bm, err := seg.invertedIdx.SearchField(iip.Field, iip.Value)
			if err != nil {
				logger.Warn("inverted index field search error",
					"segment", seg.meta.ID, "field", iip.Field, "value", iip.Value, "error", err)

				continue
			}
			rs.usedInvertedIndex = true
			if searchBitmap == nil {
				searchBitmap = bm
			} else {
				searchBitmap.And(bm)
			}
		}
		if searchBitmap != nil && searchBitmap.GetCardinality() == 0 {
			rs.readDurationNS = time.Since(readStart).Nanoseconds()
			rs.rowsAfterFilter = 0

			return nil, rs, nil
		}
	}

	// Selectivity gate: when bitmap covers >threshold of rows, discard it
	// and fall through to sequential scan with projection. Avoids per-row
	// roaring bitmap lookup overhead when the filter is non-selective.
	if searchBitmap != nil && shouldOverrideBitmap(searchBitmap.GetCardinality(), reader.EventCount(), bitmapSelectivityThreshold) {
		searchBitmap = nil
		rs.bitmapOverridden = true
	}

	// Track column projection for instrumentation.
	if requiredCols != nil {
		rs.columnsProjected = len(requiredCols)
	}

	if len(hints.FieldPredicates) > 0 {
		segPreds := make([]segment.Predicate, len(hints.FieldPredicates))
		for i, fp := range hints.FieldPredicates {
			segPreds[i] = segment.Predicate{
				Field: fp.Field,
				Op:    fp.Op,
				Value: fp.Value,
			}
		}
		// ReadEventsFiltered uses DictFilter internally for dict-encoded equality predicates.
		rs.usedDictFilter = hasDictFilterEligiblePredicate(segPreds)

		events, err := reader.ReadEventsFiltered(segPreds, searchBitmap, requiredCols)
		rs.readDurationNS = time.Since(readStart).Nanoseconds()
		rs.rowsAfterFilter = int64(len(events))

		return events, rs, err
	}

	if searchBitmap != nil && searchBitmap.GetCardinality() > 0 {
		events, err := reader.ReadEventsByBitmap(searchBitmap, requiredCols)
		rs.readDurationNS = time.Since(readStart).Nanoseconds()
		rs.rowsAfterFilter = int64(len(events))

		return events, rs, err
	}

	if reader.RowGroupCount() > 1 {
		rs.usedPrefetch = true
		pr := segment.NewPrefetchReader(reader)
		qh := segment.QueryHints{
			Columns:     requiredCols,
			SearchTerms: hints.SearchTerms,
		}
		if hints.TimeBounds != nil {
			if !hints.TimeBounds.Earliest.IsZero() {
				t := hints.TimeBounds.Earliest
				qh.MinTime = &t
			}
			if !hints.TimeBounds.Latest.IsZero() {
				t := hints.TimeBounds.Latest
				qh.MaxTime = &t
			}
		}

		events, err := pr.ReadEventsWithPrefetch(qh)
		rs.readDurationNS = time.Since(readStart).Nanoseconds()
		rs.rowsAfterFilter = int64(len(events))

		return events, rs, err
	}

	if requiredCols != nil {
		events, err := reader.ReadEventsWithColumns(requiredCols)
		rs.readDurationNS = time.Since(readStart).Nanoseconds()
		rs.rowsAfterFilter = int64(len(events))

		return events, rs, err
	}

	events, err := reader.ReadEvents()
	rs.readDurationNS = time.Since(readStart).Nanoseconds()
	rs.rowsAfterFilter = int64(len(events))

	return events, rs, err
}

// hasDictFilterEligiblePredicate returns true if any predicate uses equality operators
// that can benefit from the DictFilter fast path on dict-encoded columns.
func hasDictFilterEligiblePredicate(preds []segment.Predicate) bool {
	for _, p := range preds {
		if p.Op == "=" || p.Op == "==" || p.Op == "!=" {
			return true
		}
	}

	return false
}

// buildPartialAggStore builds partial aggregation results from segments.
// Instead of collecting all events, each worker computes partial aggregates per segment,
// reducing memory from O(events) to O(groups x segments).
func (e *Engine) buildPartialAggStore(
	ctx context.Context,
	hints *spl2.QueryHints,
	spec *enginepipeline.PartialAggSpec,
	onProgress func(*SearchProgress),
) ([][]*enginepipeline.PartialAggGroup, storeStats) {
	// Flush buffered events so they are visible to the query scan.
	if e.batcher != nil && e.batcher.BufferedEvents() > 0 {
		if err := e.batcher.Flush(); err != nil {
			e.logger.Warn("pre-query batcher flush failed", "error", err)
		}
	}

	// Pin epoch to prevent retired segments from being munmap'd during scan.
	ep := e.pinEpoch()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)
	dataDir := e.dataDir

	var allPartials [][]*enginepipeline.PartialAggGroup
	var ss storeStats

	// No separate buffer to scan — all data is in segments.

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:          PhaseFilteringSegments,
			BufferedEvents: 0,
		})
	}

	// Sort segments by MaxTime descending (newest first).
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].meta.MaxTime.After(segs[j].meta.MaxTime)
	})

	ss.SegmentsTotal = len(segs)

	requiredCols := hints.RequiredCols
	if len(requiredCols) == 0 {
		requiredCols = nil
	}

	type segPartialResult struct {
		partials  []*enginepipeline.PartialAggGroup
		bytesRead int64
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(segs) {
		workers = len(segs)
	}
	if workers < 1 {
		workers = 1
	}

	type segJob struct {
		seg        *segmentHandle
		timeBounds *spl2.TimeBounds
	}
	jobCh := make(chan segJob, len(segs))
	resultCh := make(chan segPartialResult, len(segs))

	// ctx from caller — enables query cancellation to stop segment workers.
	var partialSegErrors atomic.Int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if ctx.Err() != nil {
					continue // drain channel on cancellation
				}

				seg := job.seg
				reader := seg.reader
				if reader == nil {
					reader = e.loadRemoteSegment(seg)
				}
				if reader == nil {
					continue
				}

				events, rs, err := readSegmentEvents(seg, reader, hints, requiredCols, e.logger, e.queryCfg.BitmapSelectivityThreshold)
				if err != nil {
					e.logger.Warn("segment read error (partial agg)",
						"segment", seg.meta.ID, "error", err)
					partialSegErrors.Add(1)

					continue
				}
				if events == nil {
					continue
				}

				events = filterEventsByTime(events, job.timeBounds)
				e.metrics.SegmentReads.Add(1)

				// Compute partial aggregates instead of sending raw events.
				partial := enginepipeline.ComputePartialAgg(events, spec)
				resultCh <- segPartialResult{partials: partial, bytesRead: rs.bytesRead}
			}
		}()
	}

	dispatched := 0
	for _, seg := range segs {
		if seg.reader == nil && dataDir == "" {
			continue
		}
		if shouldSkipSegment(seg, hints, &ss) {
			continue
		}
		ss.SegmentsScanned++
		jobCh <- segJob{seg: seg, timeBounds: hints.TimeBounds}
		dispatched++
	}
	close(jobCh)

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:                PhaseScanningSegments,
			SegmentsTotal:        ss.SegmentsTotal,
			SegmentsDispatched:   dispatched,
			SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
			SegmentsSkippedTime:  ss.SegmentsSkippedTime,
			SegmentsSkippedStat:  ss.SegmentsSkippedStat,
			SegmentsSkippedBF:    ss.SegmentsSkippedBF,
			SegmentsSkippedRange: ss.SegmentsSkippedRange,
			BufferedEvents:       ss.BufferedEvents,
		})
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	scannedSoFar := 0
	for res := range resultCh {
		allPartials = append(allPartials, res.partials)
		ss.TotalBytesRead += res.bytesRead
		scannedSoFar++
		if onProgress != nil {
			onProgress(&SearchProgress{
				Phase:                PhaseScanningSegments,
				SegmentsTotal:        ss.SegmentsTotal,
				SegmentsScanned:      scannedSoFar,
				SegmentsDispatched:   dispatched,
				SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
				SegmentsSkippedTime:  ss.SegmentsSkippedTime,
				SegmentsSkippedStat:  ss.SegmentsSkippedStat,
				SegmentsSkippedBF:    ss.SegmentsSkippedBF,
				SegmentsSkippedRange: ss.SegmentsSkippedRange,
				BufferedEvents:       ss.BufferedEvents,
			})
		}
	}

	wg.Wait()
	ep.unpin()

	ss.SegmentsErrored = int(partialSegErrors.Load())

	return allPartials, ss
}

// buildTransformPartialAggStore runs per-segment mini-pipelines
// (transforms → partial agg) in parallel. Same worker pool pattern as
// buildPartialAggStore, but each worker applies the transform commands
// (rex, eval, where, etc.) before computing partial aggregates.
func (e *Engine) buildTransformPartialAggStore(
	ctx context.Context,
	hints *spl2.QueryHints,
	tSpec *optimizer.TransformPartialAggAnnotation,
	onProgress func(*SearchProgress),
) ([][]*enginepipeline.PartialAggGroup, storeStats) {
	// Flush buffered events so they are visible to the query scan.
	if e.batcher != nil && e.batcher.BufferedEvents() > 0 {
		if err := e.batcher.Flush(); err != nil {
			e.logger.Warn("pre-query batcher flush failed", "error", err)
		}
	}

	// Pin epoch to prevent retired segments from being munmap'd during scan.
	ep := e.pinEpoch()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)
	dataDir := e.dataDir

	var allPartials [][]*enginepipeline.PartialAggGroup
	var ss storeStats

	// No separate buffer to scan — all data is in segments.

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:          PhaseFilteringSegments,
			BufferedEvents: 0,
		})
	}

	// Sort segments by MaxTime descending (newest first).
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].meta.MaxTime.After(segs[j].meta.MaxTime)
	})

	ss.SegmentsTotal = len(segs)

	requiredCols := hints.RequiredCols
	if len(requiredCols) == 0 {
		requiredCols = nil
	}

	type segPartialResult struct {
		partials  []*enginepipeline.PartialAggGroup
		bytesRead int64
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(segs) {
		workers = len(segs)
	}
	if workers < 1 {
		workers = 1
	}

	type segJob struct {
		seg        *segmentHandle
		timeBounds *spl2.TimeBounds
	}
	jobCh := make(chan segJob, len(segs))
	resultCh := make(chan segPartialResult, len(segs))

	var partialSegErrors atomic.Int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if ctx.Err() != nil {
					continue // drain channel on cancellation
				}

				seg := job.seg
				reader := seg.reader
				if reader == nil {
					reader = e.loadRemoteSegment(seg)
				}
				if reader == nil {
					continue
				}

				events, rs, err := readSegmentEvents(seg, reader, hints, requiredCols, e.logger, e.queryCfg.BitmapSelectivityThreshold)
				if err != nil {
					e.logger.Warn("segment read error (transform partial agg)",
						"segment", seg.meta.ID, "error", err)
					partialSegErrors.Add(1)

					continue
				}
				if events == nil {
					continue
				}

				events = filterEventsByTime(events, job.timeBounds)
				e.metrics.SegmentReads.Add(1)

				// Run transform mini-pipeline then partial agg.
				partial := e.runTransformAndAgg(ctx, events, tSpec)
				resultCh <- segPartialResult{partials: partial, bytesRead: rs.bytesRead}
			}
		}()
	}

	dispatched := 0
	for _, seg := range segs {
		if seg.reader == nil && dataDir == "" {
			continue
		}
		if shouldSkipSegment(seg, hints, &ss) {
			continue
		}
		ss.SegmentsScanned++
		jobCh <- segJob{seg: seg, timeBounds: hints.TimeBounds}
		dispatched++
	}
	close(jobCh)

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:                PhaseScanningSegments,
			SegmentsTotal:        ss.SegmentsTotal,
			SegmentsDispatched:   dispatched,
			SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
			SegmentsSkippedTime:  ss.SegmentsSkippedTime,
			SegmentsSkippedStat:  ss.SegmentsSkippedStat,
			SegmentsSkippedBF:    ss.SegmentsSkippedBF,
			SegmentsSkippedRange: ss.SegmentsSkippedRange,
			BufferedEvents:       ss.BufferedEvents,
		})
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	scannedSoFar := 0
	for res := range resultCh {
		allPartials = append(allPartials, res.partials)
		ss.TotalBytesRead += res.bytesRead
		scannedSoFar++
		if onProgress != nil {
			onProgress(&SearchProgress{
				Phase:                PhaseScanningSegments,
				SegmentsTotal:        ss.SegmentsTotal,
				SegmentsScanned:      scannedSoFar,
				SegmentsDispatched:   dispatched,
				SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
				SegmentsSkippedTime:  ss.SegmentsSkippedTime,
				SegmentsSkippedStat:  ss.SegmentsSkippedStat,
				SegmentsSkippedBF:    ss.SegmentsSkippedBF,
				SegmentsSkippedRange: ss.SegmentsSkippedRange,
				BufferedEvents:       ss.BufferedEvents,
			})
		}
	}

	wg.Wait()
	ep.unpin()

	ss.SegmentsErrored = int(partialSegErrors.Load())

	return allPartials, ss
}

// runTransformAndAgg converts events to a batch, runs transform commands through
// a mini Volcano pipeline, then computes partial aggregates from the output batches.
// Thread-safe: each call creates its own pipeline instances (regex compilation is
// per-worker, the compiled *regexp.Regexp objects are immutable and goroutine-safe).
func (e *Engine) runTransformAndAgg(
	ctx context.Context,
	events []*event.Event,
	tSpec *optimizer.TransformPartialAggAnnotation,
) []*enginepipeline.PartialAggGroup {
	if len(events) == 0 {
		return nil
	}

	// Build a mini-pipeline: Scan → [transform commands...].
	// Use a simple query with the transform commands and a synthetic source.
	miniStore := &enginepipeline.ServerIndexStore{
		Events: map[string][]*event.Event{"_transform": events},
	}
	miniQuery := &spl2.Query{
		Source:   &spl2.SourceClause{Index: "_transform"},
		Commands: tSpec.TransformCommands,
	}

	iter, err := enginepipeline.BuildPipeline(ctx, miniQuery, miniStore, 0)
	if err != nil {
		e.logger.Warn("transform mini-pipeline build error", "error", err)

		return nil
	}
	defer iter.Close()

	if initErr := iter.Init(ctx); initErr != nil {
		e.logger.Warn("transform mini-pipeline init error", "error", initErr)

		return nil
	}

	// Drain all batches from the mini-pipeline.
	var outputBatches []*enginepipeline.Batch
	for {
		batch, nextErr := iter.Next(ctx)
		if nextErr != nil {
			e.logger.Warn("transform mini-pipeline next error", "error", nextErr)

			break
		}
		if batch == nil {
			break
		}
		outputBatches = append(outputBatches, batch)
	}

	return enginepipeline.ComputePartialAggFromBatches(outputBatches, tSpec.AggSpec)
}

// readSegmentColumnar reads a segment using the direct columnar path, returning
// a ColumnarResult instead of []*event.Event. This mirrors the readSegmentEvents
// decision tree: inverted index bitmap search, field predicate filtering, row group
// pruning, or full columnar scan. Returns (nil, stats, nil) when the segment is
// eliminated entirely (e.g., empty bitmap after inverted index intersection).
func readSegmentColumnar(
	seg *segmentHandle,
	reader *segment.Reader,
	hints *spl2.QueryHints,
	requiredCols []string,
	logger *slog.Logger,
	bitmapSelectivityThreshold float64,
) (*segment.ColumnarResult, segReadStats, error) {
	var rs segReadStats
	rs.segmentID = seg.meta.ID
	rs.source = "disk"
	rs.rowsRead = seg.meta.EventCount
	rs.bytesRead = seg.meta.SizeBytes

	readStart := time.Now()

	// Search terms → bitmap via inverted index.
	var searchBitmap *roaring.Bitmap
	if len(hints.SearchTerms) > 0 && seg.invertedIdx != nil && seg.meta.BloomVersion >= 2 {
		for i, term := range hints.SearchTerms {
			bm, err := seg.invertedIdx.Search(term)
			if err != nil {
				logger.Warn("inverted index search error",
					"segment", seg.meta.ID, "term", term, "error", err)

				continue
			}
			rs.usedInvertedIndex = true
			if i == 0 {
				searchBitmap = bm
			} else {
				searchBitmap.And(bm)
			}
			if searchBitmap.GetCardinality() == 0 {
				break
			}
		}
		if searchBitmap != nil && searchBitmap.GetCardinality() == 0 {
			rs.readDurationNS = time.Since(readStart).Nanoseconds()
			rs.rowsAfterFilter = 0

			return nil, rs, nil
		}
	}

	// Inverted index field=value lookups.
	if len(hints.InvertedIndexPredicates) > 0 && seg.invertedIdx != nil {
		for _, iip := range hints.InvertedIndexPredicates {
			bm, err := seg.invertedIdx.SearchField(iip.Field, iip.Value)
			if err != nil {
				logger.Warn("inverted index field search error",
					"segment", seg.meta.ID, "field", iip.Field, "value", iip.Value, "error", err)

				continue
			}
			rs.usedInvertedIndex = true
			if searchBitmap == nil {
				searchBitmap = bm
			} else {
				searchBitmap.And(bm)
			}
		}
		if searchBitmap != nil && searchBitmap.GetCardinality() == 0 {
			rs.readDurationNS = time.Since(readStart).Nanoseconds()
			rs.rowsAfterFilter = 0

			return nil, rs, nil
		}
	}

	// Selectivity gate: discard non-selective bitmaps.
	if searchBitmap != nil && shouldOverrideBitmap(searchBitmap.GetCardinality(), reader.EventCount(), bitmapSelectivityThreshold) {
		searchBitmap = nil
		rs.bitmapOverridden = true
	}

	if requiredCols != nil {
		rs.columnsProjected = len(requiredCols)
	}

	// Field predicates → ReadColumnarFiltered (uses DictFilter internally).
	if len(hints.FieldPredicates) > 0 {
		segPreds := make([]segment.Predicate, len(hints.FieldPredicates))
		for i, fp := range hints.FieldPredicates {
			segPreds[i] = segment.Predicate{
				Field: fp.Field,
				Op:    fp.Op,
				Value: fp.Value,
			}
		}
		rs.usedDictFilter = hasDictFilterEligiblePredicate(segPreds)

		cr, err := reader.ReadColumnarFiltered(segPreds, searchBitmap, requiredCols)
		rs.readDurationNS = time.Since(readStart).Nanoseconds()
		if cr != nil {
			rs.rowsAfterFilter = int64(cr.Count)
		}

		return cr, rs, err
	}

	// Bitmap-only read.
	if searchBitmap != nil && searchBitmap.GetCardinality() > 0 {
		cr, err := reader.ReadColumnar(requiredCols, searchBitmap)
		rs.readDurationNS = time.Since(readStart).Nanoseconds()
		if cr != nil {
			rs.rowsAfterFilter = int64(cr.Count)
		}

		return cr, rs, err
	}

	// Multi-row-group segments: use ReadColumnarWithHints for row group pruning.
	if reader.RowGroupCount() > 1 {
		qh := segment.QueryHints{
			Columns:     requiredCols,
			SearchTerms: hints.SearchTerms,
		}
		if hints.TimeBounds != nil {
			if !hints.TimeBounds.Earliest.IsZero() {
				t := hints.TimeBounds.Earliest
				qh.MinTime = &t
			}
			if !hints.TimeBounds.Latest.IsZero() {
				t := hints.TimeBounds.Latest
				qh.MaxTime = &t
			}
		}

		cr, err := reader.ReadColumnarWithHints(qh)
		rs.readDurationNS = time.Since(readStart).Nanoseconds()
		if cr != nil {
			rs.rowsAfterFilter = int64(cr.Count)
		}

		return cr, rs, err
	}

	// Single row group: ReadColumnar with column projection (or full scan).
	cr, err := reader.ReadColumnar(requiredCols, nil)
	rs.readDurationNS = time.Since(readStart).Nanoseconds()
	if cr != nil {
		rs.rowsAfterFilter = int64(cr.Count)
	}

	return cr, rs, err
}

// buildColumnarStore builds a columnar batch store from the engine's internal data.
// Data flows directly from compressed .lsg segments into columnar Batch objects,
// bypassing the per-event *event.Event intermediate representation. Memtable
// events are still converted via BatchFromEvents since they originate as Event structs.
//
// This path materializes all matching segment data into memory upfront.
// It is only used when an external IndexStore is injected (test path); all
// production server-mode queries use the streaming path (runStreamingPipeline)
// which reads row groups on-demand and allows operators to spill to disk.
func (e *Engine) buildColumnarStore(ctx context.Context, hints *spl2.QueryHints, onProgress func(*SearchProgress), monitor *stats.BudgetMonitor, traceSegments ...bool) (map[string][]*enginepipeline.Batch, storeStats, error) {
	trace := len(traceSegments) > 0 && traceSegments[0]
	_ = trace

	// Flush buffered events so they are visible to the query scan.
	if e.batcher != nil && e.batcher.BufferedEvents() > 0 {
		if err := e.batcher.Flush(); err != nil {
			e.logger.Warn("pre-query batcher flush failed", "error", err)
		}
	}

	// Pin epoch to prevent retired segments from being munmap'd during scan.
	ep := e.pinEpoch()
	segs := make([]*segmentHandle, len(ep.segments))
	copy(segs, ep.segments)
	dataDir := e.dataDir

	e.mu.RLock()
	indexStore := e.indexStore
	e.mu.RUnlock()

	// If an external IndexStore was injected (test path), convert to batches.
	if indexStore != nil {
		ep.unpin() // no segment I/O needed for IndexStore path
		batchStore := make(map[string][]*enginepipeline.Batch)
		for name, rows := range indexStore.Indexes {
			events := make([]*event.Event, len(rows))
			for i, row := range rows {
				ev := &event.Event{
					Fields: make(map[string]event.Value, len(row.Fields)),
				}
				for k, v := range row.Fields {
					ev.Fields[k] = event.ValueFromInterface(v)
				}
				if raw, ok := row.Fields["_raw"].(string); ok {
					ev.Raw = raw
				}
				if src, ok := row.Fields["_source"].(string); ok {
					ev.Source = src
				}
				if st, ok := row.Fields["_sourcetype"].(string); ok {
					ev.SourceType = st
				}
				if h, ok := row.Fields["host"].(string); ok {
					ev.Host = h
				}
				if idx, ok := row.Fields["index"].(string); ok {
					ev.Index = idx
				}
				events[i] = ev
			}
			batchStore[name] = eventsToBatches(events, enginepipeline.DefaultBatchSize)
		}

		return batchStore, storeStats{}, nil
	}

	batchStore := make(map[string][]*enginepipeline.Batch)
	var ss storeStats

	// No separate buffer to scan — all data is in segments.

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:          PhaseFilteringSegments,
			BufferedEvents: 0,
		})
	}

	// Sort segments by MaxTime descending (newest first).
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].meta.MaxTime.After(segs[j].meta.MaxTime)
	})
	_ = dataDir

	ss.SegmentsTotal = len(segs)

	requiredCols := hints.RequiredCols
	if len(requiredCols) == 0 {
		requiredCols = nil
	}

	type columnarSegResult struct {
		cr        *segment.ColumnarResult
		index     string
		readStats segReadStats
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(segs) {
		workers = len(segs)
	}
	if workers < 1 {
		workers = 1
	}

	type segJob struct {
		seg        *segmentHandle
		timeBounds *spl2.TimeBounds
	}
	jobCh := make(chan segJob, len(segs))
	resultCh := make(chan columnarSegResult, len(segs))

	var segErrors atomic.Int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				if ctx.Err() != nil {
					continue // drain channel on cancellation
				}

				seg := job.seg
				reader := seg.reader
				if reader == nil {
					reader = e.loadRemoteSegment(seg)
				}
				if reader == nil {
					continue
				}

				idx := seg.index
				if idx == "" {
					idx = DefaultIndexName
				}

				cr, rs, err := readSegmentColumnar(seg, reader, hints, requiredCols, e.logger, e.queryCfg.BitmapSelectivityThreshold)
				if err != nil {
					e.logger.Warn("segment columnar read error",
						"segment", seg.meta.ID, "error", err)
					segErrors.Add(1)

					continue
				}
				if cr == nil || cr.Count == 0 {
					continue
				}

				// Post-read time filtering on the columnar result.
				if job.timeBounds != nil {
					cr.FilterByTimeRange(job.timeBounds.Earliest, job.timeBounds.Latest)
				}
				if cr.Count == 0 {
					continue
				}

				e.metrics.SegmentReads.Add(1)
				resultCh <- columnarSegResult{cr: cr, index: idx, readStats: rs}
			}
		}()
	}

	dispatched := 0
	for _, seg := range segs {
		if seg.reader == nil && dataDir == "" {
			continue
		}
		if shouldSkipSegment(seg, hints, &ss) {
			continue
		}
		ss.SegmentsScanned++
		jobCh <- segJob{seg: seg, timeBounds: hints.TimeBounds}
		dispatched++
	}
	close(jobCh)

	if onProgress != nil {
		onProgress(&SearchProgress{
			Phase:                PhaseScanningSegments,
			SegmentsTotal:        ss.SegmentsTotal,
			SegmentsDispatched:   dispatched,
			SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
			SegmentsSkippedTime:  ss.SegmentsSkippedTime,
			SegmentsSkippedStat:  ss.SegmentsSkippedStat,
			SegmentsSkippedBF:    ss.SegmentsSkippedBF,
			SegmentsSkippedRange: ss.SegmentsSkippedRange,
			BufferedEvents:       ss.BufferedEvents,
		})
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Per-row memory estimate for columnar batches (Value structs + string data).
	const estimatedColumnarRowBytes int64 = 200

	var scanAcct *stats.BoundAccount
	if monitor != nil {
		scanAcct = monitor.NewAccount("scan")
	} else if e.queryCfg.MaxQueryMemory > 0 {
		localMon := stats.NewBudgetMonitor("scan", int64(e.queryCfg.MaxQueryMemory))
		scanAcct = localMon.NewAccount("scan")
	}
	defer scanAcct.Close()

	// Effective early-termination limit.
	earlyLimit := hints.Limit
	if earlyLimit == 0 && hints.TailLimit > 0 && hints.ReverseScan {
		earlyLimit = hints.TailLimit
	}

	scannedSoFar := 0
	totalRows := 0
	for res := range resultCh {
		batches := enginepipeline.SplitColumnarBatches(res.cr, enginepipeline.DefaultBatchSize)
		batchStore[res.index] = append(batchStore[res.index], batches...)
		totalRows += res.cr.Count

		// Accumulate per-segment read feature flags.
		if res.readStats.usedInvertedIndex {
			ss.InvertedIndexHits++
		}
		if res.readStats.usedPrefetch {
			ss.PrefetchUsed = true
		}
		if res.readStats.usedDictFilter {
			ss.DictFilterUsed = true
		}
		ss.TotalBytesRead += res.readStats.bytesRead

		if trace {
			ss.SegmentDetails = append(ss.SegmentDetails, SegmentDetailStat{
				SegmentID:        res.readStats.segmentID,
				Source:           res.readStats.source,
				Rows:             res.readStats.rowsRead,
				RowsAfterFilter:  res.readStats.rowsAfterFilter,
				BloomHit:         res.readStats.usedInvertedIndex,
				InvertedUsed:     res.readStats.usedInvertedIndex,
				ReadDurationNS:   res.readStats.readDurationNS,
				ColumnsProjected: res.readStats.columnsProjected,
			})
		}

		if err := scanAcct.Grow(int64(res.cr.Count) * estimatedColumnarRowBytes); err != nil {
			wg.Wait()
			ep.unpin()
			ss.SegmentsErrored = int(segErrors.Load())

			return nil, ss, err
		}

		// Early termination: stop collecting once we have enough rows for the limit.
		if earlyLimit > 0 && totalRows >= earlyLimit {
			break
		}

		scannedSoFar++
		if onProgress != nil {
			onProgress(&SearchProgress{
				Phase:                PhaseScanningSegments,
				SegmentsTotal:        ss.SegmentsTotal,
				SegmentsScanned:      scannedSoFar,
				SegmentsDispatched:   dispatched,
				SegmentsSkippedIdx:   ss.SegmentsSkippedIdx,
				SegmentsSkippedTime:  ss.SegmentsSkippedTime,
				SegmentsSkippedStat:  ss.SegmentsSkippedStat,
				SegmentsSkippedBF:    ss.SegmentsSkippedBF,
				SegmentsSkippedRange: ss.SegmentsSkippedRange,
				BufferedEvents:       ss.BufferedEvents,
				RowsReadSoFar:        int64(totalRows),
			})
		}
	}

	wg.Wait()
	ep.unpin()

	ss.SegmentsErrored = int(segErrors.Load())

	return batchStore, ss, nil
}

// eventsToBatches converts a slice of events into pipeline-sized Batch slices.
// Used for buffered events and test paths where data originates as Event structs.
func eventsToBatches(events []*event.Event, batchSize int) []*enginepipeline.Batch {
	if len(events) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = enginepipeline.DefaultBatchSize
	}

	numBatches := (len(events) + batchSize - 1) / batchSize
	batches := make([]*enginepipeline.Batch, 0, numBatches)
	for start := 0; start < len(events); start += batchSize {
		end := start + batchSize
		if end > len(events) {
			end = len(events)
		}
		batches = append(batches, enginepipeline.BatchFromEvents(events[start:end]))
	}

	return batches
}

package pipeline

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// scanPhase tracks the current iteration phase of SegmentStreamIterator.
type scanPhase int

const (
	phaseMemtable scanPhase = iota
	phaseSegments
	phaseDone
)

// SegmentSource provides the segment reader interface needed by SegmentStreamIterator.
// This abstracts away server-internal types (segmentHandle) so the iterator
// lives in pkg/engine/pipeline (no import cycle with pkg/server).
type SegmentSource struct {
	Reader      *segment.Reader
	Index       string // index name for this segment
	InvertedIdx *index.SerializedIndex
	Bloom       *index.BloomFilter
	Meta        SegmentMeta // subset of segment metadata needed
}

// SegmentMeta holds the subset of segment metadata needed by the streaming iterator.
type SegmentMeta struct {
	ID           string
	MinTime      time.Time
	MaxTime      time.Time
	EventCount   int64
	SizeBytes    int64
	BloomVersion int
}

// SegmentStreamHints contains query hints that control scan optimization
// for the streaming segment iterator.
type SegmentStreamHints struct {
	IndexName                  string
	TimeBounds                 *spl2.TimeBounds
	SearchTerms                []string
	SearchTermTree             *spl2.SearchTermTree // structured boolean tree for inverted index OR/AND
	FieldPreds                 []spl2.FieldPredicate
	RangePreds                 []spl2.RangePredicate
	InvertedPreds              []spl2.InvertedIndexPredicate
	RequiredCols               []string
	Limit                      int  // early termination for head/tail (0 = unlimited)
	SortAscending              bool // true = oldest-first iteration order
	BitmapSelectivityThreshold float64

	// Multi-source fields for wildcard/list queries.
	SourceIndices      []string // from parser: FROM a, b, c
	SourceGlob         string   // from parser: FROM logs*
	SourceScopeType    string   // from optimizer: "all", "single", "list", "glob"
	SourceScopeSources []string // resolved source names for scope
	SourceScopePattern string   // glob pattern for scope
}

// SegmentStreamProgress holds real-time progress for the streaming scan.
// The onProgress callback receives this per-row-group to enable fine-grained
// UI updates (not stuck on large segments).
type SegmentStreamProgress struct {
	Phase                string // "memtable", "scanning", "done"
	SegmentsTotal        int
	SegmentsScanned      int
	SegmentsSkipped      int // total skips (all reasons)
	SegmentsSkippedTime  int // skipped by time range pruning
	SegmentsSkippedBloom int // skipped by bloom filter
	RowGroupsScanned     int
	EventsScanned        int64
	EventsMatched        int64
	BytesRead            int64
	CurrentSegmentID     string
	CurrentRGIndex       int
	CurrentRGTotal       int
}

// SegmentStreamStats holds execution statistics for the streaming scan.
type SegmentStreamStats struct {
	SegmentsTotal    int
	SegmentsScanned  int
	SegmentsSkipped  int
	RowGroupsTotal   int
	RowGroupsScanned int
	RowGroupsSkipped int // pruned by time or bloom
	EventsScanned    int64
	EventsMatched    int64
	BytesRead        int64
	BloomSkips       int   // row groups skipped by bloom filter
	TimeSkips        int   // row groups skipped by time range pruning
	BitmapHits       int64 // events selected by inverted index bitmap
	PeakMemoryBytes  int64 // max memory used at any point

	// Row-group-level skip counters from the unified RG filter evaluator.
	RGConstSkips    int // row groups skipped by const column mismatch
	RGPresenceSkips int // row groups skipped by column absence
	RGZoneMapSkips  int // row groups skipped by zone map exclusion
	RGBloomSkips    int // row groups skipped by per-column bloom filter
	RGBloomsChecked int // total bloom filter consultations across all RGs

	// Segment-level skip counters (complement BloomSkips/TimeSkips which are row-group-level).
	SegmentBloomSkips int // segments skipped by bloom filter
	SegmentTimeSkips  int // segments skipped by time range
	ScopeSkips        int // segments skipped by source scope mismatch
	EmptyBitmapSkips  int // segments skipped by empty inverted index bitmap
}

// SegmentStreamIterator streams events from storage segments and in-memory
// batcher events, row-group-by-row-group, yielding batches of DefaultBatchSize
// to the pipeline.
//
// Unlike ScanIterator (which takes a pre-materialized []*event.Event),
// this iterator pulls from segment readers lazily. Memory footprint is
// bounded by one row group (~8K events) plus pipeline working set.
//
// NOT thread-safe. Designed for single-goroutine Volcano pipeline.
//
// Key invariant: at any moment, only ONE row group's events are in rgEvents.
// When advancing to the next row group, rgEvents is replaced (old slice
// becomes eligible for GC). When advancing to the next segment, bitmap
// is set to nil (old bitmap becomes eligible for GC).
//
// Memory at any point: <= 1 row group (~1.6MB) + 1 bitmap + pipeline working set.
type SegmentStreamIterator struct {
	// Configuration (immutable after construction)
	segments  []*SegmentSource
	memEvents []*event.Event
	hints     *SegmentStreamHints
	batchSize int
	acct      stats.MemoryAccount

	// Mutable scan state
	phase    scanPhase
	memOff   int                 // position within memEvents
	segIdx   int                 // current segment index
	rgIdx    int                 // current row group within segment
	rgEvents []*event.Event      // buffered events from current row group
	rgOff    int                 // position within rgEvents
	bitmap   *roaring.Bitmap     // search bitmap for current segment (nil = no filter)
	rgTotal  int                 // total row groups in current segment
	needCols map[string]bool     // column projection set (nil = all)
	segPreds []segment.Predicate // converted field predicates for segment reader

	// Row-group filter evaluator (unified boolean filter tree).
	rgFilterNode  *segment.RGFilterNode      // query-level (computed once in constructor)
	rgFilter      *segment.RGFilterEvaluator // per-segment (recreated on segment change)
	rgFilterStats segment.RGFilterStats      // accumulated across all segments

	// Lazy source set for O(1) lookup when SourceScopeSources list > 16.
	sourceSet map[string]struct{}

	// Stats + budget
	totalYielded      int
	lastBatchEstimate int64 // tracks previous batch size for Shrink
	streamStats       SegmentStreamStats

	// Progress reporting
	onProgress func(SegmentStreamProgress)
}

// NewSegmentStreamIterator creates a streaming segment scan iterator.
// Segments should be pre-sorted by time (newest-first by default).
// MemEvents are in-memory events from the batcher (not yet flushed to parts).
// Acct is the per-operator budget account (nil = no budget tracking).
func NewSegmentStreamIterator(
	segments []*SegmentSource,
	memEvents []*event.Event,
	hints *SegmentStreamHints,
	batchSize int,
	acct stats.MemoryAccount,
) *SegmentStreamIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if hints == nil {
		hints = &SegmentStreamHints{}
	}

	// Pre-convert field predicates to segment.Predicate for ReadRowGroupFiltered.
	var segPreds []segment.Predicate
	for _, fp := range hints.FieldPreds {
		segPreds = append(segPreds, segment.Predicate{
			Field: fp.Field,
			Op:    fp.Op,
			Value: fp.Value,
		})
	}
	// Convert range predicates (Min/Max bounds) to >= / <= segment predicates.
	for _, rp := range hints.RangePreds {
		if rp.Min != "" {
			segPreds = append(segPreds, segment.Predicate{
				Field: rp.Field,
				Op:    ">=",
				Value: rp.Min,
			})
		}
		if rp.Max != "" {
			segPreds = append(segPreds, segment.Predicate{
				Field: rp.Field,
				Op:    "<=",
				Value: rp.Max,
			})
		}
	}

	// Build column projection set.
	var needCols map[string]bool
	if len(hints.RequiredCols) > 0 {
		needCols = make(map[string]bool, len(hints.RequiredCols))
		for _, c := range hints.RequiredCols {
			needCols[c] = true
		}
	}

	iter := &SegmentStreamIterator{
		segments:  segments,
		memEvents: memEvents,
		hints:     hints,
		batchSize: batchSize,
		acct:      stats.EnsureAccount(acct),
		phase:     phaseMemtable,
		segIdx:    -1, // Start at -1 so the first advanceRowGroup() initializes segment 0.
		needCols:  needCols,
		segPreds:  segPreds,
	}

	// Build RG filter tree from hints (once per query).
	iter.rgFilterNode = BuildRGFilter(hints)

	// Reverse segment order for ascending sort (oldest-first).
	if hints.SortAscending && len(segments) > 1 {
		reversed := make([]*SegmentSource, len(segments))
		for i, s := range segments {
			reversed[len(segments)-1-i] = s
		}
		iter.segments = reversed
	}

	// Count total row groups for stats.
	for _, seg := range iter.segments {
		if seg.Reader != nil {
			iter.streamStats.RowGroupsTotal += seg.Reader.RowGroupCount()
		}
	}
	iter.streamStats.SegmentsTotal = len(iter.segments)

	return iter
}

// SetOnProgress sets the progress callback. Called per-row-group completion,
// per-segment skip, and on phase transitions.
func (s *SegmentStreamIterator) SetOnProgress(fn func(SegmentStreamProgress)) {
	s.onProgress = fn
}

// Init prepares the iterator. Called once before first Next().
func (s *SegmentStreamIterator) Init(_ context.Context) error {
	return nil
}

// Next returns the next batch of results. Returns (nil, nil) when exhausted.
func (s *SegmentStreamIterator) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	for {
		if s.phase == phaseDone {
			return nil, nil
		}

		// Check early termination via limit.
		if s.hints.Limit > 0 && s.totalYielded >= s.hints.Limit {
			s.phase = phaseDone

			return nil, nil
		}

		switch s.phase {
		case phaseMemtable:
			batch, err := s.nextMemtableBatch(ctx)
			if err != nil {
				return nil, err
			}
			if batch != nil {
				return batch, nil
			}
			// Memtable exhausted — move to segments.
			s.phase = phaseSegments
			s.reportProgress()

		case phaseSegments:
			batch, err := s.nextSegmentBatch(ctx)
			if err != nil {
				return nil, err
			}
			if batch != nil {
				return batch, nil
			}
			// All segments exhausted.
			s.phase = phaseDone
			s.reportProgress()

			return nil, nil
		}
	}
}

// Close releases resources. MUST be called even on error path.
func (s *SegmentStreamIterator) Close() error {
	if s.acct != nil && s.lastBatchEstimate > 0 {
		s.acct.Shrink(s.lastBatchEstimate)
		s.lastBatchEstimate = 0
	}
	s.rgEvents = nil
	s.bitmap = nil
	s.memEvents = nil
	s.acct.Close()

	return nil
}

// Schema returns nil — schema is inferred from batch columns.
func (s *SegmentStreamIterator) Schema() []FieldInfo { return nil }

// Stats returns the accumulated scan statistics.
func (s *SegmentStreamIterator) Stats() SegmentStreamStats {
	if s.acct != nil {
		s.streamStats.PeakMemoryBytes = s.acct.Used()
	}

	// Copy RG filter stats into the stream stats.
	s.streamStats.RGConstSkips = s.rgFilterStats.ConstSkips
	s.streamStats.RGPresenceSkips = s.rgFilterStats.PresenceSkips
	s.streamStats.RGZoneMapSkips = s.rgFilterStats.ZoneMapSkips
	s.streamStats.RGBloomSkips = s.rgFilterStats.BloomSkips
	s.streamStats.RGBloomsChecked = s.rgFilterStats.BloomsChecked

	return s.streamStats
}

// MemoryUsed returns the current tracked memory for this operator.
// Implements the MemoryAccounter interface for InstrumentedIterator.
func (s *SegmentStreamIterator) MemoryUsed() int64 {
	return s.acct.Used()
}

// Memtable phase

func (s *SegmentStreamIterator) nextMemtableBatch(_ context.Context) (*Batch, error) {
	if s.memOff >= len(s.memEvents) {
		return nil, nil
	}

	// Shrink previous batch — it has been consumed by downstream operator.
	if s.acct != nil && s.lastBatchEstimate > 0 {
		s.acct.Shrink(s.lastBatchEstimate)
		s.lastBatchEstimate = 0
	}

	batchSize := s.batchSize

	for batchSize > 0 {
		end := s.memOff + batchSize
		if end > len(s.memEvents) {
			end = len(s.memEvents)
		}

		// Apply limit.
		if s.hints.Limit > 0 {
			remaining := s.hints.Limit - s.totalYielded
			if remaining <= 0 {
				return nil, nil
			}
			if end-s.memOff > remaining {
				end = s.memOff + remaining
			}
		}

		slice := s.memEvents[s.memOff:end]

		// Budget tracking with hard error on genuine budget pressure.
		// After Shrink, the account reflects only the new batch. If Grow still
		// fails, retry with a smaller batch before giving up.
		//
		// CRITICAL: s.memOff is advanced AFTER the Grow check. If sort catches a
		// BudgetExceededError from this iterator, spills its buffer, and retries
		// Next(), the offset must still point at this batch so no events are skipped.
		if s.acct != nil {
			var estimate int64
			for _, ev := range slice {
				estimate += event.EstimateEventSize(ev)
			}
			if err := s.acct.Grow(estimate); err != nil {
				if stats.IsMemoryExhausted(err) && batchSize > minAdaptiveBatchSize {
					batchSize /= 2

					continue // retry with smaller batch
				}

				return nil, fmt.Errorf("query memory limit exceeded after scanning %d events: %w",
					s.totalYielded, err)
			}
			s.lastBatchEstimate = estimate
		}

		s.memOff = end

		batch := BatchFromEvents(slice)
		s.totalYielded += batch.Len
		s.streamStats.EventsMatched += int64(batch.Len)

		return batch, nil
	}

	// Fell through: even minAdaptiveBatchSize didn't fit.
	return nil, fmt.Errorf("query memory limit exceeded after scanning %d events: "+
		"cannot fit minimum batch (%d events) in memory budget; "+
		"consider increasing --memory or adding filters to reduce data volume",
		s.totalYielded, minAdaptiveBatchSize)
}

// Segment phase

func (s *SegmentStreamIterator) nextSegmentBatch(ctx context.Context) (*Batch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// If we have buffered row group events, yield from them.
		if s.rgOff < len(s.rgEvents) {
			batch, err := s.yieldFromRGBuffer()
			if err != nil {
				return nil, err
			}
			if batch != nil {
				return batch, nil
			}
		}

		// Advance to next row group or next segment.
		if !s.advanceRowGroup() {
			return nil, nil // all segments exhausted
		}

		// Load the new row group.
		if err := s.loadCurrentRowGroup(); err != nil {
			return nil, fmt.Errorf("segment_stream.loadRowGroup: %w", err)
		}
	}
}

// minAdaptiveBatchSize is the floor for adaptive batch size reduction.
// If the scan cannot fit even this many events, it returns an actionable error.
const minAdaptiveBatchSize = 64

// yieldFromRGBuffer yields the next batch from the current row group buffer.
// When memory budget is tight, it adaptively halves the batch size down to
// minAdaptiveBatchSize before failing with an actionable error message.
func (s *SegmentStreamIterator) yieldFromRGBuffer() (*Batch, error) {
	// Shrink previous batch — it has been consumed by downstream operator.
	if s.acct != nil && s.lastBatchEstimate > 0 {
		s.acct.Shrink(s.lastBatchEstimate)
		s.lastBatchEstimate = 0
	}

	batchSize := s.batchSize

	for batchSize > 0 {
		end := s.rgOff + batchSize
		if end > len(s.rgEvents) {
			end = len(s.rgEvents)
		}

		// Apply limit.
		if s.hints.Limit > 0 {
			remaining := s.hints.Limit - s.totalYielded
			if remaining <= 0 {
				s.phase = phaseDone

				return nil, nil
			}
			if end-s.rgOff > remaining {
				end = s.rgOff + remaining
			}
		}

		slice := s.rgEvents[s.rgOff:end]

		// Track memory for observability (PeakMemoryBytes).
		// After Shrink, the account reflects only the new batch. If Grow still
		// fails, it means real pressure from downstream operators (dedup hash map,
		// sort buffer, etc.) — retry with a smaller batch before giving up.
		//
		// CRITICAL: s.rgOff is advanced AFTER the Grow check. If sort catches a
		// BudgetExceededError from this iterator, spills its buffer, and retries
		// Next(), the offset must still point at this batch so no events are skipped.
		if s.acct != nil {
			var estimate int64
			for _, ev := range slice {
				estimate += event.EstimateEventSize(ev)
			}
			if err := s.acct.Grow(estimate); err != nil {
				if stats.IsMemoryExhausted(err) && batchSize > minAdaptiveBatchSize {
					batchSize /= 2

					continue // retry with smaller batch
				}

				return nil, fmt.Errorf("query memory limit exceeded after scanning %d events: %w",
					s.totalYielded, err)
			}
			s.lastBatchEstimate = estimate
		}

		s.rgOff = end

		batch := BatchFromEvents(slice)
		s.totalYielded += batch.Len
		s.streamStats.EventsMatched += int64(batch.Len)

		return batch, nil
	}

	// Fell through: even minAdaptiveBatchSize didn't fit.
	return nil, fmt.Errorf("query memory limit exceeded after scanning %d events: "+
		"cannot fit minimum batch (%d events) in memory budget; "+
		"consider increasing --memory or adding filters to reduce data volume",
		s.totalYielded, minAdaptiveBatchSize)
}

// advanceRowGroup advances to the next available row group, potentially
// moving to the next segment. Returns false when all segments are exhausted.
func (s *SegmentStreamIterator) advanceRowGroup() bool {
	for {
		// Try next row group in current segment.
		if s.segIdx < len(s.segments) && s.rgIdx < s.rgTotal {
			s.rgIdx++
			if s.rgIdx < s.rgTotal {
				return true
			}
		}

		// Current segment exhausted — release bitmap for GC.
		s.bitmap = nil
		s.rgEvents = nil
		s.rgOff = 0

		// Move to next segment.
		s.segIdx++
		if s.segIdx >= len(s.segments) {
			return false
		}

		// Initialize new segment.
		seg := s.segments[s.segIdx]

		// Skip segments with no reader.
		if seg.Reader == nil {
			s.streamStats.SegmentsSkipped++
			s.reportSegmentSkipped()

			continue
		}

		// Skip by source scope (index name, multi-source list, glob).
		if !s.matchesStreamSourceScope(seg.Index) {
			s.streamStats.SegmentsSkipped++
			s.streamStats.ScopeSkips++
			s.reportSegmentSkipped()

			continue
		}

		// Skip by time bounds.
		if s.shouldSkipByTime(seg) {
			s.streamStats.SegmentsSkipped++
			s.streamStats.SegmentTimeSkips++
			s.reportSegmentSkipped()

			continue
		}

		// Skip by bloom filter (segment-level).
		if s.shouldSkipByBloom(seg) {
			s.streamStats.SegmentsSkipped++
			s.streamStats.SegmentBloomSkips++
			s.reportSegmentSkipped()

			continue
		}

		// Compute inverted index bitmap for this segment.
		s.bitmap = s.computeSegmentBitmap(seg)

		// If bitmap is non-nil and empty, skip entire segment.
		if s.bitmap != nil && s.bitmap.GetCardinality() == 0 {
			s.bitmap = nil
			s.streamStats.SegmentsSkipped++
			s.streamStats.EmptyBitmapSkips++
			s.reportSegmentSkipped()

			continue
		}

		// Apply selectivity threshold: discard non-selective bitmaps.
		if s.bitmap != nil && s.hints.BitmapSelectivityThreshold > 0 {
			card := s.bitmap.GetCardinality()
			total := seg.Reader.EventCount()
			if total > 0 && float64(card)/float64(total) > s.hints.BitmapSelectivityThreshold {
				s.bitmap = nil
			}
		}

		// Create per-segment RG filter evaluator.
		if s.rgFilterNode != nil {
			s.rgFilter = segment.NewRGFilterEvaluator(s.rgFilterNode, seg.Reader)
		} else {
			s.rgFilter = nil
		}

		s.rgTotal = seg.Reader.RowGroupCount()
		s.rgIdx = 0
		s.streamStats.SegmentsScanned++
		s.reportSegmentEntered(seg)

		return true
	}
}

// loadCurrentRowGroup reads the current row group into rgEvents.
// CRITICAL: this replaces rgEvents, making the old slice GC-eligible.
func (s *SegmentStreamIterator) loadCurrentRowGroup() error {
	seg := s.segments[s.segIdx]
	reader := seg.Reader

	// Check if this row group can be pruned by time.
	if s.canPruneRowGroupByTime(reader, s.rgIdx) {
		s.rgEvents = nil
		s.rgOff = 0
		s.streamStats.RowGroupsSkipped++
		s.streamStats.TimeSkips++
		s.reportRowGroupDone()

		return nil
	}

	// Unified RG filter: check const columns, presence, zone maps, and per-column blooms.
	// This subsumes the old canPruneRowGroupByBloom check with broader coverage.
	if s.rgFilter != nil {
		if s.rgFilter.EvaluateRowGroup(s.rgIdx, &s.rgFilterStats) == segment.RGSkip {
			s.rgEvents = nil
			s.rgOff = 0
			s.streamStats.RowGroupsSkipped++
			s.streamStats.BloomSkips++ // backward compat counter
			s.reportRowGroupDone()

			return nil
		}
	} else if s.canPruneRowGroupByBloom(reader, s.rgIdx) {
		// Fallback: flat _raw bloom check when no RG filter tree exists.
		s.rgEvents = nil
		s.rgOff = 0
		s.streamStats.RowGroupsSkipped++
		s.streamStats.BloomSkips++
		s.reportRowGroupDone()

		return nil
	}

	// Read the row group with bitmap and predicate filtering.
	events, err := reader.ReadRowGroupFiltered(
		s.rgIdx,
		s.bitmap,
		s.segPreds,
		s.hints.RequiredCols,
	)
	if err != nil {
		return fmt.Errorf("segment %s rg %d: %w", seg.Meta.ID, s.rgIdx, err)
	}

	// Apply time bounds filtering on the event level for partial overlaps.
	events = filterEventsByTimeBounds(events, s.hints.TimeBounds)

	s.rgEvents = events
	s.rgOff = 0
	s.streamStats.RowGroupsScanned++
	s.streamStats.EventsScanned += int64(len(events))
	s.streamStats.BytesRead += seg.Meta.SizeBytes / int64(max(s.rgTotal, 1))
	s.reportRowGroupDone()

	return nil
}

// Segment skip checks

// matchesStreamSourceScope checks if a segment's index matches the streaming
// query's source scope. Returns true if the segment should be scanned.
func (s *SegmentStreamIterator) matchesStreamSourceScope(segIndex string) bool {
	h := s.hints

	// Optimizer-resolved source scope.
	switch h.SourceScopeType {
	case "all":
		return true
	case "single":
		if len(h.SourceScopeSources) > 0 {
			return segIndex == h.SourceScopeSources[0]
		}
	case "list":
		// Use O(1) set lookup for large source lists (>16 entries).
		// Build the set lazily on first call and cache it.
		if len(h.SourceScopeSources) > 16 {
			if s.sourceSet == nil {
				s.sourceSet = make(map[string]struct{}, len(h.SourceScopeSources))
				for _, src := range h.SourceScopeSources {
					s.sourceSet[src] = struct{}{}
				}
			}
			_, ok := s.sourceSet[segIndex]

			return ok
		}

		for _, src := range h.SourceScopeSources {
			if segIndex == src {
				return true
			}
		}

		return false
	case "glob":
		if h.SourceScopePattern != "" {
			matched, _ := path.Match(h.SourceScopePattern, segIndex)

			return matched
		}
	}

	// Parser-level fallbacks.
	if len(h.SourceIndices) > 0 {
		for _, idx := range h.SourceIndices {
			if segIndex == idx {
				return true
			}
		}

		return false
	}
	if h.SourceGlob != "" {
		if h.SourceGlob == "*" {
			return true
		}
		matched, _ := path.Match(h.SourceGlob, segIndex)

		return matched
	}

	// Single IndexName exact match.
	if h.IndexName != "" {
		return segIndex == h.IndexName
	}

	// No filter — scan everything.
	return true
}

func (s *SegmentStreamIterator) shouldSkipByTime(seg *SegmentSource) bool {
	tb := s.hints.TimeBounds
	if tb == nil {
		return false
	}
	if !tb.Earliest.IsZero() && !tb.Latest.IsZero() {
		return seg.Meta.MaxTime.Before(tb.Earliest) || seg.Meta.MinTime.After(tb.Latest)
	}
	if !tb.Earliest.IsZero() {
		return seg.Meta.MaxTime.Before(tb.Earliest)
	}
	if !tb.Latest.IsZero() {
		return seg.Meta.MinTime.After(tb.Latest)
	}

	return false
}

func (s *SegmentStreamIterator) shouldSkipByBloom(seg *SegmentSource) bool {
	if len(s.hints.SearchTerms) == 0 || seg.Bloom == nil || seg.Meta.BloomVersion < 2 {
		return false
	}

	return !seg.Bloom.MayContainAll(s.hints.SearchTerms)
}

func (s *SegmentStreamIterator) computeSegmentBitmap(seg *SegmentSource) *roaring.Bitmap {
	var bm *roaring.Bitmap

	// Prefer structured term tree (supports OR) over flat SearchTerms.
	if s.hints.SearchTermTree != nil && seg.InvertedIdx != nil {
		bm = evaluateTermTree(s.hints.SearchTermTree, seg.InvertedIdx)
		if bm != nil {
			s.streamStats.BitmapHits += int64(bm.GetCardinality())
		}
	} else if len(s.hints.SearchTerms) > 0 && seg.InvertedIdx != nil && seg.Meta.BloomVersion >= 2 {
		// Fallback: flat terms (AND only) — existing logic unchanged.
		for i, term := range s.hints.SearchTerms {
			termBM, err := seg.InvertedIdx.Search(term)
			if err != nil {
				continue
			}
			if i == 0 {
				bm = termBM
			} else {
				bm.And(termBM)
			}
			if bm.GetCardinality() == 0 {
				return bm
			}
		}
		if bm != nil {
			s.streamStats.BitmapHits += int64(bm.GetCardinality())
		}
	}

	// Inverted index field=value lookups — unchanged.
	if len(s.hints.InvertedPreds) > 0 && seg.InvertedIdx != nil {
		for _, iip := range s.hints.InvertedPreds {
			fieldBM, err := seg.InvertedIdx.SearchField(iip.Field, iip.Value)
			if err != nil {
				continue
			}
			if bm == nil {
				bm = fieldBM
			} else {
				bm.And(fieldBM)
			}
		}
	}

	return bm
}

// evaluateTermTree recursively evaluates a SearchTermTree against a serialized
// inverted index, producing a roaring bitmap of matching row IDs.
//
// Leaf: AND-intersect all term bitmaps.
// And:  intersect children bitmaps.
// Or:   union children bitmaps (using FastOr for efficiency).
//
// Returns nil when a branch has no constraint (match everything).
func evaluateTermTree(tree *spl2.SearchTermTree, idx *index.SerializedIndex) *roaring.Bitmap {
	if tree == nil {
		return nil
	}

	switch tree.Op {
	case spl2.SearchTermLeaf:
		var result *roaring.Bitmap
		for _, term := range tree.Terms {
			bm, err := idx.Search(term)
			if err != nil {
				continue
			}
			if result == nil {
				result = bm
			} else {
				result.And(bm)
			}
			if result.IsEmpty() {
				return result
			}
		}

		return result

	case spl2.SearchTermAnd:
		var result *roaring.Bitmap
		for _, child := range tree.Children {
			childBm := evaluateTermTree(child, idx)
			if childBm == nil {
				continue // nil = no constraint from this branch
			}
			if result == nil {
				result = childBm
			} else {
				result.And(childBm)
			}
			if result.IsEmpty() {
				return result
			}
		}

		return result

	case spl2.SearchTermOr:
		bitmaps := make([]*roaring.Bitmap, 0, len(tree.Children))
		for _, child := range tree.Children {
			childBm := evaluateTermTree(child, idx)
			if childBm == nil {
				// One OR branch matches everything → whole OR matches everything.
				return nil
			}
			bitmaps = append(bitmaps, childBm)
		}
		if len(bitmaps) == 0 {
			return nil
		}

		return roaring.FastOr(bitmaps...)
	}

	return nil
}

// Row group pruning

func (s *SegmentStreamIterator) canPruneRowGroupByTime(reader *segment.Reader, rgIdx int) bool {
	tb := s.hints.TimeBounds
	if tb == nil {
		return false
	}
	if tb.Earliest.IsZero() && tb.Latest.IsZero() {
		return false
	}

	var minT, maxT *time.Time
	if !tb.Earliest.IsZero() {
		t := tb.Earliest
		minT = &t
	}
	if !tb.Latest.IsZero() {
		t := tb.Latest
		maxT = &t
	}

	return reader.CanPruneRowGroupByIndex(rgIdx, minT, maxT)
}

func (s *SegmentStreamIterator) canPruneRowGroupByBloom(reader *segment.Reader, rgIdx int) bool {
	if len(s.hints.SearchTerms) == 0 {
		return false
	}

	bf, err := reader.BloomFilterForRowGroup(rgIdx)
	if err != nil || bf == nil {
		return false
	}

	return !bf.MayContainAll(s.hints.SearchTerms)
}

// Progress reporting

func (s *SegmentStreamIterator) buildProgress() SegmentStreamProgress {
	return SegmentStreamProgress{
		SegmentsTotal:        s.streamStats.SegmentsTotal,
		SegmentsScanned:      s.streamStats.SegmentsScanned,
		SegmentsSkipped:      s.streamStats.SegmentsSkipped,
		SegmentsSkippedTime:  s.streamStats.SegmentTimeSkips,
		SegmentsSkippedBloom: s.streamStats.SegmentBloomSkips,
		RowGroupsScanned:     s.streamStats.RowGroupsScanned,
		EventsScanned:        s.streamStats.EventsScanned,
		EventsMatched:        s.streamStats.EventsMatched,
		BytesRead:            s.streamStats.BytesRead,
	}
}

func (s *SegmentStreamIterator) reportProgress() {
	if s.onProgress == nil {
		return
	}
	p := s.buildProgress()
	switch s.phase {
	case phaseMemtable:
		p.Phase = "memtable"
	case phaseDone:
		p.Phase = "done"
	default:
		p.Phase = "scanning"
	}
	s.onProgress(p)
}

func (s *SegmentStreamIterator) reportSegmentEntered(seg *SegmentSource) {
	if s.onProgress == nil {
		return
	}
	p := s.buildProgress()
	p.Phase = "scanning"
	p.CurrentSegmentID = seg.Meta.ID
	p.CurrentRGIndex = 0
	p.CurrentRGTotal = s.rgTotal
	s.onProgress(p)
}

func (s *SegmentStreamIterator) reportSegmentSkipped() {
	if s.onProgress == nil {
		return
	}
	p := s.buildProgress()
	p.Phase = "scanning"
	s.onProgress(p)
}

func (s *SegmentStreamIterator) reportRowGroupDone() {
	if s.onProgress == nil {
		return
	}
	p := s.buildProgress()
	p.Phase = "scanning"
	if s.segIdx < len(s.segments) {
		p.CurrentSegmentID = s.segments[s.segIdx].Meta.ID
	}
	p.CurrentRGIndex = s.rgIdx
	p.CurrentRGTotal = s.rgTotal
	s.onProgress(p)
}

// Helpers

// filterEventsByTimeBounds filters events by time bounds.
// Returns the input slice unmodified if no bounds are set.
func filterEventsByTimeBounds(events []*event.Event, tb *spl2.TimeBounds) []*event.Event {
	if tb == nil || len(events) == 0 {
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

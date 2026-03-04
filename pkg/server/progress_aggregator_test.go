package server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enginepipeline "github.com/lynxbase/lynxdb/pkg/engine/pipeline"
)

// TestProgressAggregator_SumsAcrossIterators verifies that the aggregator
// correctly sums progress from 3 independent iterators (APPEND scenario).
func TestProgressAggregator_SumsAcrossIterators(t *testing.T) {
	var lastProgress atomic.Pointer[SearchProgress]
	agg := &progressAggregator{
		baseSS: storeStats{
			SegmentsTotal:       100,
			SegmentsSkippedTime: 5,
			SegmentsSkippedBF:   3,
			SegmentsSkippedIdx:  2,
			SegmentsSkippedStat: 1,
		},
		memEvents: 50,
		startTime: time.Now(),
		onReport: func(sp *SearchProgress) {
			lastProgress.Store(sp)
		},
	}

	// Simulate 3 iterators reporting progress.
	// Each iterator receives 89 sources (100 total - 11 pre-skipped).
	agg.update(0, enginepipeline.SegmentStreamProgress{
		SegmentsTotal:        89,
		SegmentsScanned:      10,
		SegmentsSkippedTime:  2,
		SegmentsSkippedBloom: 1,
		EventsScanned:        5000,
	})
	agg.update(1, enginepipeline.SegmentStreamProgress{
		SegmentsTotal:        89,
		SegmentsScanned:      8,
		SegmentsSkippedTime:  1,
		SegmentsSkippedBloom: 3,
		EventsScanned:        3000,
	})
	agg.update(2, enginepipeline.SegmentStreamProgress{
		SegmentsTotal:        89,
		SegmentsScanned:      12,
		SegmentsSkippedTime:  0,
		SegmentsSkippedBloom: 2,
		EventsScanned:        7000,
	})

	sp := lastProgress.Load()
	if sp == nil {
		t.Fatal("expected progress to be reported")
	}

	// SegmentsScanned = base(0) + iter0(10) + iter1(8) + iter2(12) = 30
	if sp.SegmentsScanned != 30 {
		t.Errorf("SegmentsScanned: got %d, want 30", sp.SegmentsScanned)
	}

	// SegmentsSkippedTime = base(5) + iter0(2) + iter1(1) + iter2(0) = 8
	if sp.SegmentsSkippedTime != 8 {
		t.Errorf("SegmentsSkippedTime: got %d, want 8", sp.SegmentsSkippedTime)
	}

	// SegmentsSkippedBF = base(3) + iter0(1) + iter1(3) + iter2(2) = 9
	if sp.SegmentsSkippedBF != 9 {
		t.Errorf("SegmentsSkippedBF: got %d, want 9", sp.SegmentsSkippedBF)
	}

	// SegmentsSkippedIdx = base(2) (no iterator contribution)
	if sp.SegmentsSkippedIdx != 2 {
		t.Errorf("SegmentsSkippedIdx: got %d, want 2", sp.SegmentsSkippedIdx)
	}

	// RowsReadSoFar = iter0(5000) + iter1(3000) + iter2(7000) = 15000
	if sp.RowsReadSoFar != 15000 {
		t.Errorf("RowsReadSoFar: got %d, want 15000", sp.RowsReadSoFar)
	}

	// BufferedEvents from base
	if sp.BufferedEvents != 50 {
		t.Errorf("BufferedEvents: got %d, want 50", sp.BufferedEvents)
	}

	// SegmentsTotal = iter0(89) + iter1(89) + iter2(89) + preSkipped(5+3+2+1+0) = 278
	if sp.SegmentsTotal != 278 {
		t.Errorf("SegmentsTotal: got %d, want 278", sp.SegmentsTotal)
	}
}

// TestProgressAggregator_MonotonicallyIncreasing verifies that updating a
// single iterator slot replaces (not adds) its previous value, so the sum
// never double-counts.
func TestProgressAggregator_MonotonicallyIncreasing(t *testing.T) {
	var reports []*SearchProgress
	var mu sync.Mutex
	agg := &progressAggregator{
		baseSS:    storeStats{SegmentsTotal: 10},
		startTime: time.Now(),
		onReport: func(sp *SearchProgress) {
			mu.Lock()
			reports = append(reports, sp)
			mu.Unlock()
		},
	}

	// Iterator 0 reports increasing progress.
	agg.update(0, enginepipeline.SegmentStreamProgress{EventsScanned: 1000})
	agg.update(0, enginepipeline.SegmentStreamProgress{EventsScanned: 2000})
	agg.update(0, enginepipeline.SegmentStreamProgress{EventsScanned: 3000})

	mu.Lock()
	defer mu.Unlock()

	if len(reports) != 3 {
		t.Fatalf("expected 3 reports, got %d", len(reports))
	}

	// Each report should show the latest value, not cumulative.
	if reports[0].RowsReadSoFar != 1000 {
		t.Errorf("report[0].RowsReadSoFar: got %d, want 1000", reports[0].RowsReadSoFar)
	}
	if reports[1].RowsReadSoFar != 2000 {
		t.Errorf("report[1].RowsReadSoFar: got %d, want 2000", reports[1].RowsReadSoFar)
	}
	if reports[2].RowsReadSoFar != 3000 {
		t.Errorf("report[2].RowsReadSoFar: got %d, want 3000", reports[2].RowsReadSoFar)
	}
}

// TestProgressAggregator_ConcurrentUpdates verifies thread-safety of
// concurrent updates from multiple iterators.
func TestProgressAggregator_ConcurrentUpdates(t *testing.T) {
	var lastProgress atomic.Pointer[SearchProgress]
	agg := &progressAggregator{
		baseSS:    storeStats{SegmentsTotal: 30},
		startTime: time.Now(),
		onReport: func(sp *SearchProgress) {
			lastProgress.Store(sp)
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				agg.update(idx, enginepipeline.SegmentStreamProgress{
					SegmentsTotal:   30,
					SegmentsScanned: j + 1,
					EventsScanned:   int64((j + 1) * 1000),
				})
			}
		}(i)
	}
	wg.Wait()

	sp := lastProgress.Load()
	if sp == nil {
		t.Fatal("expected progress to be reported")
	}

	// After all goroutines finish, each slot has final value: SegmentsScanned=100, EventsScanned=100000
	// Sum: 300, 300000. SegmentsTotal = 30*3 + 0 preSkipped = 90.
	if sp.SegmentsScanned != 300 {
		t.Errorf("SegmentsScanned: got %d, want 300", sp.SegmentsScanned)
	}
	if sp.RowsReadSoFar != 300000 {
		t.Errorf("RowsReadSoFar: got %d, want 300000", sp.RowsReadSoFar)
	}
	if sp.SegmentsTotal != 90 {
		t.Errorf("SegmentsTotal: got %d, want 90", sp.SegmentsTotal)
	}
}

// TestOnProgressWrapper_PhaseTransitionPreservesCounters verifies that
// phase-only progress updates (Bug 2) merge with previous counters.
func TestOnProgressWrapper_PhaseTransitionPreservesCounters(t *testing.T) {
	start := time.Now()
	var lastProgress atomic.Pointer[SearchProgress]
	var stored []*SearchProgress

	onProgress := func(p *SearchProgress) {
		prev := lastProgress.Load()
		if prev != nil && p.SegmentsTotal == 0 && p.RowsReadSoFar == 0 && p.BufferedEvents == 0 {
			merged := *prev
			merged.Phase = p.Phase
			merged.ElapsedMS = float64(time.Since(start).Milliseconds())
			p = &merged
		}
		p.ElapsedMS = float64(time.Since(start).Milliseconds())
		lastProgress.Store(p)
		stored = append(stored, p)
	}

	// Parsing (no previous state).
	onProgress(&SearchProgress{Phase: PhaseParsing})

	// Buffer scan with counters.
	onProgress(&SearchProgress{
		Phase:           PhaseBufferScan,
		BufferedEvents:  100,
		SegmentsTotal:   50,
		SegmentsScanned: 10,
		RowsReadSoFar:   5000,
	})

	// Executing pipeline — phase-only update, should preserve counters.
	onProgress(&SearchProgress{Phase: PhaseExecutingPipeline})

	if len(stored) != 3 {
		t.Fatalf("expected 3 stored progress, got %d", len(stored))
	}

	// First update: no merge (no previous).
	if stored[0].Phase != PhaseParsing {
		t.Errorf("stored[0].Phase: got %q, want %q", stored[0].Phase, PhaseParsing)
	}

	// Second update: has counters, stored as-is.
	if stored[1].BufferedEvents != 100 {
		t.Errorf("stored[1].BufferedEvents: got %d, want 100", stored[1].BufferedEvents)
	}

	// Third update: phase-only, should inherit previous counters.
	p3 := stored[2]
	if p3.Phase != PhaseExecutingPipeline {
		t.Errorf("stored[2].Phase: got %q, want %q", p3.Phase, PhaseExecutingPipeline)
	}
	if p3.BufferedEvents != 100 {
		t.Errorf("stored[2].BufferedEvents: got %d, want 100 (should preserve)", p3.BufferedEvents)
	}
	if p3.SegmentsTotal != 50 {
		t.Errorf("stored[2].SegmentsTotal: got %d, want 50 (should preserve)", p3.SegmentsTotal)
	}
	if p3.SegmentsScanned != 10 {
		t.Errorf("stored[2].SegmentsScanned: got %d, want 10 (should preserve)", p3.SegmentsScanned)
	}
	if p3.RowsReadSoFar != 5000 {
		t.Errorf("stored[2].RowsReadSoFar: got %d, want 5000 (should preserve)", p3.RowsReadSoFar)
	}
}

// TestProgressAggregator_APPENDTotalExceedsBase is a regression test for the
// bug where APPEND queries showed "17 scanned / 9 total" because SegmentsTotal
// was not scaled for multi-iterator queries.
func TestProgressAggregator_APPENDTotalExceedsBase(t *testing.T) {
	var lastProgress atomic.Pointer[SearchProgress]
	agg := &progressAggregator{
		baseSS: storeStats{
			SegmentsTotal: 9, // no pre-skips
		},
		memEvents: 0,
		startTime: time.Now(),
		onReport: func(sp *SearchProgress) {
			lastProgress.Store(sp)
		},
	}

	// APPEND creates 2 iterators, each receiving 9 sources.
	// Iterator 0: scans all 9.
	agg.update(0, enginepipeline.SegmentStreamProgress{
		SegmentsTotal:   9,
		SegmentsScanned: 9,
		EventsScanned:   50000,
	})
	// Iterator 1: scans 8, bloom-skips 1.
	agg.update(1, enginepipeline.SegmentStreamProgress{
		SegmentsTotal:        9,
		SegmentsScanned:      8,
		SegmentsSkippedBloom: 1,
		EventsScanned:        40000,
	})

	sp := lastProgress.Load()
	if sp == nil {
		t.Fatal("expected progress to be reported")
	}

	// effectiveTotal = iter0(9) + iter1(9) + preSkipped(0) = 18
	if sp.SegmentsTotal != 18 {
		t.Errorf("SegmentsTotal: got %d, want 18", sp.SegmentsTotal)
	}

	// SegmentsScanned = base(0) + iter0(9) + iter1(8) = 17
	if sp.SegmentsScanned != 17 {
		t.Errorf("SegmentsScanned: got %d, want 17", sp.SegmentsScanned)
	}

	// SegmentsSkippedBF = base(0) + iter1(1) = 1
	if sp.SegmentsSkippedBF != 1 {
		t.Errorf("SegmentsSkippedBF: got %d, want 1", sp.SegmentsSkippedBF)
	}

	// Invariant: scanned + skipped <= total
	totalAccountedFor := sp.SegmentsScanned + sp.SegmentsSkippedBF + sp.SegmentsSkippedTime +
		sp.SegmentsSkippedIdx + sp.SegmentsSkippedStat + sp.SegmentsSkippedRange
	if totalAccountedFor > sp.SegmentsTotal {
		t.Errorf("invariant violation: scanned+skipped (%d) > total (%d)",
			totalAccountedFor, sp.SegmentsTotal)
	}
}

// TestProgressAggregator_SingleIteratorNoRegression verifies that a single
// iterator with pre-skips still reports the correct total (no regression).
func TestProgressAggregator_SingleIteratorNoRegression(t *testing.T) {
	var lastProgress atomic.Pointer[SearchProgress]
	agg := &progressAggregator{
		baseSS: storeStats{
			SegmentsTotal:       11,
			SegmentsSkippedTime: 1,
			SegmentsSkippedBF:   1,
		},
		memEvents: 10,
		startTime: time.Now(),
		onReport: func(sp *SearchProgress) {
			lastProgress.Store(sp)
		},
	}

	// Single iterator receives 9 sources (11 - 2 pre-skipped).
	agg.update(0, enginepipeline.SegmentStreamProgress{
		SegmentsTotal:        9,
		SegmentsScanned:      7,
		SegmentsSkippedBloom: 2,
		EventsScanned:        35000,
	})

	sp := lastProgress.Load()
	if sp == nil {
		t.Fatal("expected progress to be reported")
	}

	// effectiveTotal = iter0(9) + preSkipped(1+1+0+0+0) = 11
	if sp.SegmentsTotal != 11 {
		t.Errorf("SegmentsTotal: got %d, want 11", sp.SegmentsTotal)
	}

	// SegmentsScanned = base(0) + iter0(7) = 7
	if sp.SegmentsScanned != 7 {
		t.Errorf("SegmentsScanned: got %d, want 7", sp.SegmentsScanned)
	}

	// SegmentsSkippedBF = base(1) + iter0(2) = 3
	if sp.SegmentsSkippedBF != 3 {
		t.Errorf("SegmentsSkippedBF: got %d, want 3", sp.SegmentsSkippedBF)
	}

	// SegmentsSkippedTime = base(1) + iter0(0) = 1
	if sp.SegmentsSkippedTime != 1 {
		t.Errorf("SegmentsSkippedTime: got %d, want 1", sp.SegmentsSkippedTime)
	}

	if sp.BufferedEvents != 10 {
		t.Errorf("BufferedEvents: got %d, want 10", sp.BufferedEvents)
	}

	// Invariant: scanned + skipped <= total
	totalAccountedFor := sp.SegmentsScanned + sp.SegmentsSkippedBF + sp.SegmentsSkippedTime +
		sp.SegmentsSkippedIdx + sp.SegmentsSkippedStat + sp.SegmentsSkippedRange
	if totalAccountedFor > sp.SegmentsTotal {
		t.Errorf("invariant violation: scanned+skipped (%d) > total (%d)",
			totalAccountedFor, sp.SegmentsTotal)
	}
}

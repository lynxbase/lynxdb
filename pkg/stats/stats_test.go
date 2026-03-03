package stats

import (
	"testing"
	"time"
)

func TestScanRateRowsPerSec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scanned  int64
		duration time.Duration
		want     float64
	}{
		{"zero duration", 1000, 0, 0},
		{"zero rows", 0, time.Second, 0},
		{"1M rows in 1s", 1_000_000, time.Second, 1_000_000},
		{"100K rows in 100ms", 100_000, 100 * time.Millisecond, 1_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &QueryStats{ScannedRows: tt.scanned, ExecDuration: tt.duration}
			got := s.ScanRateRowsPerSec()
			if got != tt.want {
				t.Errorf("ScanRateRowsPerSec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelectivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scanned int64
		matched int64
		want    float64
	}{
		{"zero scanned", 0, 0, 0},
		{"all matched", 1000, 1000, 1.0},
		{"half matched", 1000, 500, 0.5},
		{"2.7% matched", 10000, 270, 0.027},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &QueryStats{ScannedRows: tt.scanned, MatchedRows: tt.matched}
			got := s.Selectivity()
			if got != tt.want {
				t.Errorf("Selectivity() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSkipRatio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		total   int64
		scanned int64
		want    float64
	}{
		{"zero total", 0, 0, 0},
		{"no skip", 1000, 1000, 0},
		{"50% skip", 1000, 500, 0.5},
		{"93.5% skip", 1_000_000, 65_000, 0.935},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &QueryStats{TotalRowsInRange: tt.total, ScannedRows: tt.scanned}
			got := s.SkipRatio()
			if got != tt.want {
				t.Errorf("SkipRatio() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheHitRatio(t *testing.T) {
	t.Parallel()

	s := &QueryStats{CacheHit: true}
	if s.CacheHitRatio() != 1.0 {
		t.Errorf("CacheHitRatio() with hit = %v, want 1.0", s.CacheHitRatio())
	}

	s.CacheHit = false
	if s.CacheHitRatio() != 0.0 {
		t.Errorf("CacheHitRatio() without hit = %v, want 0.0", s.CacheHitRatio())
	}
}

func TestSkippedSegments(t *testing.T) {
	t.Parallel()

	s := &QueryStats{
		BloomSkippedSegments: 10,
		TimeSkippedSegments:  5,
		IndexSkippedSegments: 3,
		StatSkippedSegments:  2,
		RangeSkippedSegments: 1,
	}
	if got := s.SkippedSegments(); got != 21 {
		t.Errorf("SkippedSegments() = %d, want 21", got)
	}
}

func TestSkipPercent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		total   int
		scanned int
		want    float64
	}{
		{"zero total", 0, 0, 0},
		{"all scanned", 10, 10, 0},
		{"half skipped", 10, 5, 50},
		{"90% skipped", 100, 10, 90},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &QueryStats{TotalSegments: tt.total, ScannedSegments: tt.scanned}
			got := s.SkipPercent()
			if got != tt.want {
				t.Errorf("SkipPercent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBytesPerSec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bytes    int64
		duration time.Duration
		want     float64
	}{
		{"zero duration", 1_000_000, 0, 0},
		{"zero bytes", 0, time.Second, 0},
		{"10MB in 1s", 10_000_000, time.Second, 10_000_000},
		{"5MB in 500ms", 5_000_000, 500 * time.Millisecond, 10_000_000},
		{"both zero", 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := &QueryStats{ProcessedBytes: tt.bytes, ExecDuration: tt.duration}
			got := s.BytesPerSec()
			if got != tt.want {
				t.Errorf("BytesPerSec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEventsPerSec(t *testing.T) {
	t.Parallel()

	s := &QueryStats{MatchedRows: 500_000, ExecDuration: time.Second}
	got := s.EventsPerSec()
	if got != 500_000 {
		t.Errorf("EventsPerSec() = %v, want 500000", got)
	}

	s.ExecDuration = 0
	if s.EventsPerSec() != 0 {
		t.Errorf("EventsPerSec() with zero duration = %v, want 0", s.EventsPerSec())
	}
}

// FromSearchStats tests

func TestFromSearchStats_ScanType_MetadataOnly(t *testing.T) {
	t.Parallel()

	st := FromSearchStats(
		0,                           // rowsScanned
		1,                           // rowsReturned
		10.0,                        // elapsedMS
		100, 0, 0, 0, 0, 0, 0, 0, 0, // segments, skips, errors, memtable, invIdx
		0,                          // invertedIndexHits
		true,                       // countStarOptimized
		false,                      // partialAggUsed
		false, false, false, false, // topK, prefetch, vectorized, dict
		"",   // joinStrategy
		0, 0, // scanMS, pipelineMS
		nil, // indexesUsed
		"",  // acceleratedBy
	)

	if st.ScanType != "metadata_only" {
		t.Errorf("ScanType = %q, want %q", st.ScanType, "metadata_only")
	}
	if !st.CountStarOptimized {
		t.Error("CountStarOptimized should be true")
	}
	if st.ResultRows != 1 {
		t.Errorf("ResultRows = %d, want 1", st.ResultRows)
	}
}

func TestFromSearchStats_ScanType_IndexScan(t *testing.T) {
	t.Parallel()

	st := FromSearchStats(
		50000, // rowsScanned
		100,   // rowsReturned
		250.0, // elapsedMS
		200,   // segmentsTotal
		50,    // segmentsScanned
		0,     // segmentsSkippedIdx
		100,   // segmentsSkippedTime
		0,     // segmentsSkippedStat
		50,    // segmentsSkippedBF
		0,     // segmentsSkippedRange
		0,     // segmentsErrored
		0,     // memtableEvents
		42,    // invertedIndexHits
		false, // countStarOptimized
		true,  // partialAggUsed
		false, false, false, false,
		"",
		150.0, 100.0,
		[]string{"main"},
		"", // acceleratedBy
	)

	if st.ScanType != "index_scan" {
		t.Errorf("ScanType = %q, want %q", st.ScanType, "index_scan")
	}
	if st.InvertedIndexHits != 42 {
		t.Errorf("InvertedIndexHits = %d, want 42", st.InvertedIndexHits)
	}
	if !st.PartialAggUsed {
		t.Error("PartialAggUsed should be true")
	}
	if st.ScannedRows != 50000 {
		t.Errorf("ScannedRows = %d, want 50000", st.ScannedRows)
	}
	if st.ResultRows != 100 {
		t.Errorf("ResultRows = %d, want 100", st.ResultRows)
	}
	if len(st.IndexesUsed) != 1 || st.IndexesUsed[0] != "main" {
		t.Errorf("IndexesUsed = %v, want [main]", st.IndexesUsed)
	}
}

func TestFromSearchStats_ScanType_FilteredScan(t *testing.T) {
	t.Parallel()

	st := FromSearchStats(
		10000, 500, 100.0,
		100, 20, 0, 30, 0, 50, 0, 0, 0,
		0,     // invertedIndexHits = 0 → not index_scan
		false, // countStarOptimized = false
		false, false, false, false, false,
		"", 0, 0, nil, "",
	)

	if st.ScanType != "filtered_scan" {
		t.Errorf("ScanType = %q, want %q", st.ScanType, "filtered_scan")
	}
}

func TestFromSearchStats_ScanType_FullScan(t *testing.T) {
	t.Parallel()

	st := FromSearchStats(
		100000, 1000, 500.0,
		50, 50, 0, 0, 0, 0, 0, 0, 0,
		0, false, false, false, false, false, false,
		"", 0, 0, nil, "",
	)

	if st.ScanType != "full_scan" {
		t.Errorf("ScanType = %q, want %q", st.ScanType, "full_scan")
	}
}

func TestFromSearchStats_ScanType_Default(t *testing.T) {
	t.Parallel()

	// Zero segments → falls through to default "full_scan".
	st := FromSearchStats(
		0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, false, false, false, false, false, false,
		"", 0, 0, nil, "",
	)

	if st.ScanType != "full_scan" {
		t.Errorf("ScanType = %q, want %q", st.ScanType, "full_scan")
	}
}

func TestFromSearchStats_TotalRowsEstimation(t *testing.T) {
	t.Parallel()

	// 10 segments scanned, 100K rows scanned → avg 10K rows/seg.
	// 20 segments skipped (5 bloom + 10 time + 5 stat) → estimate 200K skipped.
	// Total = 100K scanned + 200K estimated = 300K.
	st := FromSearchStats(
		100000, 500, 200.0,
		30, 10, 0, 10, 5, 5, 0, 0, 0,
		0, false, false, false, false, false, false,
		"", 0, 0, nil, "",
	)

	// avgRowsPerSegment = 100000/10 = 10000
	// totalSkipped = 0 + 10 + 5 + 5 + 0 = 20
	// TotalRowsInRange = 100000 + 20*10000 = 300000
	if st.TotalRowsInRange != 300000 {
		t.Errorf("TotalRowsInRange = %d, want 300000", st.TotalRowsInRange)
	}
}

func TestFromSearchStats_TotalRowsEstimation_NoScannedSegments(t *testing.T) {
	t.Parallel()

	// segmentsScanned=0 → no estimation, TotalRowsInRange = rowsScanned.
	st := FromSearchStats(
		5000, 100, 50.0,
		10, 0, 0, 10, 0, 0, 0, 0, 0,
		0, false, false, false, false, false, false,
		"", 0, 0, nil, "",
	)

	if st.TotalRowsInRange != 5000 {
		t.Errorf("TotalRowsInRange = %d, want 5000 (no estimation when segmentsScanned=0)", st.TotalRowsInRange)
	}
}

func TestFromSearchStats_ExecDuration(t *testing.T) {
	t.Parallel()

	st := FromSearchStats(
		0, 0, 123.456,
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, false, false, false, false, false, false,
		"", 0, 0, nil, "",
	)

	// 123.456ms → 123.456 * 1e6 ns = 123456000 ns
	wantDur := time.Duration(123.456 * float64(time.Millisecond))
	if st.ExecDuration != wantDur {
		t.Errorf("ExecDuration = %v, want %v", st.ExecDuration, wantDur)
	}
}

func TestFromSearchStats_AllOptimizations(t *testing.T) {
	t.Parallel()

	st := FromSearchStats(
		0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		0,
		true, // countStar
		true, // partialAgg
		true, // topK
		true, // prefetch
		true, // vectorized
		true, // dict
		"hash_join",
		0, 0, nil, "",
	)

	if !st.CountStarOptimized {
		t.Error("CountStarOptimized should be true")
	}
	if !st.PartialAggUsed {
		t.Error("PartialAggUsed should be true")
	}
	if !st.TopKUsed {
		t.Error("TopKUsed should be true")
	}
	if !st.PrefetchUsed {
		t.Error("PrefetchUsed should be true")
	}
	if !st.VectorizedFilterUsed {
		t.Error("VectorizedFilterUsed should be true")
	}
	if !st.DictFilterUsed {
		t.Error("DictFilterUsed should be true")
	}
	if st.JoinStrategy != "hash_join" {
		t.Errorf("JoinStrategy = %q, want %q", st.JoinStrategy, "hash_join")
	}
}

func TestFromSearchStats_SegmentFields(t *testing.T) {
	t.Parallel()

	st := FromSearchStats(
		50000, 200, 100.0,
		200,   // segmentsTotal
		50,    // segmentsScanned
		10,    // segmentsSkippedIdx
		80,    // segmentsSkippedTime
		20,    // segmentsSkippedStat
		30,    // segmentsSkippedBF
		10,    // segmentsSkippedRange
		3,     // segmentsErrored
		15000, // memtableEvents
		5,     // invertedIndexHits
		false, false, false, false, false, false,
		"", 0, 0, nil, "",
	)

	if st.TotalSegments != 200 {
		t.Errorf("TotalSegments = %d, want 200", st.TotalSegments)
	}
	if st.ScannedSegments != 50 {
		t.Errorf("ScannedSegments = %d, want 50", st.ScannedSegments)
	}
	if st.IndexSkippedSegments != 10 {
		t.Errorf("IndexSkippedSegments = %d, want 10", st.IndexSkippedSegments)
	}
	if st.TimeSkippedSegments != 80 {
		t.Errorf("TimeSkippedSegments = %d, want 80", st.TimeSkippedSegments)
	}
	if st.StatSkippedSegments != 20 {
		t.Errorf("StatSkippedSegments = %d, want 20", st.StatSkippedSegments)
	}
	if st.BloomSkippedSegments != 30 {
		t.Errorf("BloomSkippedSegments = %d, want 30", st.BloomSkippedSegments)
	}
	if st.RangeSkippedSegments != 10 {
		t.Errorf("RangeSkippedSegments = %d, want 10", st.RangeSkippedSegments)
	}
	if st.SegmentsErrored != 3 {
		t.Errorf("SegmentsErrored = %d, want 3", st.SegmentsErrored)
	}
	if st.BufferedRowsScanned != 15000 {
		t.Errorf("BufferedRowsScanned = %d, want 15000", st.BufferedRowsScanned)
	}
}

// FromMeta tests

func TestFromMeta_BasicFields(t *testing.T) {
	t.Parallel()

	st := FromMeta(42.5, 10000, 25)

	if st.ScannedRows != 10000 {
		t.Errorf("ScannedRows = %d, want 10000", st.ScannedRows)
	}
	if st.ResultRows != 25 {
		t.Errorf("ResultRows = %d, want 25", st.ResultRows)
	}
	if st.ScanType != "full_scan" {
		t.Errorf("ScanType = %q, want %q", st.ScanType, "full_scan")
	}

	wantDur := time.Duration(42.5 * float64(time.Millisecond))
	if st.ExecDuration != wantDur {
		t.Errorf("ExecDuration = %v, want %v", st.ExecDuration, wantDur)
	}
}

func TestFromMeta_ZeroValues(t *testing.T) {
	t.Parallel()

	st := FromMeta(0, 0, 0)

	if st.ScannedRows != 0 {
		t.Errorf("ScannedRows = %d, want 0", st.ScannedRows)
	}
	if st.ResultRows != 0 {
		t.Errorf("ResultRows = %d, want 0", st.ResultRows)
	}
	if st.ExecDuration != 0 {
		t.Errorf("ExecDuration = %v, want 0", st.ExecDuration)
	}
}

func TestFromMeta_DoesNotSetTotalDuration(t *testing.T) {
	t.Parallel()

	st := FromMeta(100.0, 5000, 10)

	// TotalDuration is set by the caller (includes network round-trip),
	// so FromMeta should leave it at zero.
	if st.TotalDuration != 0 {
		t.Errorf("TotalDuration = %v, want 0 (caller sets it)", st.TotalDuration)
	}
}

// timeDurFromMS tests

func TestTimeDurFromMS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ms   float64
		want time.Duration
	}{
		{"zero", 0, 0},
		{"one millisecond", 1.0, time.Millisecond},
		{"fractional", 0.5, 500 * time.Microsecond},
		{"large", 1500.0, 1500 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := timeDurFromMS(tt.ms)
			if got != tt.want {
				t.Errorf("timeDurFromMS(%v) = %v, want %v", tt.ms, got, tt.want)
			}
		})
	}
}

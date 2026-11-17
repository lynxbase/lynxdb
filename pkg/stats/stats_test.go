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

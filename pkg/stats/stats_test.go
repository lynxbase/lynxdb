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

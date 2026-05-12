package pipeline

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestResolveTimeRangeSignedDurations(t *testing.T) {
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		timeRange    *spl2.SourceTimeRange
		wantEarliest time.Time
		wantLatest   time.Time
	}{
		{
			name:         "negative duration uses past to now",
			timeRange:    &spl2.SourceTimeRange{Relative: "-1h"},
			wantEarliest: now.Add(-time.Hour),
			wantLatest:   now,
		},
		{
			name:         "positive duration uses now to future",
			timeRange:    &spl2.SourceTimeRange{Relative: "+30m"},
			wantEarliest: now,
			wantLatest:   now.Add(30 * time.Minute),
		},
		{
			name:         "signed explicit range",
			timeRange:    &spl2.SourceTimeRange{Relative: "-1h", End: "+30m"},
			wantEarliest: now.Add(-time.Hour),
			wantLatest:   now.Add(30 * time.Minute),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			earliest, latest := resolveTimeRange(tt.timeRange, now)
			if !earliest.Equal(tt.wantEarliest) {
				t.Fatalf("earliest: got %s, want %s", earliest, tt.wantEarliest)
			}
			if !latest.Equal(tt.wantLatest) {
				t.Fatalf("latest: got %s, want %s", latest, tt.wantLatest)
			}
		})
	}
}

func TestResolveTimeRangeSnapVariants(t *testing.T) {
	now := time.Date(2026, 5, 13, 15, 4, 5, 123, time.UTC)

	tests := []struct {
		name         string
		timeRange    *spl2.SourceTimeRange
		wantEarliest time.Time
		wantLatest   time.Time
	}{
		{
			name:         "week snap sunday",
			timeRange:    &spl2.SourceTimeRange{SnapTo: "w0"},
			wantEarliest: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
			wantLatest:   now,
		},
		{
			name:         "week snap monday",
			timeRange:    &spl2.SourceTimeRange{SnapTo: "w1"},
			wantEarliest: time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC),
			wantLatest:   now,
		},
		{
			name:         "duration snap suffix",
			timeRange:    &spl2.SourceTimeRange{Relative: "-1d@d", SnapTo: "d"},
			wantEarliest: time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC),
			wantLatest:   now,
		},
		{
			name:         "range end snap suffix",
			timeRange:    &spl2.SourceTimeRange{Relative: "-7d@d", SnapTo: "d", End: "-1d@d"},
			wantEarliest: time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC),
			wantLatest:   time.Date(2026, 5, 12, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			earliest, latest := resolveTimeRange(tt.timeRange, now)
			if !earliest.Equal(tt.wantEarliest) {
				t.Fatalf("earliest: got %s, want %s", earliest, tt.wantEarliest)
			}
			if !latest.Equal(tt.wantLatest) {
				t.Fatalf("latest: got %s, want %s", latest, tt.wantLatest)
			}
		})
	}
}

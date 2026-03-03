package model

import (
	"testing"
	"time"
)

func TestSegmentMetaTimeRange(t *testing.T) {
	s := SegmentMeta{
		MinTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	d := s.TimeRange()
	if d != time.Hour {
		t.Fatalf("expected 1h, got %v", d)
	}
}

func TestSegmentMetaOverlaps(t *testing.T) {
	s := SegmentMeta{
		MinTime: time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name  string
		start time.Time
		end   time.Time
		want  bool
	}{
		{
			"fully contained",
			time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC),
			true,
		},
		{
			"overlaps start",
			time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 10, 30, 0, 0, time.UTC),
			true,
		},
		{
			"overlaps end",
			time.Date(2024, 1, 1, 11, 30, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
			true,
		},
		{
			"fully encompasses",
			time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
			true,
		},
		{
			"before",
			time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
			false,
		},
		{
			"after",
			time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 14, 0, 0, 0, time.UTC),
			false,
		},
		{
			"exact match",
			time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			true,
		},
		{
			"touches start boundary",
			time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC),
			true,
		},
		{
			"touches end boundary",
			time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := s.Overlaps(tt.start, tt.end)
			if got != tt.want {
				t.Fatalf("Overlaps(%v, %v) = %v, want %v", tt.start, tt.end, got, tt.want)
			}
		})
	}
}

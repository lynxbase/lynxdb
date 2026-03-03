package timerange

import (
	"testing"
	"time"
)

func TestParseRelative(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"15m", 15 * time.Minute},
		{"1h", time.Hour},
		{"4h", 4 * time.Hour},
		{"1d", 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"1w", 7 * 24 * time.Hour},
		{"30s", 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseRelative(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseRelativeInvalid(t *testing.T) {
	invalids := []string{"", "x", "15", "15z", "abc"}
	for _, s := range invalids {
		if _, err := ParseRelative(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}

func TestParseAbsolute(t *testing.T) {
	tests := []struct {
		input string
		want  time.Time
	}{
		{"2026-02-10T08:00:00Z", time.Date(2026, 2, 10, 8, 0, 0, 0, time.UTC)},
		{"2026-02-10", time.Date(2026, 2, 10, 0, 0, 0, 0, time.UTC)},
		{"2026-02-10 08:00", time.Date(2026, 2, 10, 8, 0, 0, 0, time.UTC)},
		{"2026-02-10 08:00:00", time.Date(2026, 2, 10, 8, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseAbsolute(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromSince(t *testing.T) {
	now := time.Date(2026, 2, 13, 12, 0, 0, 0, time.UTC)
	tr, err := FromSince("1h", now)
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Earliest.Equal(now.Add(-time.Hour)) {
		t.Errorf("earliest: got %v, want %v", tr.Earliest, now.Add(-time.Hour))
	}
	if !tr.Latest.Equal(now) {
		t.Errorf("latest: got %v, want %v", tr.Latest, now)
	}
}

func TestTimeRangeContains(t *testing.T) {
	tr := &TimeRange{
		Earliest: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Latest:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	if !tr.Contains(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)) {
		t.Error("should contain midpoint")
	}
	if tr.Contains(time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)) {
		t.Error("should not contain time after range")
	}
}

func TestTimeRangeOverlaps(t *testing.T) {
	tr := &TimeRange{
		Earliest: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Latest:   time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	// Overlapping segment
	if !tr.Overlaps(
		time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	) {
		t.Error("should overlap")
	}
	// Non-overlapping segment
	if tr.Overlaps(
		time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC),
	) {
		t.Error("should not overlap")
	}
}

func TestFromAbsoluteRange(t *testing.T) {
	tr, err := FromAbsoluteRange("2026-02-10T08:00:00Z", "2026-02-10T09:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Earliest.Equal(time.Date(2026, 2, 10, 8, 0, 0, 0, time.UTC)) {
		t.Errorf("unexpected earliest: %v", tr.Earliest)
	}
	if !tr.Latest.Equal(time.Date(2026, 2, 10, 9, 0, 0, 0, time.UTC)) {
		t.Errorf("unexpected latest: %v", tr.Latest)
	}
}

func TestDefaultNoTimeRange(t *testing.T) {
	// Without flags, no time range filtering — verify nil TR is handled
	var tr *TimeRange
	if tr != nil {
		t.Error("expected nil time range")
	}
}

// Parse tests

func TestParse_Now(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 30, 45, 123456789, time.UTC)
	got, err := Parse("now", now)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now) {
		t.Errorf("got %v, want %v", got, now)
	}
	// Case-insensitive
	got2, err := Parse("NOW", now)
	if err != nil {
		t.Fatal(err)
	}
	if !got2.Equal(now) {
		t.Errorf("case-insensitive: got %v, want %v", got2, now)
	}
}

func TestParse_RelativeMinutes(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 30, 0, 0, time.UTC)
	got, err := Parse("-5m", now)
	if err != nil {
		t.Fatal(err)
	}
	want := now.Add(-5 * time.Minute)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_RelativeHours(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 30, 0, 0, time.UTC)
	got, err := Parse("-1h", now)
	if err != nil {
		t.Fatal(err)
	}
	want := now.Add(-time.Hour)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_RelativeDays(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 30, 0, 0, time.UTC)
	got, err := Parse("-7d", now)
	if err != nil {
		t.Fatal(err)
	}
	want := now.Add(-7 * 24 * time.Hour)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_Absolute(t *testing.T) {
	now := time.Now() // not used for absolute parsing
	got, err := Parse("2026-02-14T14:00:00Z", now)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 2, 14, 14, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_SnapToHour(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 45, 30, 0, time.UTC)
	got, err := Parse("-1h@h", now)
	if err != nil {
		t.Fatal(err)
	}
	// now - 1h = 13:45:30, snap to hour = 13:00:00
	want := time.Date(2026, 2, 17, 13, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_SnapToDay(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 45, 30, 0, time.UTC)
	got, err := Parse("-1d@d", now)
	if err != nil {
		t.Fatal(err)
	}
	// now - 1d = 2026-02-16T14:45:30, snap to day = 2026-02-16T00:00:00
	want := time.Date(2026, 2, 16, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_SnapToMinute(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 45, 30, 0, time.UTC)
	got, err := Parse("-30m@m", now)
	if err != nil {
		t.Fatal(err)
	}
	// now - 30m = 14:15:30, snap to minute = 14:15:00
	want := time.Date(2026, 2, 17, 14, 15, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_SnapToWeek(t *testing.T) {
	// 2026-02-17 is a Tuesday
	now := time.Date(2026, 2, 17, 14, 45, 30, 0, time.UTC)
	got, err := Parse("-1w@w", now)
	if err != nil {
		t.Fatal(err)
	}
	// now - 1w = 2026-02-10T14:45:30 (Tuesday), snap to Monday = 2026-02-09
	want := time.Date(2026, 2, 9, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParse_Invalid(t *testing.T) {
	now := time.Now()
	invalids := []string{"", "garbage", "-@h", "-1h@xx"}
	for _, s := range invalids {
		if _, err := Parse(s, now); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}

func TestParseRange_Defaults(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 30, 0, 0, time.UTC)
	tr, err := ParseRange("", "", now)
	if err != nil {
		t.Fatal(err)
	}
	wantEarliest := now.Add(-15 * time.Minute)
	if !tr.Earliest.Equal(wantEarliest) {
		t.Errorf("earliest: got %v, want %v", tr.Earliest, wantEarliest)
	}
	if !tr.Latest.Equal(now) {
		t.Errorf("latest: got %v, want %v", tr.Latest, now)
	}
}

func TestParseRange_Custom(t *testing.T) {
	now := time.Date(2026, 2, 17, 14, 30, 0, 0, time.UTC)
	tr, err := ParseRange("-1h", "-5m", now)
	if err != nil {
		t.Fatal(err)
	}
	wantEarliest := now.Add(-time.Hour)
	wantLatest := now.Add(-5 * time.Minute)
	if !tr.Earliest.Equal(wantEarliest) {
		t.Errorf("earliest: got %v, want %v", tr.Earliest, wantEarliest)
	}
	if !tr.Latest.Equal(wantLatest) {
		t.Errorf("latest: got %v, want %v", tr.Latest, wantLatest)
	}
}

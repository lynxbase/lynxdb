package timerange

import (
	"errors"
	"strings"
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

// testNow is a Tuesday.
var testNow = time.Date(2026, 11, 24, 14, 45, 30, 500, time.UTC)

func TestParse(t *testing.T) {
	day := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		input string
		want  time.Time
	}{
		{"now", testNow},
		{"NOW", testNow},
		{"  now  ", testNow},
		{"-30s", testNow.Add(-30 * time.Second)},
		{"-15m", testNow.Add(-15 * time.Minute)},
		{"-1h", testNow.Add(-time.Hour)},
		{"-7d", testNow.Add(-7 * 24 * time.Hour)},
		{"-2w", testNow.Add(-14 * 24 * time.Hour)},
		{"-0s", testNow},
		{"-007m", testNow.Add(-7 * time.Minute)},
		{"-1H", testNow.Add(-time.Hour)},
		{"-15M", testNow.Add(-15 * time.Minute)},
		{" -1h ", testNow.Add(-time.Hour)},
		{"-1d@d", day.AddDate(0, 0, -1)},
		{"-1D@D", day.AddDate(0, 0, -1)},
		{"-0d@d", day},
		{"-1h@h", time.Date(2026, 11, 24, 13, 0, 0, 0, time.UTC)},
		{"-30m@m", time.Date(2026, 11, 24, 14, 15, 0, 0, time.UTC)},
		{"-10s@s", time.Date(2026, 11, 24, 14, 45, 20, 0, time.UTC)},
		{"-1w@w", time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)},
		{"-2d@w", time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)},
		{"@s", time.Date(2026, 11, 24, 14, 45, 30, 0, time.UTC)},
		{"@m", time.Date(2026, 11, 24, 14, 45, 0, 0, time.UTC)},
		{"@h", time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)},
		{"@d", day},
		{"@D", day},
		{"@w", time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC)},
		{" @d ", day},
		{"2026-11-24", day},
		{"2026-11-24T09:00:00Z", time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)},
		{"2026-11-24T09:00:00.250Z", time.Date(2026, 11, 24, 9, 0, 0, 250e6, time.UTC)},
		{"2026-11-24T09:00:00+02:00", time.Date(2026, 11, 24, 7, 0, 0, 0, time.UTC)},
		{"2026-11-24T09:00:00", time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)},
		{"2026-11-24 09:00:00", time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)},
		{"2026-11-24 09:00", time.Date(2026, 11, 24, 9, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input, testNow)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// TestParseSnapOfSunday pins that a week starts on the Monday before a
// Sunday.
func TestParseSnapOfSunday(t *testing.T) {
	sunday := time.Date(2026, 11, 29, 23, 0, 0, 0, time.UTC)
	got, err := Parse("@w", sunday)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 23, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseRejects(t *testing.T) {
	for _, s := range []string{
		"", "   ", "-", "@", "-h", "-1", "1h", "15m", "1d@d",
		"-1x", "-1y", "-1mon", "-1q", "-1h@z", "-1h@xx", "-1h@", "@x", "@dd", "@1d",
		"--5m", "-+5m", "+1h", "- 1h", "-1 h", "-1.5h",
		"-9999999999d", "-106752d", "-99999999999999999999s",
		"1h30m", "-1h30m", "-1d@d-1h", "-1d@d@h",
		"yesterday", "today", "now@d", "now-1h",
		"2026-13-01", "2026-11-24T25:00:00Z", "2026-11-24t09:00:00z",
	} {
		_, err := Parse(s, testNow)
		var pe *ParseError
		if !errors.As(err, &pe) {
			t.Errorf("Parse(%q) error = %v, want a *ParseError", s, err)
		}
	}
}

// TestParseLongestOffset pins the longest offset in days that a duration
// holds.
func TestParseLongestOffset(t *testing.T) {
	got, err := Parse("-106751d", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if want := testNow.Add(-106751 * 24 * time.Hour); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseErrorText(t *testing.T) {
	_, errTime := Parse(" Banana-1H ", testNow)
	_, errFrom := ParseOptionalRange("banana", "now", testNow)
	_, errTo := ParseOptionalRange("-1h", "-1h@z", testNow)
	_, errRange := ParseRange("--5m", "now", testNow)
	for _, c := range []struct {
		err  error
		want string
	}{
		{errTime, `invalid time " Banana-1H ": `},
		{errFrom, `invalid from "banana": `},
		{errTo, `invalid to "-1h@z": `},
		{errRange, `invalid from "--5m": `},
	} {
		if c.err == nil {
			t.Fatalf("want an error that starts with %q", c.want)
		}
		msg := c.err.Error()
		if !strings.HasPrefix(msg, c.want) {
			t.Errorf("error %q does not start with %q", msg, c.want)
		}
		if strings.Count(msg, `"`) != 2 {
			t.Errorf("error %q quotes more than the value", msg)
		}
		for _, form := range []string{"now", "-15m", "-1h", "-7d", "-2w", "-1d@d", "@d", "2026-11-24", "2026-11-24T09:00:00Z"} {
			if !strings.Contains(msg, form) {
				t.Errorf("error %q lacks the form %s", msg, form)
			}
		}
		if strings.Contains(msg, "strconv") {
			t.Errorf("error %q holds a strconv error", msg)
		}
	}
}

func TestParseOptionalRange(t *testing.T) {
	tr, err := ParseOptionalRange("", "@d", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if !tr.Earliest.IsZero() || !tr.Latest.Equal(time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("got %+v, want an open start and the start of the day", tr)
	}
	if tr, err := ParseOptionalRange("now", "now", testNow); err != nil || !tr.Earliest.Equal(tr.Latest) {
		t.Errorf("equal bounds: got %+v, %v, want an empty range", tr, err)
	}
	if _, err := ParseOptionalRange("now", "-1s", testNow); err == nil {
		t.Error("from after to: want an error")
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

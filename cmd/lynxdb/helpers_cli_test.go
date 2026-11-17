package main

import (
	"testing"
	"time"
)

func TestFormatCount_Zero(t *testing.T) {
	if got := formatCount(0); got != "0" {
		t.Errorf("formatCount(0) = %q, want %q", got, "0")
	}
}

func TestFormatCount_SmallNumber(t *testing.T) {
	if got := formatCount(999); got != "999" {
		t.Errorf("formatCount(999) = %q, want %q", got, "999")
	}
}

func TestFormatCount_Thousands(t *testing.T) {
	if got := formatCount(1000); got != "1,000" {
		t.Errorf("formatCount(1000) = %q, want %q", got, "1,000")
	}
}

func TestFormatCount_Millions(t *testing.T) {
	if got := formatCount(1000000); got != "1,000,000" {
		t.Errorf("formatCount(1000000) = %q, want %q", got, "1,000,000")
	}
}

func TestFormatCount_LargeNumber(t *testing.T) {
	if got := formatCount(1234567); got != "1,234,567" {
		t.Errorf("formatCount(1234567) = %q, want %q", got, "1,234,567")
	}
}

func TestFormatCount_Negative(t *testing.T) {
	if got := formatCount(-1234); got != "-1,234" {
		t.Errorf("formatCount(-1234) = %q, want %q", got, "-1,234")
	}
}

func TestFormatCount_Billion(t *testing.T) {
	if got := formatCount(1_234_567_890); got != "1,234,567,890" {
		t.Errorf("formatCount(1234567890) = %q, want %q", got, "1,234,567,890")
	}
}

func TestFormatCount_TenBillion(t *testing.T) {
	if got := formatCount(10_000_000_000); got != "10,000,000,000" {
		t.Errorf("formatCount(10000000000) = %q, want %q", got, "10,000,000,000")
	}
}

func TestFormatCount_Trillion(t *testing.T) {
	if got := formatCount(1_234_567_890_123); got != "1,234,567,890,123" {
		t.Errorf("formatCount(1234567890123) = %q, want %q", got, "1,234,567,890,123")
	}
}

func TestFormatBytes_Zero(t *testing.T) {
	if got := formatBytes(0); got != "0 B" {
		t.Errorf("formatBytes(0) = %q, want %q", got, "0 B")
	}
}

func TestFormatBytes_Bytes(t *testing.T) {
	if got := formatBytes(512); got != "512 B" {
		t.Errorf("formatBytes(512) = %q, want %q", got, "512 B")
	}
}

func TestFormatBytes_Kilobytes(t *testing.T) {
	if got := formatBytes(1024); got != "1.0 KB" {
		t.Errorf("formatBytes(1024) = %q, want %q", got, "1.0 KB")
	}
}

func TestFormatBytes_Megabytes(t *testing.T) {
	if got := formatBytes(1048576); got != "1.0 MB" {
		t.Errorf("formatBytes(1048576) = %q, want %q", got, "1.0 MB")
	}
}

func TestFormatBytes_Gigabytes(t *testing.T) {
	if got := formatBytes(1073741824); got != "1.0 GB" {
		t.Errorf("formatBytes(1073741824) = %q, want %q", got, "1.0 GB")
	}
}

func TestFormatDuration_ZeroSeconds(t *testing.T) {
	if got := formatDuration(0); got != "0m" {
		t.Errorf("formatDuration(0) = %q, want %q", got, "0m")
	}
}

func TestFormatDuration_LessThanMinute(t *testing.T) {
	if got := formatDuration(30); got != "0m" {
		t.Errorf("formatDuration(30) = %q, want %q", got, "0m")
	}
}

func TestFormatDuration_OneMinute(t *testing.T) {
	if got := formatDuration(60); got != "1m" {
		t.Errorf("formatDuration(60) = %q, want %q", got, "1m")
	}
}

func TestFormatDuration_HoursAndMinutes(t *testing.T) {
	if got := formatDuration(3700); got != "1h 1m" {
		t.Errorf("formatDuration(3700) = %q, want %q", got, "1h 1m")
	}
}

func TestFormatDuration_DaysHoursMinutes(t *testing.T) {
	got := formatDuration(90000) // 1d 1h 0m
	if got != "1d 1h 0m" {
		t.Errorf("formatDuration(90000) = %q, want %q", got, "1d 1h 0m")
	}
}

func TestFormatElapsed_SubSecond(t *testing.T) {
	d := 250 * time.Millisecond
	got := formatElapsed(d)
	if got != "250ms" {
		t.Errorf("formatElapsed(250ms) = %q, want %q", got, "250ms")
	}
}

func TestFormatElapsed_Seconds(t *testing.T) {
	d := 1500 * time.Millisecond
	got := formatElapsed(d)
	if got != "1.5s" {
		t.Errorf("formatElapsed(1.5s) = %q, want %q", got, "1.5s")
	}
}

func TestFormatElapsed_MinutesAndSeconds(t *testing.T) {
	d := 90 * time.Second
	got := formatElapsed(d)
	if got != "1m 30s" {
		t.Errorf("formatElapsed(90s) = %q, want %q", got, "1m 30s")
	}
}

func TestTruncateStr_ShortString(t *testing.T) {
	if got := truncateStr("hello", 10); got != "hello" {
		t.Errorf("truncateStr short = %q, want %q", got, "hello")
	}
}

func TestTruncateStr_ExactLength(t *testing.T) {
	if got := truncateStr("hello", 5); got != "hello" {
		t.Errorf("truncateStr exact = %q, want %q", got, "hello")
	}
}

func TestTruncateStr_LongString(t *testing.T) {
	got := truncateStr("hello world", 8)
	if got != "hello..." {
		t.Errorf("truncateStr long = %q, want %q", got, "hello...")
	}
}

func TestTruncateStr_VeryShortMax(t *testing.T) {
	got := truncateStr("hello", 3)
	if got != "hel" {
		t.Errorf("truncateStr tiny max = %q, want %q", got, "hel")
	}
}

func TestTruncateStr_Unicode(t *testing.T) {
	// "сервис-api" is 10 runes. Truncating to 8 should give 5 runes + "...".
	got := truncateStr("сервис-api", 8)
	want := "серви..."
	if got != want {
		t.Errorf("truncateStr unicode = %q, want %q", got, want)
	}
}

func TestTruncateStr_UnicodeNoTruncation(t *testing.T) {
	// Exactly 10 runes, maxLen=10 — no truncation.
	got := truncateStr("сервис-api", 10)
	want := "сервис-api"
	if got != want {
		t.Errorf("truncateStr unicode exact = %q, want %q", got, want)
	}
}

func TestSuggestWiderTimeRange_KnownRanges(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"5m", "1h"},
		{"1h", "6h"},
		{"24h", "7d"},
		{"7d", "30d"},
		{"30d", ""},
	}

	for _, tt := range tests {
		got := suggestWiderTimeRange(tt.input)
		if got != tt.want {
			t.Errorf("suggestWiderTimeRange(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestExtractFilterFields_SimpleFilter(t *testing.T) {
	got := extractFilterFields("search source=nginx level=warn host=web-01")
	if got["level"] != "warn" {
		t.Errorf("expected level=warn, got %v", got)
	}

	if got["host"] != "web-01" {
		t.Errorf("expected host=web-01, got %v", got)
	}

	if _, ok := got["source"]; ok {
		t.Errorf("source=<index> is not a filter field, got %v", got)
	}
}

func TestExtractFilterFields_QuotedValues(t *testing.T) {
	got := extractFilterFields(`level="warn"`)
	if got["level"] != "warn" {
		t.Errorf("expected level=warn (quotes stripped), got %v", got)
	}
}

func TestExtractFilterFields_ComparisonOperators(t *testing.T) {
	got := extractFilterFields("status>=500")
	if got["status"] != "500" {
		t.Errorf("expected status=500, got %v", got)
	}
}

func TestExtractFilterFields_SkipsPipes(t *testing.T) {
	got := extractFilterFields("| stats count()")
	if len(got) != 0 {
		t.Errorf("expected empty map for pipe command, got %v", got)
	}
}

func TestLevenshteinDistance_SameStrings(t *testing.T) {
	if got := levenshteinDistance("hello", "hello"); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}

func TestLevenshteinDistance_OneEdit(t *testing.T) {
	if got := levenshteinDistance("hello", "hallo"); got != 1 {
		t.Errorf("expected 1, got %d", got)
	}
}

func TestLevenshteinDistance_EmptyStrings(t *testing.T) {
	if got := levenshteinDistance("", "abc"); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}

	if got := levenshteinDistance("abc", ""); got != 3 {
		t.Errorf("expected 3, got %d", got)
	}
}

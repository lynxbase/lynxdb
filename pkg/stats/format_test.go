package stats

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestFormatTTY_Quiet(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    3,
		TotalDuration: 165 * time.Millisecond,
		ScannedRows:   1000,
	}
	FormatTTY(&buf, s, false, true)
	out := buf.String()

	if !strings.Contains(out, "3 results") {
		t.Errorf("quiet output missing result count: %q", out)
	}
	if !strings.Contains(out, "165ms") {
		t.Errorf("quiet output missing duration: %q", out)
	}
	// The caller prints the separator.
	if strings.Contains(out, "───") {
		t.Errorf("FormatTTY should not print separator (caller responsibility): %q", out)
	}
	if strings.Contains(out, "scanned") {
		t.Errorf("quiet output should not contain scanned rows: %q", out)
	}
}

func TestFormatTTY_SingleResult(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    1,
		TotalDuration: 10 * time.Millisecond,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "1 result") {
		t.Errorf("should show singular 'result': %q", out)
	}
	// Should not say "1 results".
	if strings.Contains(out, "1 results") {
		t.Errorf("should not pluralize for count=1: %q", out)
	}
}

func TestFormatTTY_ScannedRows(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    3,
		TotalDuration: 165 * time.Millisecond,
		ExecDuration:  150 * time.Millisecond,
		ScannedRows:   142_847,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	expectations := []string{
		"3 results",
		"165ms",
		"143K scanned",
		"funnel",
		"142,847 scanned → 3 results",
	}
	for _, exp := range expectations {
		if !strings.Contains(out, exp) {
			t.Errorf("output missing %q:\n%s", exp, out)
		}
	}
}

func TestFormatTTY_Verbose_TimingBreakdown(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    3,
		TotalDuration: 165 * time.Millisecond,
		ParseDuration: 2 * time.Millisecond,
		ExecDuration:  150 * time.Millisecond,
		ScannedRows:   142_847,
	}
	FormatTTY(&buf, s, true, false)
	out := buf.String()

	for _, exp := range []string{"timing", "parse: 2ms", "exec: 150ms"} {
		if !strings.Contains(out, exp) {
			t.Errorf("verbose output missing %q: %q", exp, out)
		}
	}
}

func TestFormatJSON_Basic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    3,
		TotalDuration: 165 * time.Millisecond,
		ParseDuration: 2 * time.Millisecond,
		ExecDuration:  150 * time.Millisecond,
		ScannedRows:   142_847,
	}
	if err := FormatJSON(&buf, s); err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("invalid JSON: %v\nraw: %s", err, buf.String())
	}
	statsRaw, ok := envelope["stats"]
	if !ok {
		t.Fatalf("missing 'stats' key in JSON: %s", buf.String())
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(statsRaw, &stats); err != nil {
		t.Fatalf("invalid stats JSON: %v", err)
	}

	want := "exec_ms,parse_ms,result_rows,scan_rate_per_sec,scanned_rows,total_ms"
	if got := strings.Join(slices.Sorted(maps.Keys(stats)), ","); got != want {
		t.Errorf("keys = %s, want %s", got, want)
	}

	if v, ok := stats["total_ms"].(float64); !ok || v != 165 {
		t.Errorf("total_ms = %v, want 165", stats["total_ms"])
	}
	if v, ok := stats["parse_ms"].(float64); !ok || v != 2 {
		t.Errorf("parse_ms = %v, want 2", stats["parse_ms"])
	}
	if v, ok := stats["exec_ms"].(float64); !ok || v != 150 {
		t.Errorf("exec_ms = %v, want 150", stats["exec_ms"])
	}
	if v, ok := stats["scanned_rows"].(float64); !ok || v != 142_847 {
		t.Errorf("scanned_rows = %v, want 142847", stats["scanned_rows"])
	}
	if v, ok := stats["result_rows"].(float64); !ok || v != 3 {
		t.Errorf("result_rows = %v, want 3", stats["result_rows"])
	}
}

func TestFormatJSON_OmitsZeroOptionals(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    0,
		TotalDuration: 1 * time.Millisecond,
	}
	if err := FormatJSON(&buf, s); err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}

	raw := buf.String()
	for _, key := range []string{"parse_ms", "exec_ms", "scan_rate_per_sec"} {
		if strings.Contains(raw, key) {
			t.Errorf("should omit %s when zero: %s", key, raw)
		}
	}
}

func TestFormatHumanInt64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1_000, "1.0K"},
		{9_999, "10.0K"},
		{10_000, "10K"},
		{142_847, "143K"},
		{1_000_000, "1.0M"},
		{1_500_000, "1.5M"},
		{1_000_000_000, "1.0G"},
		{-5, "-5"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			got := formatHumanInt64(tt.n)
			if got != tt.want {
				t.Errorf("formatHumanInt64(%d) = %q, want %q", tt.n, got, tt.want)
			}
		})
	}
}

func TestFormatDur(t *testing.T) {
	t.Parallel()

	tests := []struct {
		d    time.Duration
		want string
	}{
		{500 * time.Nanosecond, "500ns"},
		{50 * time.Microsecond, "50µs"},
		{165 * time.Millisecond, "165ms"},
		{1500 * time.Millisecond, "1.5s"},
		{90 * time.Second, "1.5m"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			got := formatDur(tt.d)
			if got != tt.want {
				t.Errorf("formatDur(%v) = %q, want %q", tt.d, got, tt.want)
			}
		})
	}
}

func TestFormatTTY_NoThroughputInDefault(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    1,
		TotalDuration: 1100 * time.Millisecond,
		ExecDuration:  1000 * time.Millisecond,
		ScannedRows:   37_000,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "37K scanned") {
		t.Errorf("should show scanned count: %q", out)
	}
	if strings.Contains(out, "rows/s") {
		t.Errorf("default mode should NOT show throughput: %q", out)
	}
}

func TestFormatTTY_Verbose_Throughput(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    1,
		TotalDuration: 1100 * time.Millisecond,
		ExecDuration:  1000 * time.Millisecond,
		ScannedRows:   37_000,
	}
	FormatTTY(&buf, s, true, false) // verbose=true
	out := buf.String()

	if !strings.Contains(out, "throughput") {
		t.Errorf("verbose should show throughput line: %q", out)
	}
	if !strings.Contains(out, "37K rows/s") {
		t.Errorf("verbose should show rows/s: %q", out)
	}
}

func TestFormatTTY_NoSeparator(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    1,
		TotalDuration: 5 * time.Millisecond,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	// The caller prints the separator.
	if strings.Contains(out, "───") {
		t.Errorf("FormatTTY should not print separator: %q", out)
	}
}

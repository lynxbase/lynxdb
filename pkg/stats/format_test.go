package stats

import (
	"bytes"
	"encoding/json"
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
	}
	FormatTTY(&buf, s, false, true)
	out := buf.String()

	if !strings.Contains(out, "3 results") {
		t.Errorf("quiet output missing result count: %q", out)
	}
	if !strings.Contains(out, "165ms") {
		t.Errorf("quiet output missing duration: %q", out)
	}
	// FormatTTY no longer prints a separator — the caller does.
	if strings.Contains(out, "───") {
		t.Errorf("FormatTTY should not print separator (caller responsibility): %q", out)
	}
	// Quiet mode should NOT contain segment details.
	if strings.Contains(out, "segments") {
		t.Errorf("quiet output should not contain segments: %q", out)
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

func TestFormatTTY_WithSegments(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:           3,
		TotalDuration:        165 * time.Millisecond,
		ExecDuration:         150 * time.Millisecond,
		ScannedRows:          142_847,
		MatchedRows:          3891,
		TotalSegments:        124,
		ScannedSegments:      8,
		BloomSkippedSegments: 98,
		TimeSkippedSegments:  18,
		ScanType:             "full_scan",
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	expectations := []string{
		"3 results",
		"165ms",
		"143K scanned",
		"8 of 124 scanned",
		"skipped",
		"bloom: 98 skipped",
		"time: 18 skipped",
		"full_scan",
	}
	for _, exp := range expectations {
		if !strings.Contains(out, exp) {
			t.Errorf("output missing %q:\n%s", exp, out)
		}
	}
}

func TestFormatTTY_Ephemeral_NoSegments(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    5,
		TotalDuration: 42 * time.Millisecond,
		ScannedRows:   1000,
		MatchedRows:   5,
		Ephemeral:     true,
		ScanType:      "ephemeral",
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if strings.Contains(out, "segments") {
		t.Errorf("ephemeral mode should not show segments: %q", out)
	}
	if !strings.Contains(out, "1.0K scanned") {
		t.Errorf("should show scanned rows: %q", out)
	}
}

func TestFormatTTY_CacheHit(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    10,
		TotalDuration: 1 * time.Millisecond,
		CacheHit:      true,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "cache hit") {
		t.Errorf("should indicate cache hit: %q", out)
	}
}

func TestFormatTTY_Optimizations(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:         1,
		TotalDuration:      5 * time.Millisecond,
		CountStarOptimized: true,
		PartialAggUsed:     true,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "count(*) metadata") {
		t.Errorf("should show count(*) optimization: %q", out)
	}
	if !strings.Contains(out, "partial aggregation") {
		t.Errorf("should show partial agg: %q", out)
	}
}

func TestFormatTTY_MVAcceleration(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    5,
		TotalDuration: 3 * time.Millisecond,
		AcceleratedBy: "mv_errors_5m",
		MVSpeedup:     "~400x",
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "mv_errors_5m") {
		t.Errorf("should show MV name: %q", out)
	}
	if !strings.Contains(out, "~400x") {
		t.Errorf("should show speedup: %q", out)
	}
}

func TestFormatTTY_Verbose_TimingBreakdown(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:       3,
		TotalDuration:    165 * time.Millisecond,
		ParseDuration:    2 * time.Millisecond,
		OptimizeDuration: 1 * time.Millisecond,
		ExecDuration:     150 * time.Millisecond,
		ScannedRows:      142_847,
		MatchedRows:      3891,
		PeakMemoryBytes:  4_404_019,
		MemAllocBytes:    18_700_000,
	}
	FormatTTY(&buf, s, true, false)
	out := buf.String()

	if !strings.Contains(out, "timing") {
		t.Errorf("verbose should show timing breakdown: %q", out)
	}
	if !strings.Contains(out, "parse:") {
		t.Errorf("verbose should show parse time: %q", out)
	}
	if !strings.Contains(out, "memory") {
		t.Errorf("verbose should show memory: %q", out)
	}
}

func TestFormatTTY_Verbose_PipelineStages(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    3,
		TotalDuration: 165 * time.Millisecond,
		ExecDuration:  150 * time.Millisecond,
		ScannedRows:   142_847,
		MatchedRows:   3891,
		Stages: []StageStats{
			{Name: "Scan", InputRows: 142_847, OutputRows: 142_847, Duration: 112 * time.Millisecond},
			{Name: "Filter", InputRows: 142_847, OutputRows: 3891, Duration: 38 * time.Millisecond},
			{Name: "Head", InputRows: 3891, OutputRows: 3, Duration: 0},
		},
	}
	FormatTTY(&buf, s, true, false)
	out := buf.String()

	if !strings.Contains(out, "Pipeline:") {
		t.Errorf("verbose should show pipeline: %q", out)
	}
	if !strings.Contains(out, "Scan") {
		t.Errorf("verbose should show Scan stage: %q", out)
	}
	if !strings.Contains(out, "Filter") {
		t.Errorf("verbose should show Filter stage: %q", out)
	}
}

func TestFormatTTY_SegmentErrors(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:      10,
		TotalDuration:   100 * time.Millisecond,
		SegmentsErrored: 2,
		TotalSegments:   50,
		ScannedSegments: 48,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "2 segment(s) failed") {
		t.Errorf("should show segment errors: %q", out)
	}
}

func TestFormatJSON_Basic(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:      3,
		TotalDuration:   165 * time.Millisecond,
		ScannedRows:     142_847,
		MatchedRows:     3891,
		TotalSegments:   124,
		ScannedSegments: 8,
		ScanType:        "full_scan",
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

	if v, ok := stats["total_ms"].(float64); !ok || v != 165 {
		t.Errorf("total_ms = %v, want 165", stats["total_ms"])
	}
	if v, ok := stats["scanned_rows"].(float64); !ok || v != 142_847 {
		t.Errorf("scanned_rows = %v, want 142847", stats["scanned_rows"])
	}
	if v, ok := stats["result_rows"].(float64); !ok || v != 3 {
		t.Errorf("result_rows = %v, want 3", stats["result_rows"])
	}
	if v, ok := stats["scan_type"].(string); !ok || v != "full_scan" {
		t.Errorf("scan_type = %v, want full_scan", stats["scan_type"])
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
	// Optional fields with zero values should be omitted.
	if strings.Contains(raw, "bloom_skipped") {
		t.Errorf("should omit bloom_skipped when zero: %s", raw)
	}
	if strings.Contains(raw, "peak_memory_bytes") {
		t.Errorf("should omit peak_memory_bytes when zero: %s", raw)
	}
	if strings.Contains(raw, "accelerated_by") {
		t.Errorf("should omit accelerated_by when empty: %s", raw)
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

func TestFormatBytesHuman(t *testing.T) {
	t.Parallel()

	tests := []struct {
		b    int64
		want string
	}{
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1_048_576, "1.0 MB"},
		{4_404_019, "4.2 MB"},
		{1_073_741_824, "1.0 GB"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			got := formatBytesHuman(tt.b)
			if got != tt.want {
				t.Errorf("formatBytesHuman(%d) = %q, want %q", tt.b, got, tt.want)
			}
		})
	}
}

func TestFormatTTY_NoThroughputInDefault(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:     1,
		TotalDuration:  1100 * time.Millisecond,
		ExecDuration:   1000 * time.Millisecond,
		ScannedRows:    37_000,
		ProcessedBytes: 7_700_000, // ~7.3 MB
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	// Volume: "37K scanned  7.3 MB read"
	if !strings.Contains(out, "37K scanned") {
		t.Errorf("should show scanned count: %q", out)
	}
	if !strings.Contains(out, "7.3 MB read") {
		t.Errorf("should show processed bytes with 'read' suffix: %q", out)
	}
	// Default mode should NOT show throughput in headline.
	if strings.Contains(out, "rows/s") {
		t.Errorf("default mode should NOT show throughput: %q", out)
	}
	if strings.Contains(out, "MB/s") {
		t.Errorf("default mode should NOT show MB/s throughput: %q", out)
	}
}

func TestFormatTTY_Verbose_Throughput(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:     1,
		TotalDuration:  1100 * time.Millisecond,
		ExecDuration:   1000 * time.Millisecond,
		ScannedRows:    37_000,
		ProcessedBytes: 7_700_000,
	}
	FormatTTY(&buf, s, true, false) // verbose=true
	out := buf.String()

	if !strings.Contains(out, "throughput") {
		t.Errorf("verbose should show throughput line: %q", out)
	}
	if !strings.Contains(out, "rows/s") {
		t.Errorf("verbose should show rows/s: %q", out)
	}
}

func TestFormatTTY_Throughput_NoBytesWhenZero(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    5,
		TotalDuration: 100 * time.Millisecond,
		ExecDuration:  90 * time.Millisecond,
		ScannedRows:   1000,
		// ProcessedBytes is zero — should not show MB portion.
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "1.0K scanned") {
		t.Errorf("should show row count: %q", out)
	}
	if strings.Contains(out, "MB") {
		t.Errorf("should not show MB when ProcessedBytes is zero: %q", out)
	}
}

func TestFormatJSON_ProcessedBytes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:     1,
		TotalDuration:  500 * time.Millisecond,
		ExecDuration:   400 * time.Millisecond,
		ScannedRows:    50_000,
		ProcessedBytes: 10_000_000, // 10 MB
	}
	if err := FormatJSON(&buf, s); err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("invalid JSON: %v\nraw: %s", err, buf.String())
	}
	var js map[string]interface{}
	if err := json.Unmarshal(envelope["stats"], &js); err != nil {
		t.Fatalf("invalid stats JSON: %v", err)
	}

	if v, ok := js["processed_bytes"].(float64); !ok || v != 10_000_000 {
		t.Errorf("processed_bytes = %v, want 10000000", js["processed_bytes"])
	}
	if _, ok := js["bytes_per_sec"]; !ok {
		t.Error("should include bytes_per_sec when ProcessedBytes > 0")
	}
}

func TestFormatJSON_OmitsProcessedBytesWhenZero(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    1,
		TotalDuration: 10 * time.Millisecond,
		ScannedRows:   100,
	}
	if err := FormatJSON(&buf, s); err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}

	raw := buf.String()
	if strings.Contains(raw, "processed_bytes") {
		t.Errorf("should omit processed_bytes when zero: %s", raw)
	}
	if strings.Contains(raw, "bytes_per_sec") {
		t.Errorf("should omit bytes_per_sec when zero: %s", raw)
	}
}

func TestFormatTTY_MVAcceleration_WithDetails(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:        5,
		TotalDuration:     3 * time.Millisecond,
		ScannedRows:       142_000,
		AcceleratedBy:     "mv_errors_5m",
		MVSpeedup:         "~400x",
		MVStatus:          "active",
		MVOriginalScan:    12_400_000,
		MVCoveragePercent: 100,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "mv_errors_5m") {
		t.Errorf("should show MV name: %q", out)
	}
	if !strings.Contains(out, "active") {
		t.Errorf("should show MV status: %q", out)
	}
	if !strings.Contains(out, "12.4M events") {
		t.Errorf("should show original scan estimate: %q", out)
	}
	if !strings.Contains(out, "~400x speedup") {
		t.Errorf("should show speedup in headline or detail: %q", out)
	}
}

func TestFormatTTY_MVAcceleration_PartialCoverage(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:        5,
		TotalDuration:     10 * time.Millisecond,
		ScannedRows:       1000,
		AcceleratedBy:     "mv_hourly",
		MVStatus:          "backfilling",
		MVCoveragePercent: 73,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "73%") {
		t.Errorf("should show coverage percent: %q", out)
	}
}

func TestFormatTTY_IOLine(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    10,
		TotalDuration: 100 * time.Millisecond,
		ScannedRows:   50_000,
		DiskBytesRead: 12_582_912, // 12 MB
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "io") {
		t.Errorf("should show io line: %q", out)
	}
	if !strings.Contains(out, "12.0 MB") {
		t.Errorf("should show disk bytes: %q", out)
	}
}

func TestFormatTTY_IOLine_WithS3(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    10,
		TotalDuration: 500 * time.Millisecond,
		ScannedRows:   100_000,
		DiskBytesRead: 5_242_880,
		S3BytesRead:   10_485_760,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if !strings.Contains(out, "s3:") {
		t.Errorf("should show s3 line: %q", out)
	}
	if !strings.Contains(out, "10.0 MB") {
		t.Errorf("should show S3 bytes: %q", out)
	}
}

func TestFormatTTY_IOLine_Hidden_WhenNoBytes(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:    10,
		TotalDuration: 50 * time.Millisecond,
		ScannedRows:   1000,
		Ephemeral:     true,
	}
	FormatTTY(&buf, s, false, false)
	out := buf.String()

	if strings.Contains(out, "io") && strings.Contains(out, "from disk") {
		t.Errorf("should not show io line when no bytes: %q", out)
	}
}

func TestFormatTTY_Verbose_CPUTime(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:      3,
		TotalDuration:   165 * time.Millisecond,
		ExecDuration:    150 * time.Millisecond,
		ScannedRows:     142_847,
		PeakMemoryBytes: 4_000_000,
		CPUTimeUser:     48 * time.Millisecond,
		CPUTimeSys:      12 * time.Millisecond,
	}
	FormatTTY(&buf, s, true, false)
	out := buf.String()

	if !strings.Contains(out, "cpu") {
		t.Errorf("verbose should show cpu line: %q", out)
	}
	if !strings.Contains(out, "48ms user") {
		t.Errorf("verbose should show user CPU: %q", out)
	}
	if !strings.Contains(out, "12ms sys") {
		t.Errorf("verbose should show sys CPU: %q", out)
	}
}

func TestFormatJSON_NewFields(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	s := &QueryStats{
		ResultRows:     5,
		TotalDuration:  10 * time.Millisecond,
		ScannedRows:    1000,
		AcceleratedBy:  "mv_test",
		MVStatus:       "active",
		MVOriginalScan: 500_000,
		DiskBytesRead:  1_048_576,
		S3BytesRead:    2_097_152,
		CPUTimeUser:    25 * time.Millisecond,
	}
	if err := FormatJSON(&buf, s); err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &envelope); err != nil {
		t.Fatalf("invalid JSON: %v\nraw: %s", err, buf.String())
	}
	var js map[string]interface{}
	if err := json.Unmarshal(envelope["stats"], &js); err != nil {
		t.Fatalf("invalid stats JSON: %v", err)
	}

	if v, ok := js["mv_status"].(string); !ok || v != "active" {
		t.Errorf("mv_status = %v, want active", js["mv_status"])
	}
	if v, ok := js["mv_original_scan"].(float64); !ok || v != 500_000 {
		t.Errorf("mv_original_scan = %v, want 500000", js["mv_original_scan"])
	}
	if v, ok := js["disk_bytes_read"].(float64); !ok || v != 1_048_576 {
		t.Errorf("disk_bytes_read = %v, want 1048576", js["disk_bytes_read"])
	}
	if v, ok := js["s3_bytes_read"].(float64); !ok || v != 2_097_152 {
		t.Errorf("s3_bytes_read = %v, want 2097152", js["s3_bytes_read"])
	}
	if _, ok := js["cpu_user_ms"]; !ok {
		t.Error("should include cpu_user_ms")
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

	// FormatTTY no longer prints a separator — the caller is responsible.
	if strings.Contains(out, "──────") {
		t.Errorf("FormatTTY should not print separator: %q", out)
	}
	if strings.Contains(out, "───") {
		t.Errorf("FormatTTY should not print separator: %q", out)
	}
}

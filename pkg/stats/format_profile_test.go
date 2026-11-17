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

func TestFormatProfile_PhaseBreakdown(t *testing.T) {
	s := &QueryStats{
		TotalDuration:     40 * time.Millisecond,
		ParseDuration:     200 * time.Microsecond,
		ExecDuration:      39 * time.Millisecond,
		SerializeDuration: 700 * time.Microsecond,
		ScannedRows:       1000,
		ResultRows:        10,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	for _, exp := range []string{"Query Profile", "Phase Breakdown", "parse", "exec", "serialize", "total"} {
		if !strings.Contains(out, exp) {
			t.Errorf("profile missing %q:\n%s", exp, out)
		}
	}
}

func TestFormatProfile_RowFunnel(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 10 * time.Millisecond,
		ExecDuration:  8 * time.Millisecond,
		ScannedRows:   100_000,
		ResultRows:    10,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if !strings.Contains(out, "Rows:  100,000 scanned  →  10 results") {
		t.Errorf("missing row funnel:\n%s", out)
	}
}

func TestFormatProfile_NoRowsWithoutScan(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 10 * time.Millisecond,
		ExecDuration:  8 * time.Millisecond,
		ResultRows:    10,
	}

	var buf bytes.Buffer
	FormatProfile(&buf, s)
	out := buf.String()

	if strings.Contains(out, "Rows:") {
		t.Errorf("should not show rows when none were scanned:\n%s", out)
	}
}

func TestFormatProfileJSON_Structure(t *testing.T) {
	s := &QueryStats{
		TotalDuration:     40 * time.Millisecond,
		ParseDuration:     200 * time.Microsecond,
		ExecDuration:      39 * time.Millisecond,
		SerializeDuration: 700 * time.Microsecond,
		ScannedRows:       120_000,
		ResultRows:        10,
	}

	var buf bytes.Buffer
	if err := FormatProfileJSON(&buf, s); err != nil {
		t.Fatalf("FormatProfileJSON error: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}

	profile, ok := result["profile"].(map[string]interface{})
	if !ok {
		t.Fatal("missing profile key in JSON output")
	}

	want := "exec_ms,parse_ms,result_rows,scanned_rows,serialize_ms,total_ms"
	if got := strings.Join(slices.Sorted(maps.Keys(profile)), ","); got != want {
		t.Errorf("keys = %s, want %s", got, want)
	}
}

func TestFormatProfileJSON_EmptyStats(t *testing.T) {
	s := &QueryStats{
		TotalDuration: 5 * time.Millisecond,
	}

	var buf bytes.Buffer
	if err := FormatProfileJSON(&buf, s); err != nil {
		t.Fatalf("FormatProfileJSON error: %v", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	profile := result["profile"].(map[string]interface{})

	want := "result_rows,scanned_rows,total_ms"
	if got := strings.Join(slices.Sorted(maps.Keys(profile)), ","); got != want {
		t.Errorf("keys = %s, want %s", got, want)
	}
}

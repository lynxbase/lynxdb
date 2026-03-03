package output

import (
	"bytes"
	"strings"
	"testing"
)

func TestTableFormatter(t *testing.T) {
	rows := []map[string]interface{}{
		{"host": "web-01", "count": int64(42)},
		{"host": "web-02", "count": int64(17)},
	}
	var buf bytes.Buffer
	f := &TableFormatter{}
	if err := f.Format(&buf, rows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "host") {
		t.Error("expected 'host' header")
	}
	if !strings.Contains(out, "web-01") {
		t.Error("expected 'web-01' value")
	}
	if !strings.Contains(out, "42") {
		t.Error("expected '42' value")
	}
	t.Log(out)
}

func TestTableFormatter_Empty(t *testing.T) {
	var buf bytes.Buffer
	f := &TableFormatter{}
	if err := f.Format(&buf, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "No results") {
		t.Error("expected 'No results'")
	}
}

func TestJSONFormatter(t *testing.T) {
	rows := []map[string]interface{}{
		{"host": "web-01", "count": 42},
	}
	var buf bytes.Buffer
	f := &JSONFormatter{}
	if err := f.Format(&buf, rows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"host":"web-01"`) && !strings.Contains(out, `"host": "web-01"`) {
		t.Errorf("expected JSON with host, got: %s", out)
	}
}

func TestCSVFormatter(t *testing.T) {
	rows := []map[string]interface{}{
		{"host": "web-01", "count": 42},
		{"host": "web-02", "count": 17},
	}
	var buf bytes.Buffer
	f := &CSVFormatter{}
	if err := f.Format(&buf, rows); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 { // header + 2 rows
		t.Errorf("expected 3 lines, got %d", len(lines))
	}
}

func TestRawFormatter(t *testing.T) {
	rows := []map[string]interface{}{
		{"_raw": "2025-01-15 ERROR something failed"},
	}
	var buf bytes.Buffer
	f := &RawFormatter{}
	if err := f.Format(&buf, rows); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "something failed") {
		t.Error("expected raw output")
	}
}

func TestSingleValueFormatter(t *testing.T) {
	rows := []map[string]interface{}{
		{"count": int64(1247)},
	}
	var buf bytes.Buffer
	f := &SingleValueFormatter{}
	if err := f.Format(&buf, rows); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(buf.String()) != "1247" {
		t.Errorf("expected '1247', got: %q", buf.String())
	}
}

func TestDetectFormat_JSON(t *testing.T) {
	f := DetectFormat(FormatJSON, nil)
	if _, ok := f.(*JSONFormatter); !ok {
		t.Error("expected JSONFormatter")
	}
}

func TestDetectFormat_Table(t *testing.T) {
	f := DetectFormat(FormatTable, nil)
	if _, ok := f.(*TableFormatter); !ok {
		t.Error("expected TableFormatter")
	}
}

func TestDetectFormat_CSV(t *testing.T) {
	f := DetectFormat(FormatCSV, nil)
	if _, ok := f.(*CSVFormatter); !ok {
		t.Error("expected CSVFormatter")
	}
}

func TestDetectFormat_NDJSON(t *testing.T) {
	f := DetectFormat(FormatNDJSON, nil)
	if _, ok := f.(*JSONFormatter); !ok {
		t.Errorf("expected JSONFormatter for NDJSON, got %T", f)
	}
}

func TestDetectFormat_TSV(t *testing.T) {
	f := DetectFormat(FormatTSV, nil)
	if _, ok := f.(*TSVFormatter); !ok {
		t.Errorf("expected TSVFormatter, got %T", f)
	}
}

func TestTSVFormatter(t *testing.T) {
	rows := []map[string]interface{}{
		{"host": "web-01", "count": int64(42)},
		{"host": "web-02", "count": int64(17)},
	}
	var buf bytes.Buffer
	f := &TSVFormatter{}
	if err := f.Format(&buf, rows); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 { // header + 2 rows
		t.Fatalf("expected 3 lines, got %d: %v", len(lines), lines)
	}
	// Header should be tab-separated columns.
	header := lines[0]
	if !strings.Contains(header, "\t") {
		t.Errorf("expected tab-separated header, got: %q", header)
	}
	// Data should contain values.
	if !strings.Contains(lines[1], "web-01") {
		t.Errorf("expected 'web-01' in first data row, got: %q", lines[1])
	}
}

func TestTSVFormatter_Empty(t *testing.T) {
	var buf bytes.Buffer
	f := &TSVFormatter{}
	if err := f.Format(&buf, nil); err != nil {
		t.Fatalf("Format nil rows: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected empty output for nil rows, got: %q", buf.String())
	}
}

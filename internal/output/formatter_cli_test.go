package output

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"
)

// TableFormatter edge cases

func TestTableFormatter_ManyColumns_NoPanic(t *testing.T) {
	row := make(map[string]interface{})
	for i := 0; i < 25; i++ {
		row[strings.Repeat("c", i+1)] = i
	}

	var buf bytes.Buffer
	f := &TableFormatter{}

	if err := f.Format(&buf, []map[string]interface{}{row}); err != nil {
		t.Fatalf("Format with 25 columns: %v", err)
	}

	if buf.Len() == 0 {
		t.Fatal("expected non-empty output for 25-column table")
	}
}

func TestTableFormatter_LongValues_NoBreak(t *testing.T) {
	rows := []map[string]interface{}{
		{"msg": strings.Repeat("x", 500), "count": 1},
		{"msg": "short", "count": 2},
	}

	var buf bytes.Buffer
	f := &TableFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format with long values: %v", err)
	}

	if !strings.Contains(buf.String(), "short") {
		t.Errorf("expected 'short' in output")
	}
}

// JSONFormatter edge cases

func TestJSONFormatter_EmptyRows_NoOutput(t *testing.T) {
	var buf bytes.Buffer
	f := &JSONFormatter{}

	if err := f.Format(&buf, nil); err != nil {
		t.Fatalf("Format nil rows: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected empty output for nil rows, got: %q", buf.String())
	}
}

func TestJSONFormatter_EmptySlice_NoOutput(t *testing.T) {
	var buf bytes.Buffer
	f := &JSONFormatter{}

	if err := f.Format(&buf, []map[string]interface{}{}); err != nil {
		t.Fatalf("Format empty slice: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected empty output for empty slice, got: %q", buf.String())
	}
}

func TestJSONFormatter_NilValues_RendersNull(t *testing.T) {
	rows := []map[string]interface{}{
		{"key": nil},
	}

	var buf bytes.Buffer
	f := &JSONFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format with nil value: %v", err)
	}

	if !strings.Contains(buf.String(), "null") {
		t.Errorf("expected 'null' for nil value, got: %q", buf.String())
	}
}

// CSVFormatter edge cases

func TestCSVFormatter_EmptyRows_NoOutput(t *testing.T) {
	var buf bytes.Buffer
	f := &CSVFormatter{}

	if err := f.Format(&buf, nil); err != nil {
		t.Fatalf("Format nil rows: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected empty output for nil rows, got: %q", buf.String())
	}
}

func TestCSVFormatter_FieldsWithCommas_Quoted(t *testing.T) {
	rows := []map[string]interface{}{
		{"msg": "hello, world", "count": 1},
	}

	var buf bytes.Buffer
	f := &CSVFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format: %v", err)
	}

	// Parse back as CSV to verify quoting is correct.
	reader := csv.NewReader(strings.NewReader(buf.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV re-parse failed (quoting issue): %v", err)
	}

	// header + 1 data row.
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}

	// Find the msg column and verify it contains the comma.
	if len(records) >= 2 {
		found := false
		for _, val := range records[1] {
			if val == "hello, world" {
				found = true
			}
		}

		if !found {
			t.Errorf("expected 'hello, world' in CSV data row, got: %v", records[1])
		}
	}
}

func TestCSVFormatter_FieldsWithNewlines_Quoted(t *testing.T) {
	rows := []map[string]interface{}{
		{"msg": "line1\nline2"},
	}

	var buf bytes.Buffer
	f := &CSVFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format: %v", err)
	}

	// Parse back as CSV to verify quoting handles newlines.
	reader := csv.NewReader(strings.NewReader(buf.String()))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("CSV re-parse failed (newline quoting issue): %v", err)
	}

	if len(records) < 2 {
		t.Fatalf("expected at least 2 records, got %d", len(records))
	}

	found := false
	for _, val := range records[1] {
		if val == "line1\nline2" {
			found = true
		}
	}

	if !found {
		t.Errorf("expected 'line1\\nline2' in CSV data, got: %v", records[1])
	}
}

// RawFormatter edge cases

func TestRawFormatter_NoRawField_FallsBackToKeyValue(t *testing.T) {
	rows := []map[string]interface{}{
		{"host": "web-01", "count": 42},
	}

	var buf bytes.Buffer
	f := &RawFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "host=web-01") {
		t.Errorf("expected 'host=web-01' in key=value fallback, got: %q", out)
	}

	if !strings.Contains(out, "count=42") {
		t.Errorf("expected 'count=42' in key=value fallback, got: %q", out)
	}
}

func TestRawFormatter_EmptyRows_NoOutput(t *testing.T) {
	var buf bytes.Buffer
	f := &RawFormatter{}

	if err := f.Format(&buf, nil); err != nil {
		t.Fatalf("Format: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected empty output for nil rows, got: %q", buf.String())
	}
}

// SingleValueFormatter edge cases

func TestSingleValueFormatter_MultiRow_FallsBackToTable(t *testing.T) {
	rows := []map[string]interface{}{
		{"count": 10},
		{"count": 20},
	}

	var buf bytes.Buffer
	f := &SingleValueFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format: %v", err)
	}

	// Should fall back to table format (contains column header).
	if !strings.Contains(buf.String(), "count") {
		t.Errorf("expected table fallback with 'count' header, got: %q", buf.String())
	}
}

// TSVFormatter edge cases

func TestTSVFormatter_FieldsWithTabs_Escaped(t *testing.T) {
	rows := []map[string]interface{}{
		{"msg": "col1\tcol2", "count": 1},
	}

	var buf bytes.Buffer
	f := &TSVFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (header + 1 row), got %d", len(lines))
	}

	// The header should have exactly one tab (two columns).
	headerTabs := strings.Count(lines[0], "\t")
	if headerTabs != 1 {
		t.Errorf("header should have 1 tab (2 cols), got %d tabs: %q", headerTabs, lines[0])
	}
}

func TestTSVFormatter_FieldsWithCommas_NotQuoted(t *testing.T) {
	// Unlike CSV, TSV should not quote fields containing commas.
	rows := []map[string]interface{}{
		{"msg": "hello, world"},
	}

	var buf bytes.Buffer
	f := &TSVFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format: %v", err)
	}

	out := buf.String()
	// The comma should appear as-is, not surrounded by quotes.
	if strings.Contains(out, `"hello, world"`) {
		t.Errorf("TSV should not quote fields with commas, got: %q", out)
	}

	if !strings.Contains(out, "hello, world") {
		t.Errorf("expected 'hello, world' in output, got: %q", out)
	}
}

func TestTSVFormatter_EmptyRows_NoOutput(t *testing.T) {
	var buf bytes.Buffer
	f := &TSVFormatter{}

	if err := f.Format(&buf, []map[string]interface{}{}); err != nil {
		t.Fatalf("Format empty slice: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected empty output for empty slice, got: %q", buf.String())
	}
}

func TestTSVFormatter_NilValues_EmptyString(t *testing.T) {
	rows := []map[string]interface{}{
		{"key": nil, "val": "ok"},
	}

	var buf bytes.Buffer
	f := &TSVFormatter{}

	if err := f.Format(&buf, rows); err != nil {
		t.Fatalf("Format with nil value: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected at least 2 lines, got %d", len(lines))
	}

	// The data row should have a tab (two columns present).
	if !strings.Contains(lines[1], "\t") {
		t.Errorf("data row missing tab separator: %q", lines[1])
	}
}

// formatValue edge cases

func TestFormatValue_Float(t *testing.T) {
	got := formatValue(3.14159)
	if got != "3.142" {
		t.Errorf("formatValue(3.14159) = %q, want %q", got, "3.142")
	}
}

func TestFormatValue_WholeFloat(t *testing.T) {
	got := formatValue(float64(42))
	if got != "42" {
		t.Errorf("formatValue(42.0) = %q, want %q", got, "42")
	}
}

func TestFormatValue_Nil(t *testing.T) {
	got := formatValue(nil)
	if got != "" {
		t.Errorf("formatValue(nil) = %q, want empty string", got)
	}
}

func TestFormatValue_String(t *testing.T) {
	got := formatValue("hello")
	if got != "hello" {
		t.Errorf("formatValue(string) = %q, want %q", got, "hello")
	}
}

// collectColumns edge cases

func TestCollectColumns_StableOrder(t *testing.T) {
	// User-only columns should come out sorted alphabetically.
	rows := []map[string]interface{}{
		{"a": 1, "b": 2},
		{"b": 3, "c": 4},
	}

	cols := collectColumns(rows)
	want := []string{"a", "b", "c"}
	if len(cols) != len(want) {
		t.Fatalf("expected %d columns, got %d: %v", len(want), len(cols), cols)
	}
	for i, w := range want {
		if cols[i] != w {
			t.Errorf("cols[%d] = %q, want %q (full: %v)", i, cols[i], w, cols)
		}
	}
}

func TestCollectColumns_BuiltinBeforeUser(t *testing.T) {
	// Builtin fields (_time, host, source) should appear first in canonical order,
	// followed by user fields alphabetically.
	rows := []map[string]interface{}{
		{"z_custom": 1, "host": "web-01", "_time": "2025-01-01", "a_field": "val", "source": "nginx"},
	}

	cols := collectColumns(rows)
	want := []string{"_time", "source", "host", "a_field", "z_custom"}
	if len(cols) != len(want) {
		t.Fatalf("expected %d columns, got %d: %v", len(want), len(cols), cols)
	}
	for i, w := range want {
		if cols[i] != w {
			t.Errorf("cols[%d] = %q, want %q (full: %v)", i, cols[i], w, cols)
		}
	}
}

func TestCollectColumns_Deterministic100Runs(t *testing.T) {
	// Go map iteration is randomized — verify 100 calls produce identical output.
	rows := []map[string]interface{}{
		{"_time": "t1", "host": "h", "level": "info", "msg": "hello", "source": "app", "z": 1, "a": 2},
	}
	baseline := collectColumns(rows)
	for i := 0; i < 100; i++ {
		got := collectColumns(rows)
		if len(got) != len(baseline) {
			t.Fatalf("run %d: length %d != baseline %d", i, len(got), len(baseline))
		}
		for j := range baseline {
			if got[j] != baseline[j] {
				t.Fatalf("run %d: cols[%d] = %q, want %q\ngot:  %v\nwant: %v", i, j, got[j], baseline[j], got, baseline)
			}
		}
	}
}

// DetectFormat edge cases

func TestDetectFormat_Raw(t *testing.T) {
	f := DetectFormat(FormatRaw, nil)
	if _, ok := f.(*RawFormatter); !ok {
		t.Errorf("expected *RawFormatter, got %T", f)
	}
}

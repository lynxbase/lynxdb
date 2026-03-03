package ui

import (
	"bytes"
	"strings"
	"testing"
)

func TestTable_Basic(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	tbl := NewTable(theme).
		SetTerminalWidth(120).
		SetColumns("HOST", "COUNT").
		AddRow("web-01", "42").
		AddRow("web-02", "17")

	got := tbl.String()

	if !strings.Contains(got, "HOST") {
		t.Errorf("table should contain 'HOST' header, got:\n%s", got)
	}

	if !strings.Contains(got, "web-01") {
		t.Errorf("table should contain 'web-01', got:\n%s", got)
	}

	if !strings.Contains(got, "42") {
		t.Errorf("table should contain '42', got:\n%s", got)
	}

	// Should have separator line.
	if !strings.Contains(got, "\u2500") {
		t.Errorf("table should contain separator, got:\n%s", got)
	}
}

func TestTable_EmptyColumns(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	tbl := NewTable(theme)
	got := tbl.String()

	if got != "" {
		t.Errorf("table with no columns should produce empty string, got: %q", got)
	}
}

func TestTable_LongValues_NoTruncation(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	longVal := strings.Repeat("x", 100)
	tbl := NewTable(theme).
		SetTerminalWidth(200).
		SetColumns("DATA").
		AddRow(longVal)

	got := tbl.String()

	// The value should NOT be truncated — full value must be present.
	if !strings.Contains(got, longVal) {
		t.Errorf("table should preserve full values (no truncation), got:\n%s", got)
	}

	// No "..." truncation marker.
	if strings.Contains(got, "...") {
		t.Errorf("table should not contain '...' truncation marker, got:\n%s", got)
	}
}

func TestTable_MissingValues(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	tbl := NewTable(theme).
		SetTerminalWidth(120).
		SetColumns("A", "B", "C").
		AddRow("1") // only one value for three columns

	got := tbl.String()

	// Should not panic and should contain the provided value.
	if !strings.Contains(got, "1") {
		t.Errorf("table should contain '1', got:\n%s", got)
	}
}

func TestTable_WrapsWithinWidth(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	longURL := "https://example.com/very/long/path/that/exceeds/normal/column/width/by/a/lot"
	tbl := NewTable(theme).
		SetTerminalWidth(80).
		SetColumns("STATUS", "URL").
		AddRow("200", longURL)

	got := tbl.String()

	// The URL may be wrapped across lines by lipgloss. Concatenate all lines
	// (stripping whitespace) and verify the full URL is present.
	collapsed := strings.ReplaceAll(got, "\n", "")
	collapsed = strings.Join(strings.Fields(collapsed), "")

	urlNoSpaces := strings.ReplaceAll(longURL, " ", "")
	if !strings.Contains(collapsed, urlNoSpaces) {
		t.Errorf("table should preserve full URL (possibly wrapped), got:\n%s", got)
	}

	// No truncation marker.
	if strings.Contains(got, "...") {
		t.Errorf("table should not truncate values, got:\n%s", got)
	}
}

func TestTable_CardLayout(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	// 15 columns at 60 chars = ~4 chars per column, well below MinColumnWidth.
	cols := make([]string, 15)
	vals := make([]string, 15)

	for i := range cols {
		cols[i] = strings.Repeat("C", 3) + strings.Repeat("0", 1) // e.g., "CCC0"
		vals[i] = "val"
	}

	tbl := NewTable(theme).
		SetTerminalWidth(60).
		SetColumns(cols...).
		AddRow(vals...)

	got := tbl.String()

	// Card layout should have "Row 1" header.
	if !strings.Contains(got, "Row 1") {
		t.Errorf("card layout should contain 'Row 1' header, got:\n%s", got)
	}

	// All values should be preserved.
	for _, v := range vals {
		if !strings.Contains(got, v) {
			t.Errorf("card layout should contain value %q, got:\n%s", v, got)
		}
	}
}

func TestTable_NarrowTerminal(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	// Even with just 2 columns, a 20-char terminal is too narrow for table.
	tbl := NewTable(theme).
		SetTerminalWidth(20).
		SetColumns("HOST", "STATUS", "MESSAGE").
		AddRow("web-01", "200", "OK")

	got := tbl.String()

	// Should fall back to card layout.
	if !strings.Contains(got, "Row 1") {
		t.Errorf("narrow terminal should trigger card layout, got:\n%s", got)
	}

	// All values preserved.
	if !strings.Contains(got, "web-01") || !strings.Contains(got, "200") || !strings.Contains(got, "OK") {
		t.Errorf("card layout should preserve all values, got:\n%s", got)
	}
}

func TestTable_FitsNaturally(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	tbl := NewTable(theme).
		SetTerminalWidth(200).
		SetColumns("A", "B").
		AddRow("short", "value")

	got := tbl.String()

	// Should render as a normal table (no "Row 1" card header).
	if strings.Contains(got, "Row 1") {
		t.Errorf("wide terminal should render table, not cards, got:\n%s", got)
	}

	if !strings.Contains(got, "short") || !strings.Contains(got, "value") {
		t.Errorf("table should contain values, got:\n%s", got)
	}
}

func TestTable_CardLayout_MultipleRows(t *testing.T) {
	var buf bytes.Buffer
	theme := NewTheme(&buf, true)

	// Force card layout with many columns on a narrow terminal.
	tbl := NewTable(theme).
		SetTerminalWidth(40).
		SetColumns("HOST", "STATUS", "METHOD", "PATH", "USER").
		AddRow("web-01", "200", "GET", "/api/v1/users", "alice").
		AddRow("web-02", "500", "POST", "/api/v1/orders", "bob")

	got := tbl.String()

	if !strings.Contains(got, "Row 1") {
		t.Errorf("should contain 'Row 1', got:\n%s", got)
	}

	if !strings.Contains(got, "Row 2") {
		t.Errorf("should contain 'Row 2', got:\n%s", got)
	}

	// All values preserved.
	for _, v := range []string{"web-01", "web-02", "/api/v1/users", "/api/v1/orders", "alice", "bob"} {
		if !strings.Contains(got, v) {
			t.Errorf("card layout should contain %q, got:\n%s", v, got)
		}
	}
}

package ui

import (
	"bytes"
	"strings"
	"testing"

	"github.com/charmbracelet/x/ansi"
)

func testTheme() *Theme {
	var buf bytes.Buffer
	return NewTheme(&buf, true)
}

func TestTable_BoxRender(t *testing.T) {
	got := NewTable(testTheme()).
		SetTerminalWidth(120).
		SetColumns("HOST", "STATUS", "COUNT").
		AddRow("web-01", "ok", "42").
		AddRow("web-02", "warn", "17").
		String()

	for _, want := range []string{"┌", "┬", "┐", "├", "┼", "┤", "└", "┴", "┘", "(2 rows)"} {
		if !strings.Contains(got, want) {
			t.Fatalf("box table missing %q:\n%s", want, got)
		}
	}
}

func TestTable_ASCIIStyle(t *testing.T) {
	got := NewTable(testTheme()).
		SetTerminalWidth(80).
		SetStyle(StyleASCII).
		SetColumns("A", "B").
		AddRow("1", "2").
		String()

	if !strings.Contains(got, "+---+---+") {
		t.Fatalf("ASCII table missing +-+ border:\n%s", got)
	}
}

func TestTable_MarkdownStyle(t *testing.T) {
	got := NewTable(testTheme()).
		SetTerminalWidth(80).
		SetStyle(StyleMarkdown).
		SetColumns("A", "B").
		AddRow("1", "2").
		String()

	if !strings.Contains(got, "|---|---|") {
		t.Fatalf("markdown table missing separator:\n%s", got)
	}
	if strings.Contains(got, "┌") || strings.Contains(got, "+") {
		t.Fatalf("markdown table should not use box/ascii borders:\n%s", got)
	}
}

func TestTable_TruncatesWithinWidth(t *testing.T) {
	got := NewTable(testTheme()).
		SetTerminalWidth(42).
		SetColumns("STATUS", "URL").
		AddRow("200", "https://example.com/very/long/path/that/exceeds/width").
		String()

	if !strings.Contains(got, "…") {
		t.Fatalf("narrow table should truncate with ellipsis:\n%s", got)
	}
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "(") {
			continue
		}
		if width := ansi.StringWidth(line); width > 42 {
			t.Fatalf("line width = %d, want <= 42: %q\n%s", width, line, got)
		}
	}
}

func TestTable_MaxRowsFooter(t *testing.T) {
	tbl := NewTable(testTheme()).
		SetTerminalWidth(80).
		SetMaxRows(2).
		SetColumns("A")
	for i := 0; i < 5; i++ {
		tbl.AddRow("x")
	}
	got := tbl.String()

	if !strings.Contains(got, "(5 rows, 2 shown — use --max-rows to see more)") {
		t.Fatalf("missing max rows footer:\n%s", got)
	}
}

func TestTable_NullValue(t *testing.T) {
	got := NewTable(testTheme()).
		SetTerminalWidth(80).
		SetNullValue("∅").
		SetColumns("A", "B").
		AddRow("value").
		String()

	if !strings.Contains(got, "∅") {
		t.Fatalf("missing null placeholder:\n%s", got)
	}
}

func TestTable_VerticalStyle(t *testing.T) {
	got := NewTable(testTheme()).
		SetTerminalWidth(40).
		SetStyle(StyleVertical).
		SetColumns("HOST", "MESSAGE").
		AddRow("web-01", "full message value").
		String()

	for _, want := range []string{"record 1", "HOST", "web-01", "full message value"} {
		if !strings.Contains(got, want) {
			t.Fatalf("vertical output missing %q:\n%s", want, got)
		}
	}
}

func TestTable_EmptyColumns(t *testing.T) {
	if got := NewTable(testTheme()).String(); got != "" {
		t.Fatalf("table with no columns should be empty, got %q", got)
	}
}

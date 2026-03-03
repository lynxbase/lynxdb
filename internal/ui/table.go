package ui

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	"charm.land/lipgloss/v2/table"
)

// cellPaddingRight is the right-padding (in chars) applied to each table cell.
const cellPaddingRight = 2

// Table is a styled table renderer built on lipgloss/table.
// It automatically adapts to terminal width: when all columns fit, it renders
// a normal table with cell wrapping. When columns would be too narrow
// (< MinColumnWidth each), it falls back to a vertical card layout that
// preserves every value without truncation.
//
// For interactive scrolling tables, use bubbles/table instead.
type Table struct {
	theme     *Theme
	columns   []string
	rows      [][]string
	termWidth int
}

// NewTable creates a new table bound to the given theme.
// The terminal width is auto-detected; use SetTerminalWidth to override.
func NewTable(theme *Theme) *Table {
	return &Table{
		theme:     theme,
		termWidth: TerminalWidth(),
	}
}

// SetColumns sets the column headers.
func (t *Table) SetColumns(cols ...string) *Table {
	t.columns = cols

	return t
}

// SetTerminalWidth overrides the auto-detected terminal width.
// Useful for tests or when rendering to a non-stdout destination.
func (t *Table) SetTerminalWidth(w int) *Table {
	t.termWidth = w

	return t
}

// AddRow appends a row of values.
func (t *Table) AddRow(values ...string) *Table {
	t.rows = append(t.rows, values)

	return t
}

// String renders the table, choosing between table layout and card layout
// based on the terminal width and number of columns.
func (t *Table) String() string {
	if len(t.columns) == 0 {
		return ""
	}

	numCols := len(t.columns)
	// Each column needs at least MinColumnWidth + cellPaddingRight.
	availablePerCol := t.termWidth / numCols

	if availablePerCol < MinColumnWidth+cellPaddingRight {
		return t.renderCards()
	}

	return t.renderTable()
}

// renderTable renders a standard horizontal table using lipgloss/table,
// constrained to the terminal width with cell wrapping (no truncation).
func (t *Table) renderTable() string {
	// Normalize rows: ensure each row has exactly len(t.columns) cells.
	rows := make([][]string, len(t.rows))
	for i, row := range t.rows {
		r := make([]string, len(t.columns))
		for j := range t.columns {
			if j < len(row) {
				r[j] = row[j]
			}
		}

		rows[i] = r
	}

	// Border with only a header separator line using "─".
	border := lipgloss.Border{
		Top: "\u2500",
	}

	headerStyle := t.theme.TableHeader
	ruleStyle := t.theme.Rule

	tbl := table.New().
		Border(border).
		BorderStyle(ruleStyle).
		BorderTop(false).
		BorderBottom(false).
		BorderLeft(false).
		BorderRight(false).
		BorderColumn(false).
		BorderRow(false).
		BorderHeader(true).
		Width(t.termWidth).
		Wrap(true).
		StyleFunc(func(row, col int) lipgloss.Style {
			if row == table.HeaderRow {
				return headerStyle.PaddingRight(cellPaddingRight)
			}

			return lipgloss.NewStyle().PaddingRight(cellPaddingRight)
		}).
		Headers(t.columns...).
		Rows(rows...)

	return tbl.String()
}

// renderCards renders rows as vertical key-value cards. This layout is used
// when the terminal is too narrow for a readable table. Every value is
// preserved in full — nothing is ever truncated.
//
// Example output:
//
//	── Row 1 ──────────────────────────
//	  host:    web-01.prod.example.com
//	  status:  200
//	  message: Connection established
func (t *Table) renderCards() string {
	var b strings.Builder

	labelStyle := t.theme.Label
	ruleStyle := t.theme.Rule

	// Find the longest column name for alignment.
	maxLabel := 0
	for _, col := range t.columns {
		if len(col) > maxLabel {
			maxLabel = len(col)
		}
	}

	for i, row := range t.rows {
		// Row header: ── Row N ──────...
		header := fmt.Sprintf(" Row %d ", i+1)
		remaining := t.termWidth - len(header) - 2 // 2 for leading "──"
		if remaining < 0 {
			remaining = 0
		}

		line := "\u2500\u2500" + header + strings.Repeat("\u2500", remaining)
		b.WriteString(ruleStyle.Render(line))
		b.WriteByte('\n')

		// Key-value pairs.
		for j, col := range t.columns {
			val := ""
			if j < len(row) {
				val = row[j]
			}

			label := labelStyle.Render(fmt.Sprintf("  %-*s", maxLabel, col))
			b.WriteString(label)
			b.WriteString("  ")
			b.WriteString(val)
			b.WriteByte('\n')
		}

		// Blank line between cards (but not after the last one).
		if i < len(t.rows)-1 {
			b.WriteByte('\n')
		}
	}

	return b.String()
}

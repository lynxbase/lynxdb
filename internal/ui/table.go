package ui

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
)

// cellPaddingRight is the right-padding (in chars) applied to each table cell.
const cellPaddingRight = 2

// Table is a styled table renderer for CLI output. It stays tabular by default
// and truncates over-wide cells to fit the terminal; callers can explicitly set
// StyleVertical for record-oriented output.
//
// For interactive scrolling tables, use bubbles/table instead.
type Table struct {
	theme     *Theme
	columns   []string
	rows      [][]string
	kinds     []ColumnKind
	termWidth int
	compact   bool
	style     TableStyle
	maxRows   int
	nullValue string
}

// TableStyle controls the human-readable table layout.
type TableStyle int

const (
	StyleBox TableStyle = iota
	StyleASCII
	StyleMarkdown
	StyleVertical
)

// ColumnKind controls per-column styling and alignment.
type ColumnKind int

const (
	ColumnAuto ColumnKind = iota
	ColumnText
	ColumnNumber
	ColumnDuration
	ColumnBytes
)

// NewTable creates a new table bound to the given theme.
// The terminal width is auto-detected; use SetTerminalWidth to override.
func NewTable(theme *Theme) *Table {
	return &Table{
		theme:     theme,
		termWidth: TerminalWidth(),
		style:     StyleBox,
	}
}

// SetColumns sets the column headers.
func (t *Table) SetColumns(cols ...string) *Table {
	t.columns = cols

	return t
}

// SetColumnKinds sets optional per-column styles. Missing entries default to text.
func (t *Table) SetColumnKinds(kinds ...ColumnKind) *Table {
	t.kinds = kinds

	return t
}

// SetCompact switches to denser padding and lower narrow-layout thresholds.
func (t *Table) SetCompact(compact bool) *Table {
	t.compact = compact

	return t
}

// SetStyle sets the table layout style.
func (t *Table) SetStyle(style TableStyle) *Table {
	t.style = style

	return t
}

// SetMaxRows limits rendered rows. Zero means no limit.
func (t *Table) SetMaxRows(maxRows int) *Table {
	t.maxRows = maxRows

	return t
}

// SetNullValue sets the placeholder used for empty cells.
func (t *Table) SetNullValue(nullValue string) *Table {
	t.nullValue = nullValue

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

// String renders the table.
func (t *Table) String() string {
	if len(t.columns) == 0 {
		return ""
	}

	if t.style == StyleVertical {
		return t.renderRecords()
	}

	return t.renderTable()
}

// renderTable renders a horizontal table constrained to terminal width.
func (t *Table) renderTable() string {
	rows := t.visibleRows()
	widths := t.columnWidths(rows)
	chars := tableCharsForStyle(t.style)

	var b strings.Builder
	if t.style != StyleMarkdown {
		b.WriteString(t.ruleLine(chars.topLeft, chars.topSep, chars.topRight, chars.horizontal, widths))
		b.WriteByte('\n')
	}
	b.WriteString(t.rowLine(chars.vertical, widths, t.columns, true))
	b.WriteByte('\n')
	b.WriteString(t.ruleLine(chars.midLeft, chars.midSep, chars.midRight, chars.horizontal, widths))
	b.WriteByte('\n')
	for _, row := range rows {
		b.WriteString(t.rowLine(chars.vertical, widths, row, false))
		b.WriteByte('\n')
	}
	if t.style != StyleMarkdown {
		b.WriteString(t.ruleLine(chars.bottomLeft, chars.bottomSep, chars.bottomRight, chars.horizontal, widths))
		b.WriteByte('\n')
	}
	b.WriteString(t.rowCountFooter(len(t.rows), len(rows)))

	return strings.TrimRight(b.String(), "\n")
}

func (t *Table) columnKind(col int) ColumnKind {
	if col >= 0 && col < len(t.kinds) && t.kinds[col] != ColumnAuto {
		return t.kinds[col]
	}

	return ColumnText
}

func (t *Table) visibleRows() [][]string {
	limit := len(t.rows)
	if t.maxRows > 0 && t.maxRows < limit {
		limit = t.maxRows
	}
	rows := make([][]string, limit)
	for i := 0; i < limit; i++ {
		r := make([]string, len(t.columns))
		for j := range t.columns {
			if j < len(t.rows[i]) {
				r[j] = t.rows[i][j]
			}
			if r[j] == "" && t.nullValue != "" {
				r[j] = t.nullValue
			}
		}
		rows[i] = r
	}

	return rows
}

func (t *Table) columnWidths(rows [][]string) []int {
	widths := make([]int, len(t.columns))
	for i, col := range t.columns {
		widths[i] = maxInt(1, ansi.StringWidth(col))
	}
	for _, row := range rows {
		for i, cell := range row {
			widths[i] = maxInt(widths[i], ansi.StringWidth(cell))
		}
	}

	target := t.termWidth
	if target <= 0 {
		target = TerminalWidth()
	}
	chrome := len(t.columns) + 1 + 2*len(t.columns)
	available := target - chrome
	if available < len(t.columns) {
		available = len(t.columns)
	}
	for sumInts(widths) > available {
		idx := widestColumn(widths)
		if widths[idx] <= 1 {
			break
		}
		widths[idx]--
	}

	return widths
}

func (t *Table) rowLine(sep string, widths []int, values []string, header bool) string {
	var b strings.Builder
	b.WriteString(sep)
	for i, width := range widths {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		if value == "" && t.nullValue != "" {
			value = t.nullValue
		}
		value = truncateCell(value, width)
		cellWidth := ansi.StringWidth(value)
		pad := strings.Repeat(" ", maxInt(0, width-cellWidth))

		style := t.cellStyle(i, header)
		if t.isRightAligned(i) && !header {
			b.WriteByte(' ')
			b.WriteString(style.Render(pad + value))
			b.WriteByte(' ')
		} else {
			b.WriteByte(' ')
			b.WriteString(style.Render(value + pad))
			b.WriteByte(' ')
		}
		b.WriteString(sep)
	}

	return b.String()
}

func (t *Table) ruleLine(left, sep, right, fill string, widths []int) string {
	var b strings.Builder
	b.WriteString(left)
	for i, width := range widths {
		b.WriteString(strings.Repeat(fill, width+2))
		if i == len(widths)-1 {
			b.WriteString(right)
		} else {
			b.WriteString(sep)
		}
	}

	if t.style == StyleMarkdown {
		return b.String()
	}

	return t.theme.Rule.Render(b.String())
}

func (t *Table) cellStyle(col int, header bool) lipgloss.Style {
	if header {
		return t.theme.TableHeader
	}
	switch t.columnKind(col) {
	case ColumnNumber:
		return t.theme.JSONNum
	case ColumnDuration:
		return t.theme.Info
	case ColumnBytes:
		return t.theme.Accent
	default:
		return lipgloss.NewStyle()
	}
}

func (t *Table) isRightAligned(col int) bool {
	switch t.columnKind(col) {
	case ColumnNumber, ColumnDuration, ColumnBytes:
		return true
	default:
		return false
	}
}

func (t *Table) rowCountFooter(total, shown int) string {
	word := "rows"
	if total == 1 {
		word = "row"
	}
	if t.maxRows > 0 && shown < total {
		return fmt.Sprintf("(%d %s, %d shown — use --max-rows to see more)", total, word, shown)
	}

	return fmt.Sprintf("(%d %s)", total, word)
}

type tableChars struct {
	topLeft, topSep, topRight          string
	midLeft, midSep, midRight          string
	bottomLeft, bottomSep, bottomRight string
	horizontal, vertical               string
}

func tableCharsForStyle(style TableStyle) tableChars {
	switch style {
	case StyleASCII:
		return tableChars{
			topLeft: "+", topSep: "+", topRight: "+",
			midLeft: "+", midSep: "+", midRight: "+",
			bottomLeft: "+", bottomSep: "+", bottomRight: "+",
			horizontal: "-", vertical: "|",
		}
	case StyleMarkdown:
		return tableChars{
			midLeft: "|", midSep: "|", midRight: "|",
			horizontal: "-", vertical: "|",
		}
	default:
		return tableChars{
			topLeft: "┌", topSep: "┬", topRight: "┐",
			midLeft: "├", midSep: "┼", midRight: "┤",
			bottomLeft: "└", bottomSep: "┴", bottomRight: "┘",
			horizontal: "─", vertical: "│",
		}
	}
}

func truncateCell(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if ansi.StringWidth(s) <= width {
		return s
	}
	if width == 1 {
		return "…"
	}

	return ansi.Truncate(s, width, "…")
}

func sumInts(values []int) int {
	sum := 0
	for _, v := range values {
		sum += v
	}

	return sum
}

func widestColumn(widths []int) int {
	idx := 0
	for i := 1; i < len(widths); i++ {
		if widths[i] > widths[idx] {
			idx = i
		}
	}

	return idx
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}

	return b
}

// renderRecords renders rows as vertical key-value records. This layout is used
// when the terminal is too narrow for a readable table. Every value is
// preserved in full — nothing is ever truncated.
//
// Example output:
//
//	host:    web-01.prod.example.com
//	status:  200
//	message: Connection established
func (t *Table) renderRecords() string {
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
		header := fmt.Sprintf(" record %d ", i+1)
		remaining := t.termWidth - len(header) - 1
		if remaining < 0 {
			remaining = 0
		}

		line := header + strings.Repeat("\u2500", remaining)
		b.WriteString(ruleStyle.Render(line))
		b.WriteByte('\n')

		// Key-value pairs.
		for j, col := range t.columns {
			val := ""
			if j < len(row) {
				val = row[j]
			}

			label := labelStyle.Render(fmt.Sprintf("  %-*s", maxLabel, col))
			prefix := label + "  "
			prefixWidth := ansi.StringWidth(prefix)
			valueWidth := t.termWidth - prefixWidth
			if valueWidth < 1 {
				valueWidth = 1
			}
			wrappedValue := ansi.Wrap(val, valueWidth, " ")
			wrappedLines := strings.Split(wrappedValue, "\n")
			if len(wrappedLines) == 0 {
				wrappedLines = []string{""}
			}

			b.WriteString(label)
			b.WriteString("  ")
			b.WriteString(wrappedLines[0])
			b.WriteByte('\n')
			continuationPrefix := strings.Repeat(" ", prefixWidth)
			for _, line := range wrappedLines[1:] {
				b.WriteString(continuationPrefix)
				b.WriteString(line)
				b.WriteByte('\n')
			}
		}

		// Blank line between records (but not after the last one).
		if i < len(t.rows)-1 {
			b.WriteByte('\n')
		}
	}

	return b.String()
}

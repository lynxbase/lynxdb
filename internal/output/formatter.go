// Package output provides formatters for query results.
package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/internal/ui"
)

// Format represents an output format.
type Format string

const (
	FormatAuto   Format = "auto"
	FormatTable  Format = "table"
	FormatJSON   Format = "json"
	FormatNDJSON Format = "ndjson" // Alias for JSON — JSONFormatter already outputs NDJSON.
	FormatCSV    Format = "csv"
	FormatTSV    Format = "tsv"
	FormatRaw    Format = "raw"
)

// Formatter writes query results to an output writer.
type Formatter interface {
	Format(w io.Writer, rows []map[string]interface{}) error
}

// isTTY returns true if f is a terminal.
func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}

// DetectFormat chooses the best format based on context.
// When theme is non-nil, styled headers and separators are used in table output.
func DetectFormat(format Format, rows []map[string]interface{}, theme ...*ui.Theme) Formatter {
	var t *ui.Theme
	if len(theme) > 0 {
		t = theme[0]
	}

	switch format {
	case FormatTable:
		return &TableFormatter{Theme: t}
	case FormatJSON, FormatNDJSON:
		return &JSONFormatter{}
	case FormatCSV:
		return &CSVFormatter{}
	case FormatTSV:
		return &TSVFormatter{}
	case FormatRaw:
		return &RawFormatter{}
	default: // auto
		if !isTTY(os.Stdout) {
			return &JSONFormatter{}
		}
		if len(rows) == 1 && len(rows[0]) == 1 {
			return &SingleValueFormatter{}
		}

		return &TableFormatter{Theme: t}
	}
}

// TableFormatter outputs aligned table format using lipgloss/table.
// When Theme is set, headers are styled bold and separators use "─" characters.
type TableFormatter struct {
	Theme *ui.Theme
}

func (f *TableFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		fmt.Fprintln(w, "No results.")

		return nil
	}

	// Collect columns in deterministic order.
	cols := collectColumns(rows)

	// Build a themed table. Use a plain theme when none provided.
	theme := f.Theme
	if theme == nil {
		theme = ui.NewTheme(w, true)
	}

	tbl := ui.NewTable(theme).SetColumns(cols...)

	for _, row := range rows {
		vals := make([]string, len(cols))
		for i, col := range cols {
			vals[i] = formatValue(row[col])
		}
		tbl.AddRow(vals...)
	}

	_, err := fmt.Fprint(w, tbl.String())

	return err
}

// JSONFormatter outputs newline-delimited JSON.
type JSONFormatter struct{}

func (f *JSONFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	enc := json.NewEncoder(w)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			return err
		}
	}

	return nil
}

// CSVFormatter outputs RFC 4180 CSV.
type CSVFormatter struct{}

func (f *CSVFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	cols := collectColumns(rows)
	cw := csv.NewWriter(w)

	// Header.
	if err := cw.Write(cols); err != nil {
		return err
	}

	// Rows.
	record := make([]string, len(cols))
	for _, row := range rows {
		for i, col := range cols {
			record[i] = formatValue(row[col])
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()

	return cw.Error()
}

// TSVFormatter outputs tab-separated values with a header row.
type TSVFormatter struct{}

func (f *TSVFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	cols := collectColumns(rows)

	// Header.
	if _, err := fmt.Fprintln(w, strings.Join(cols, "\t")); err != nil {
		return err
	}

	// Rows.
	vals := make([]string, len(cols))
	for _, row := range rows {
		for i, col := range cols {
			vals[i] = formatValue(row[col])
		}

		if _, err := fmt.Fprintln(w, strings.Join(vals, "\t")); err != nil {
			return err
		}
	}

	return nil
}

// RawFormatter outputs raw text (one line per row, tab-separated).
type RawFormatter struct{}

func (f *RawFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	for _, row := range rows {
		if raw, ok := row["_raw"]; ok {
			fmt.Fprintln(w, formatValue(raw))
		} else {
			parts := make([]string, 0, len(row))
			for k, v := range row {
				parts = append(parts, k+"="+formatValue(v))
			}
			sort.Strings(parts)
			fmt.Fprintln(w, strings.Join(parts, "\t"))
		}
	}

	return nil
}

// SingleValueFormatter outputs a single value with formatting.
type SingleValueFormatter struct{}

func (f *SingleValueFormatter) Format(w io.Writer, rows []map[string]interface{}) error {
	if len(rows) != 1 || len(rows[0]) != 1 {
		return (&TableFormatter{}).Format(w, rows)
	}
	for _, v := range rows[0] {
		fmt.Fprintln(w, formatValue(v))
	}

	return nil
}

// builtinFieldOrder defines the canonical display order for LynxDB internal
// fields. These always appear first (in this order) when present, followed
// by user-defined fields in alphabetical order. This guarantees deterministic
// column ordering across runs — Go map iteration is randomized, so we must
// never rely on insertion order.
var builtinFieldOrder = [...]string{
	"_time",
	"_raw",
	"index",
	"source",
	"_source",
	"sourcetype",
	"_sourcetype",
	"host",
}

// builtinFieldRank maps builtin field names to their sort priority.
// Lower rank = appears first. Populated once at package init time would be
// the obvious choice, but we avoid init() per project convention; a package-
// level var with a helper is equivalent and testable.
var builtinFieldRank = func() map[string]int {
	m := make(map[string]int, len(builtinFieldOrder))
	for i, name := range builtinFieldOrder {
		m[name] = i
	}

	return m
}()

// collectColumns extracts column names in a deterministic order:
//  1. LynxDB built-in fields in canonical order (_time, _raw, index, source, …)
//  2. User-defined (schema-on-read) fields in alphabetical order
func collectColumns(rows []map[string]interface{}) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			seen[k] = struct{}{}
		}
	}

	// Partition into builtin (ordered) and user (alphabetical).
	builtins := make([]string, 0, len(builtinFieldOrder))
	user := make([]string, 0, len(seen))

	for col := range seen {
		if _, ok := builtinFieldRank[col]; ok {
			builtins = append(builtins, col)
		} else {
			user = append(user, col)
		}
	}

	// Sort builtins by their canonical rank.
	sort.Slice(builtins, func(i, j int) bool {
		return builtinFieldRank[builtins[i]] < builtinFieldRank[builtins[j]]
	})
	// Sort user fields alphabetically for determinism.
	sort.Strings(user)

	return append(builtins, user...)
}

func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}

		return fmt.Sprintf("%.4g", val)
	default:
		return fmt.Sprint(v)
	}
}

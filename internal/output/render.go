package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// Column is a column of tabular output: JSON names it by Key, a lowercase
// snake_case name, and every other format by Header.
type Column struct {
	Key    string
	Header string
}

// RenderTabular renders explicitly ordered row data using the same format
// selection as query results: JSON and NDJSON print one object per row.
func RenderTabular(w io.Writer, cols []Column, rows [][]any, format Format, opts HumanTableOptions) error {
	format = normalizeFormat(format)
	if format == FormatAuto && !isTTY(os.Stdout) {
		format = FormatJSON
	}
	keys := make([]string, len(cols))
	headers := make([]string, len(cols))
	for i, c := range cols {
		keys[i], headers[i] = c.Key, c.Header
	}
	switch format {
	case FormatJSON, FormatNDJSON:
		return renderOrderedJSON(w, keys, rows)
	case FormatCSV:
		return renderDelimited(w, headers, rows, ",")
	case FormatTSV:
		return renderDelimited(w, headers, rows, "\t")
	case FormatRaw:
		return renderRaw(w, rows)
	default:
		return renderOrderedTable(w, headers, rows, format, opts)
	}
}

func renderOrderedTable(w io.Writer, cols []string, rows [][]any, format Format, opts HumanTableOptions) error {
	theme := opts.Theme
	if theme == nil {
		theme = ui.NewTheme(w, true)
	}

	mapRows := rowsToMaps(cols, rows)
	kinds := inferColumnKinds(mapRows, cols)
	tbl := ui.NewTable(theme).
		SetColumns(cols...).
		SetColumnKinds(kinds...).
		SetCompact(opts.Compact).
		SetStyle(tableStyleForFormat(format, opts.Style)).
		SetMaxRows(opts.MaxRows).
		SetNullValue(opts.NullValue)
	if width := opts.effectiveWidth(); width > 0 {
		tbl.SetTerminalWidth(width)
	}

	for _, row := range rows {
		vals := make([]string, len(cols))
		for i := range cols {
			var v any
			if i < len(row) {
				v = row[i]
			}
			vals[i] = formatHumanValue(v, kinds[i], theme, opts.NullValue)
		}
		tbl.AddRow(vals...)
	}

	_, err := fmt.Fprint(w, tbl.String())

	return err
}

func renderOrderedJSON(w io.Writer, cols []string, rows [][]any) error {
	for _, row := range rows {
		if _, err := fmt.Fprint(w, "{"); err != nil {
			return err
		}
		for i, col := range cols {
			if i > 0 {
				if _, err := fmt.Fprint(w, ","); err != nil {
					return err
				}
			}
			key, err := json.Marshal(col)
			if err != nil {
				return err
			}
			var v any
			if i < len(row) {
				v = row[i]
			}
			value, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "%s:%s", key, value); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w, "}"); err != nil {
			return err
		}
	}

	return nil
}

func renderDelimited(w io.Writer, cols []string, rows [][]any, sep string) error {
	if sep == "," {
		cw := csv.NewWriter(w)
		if err := cw.Write(cols); err != nil {
			return err
		}
		for _, row := range rows {
			vals := make([]string, len(cols))
			for i := range cols {
				if i < len(row) {
					vals[i] = formatValue(row[i])
				}
			}
			if err := cw.Write(vals); err != nil {
				return err
			}
		}
		cw.Flush()

		return cw.Error()
	}

	if _, err := fmt.Fprintln(w, strings.Join(cols, sep)); err != nil {
		return err
	}
	for _, row := range rows {
		vals := make([]string, len(cols))
		for i := range cols {
			if i < len(row) {
				vals[i] = formatValue(row[i])
			}
		}
		if _, err := fmt.Fprintln(w, strings.Join(vals, sep)); err != nil {
			return err
		}
	}

	return nil
}

func renderRaw(w io.Writer, rows [][]any) error {
	for _, row := range rows {
		vals := make([]string, len(row))
		for i := range row {
			vals[i] = formatValue(row[i])
		}
		if _, err := fmt.Fprintln(w, strings.Join(vals, "\t")); err != nil {
			return err
		}
	}

	return nil
}

func rowsToMaps(cols []string, rows [][]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		m := make(map[string]any, len(cols))
		for i, col := range cols {
			if i < len(row) {
				m[col] = row[i]
			} else {
				m[col] = nil
			}
		}
		out = append(out, m)
	}

	return out
}

package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// RenderTabular renders explicitly ordered row data using the same format
// selection as query results.
func RenderTabular(w io.Writer, cols []string, rows [][]any, format Format, opts HumanTableOptions) error {
	format = normalizeFormat(format)
	if format == FormatAuto && !isTTY(os.Stdout) {
		return renderOrderedJSON(w, cols, rows)
	}
	switch format {
	case FormatJSON, FormatNDJSON:
		return renderOrderedJSON(w, cols, rows)
	case FormatCSV:
		return renderDelimited(w, cols, rows, ",")
	case FormatTSV:
		return renderDelimited(w, cols, rows, "\t")
	case FormatRaw:
		return renderRaw(w, rows)
	case FormatVertical, FormatLine, FormatG:
		return renderOrderedVertical(w, cols, rows, opts)
	default:
		return renderOrderedTable(w, cols, rows, format, opts)
	}
}

// RenderKeyValue renders a map payload as ordered KEY/VALUE rows. JSON keeps
// the original object shape; other formats use the shared tabular renderer.
func RenderKeyValue(w io.Writer, values map[string]any, format Format, opts HumanTableOptions) error {
	format = normalizeFormat(format)
	if format == FormatJSON || format == FormatNDJSON {
		enc := json.NewEncoder(w)
		return enc.Encode(values)
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	rows := make([][]any, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, []any{key, values[key]})
	}

	return RenderTabular(w, []string{"KEY", "VALUE"}, rows, format, opts)
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

func renderOrderedVertical(w io.Writer, cols []string, rows [][]any, opts HumanTableOptions) error {
	return (&VerticalFormatter{
		Theme:     opts.Theme,
		Compact:   opts.Compact,
		MaxRows:   opts.MaxRows,
		NullValue: opts.NullValue,
	}).Format(w, rowsToMaps(cols, rows))
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

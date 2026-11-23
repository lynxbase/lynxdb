package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
)

func humanOutputOptions(theme *ui.Theme) output.HumanTableOptions {
	return output.HumanTableOptions{
		Theme:     theme,
		Compact:   globalCompact,
		MaxRows:   globalMaxRows,
		NullValue: globalNullValue,
		MaxWidth:  globalMaxWidth,
	}
}

func renderTabular(w io.Writer, cols []output.Column, rows [][]any, theme *ui.Theme) error {
	return output.RenderTabular(w, cols, rows, output.Format(globalFormat), humanOutputOptions(theme))
}

// renderKeyValues renders key and value pairs as a two-column table.
func renderKeyValues(w io.Writer, rows [][2]any, theme *ui.Theme) error {
	tableRows := make([][]any, 0, len(rows))
	for _, row := range rows {
		tableRows = append(tableRows, []any{row[0], row[1]})
	}

	return renderTabular(w, []output.Column{{Key: "key", Header: "KEY"}, {Key: "value", Header: "VALUE"}}, tableRows, theme)
}

func humanOutputActive() bool {
	switch strings.ToLower(globalFormat) {
	case "", "auto":
		return isTTY()
	case "table", "markdown":
		return true
	default:
		return false
	}
}

// jsonOutput reports whether an inspection command prints JSON: with
// --format json or ndjson, or auto when stdout is not a terminal.
func jsonOutput() bool {
	switch globalFormat {
	case formatJSON, formatNDJSON:
		return true
	case "", "auto":
		return !isTTY()
	default:
		return false
	}
}

// printJSON writes v to stdout as one line of JSON.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)

	return enc.Encode(v)
}

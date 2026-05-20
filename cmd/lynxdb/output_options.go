package main

import (
	"io"
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

func renderTabular(w io.Writer, cols []string, rows [][]any, theme *ui.Theme) error {
	return output.RenderTabular(w, cols, rows, output.Format(globalFormat), humanOutputOptions(theme))
}

func renderKeyValues(w io.Writer, rows [][2]any, theme *ui.Theme) error {
	tableRows := make([][]any, 0, len(rows))
	for _, row := range rows {
		tableRows = append(tableRows, []any{row[0], row[1]})
	}

	return renderTabular(w, []string{"KEY", "VALUE"}, tableRows, theme)
}

func humanOutputActive() bool {
	switch strings.ToLower(globalFormat) {
	case "", "auto":
		return isTTY()
	case "table", "box", "ascii", "markdown", "vertical", "line", "g":
		return true
	default:
		return false
	}
}

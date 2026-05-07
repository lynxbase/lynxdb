package sigmaqueries

import (
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestParseEveryGoldenLine(t *testing.T) {
	walkGoldenFixtures(t, func(t *testing.T, fixture, line string, lineNo int) {
		prog, err := spl2.ParseProgram(line)
		if err != nil {
			t.Fatalf("ParseProgram(%s:%d): %v\nSPL2: %s", fixture, lineNo, err, line)
		}

		rendered := renderProgram(prog)
		reparsed, err := spl2.ParseProgram(rendered)
		if err != nil {
			t.Fatalf("reparse rendered SPL2 for %s:%d: %v\nSPL2: %s\nrendered: %s", fixture, lineNo, err, line, rendered)
		}

		if got, want := renderProgram(reparsed), rendered; got != want {
			t.Fatalf("String() is not idempotent for %s:%d\nSPL2: %s\nfirst:  %s\nsecond: %s", fixture, lineNo, line, want, got)
		}
	})
}

func renderProgram(prog *spl2.Program) string {
	parts := make([]string, 0, len(prog.Datasets)+1)
	for _, ds := range prog.Datasets {
		parts = append(parts, "$"+ds.Name+" = "+renderQuery(ds.Query))
	}
	parts = append(parts, renderQuery(prog.Main))

	return strings.Join(parts, "; ")
}

func renderQuery(query *spl2.Query) string {
	parts := make([]string, 0, len(query.Commands)+1)
	if query.Source != nil {
		parts = append(parts, "FROM "+renderSource(query.Source))
	}
	for _, cmd := range query.Commands {
		parts = append(parts, cmd.String())
	}

	return strings.Join(parts, " | ")
}

func renderSource(source *spl2.SourceClause) string {
	if source.IsVariable {
		return "$" + source.Index
	}
	if len(source.Indices) > 0 {
		return strings.Join(source.Indices, ", ")
	}

	return source.Index
}

package parser

import (
	"strings"
	"testing"
)

func TestLimits(t *testing.T) {
	nested := func(n int) string {
		return "source=t | where " + strings.Repeat("(", n) + "x" + strings.Repeat(")", n) + " > 1"
	}
	pipeline := func(n int) string { return "source=t" + strings.Repeat(" | head 1", n) }
	cases := []struct {
		name, q, err string
	}{
		{"nesting within the bound", nested(MaxDepth - 3), ""},
		{"nesting beyond the bound", nested(MaxDepth + 1), "nests deeper than 256 levels"},
		{"deep nesting", nested(1 << 16), "nests deeper than 256 levels"},
		{"not chain", "source=t | where " + strings.Repeat("not ", 1<<14) + "x", "nests deeper"},
		{"sign chain", "source=t | eval y = " + strings.Repeat("- ", 1<<16) + "x", "nests deeper"},
		{"search not chain", "search source=t " + strings.Repeat("NOT ", 1<<14) + "x", "nests deeper"},
		{"search parens", "search source=t " + strings.Repeat("(", 1<<16) + "x", "nests deeper"},
		{"subsearches", "source=t | where x in " + strings.Repeat("[ source=u | where y in ", 300) + "[ source=v ]", "nests deeper"},
		{"commands within the bound", pipeline(MaxCommands - 1), ""},
		{"commands beyond the bound", pipeline(MaxCommands), "more than 1000 commands"},
		{"length beyond the bound", "source=t | where x = '" + strings.Repeat("a", MaxQueryBytes) + "'", "longer than 262144 bytes"},
	}
	for _, c := range cases {
		_, err := Parse(c.q)
		switch {
		case c.err == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%s: got %v, want an error containing %q", c.name, err, c.err)
		}
	}
}

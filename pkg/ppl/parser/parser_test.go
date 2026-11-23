package parser

import (
	"strings"
	"testing"
)

// TestCommandNames pins commandNames to the names command parses.
func TestCommandNames(t *testing.T) {
	for name := range commandNames {
		_, err := Parse("source=t | " + name)
		if err != nil && strings.Contains(err.Error(), "unknown command") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestLeadingPipe(t *testing.T) {
	cases := []struct {
		q, err string
	}{
		{"| search source=t error", ""},
		{"| source=t | head 1", ""},
		{"| makeresults count=1", ""},
		{"| union [search source=a] [search source=b]", ""},
		{"| tstats count where index=main by host", `column 3: unknown command "tstats"`},
		{"| error", `unknown command "error"`},
		{"| 'quoted'", "expected a command name"},
		{"| where x > 1", "column 1: missing source"},
		{"source=t | where x in [ | tstats count ]", `unknown command "tstats"`},
	}
	for _, c := range cases {
		_, err := Parse(c.q)
		switch {
		case c.err == "" && err != nil:
			t.Errorf("%q: %v", c.q, err)
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%q: got %v, want an error containing %q", c.q, err, c.err)
		}
	}
}

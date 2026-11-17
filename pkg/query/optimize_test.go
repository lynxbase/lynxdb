package query

import (
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

func optimized(t *testing.T, q string) string {
	t.Helper()
	c, err := Compile(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return ir.Render(c.Plan)
}

func TestOptimizerRules(t *testing.T) {
	cases := []struct {
		q    string
		want []string
	}{
		{"search source=logs error status=500",
			[]string{`prune=SEARCH_WORD(_raw, "error")`, "Filter("}},
		{"source=logs | where host = 'web-1'",
			[]string{`prune=host = "web-1"`}},
		{"source=logs | where _time >= '2024-01-01 00:00:00' and _time < '2024-01-02 00:00:00'",
			[]string{"time=[2024-01-01T00:00:00Z, 2024-01-02T00:00:00Z)"}},
		{"source=logs | sort - bytes | head 5",
			[]string{"TopN(5, bytes DESC NULLS LAST)"}},
		{"source=logs | fields host | head 3",
			[]string{"limit=3", "columns=[_time, host]"}},
		{"source=logs | where 1 + 1 = 2 | stats count() by host",
			[]string{"columns=[_time, host]"}},
		{"source=logs | eval x = abs(-3) * 2",
			[]string{"x = 6"}},
	}
	for _, c := range cases {
		got := optimized(t, c.q)
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: plan lacks %q:\n%s", c.q, w, got)
			}
		}
	}
}

func TestOptimizerKeepsVolatileCalls(t *testing.T) {
	got := optimized(t, "source=logs | eval r = rand(), n = now()")
	if !strings.Contains(got, "rand()") || !strings.Contains(got, "now()") {
		t.Fatalf("volatile or query-constant calls were folded:\n%s", got)
	}
}

// TestOptionsNoEngineRunsFailToCompile pins options that parse but that no
// engine executes: they fail in Compile, before explain or a run.
func TestOptionsNoEngineRunsFailToCompile(t *testing.T) {
	for q, option := range map[string]string{
		"source=logs | patterns _raw method=brain": "method=brain",
	} {
		_, err := Compile(q)
		pe, ok := ppl.AsError(err)
		if !ok || pe.Kind != ppl.ErrUnsupportedFeature || !strings.Contains(pe.Msg, option) || !strings.Contains(pe.Msg, "not supported") {
			t.Errorf("%s: got %v, want an unsupported_feature error naming %s", q, err, option)
		}
	}
}

func TestUnsupportedCommandsFailExplicitly(t *testing.T) {
	queries := map[string]string{
		"fieldformat":      "source=t | fieldformat x = 1",
		"foreach":          "source=t | foreach [ eval x = 1 ]",
		"timewrap":         "source=t | timewrap 1d",
		"graphlookup":      "source=t | graphlookup x",
		"rest":             "rest '/_cat/indices'",
		"show datasources": "show datasources",
		"ml":               "source=t | ml action='train'",
		"kmeans":           "source=t | kmeans centroids=3",
		"ad":               "source=t | ad",
	}
	for _, c := range ppl.Commands() {
		if c.Support != ppl.Unsupported {
			continue
		}
		q, ok := queries[c.Name]
		if !ok {
			t.Errorf("no probe query for unsupported command %s", c.Name)
			continue
		}
		_, err := Compile(q)
		pe, isPPL := ppl.AsError(err)
		if !isPPL || pe.Kind != ppl.ErrUnsupportedCommand {
			t.Errorf("%s: got %v, want an unsupported_command error", q, err)
		}
	}
}

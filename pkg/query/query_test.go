package query

import (
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ppl"
)

// TestCorrelatedSubqueriesFailToCompile pins subqueries that read a field of
// the query around them: they fail to compile instead of matching nothing.
func TestCorrelatedSubqueriesFailToCompile(t *testing.T) {
	for q, field := range map[string]string{
		"source=logs | where exists [ source=users | where host = logs.host ] | fields host":                                   "logs.host",
		"source=logs | eval top = 100 | where host in [ source=users | fields host, owner | where owner = top | fields host ]": "field top",
	} {
		_, err := Compile(q)
		pe, ok := ppl.AsError(err)
		if !ok || pe.Kind != ppl.ErrUnsupportedFeature || !strings.Contains(pe.Msg, "correlated subqueries are not supported") || !strings.Contains(pe.Msg, field) {
			t.Errorf("%s: got %v, want an unsupported_feature error naming %s", q, err, field)
		}
	}
}

// TestProjectingEventColumnsKeepsOnlyThem pins that a projection of _time or
// _raw plans those columns and no event metadata.
func TestProjectingEventColumnsKeepsOnlyThem(t *testing.T) {
	for q, want := range map[string]string{
		"source=logs | fields _time":       "_time",
		"source=logs | fields _raw":        "_raw",
		"source=logs | fields _raw, _time": "_raw _time",
		"source=logs | table _time, host":  "_time host",
	} {
		c, err := Compile(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := strings.Join(c.Plan.Root.Schema().Names(), " "); got != want {
			t.Errorf("%s: plans columns %s, want %s", q, got, want)
		}
	}
}

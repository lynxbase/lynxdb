package planner

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/model"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

func plan(t *testing.T, q string) *PlanResult {
	t.Helper()
	res, err := New().Plan(PlanRequest{Query: q})
	if err != nil {
		t.Fatalf("Plan(%q): %v", q, err)
	}
	return res
}

func TestPlan_SingleSourceHints(t *testing.T) {
	res := plan(t, "search source=web earliest=-1h error | where host = \"a\" | head 5")
	h := res.Hints
	if h.IndexName != "web" || h.SourceScopeType != model.SourceScopeSingle {
		t.Errorf("source scope = %q %q", h.IndexName, h.SourceScopeType)
	}
	if h.TimeBounds == nil || h.TimeBounds.Earliest.IsZero() {
		t.Errorf("time bounds = %+v, want an earliest bound", h.TimeBounds)
	}
	if len(h.SearchTerms) != 1 || h.SearchTerms[0] != "error" {
		t.Errorf("search terms = %v, want [error]", h.SearchTerms)
	}
	if len(h.FieldPredicates) != 1 || h.FieldPredicates[0].Field != "host" {
		t.Errorf("field predicates = %+v", h.FieldPredicates)
	}
	if !res.SkipResultCache {
		t.Error("a relative time bound must skip the result cache")
	}
}

func TestPlan_SourceScopes(t *testing.T) {
	tests := []struct {
		query string
		scope string
	}{
		{"source=a, b", model.SourceScopeList},
		{"source=web*", model.SourceScopeGlob},
		{"source=*", model.SourceScopeAll},
	}
	for _, tt := range tests {
		if got := plan(t, tt.query).Hints.SourceScopeType; got != tt.scope {
			t.Errorf("%q: scope = %q, want %q", tt.query, got, tt.scope)
		}
	}
}

func TestPlan_SeveralScansKeepOnlySourceUnion(t *testing.T) {
	res := plan(t, "search source=a error | append [ search source=b warn ]")
	h := res.Hints
	if len(h.SearchTerms) != 0 {
		t.Errorf("search terms = %v; terms of one scan must not filter the other", h.SearchTerms)
	}
	if h.SourceScopeType != model.SourceScopeList || len(h.SourceScopeSources) != 2 {
		t.Errorf("scope = %q %v, want both sources", h.SourceScopeType, h.SourceScopeSources)
	}
}

func TestPlan_ExternalTimeBounds(t *testing.T) {
	res, err := New().Plan(PlanRequest{Query: "source=main", From: "-15m", To: "now"})
	if err != nil {
		t.Fatal(err)
	}
	if res.ExternalTimeBounds == nil || res.ExternalTimeBounds.Earliest.IsZero() {
		t.Fatalf("external bounds = %+v", res.ExternalTimeBounds)
	}
	if _, err := New().Plan(PlanRequest{Query: "source=main", From: "not-a-time"}); !IsQueryError(err) {
		t.Fatalf("invalid time range: err = %v, want a planning error", err)
	}
}

func TestPlan_CompileErrorsKeepTheirKind(t *testing.T) {
	_, err := New().Plan(PlanRequest{Query: "source=main | where"})
	pe, ok := ppl.AsError(err)
	if !ok || pe.Kind != ppl.ErrSyntax {
		t.Fatalf("err = %v, want a syntax error", err)
	}
	if !IsQueryError(err) {
		t.Error("IsQueryError = false for a syntax error")
	}
}

func TestUsesCurrentTime(t *testing.T) {
	if plan(t, "source=main | stats count()").SkipResultCache {
		t.Error("a query without time functions should be cacheable")
	}
	if !plan(t, "source=main | eval t = now()").SkipResultCache {
		t.Error("now() makes the result depend on the time of execution")
	}
}

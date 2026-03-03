package planner

import (
	"errors"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/server"
)

func TestPlan_ValidQuery(t *testing.T) {
	p := New()
	plan, err := p.Plan(PlanRequest{Query: "search index=main error"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Program == nil {
		t.Fatal("expected non-nil Program")
	}
	if plan.ResultType != server.ResultTypeEvents {
		t.Errorf("expected ResultTypeEvents, got %s", plan.ResultType)
	}
	if plan.Hints == nil {
		t.Fatal("expected non-nil Hints")
	}
	if plan.RawQuery != "FROM main | search index=main error" {
		t.Errorf("expected normalized raw query, got %q", plan.RawQuery)
	}
}

func TestPlan_AggregateQuery(t *testing.T) {
	p := New()
	plan, err := p.Plan(PlanRequest{Query: "search index=main | stats count by host"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.ResultType != server.ResultTypeAggregate {
		t.Errorf("expected ResultTypeAggregate, got %s", plan.ResultType)
	}
}

func TestPlan_ParseError(t *testing.T) {
	p := New()
	_, err := p.Plan(PlanRequest{Query: "|||invalid"})
	if err == nil {
		t.Fatal("expected error for invalid query")
	}
	if !IsParseError(err) {
		t.Errorf("expected ParseError, got %T: %v", err, err)
	}
	pe := func() *ParseError {
		target := &ParseError{}
		_ = errors.As(err, &target)

		return target
	}()
	if pe.Message == "" {
		t.Error("expected non-empty error message")
	}
}

func TestPlan_TimeBounds(t *testing.T) {
	p := New()
	plan, err := p.Plan(PlanRequest{
		Query: "search error",
		From:  "-1h",
		To:    "now",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.ExternalTimeBounds == nil {
		t.Fatal("expected non-nil ExternalTimeBounds")
	}
	if plan.ExternalTimeBounds.Earliest.IsZero() {
		t.Error("expected non-zero Earliest")
	}
}

func TestPlan_NoTimeBounds(t *testing.T) {
	p := New()
	plan, err := p.Plan(PlanRequest{Query: "search error"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.ExternalTimeBounds != nil {
		t.Error("expected nil ExternalTimeBounds when From/To are empty")
	}
}

func TestPlan_ImplicitSearch(t *testing.T) {
	p := New()

	// Bare "level=error | stats count" should be normalized to "search level=error | stats count".
	plan, err := p.Plan(PlanRequest{Query: "level=error | stats count"})
	if err != nil {
		t.Fatalf("unexpected error for implicit search: %v", err)
	}
	if plan.Program == nil {
		t.Fatal("expected non-nil Program")
	}
	if plan.ResultType != server.ResultTypeAggregate {
		t.Errorf("expected ResultTypeAggregate, got %s", plan.ResultType)
	}
	// RawQuery should contain the normalized form with FROM main prepended.
	if plan.RawQuery != "FROM main | search level=error | stats count" {
		t.Errorf("expected normalized RawQuery, got %q", plan.RawQuery)
	}
}

func TestPlan_ImplicitSearchPreservesExplicit(t *testing.T) {
	p := New()

	// Explicit "search" keyword should get FROM main prepended.
	plan, err := p.Plan(PlanRequest{Query: "search level=error"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.RawQuery != "FROM main | search level=error" {
		t.Errorf("expected normalized raw query, got %q", plan.RawQuery)
	}
}

func TestPlan_OptimizerStats(t *testing.T) {
	p := New()
	plan, err := p.Plan(PlanRequest{Query: "search index=main | stats count"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.OptimizerStats == nil {
		t.Fatal("expected non-nil OptimizerStats")
	}
}

func TestPlan_TimingAndRuleDetails(t *testing.T) {
	p := New()
	plan, err := p.Plan(PlanRequest{Query: "search index=main error | stats count by host"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ParseDuration and OptimizeDuration should be non-zero for a real parse+optimize.
	if plan.ParseDuration <= 0 {
		t.Error("expected positive ParseDuration")
	}
	if plan.OptimizeDuration <= 0 {
		t.Error("expected positive OptimizeDuration")
	}
	// TotalRules should reflect the number of registered optimizer rules.
	if plan.TotalRules == 0 {
		t.Error("expected non-zero TotalRules")
	}
	// RuleDetails should be populated if any rules fired.
	if len(plan.OptimizerStats) > 0 && len(plan.RuleDetails) == 0 {
		t.Error("expected non-empty RuleDetails when OptimizerStats has entries")
	}
	// Verify RuleDetails match OptimizerStats.
	for _, rd := range plan.RuleDetails {
		if rd.Name == "" {
			t.Error("expected non-empty rule name in RuleDetails")
		}
		if rd.Count <= 0 {
			t.Errorf("expected positive count for rule %q", rd.Name)
		}
		if count, ok := plan.OptimizerStats[rd.Name]; !ok || count != rd.Count {
			t.Errorf("RuleDetail %q count %d doesn't match OptimizerStats %d", rd.Name, rd.Count, count)
		}
	}
}

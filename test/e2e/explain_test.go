//go:build e2e

package e2e

import (
	"context"
	"testing"
)

func TestE2E_Explain_ValidQuery_IsValid(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	result, err := h.Client().Explain(ctx, `source=main | stats count()`)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !result.IsValid {
		t.Errorf("expected IsValid=true for valid query, got false; errors=%v", result.Errors)
	}
	if result.Parsed == nil {
		t.Error("expected non-nil Parsed for valid query")
	}
}

func TestE2E_Explain_InvalidQuery_HasErrors(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	result, err := h.Client().Explain(ctx, `INVALID QUERY !!!`)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if result.IsValid {
		t.Error("expected IsValid=false for invalid query")
	}
	if len(result.Errors) == 0 {
		t.Error("expected non-empty errors for invalid query")
	}
}

func TestE2E_Explain_ComplexQuery_HasPlans(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	result, err := h.Client().Explain(ctx, `source=main | where host = "web-01" | stats count() as count by host | sort -count | head 3`)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !result.IsValid {
		t.Fatalf("expected valid query, errors=%v", result.Errors)
	}
	if result.Parsed == nil {
		t.Fatal("expected non-nil Parsed")
	}
	if result.Parsed.LogicalPlan == "" {
		t.Error("expected a non-empty logical plan")
	}
	if result.Parsed.OptimizedPlan == "" {
		t.Error("expected a non-empty optimized plan")
	}
	t.Logf("result_type: %s\noptimized plan:\n%s", result.Parsed.ResultType, result.Parsed.OptimizedPlan)
}

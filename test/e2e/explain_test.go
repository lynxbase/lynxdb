//go:build e2e

package e2e

import (
	"context"
	"testing"
)

func TestE2E_Explain_ValidQuery_IsValid(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	result, err := h.Client().Explain(ctx, `FROM main | stats count`)
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

func TestE2E_Explain_ComplexQuery_HasPipeline(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	result, err := h.Client().Explain(ctx, `FROM main | WHERE host="web-01" | stats count by host | sort -count | head 3`)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if !result.IsValid {
		t.Fatalf("expected valid query, errors=%v", result.Errors)
	}
	if result.Parsed == nil {
		t.Fatal("expected non-nil Parsed")
	}
	if len(result.Parsed.Pipeline) == 0 {
		t.Error("expected non-empty pipeline stages")
	}
	t.Logf("pipeline stages: %d, result_type: %s", len(result.Parsed.Pipeline), result.Parsed.ResultType)
}

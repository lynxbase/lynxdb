package usecases

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/planner"
)

func explain(t *testing.T, q string) *ExplainResult {
	t.Helper()
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})
	result, err := svc.Explain(context.Background(), ExplainRequest{Query: q})
	if err != nil {
		t.Fatalf("Explain(%q): %v", q, err)
	}
	return result
}

func TestExplain_ValidQuery(t *testing.T) {
	result := explain(t, "source=main | head 100")
	if !result.IsValid || result.Parsed == nil {
		t.Fatalf("expected a valid query, got %+v", result)
	}
	if result.Parsed.ResultType != "events" {
		t.Errorf("ResultType = %s, want events", result.Parsed.ResultType)
	}
	if !strings.Contains(result.Parsed.LogicalPlan, "Scan(main)") {
		t.Errorf("logical plan lacks the scan:\n%s", result.Parsed.LogicalPlan)
	}
	if !strings.Contains(result.Parsed.OptimizedPlan, "limit=100") {
		t.Errorf("optimized plan does not push the limit into the scan:\n%s", result.Parsed.OptimizedPlan)
	}
}

func TestExplain_AggregateQuery(t *testing.T) {
	result := explain(t, "source=main | stats count() as count by host")
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	if result.Parsed.ResultType != "aggregate" {
		t.Errorf("ResultType = %s, want aggregate", result.Parsed.ResultType)
	}
}

func TestExplain_InvalidQuery(t *testing.T) {
	result := explain(t, "source=main | where")
	if result.IsValid {
		t.Fatal("expected an invalid query")
	}
	if len(result.Errors) == 0 {
		t.Fatal("expected errors")
	}
	e := result.Errors[0]
	if e.Kind != "syntax" || e.Line != 1 || e.Column == 0 {
		t.Errorf("error = %+v, want a syntax error with a position", e)
	}
}

func TestExplain_CostEstimation(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{"source=* | head 1000", "high"},
		{"search source=main error | head 1000", "medium"},
		{"search source=main earliest=-1h error", "low"},
	}
	for _, tt := range tests {
		result := explain(t, tt.query)
		if !result.IsValid {
			t.Fatalf("%q: invalid: %+v", tt.query, result.Errors)
		}
		if result.Parsed.EstimatedCost != tt.want {
			t.Errorf("%q: cost = %s, want %s", tt.query, result.Parsed.EstimatedCost, tt.want)
		}
	}
}

func TestHistogram_ValidationErrors(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	_, err := svc.Histogram(context.Background(), HistogramRequest{From: "not-a-date", To: "now"})
	if !errors.Is(err, ErrInvalidFrom) {
		t.Errorf("expected ErrInvalidFrom, got: %v", err)
	}
	_, err = svc.Histogram(context.Background(), HistogramRequest{From: "-1h", To: "not-a-date"})
	if !errors.Is(err, ErrInvalidTo) {
		t.Errorf("expected ErrInvalidTo, got: %v", err)
	}
	_, err = svc.Histogram(context.Background(), HistogramRequest{
		From: "2025-01-02T00:00:00Z",
		To:   "2025-01-01T00:00:00Z",
	})
	if !errors.Is(err, ErrFromBeforeTo) {
		t.Errorf("expected ErrFromBeforeTo, got: %v", err)
	}
}

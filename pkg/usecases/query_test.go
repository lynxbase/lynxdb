package usecases

import (
	"context"
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/planner"
)

func TestExplain_ValidQuery(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: "search index=main error",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	if result.Parsed == nil {
		t.Fatal("expected Parsed to be non-nil")
	}
	if result.Parsed.ResultType != "events" {
		t.Errorf("expected events, got %s", result.Parsed.ResultType)
	}
	if len(result.Parsed.Pipeline) == 0 {
		t.Error("expected non-empty pipeline stages")
	}
}

func TestExplain_AggregateQuery(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: "search index=main | stats count by host",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	if result.Parsed.ResultType != "aggregate" {
		t.Errorf("expected aggregate, got %s", result.Parsed.ResultType)
	}
}

func TestExplain_InvalidQuery(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: "|||invalid",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsValid {
		t.Fatal("expected invalid query")
	}
	if len(result.Errors) == 0 {
		t.Error("expected at least one error")
	}
}

func TestExplain_CostEstimation(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	tests := []struct {
		name  string
		query string
		cost  string
	}{
		{"high cost (full scan)", "search *", "high"},
		{"medium cost (search terms)", "search error warning", "medium"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := svc.Explain(context.Background(), ExplainRequest{Query: tt.query})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !result.IsValid {
				t.Fatal("expected query to parse successfully, but IsValid=false")
			}
			if result.Parsed.EstimatedCost != tt.cost {
				t.Errorf("expected cost %q, got %q", tt.cost, result.Parsed.EstimatedCost)
			}
		})
	}
}

// E2: Physical plan tests

func TestExplain_PhysicalPlan_CountStar(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: "search index=main | stats count",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	if result.Parsed.PhysicalPlan == nil {
		t.Fatal("expected PhysicalPlan to be non-nil for count(*) query")
	}
	if !result.Parsed.PhysicalPlan.CountStarOnly {
		t.Error("expected CountStarOnly=true")
	}
}

func TestExplain_PhysicalPlan_PartialAgg(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	// The aggregation pushdown rule requires a source clause on the query AST.
	// "from main | stats count by host" ensures the parser sets Query.Source.
	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: "from main | stats count by host",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	if result.Parsed.PhysicalPlan == nil {
		t.Fatal("expected PhysicalPlan to be non-nil for stats+groupby query")
	}
	if !result.Parsed.PhysicalPlan.PartialAgg {
		t.Error("expected PartialAgg=true")
	}
}

func TestExplain_PhysicalPlan_TopKAgg(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	// The topK rule needs stats+sort+head in sequence. The earlyLimitRule may
	// convert sort+head into topn, so topKAgg must fire first (it's ordered before earlyLimit).
	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: "from main | stats count by host | sort -count | head 10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	if result.Parsed.PhysicalPlan == nil {
		t.Fatal("expected PhysicalPlan to be non-nil for topK query")
	}
	if !result.Parsed.PhysicalPlan.TopKAgg {
		t.Error("expected TopKAgg=true")
	}
	if result.Parsed.PhysicalPlan.TopK != 10 {
		t.Errorf("expected TopK=10, got %d", result.Parsed.PhysicalPlan.TopK)
	}
}

func TestExplain_PhysicalPlan_NilForSimpleQuery(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: "search index=main error | head 100",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}
	// Simple search+head has no optimizer annotations -> nil physical plan.
	if result.Parsed.PhysicalPlan != nil {
		t.Errorf("expected nil PhysicalPlan for simple query, got %+v", result.Parsed.PhysicalPlan)
	}
}

// U3: Sentinel error tests

func TestHistogram_ValidationErrors(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	_, err := svc.Histogram(context.Background(), HistogramRequest{
		From: "not-a-date",
		To:   "now",
	})
	if err == nil {
		t.Fatal("expected error for invalid from")
	}
	if !errors.Is(err, ErrInvalidFrom) {
		t.Errorf("expected ErrInvalidFrom, got: %v", err)
	}

	_, err = svc.Histogram(context.Background(), HistogramRequest{
		From: "-1h",
		To:   "not-a-date",
	})
	if err == nil {
		t.Fatal("expected error for invalid to")
	}
	if !errors.Is(err, ErrInvalidTo) {
		t.Errorf("expected ErrInvalidTo, got: %v", err)
	}

	_, err = svc.Histogram(context.Background(), HistogramRequest{
		From: "2025-01-02T00:00:00Z",
		To:   "2025-01-01T00:00:00Z",
	})
	if err == nil {
		t.Fatal("expected error for from > to")
	}
	if !errors.Is(err, ErrFromBeforeTo) {
		t.Errorf("expected ErrFromBeforeTo, got: %v", err)
	}
}

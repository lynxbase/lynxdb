package usecases

import (
	"context"
	"errors"
	"reflect"
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

// --- Pipeline field tracking tests ---

func TestExplain_FieldTracking_SourceStage(t *testing.T) {
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
	if len(result.Parsed.Pipeline) < 2 {
		t.Fatalf("expected at least 2 stages (source + search), got %d", len(result.Parsed.Pipeline))
	}

	// First stage is always the synthetic source stage.
	source := result.Parsed.Pipeline[0]
	if source.Command != "source" {
		t.Errorf("expected first stage command='source', got %q", source.Command)
	}
	if !source.FieldsUnknown {
		t.Error("expected source stage to have FieldsUnknown=true")
	}
	if !reflect.DeepEqual(source.FieldsAdded, []string{"_time", "_raw", "_source"}) {
		t.Errorf("expected source FieldsAdded=[_time, _raw, _source], got %v", source.FieldsAdded)
	}
	if !reflect.DeepEqual(source.FieldsOut, []string{"_time", "_raw", "_source"}) {
		t.Errorf("expected source FieldsOut=[_time, _raw, _source], got %v", source.FieldsOut)
	}
}

func TestExplain_FieldTracking_StatsReplacesFields(t *testing.T) {
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

	// Find the stats stage (should be the last stage).
	var statsStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "stats" {
			statsStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if statsStage == nil {
		t.Fatal("expected a stats stage in the pipeline")
	}

	// Stats replaces the field set entirely: groupby fields + agg outputs.
	expectedOut := []string{"host", "count"}
	if !reflect.DeepEqual(statsStage.FieldsOut, expectedOut) {
		t.Errorf("expected stats FieldsOut=%v, got %v", expectedOut, statsStage.FieldsOut)
	}
	if statsStage.FieldsUnknown {
		t.Error("expected stats FieldsUnknown=false (stats produces known fields)")
	}

	// Stats should report the previous fields as removed.
	if len(statsStage.FieldsRemoved) == 0 {
		t.Error("expected stats to report removed fields (the prior source fields)")
	}
}

func TestExplain_FieldTracking_EvalAddsFields(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: `search index=main | eval duration_s=duration/1000`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}

	// Find the eval stage.
	var evalStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "eval" {
			evalStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if evalStage == nil {
		t.Fatal("expected an eval stage in the pipeline")
	}

	// Eval adds "duration_s" to the field set.
	if !reflect.DeepEqual(evalStage.FieldsAdded, []string{"duration_s"}) {
		t.Errorf("expected eval FieldsAdded=[duration_s], got %v", evalStage.FieldsAdded)
	}
	if len(evalStage.FieldsRemoved) > 0 {
		t.Errorf("expected eval to not remove fields, got %v", evalStage.FieldsRemoved)
	}

	// The field should appear in FieldsOut.
	found := false
	for _, f := range evalStage.FieldsOut {
		if f == "duration_s" {
			found = true

			break
		}
	}
	if !found {
		t.Errorf("expected duration_s in FieldsOut, got %v", evalStage.FieldsOut)
	}
}

func TestExplain_FieldTracking_FieldsRemove(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: `search index=main | fields - _raw`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}

	// Find the fields stage.
	var fieldsStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "fields" {
			fieldsStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if fieldsStage == nil {
		t.Fatal("expected a fields stage in the pipeline")
	}

	// "fields - _raw" removes _raw.
	if !reflect.DeepEqual(fieldsStage.FieldsRemoved, []string{"_raw"}) {
		t.Errorf("expected fields FieldsRemoved=[_raw], got %v", fieldsStage.FieldsRemoved)
	}

	// _raw should not be in FieldsOut.
	for _, f := range fieldsStage.FieldsOut {
		if f == "_raw" {
			t.Error("expected _raw to be removed from FieldsOut")

			break
		}
	}
}

func TestExplain_FieldTracking_TableKeepsOnly(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: `search index=main | table _time, host, level`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}

	// Find the table stage.
	var tableStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "table" {
			tableStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if tableStage == nil {
		t.Fatal("expected a table stage in the pipeline")
	}

	// Table keeps only the listed fields.
	expectedOut := []string{"_time", "host", "level"}
	if !reflect.DeepEqual(tableStage.FieldsOut, expectedOut) {
		t.Errorf("expected table FieldsOut=%v, got %v", expectedOut, tableStage.FieldsOut)
	}
	if tableStage.FieldsUnknown {
		t.Error("expected table FieldsUnknown=false")
	}
}

func TestExplain_FieldTracking_MultiStage(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	// Multi-stage pipeline: source -> search -> eval -> stats -> head
	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: `search index=main | eval err=level="error" | stats count by host | head 5`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}

	// The pipeline should have at least 5 stages: source, search, eval, stats, head.
	// Note: The optimizer may rewrite some stages (e.g., sort+head -> topn), so
	// we check minimum count.
	if len(result.Parsed.Pipeline) < 4 {
		t.Fatalf("expected at least 4 stages, got %d", len(result.Parsed.Pipeline))
	}

	// After stats, the fields should be exactly [host, count].
	var statsStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "stats" {
			statsStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if statsStage == nil {
		t.Fatal("expected a stats stage")
	}
	expectedStatsOut := []string{"host", "count"}
	if !reflect.DeepEqual(statsStage.FieldsOut, expectedStatsOut) {
		t.Errorf("expected stats FieldsOut=%v, got %v", expectedStatsOut, statsStage.FieldsOut)
	}

	// The last stage (head) should preserve the same fields.
	lastStage := result.Parsed.Pipeline[len(result.Parsed.Pipeline)-1]
	if !reflect.DeepEqual(lastStage.FieldsOut, expectedStatsOut) {
		t.Errorf("expected last stage FieldsOut=%v, got %v", expectedStatsOut, lastStage.FieldsOut)
	}
}

func TestExplain_FieldTracking_RexAddsNamedGroups(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: `search index=main | rex field=_raw "host=(?P<host>\S+) status=(?P<status>\d+)"`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}

	// Find the rex stage.
	var rexStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "rex" {
			rexStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if rexStage == nil {
		t.Fatal("expected a rex stage in the pipeline")
	}

	// Rex adds named groups to the field set.
	if !reflect.DeepEqual(rexStage.FieldsAdded, []string{"host", "status"}) {
		t.Errorf("expected rex FieldsAdded=[host, status], got %v", rexStage.FieldsAdded)
	}
}

func TestExplain_FieldTracking_RenameSwapsFields(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: `search index=main | rename _source as origin`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}

	// Find the rename stage.
	var renameStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "rename" {
			renameStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if renameStage == nil {
		t.Fatal("expected a rename stage in the pipeline")
	}

	if !reflect.DeepEqual(renameStage.FieldsAdded, []string{"origin"}) {
		t.Errorf("expected rename FieldsAdded=[origin], got %v", renameStage.FieldsAdded)
	}
	if !reflect.DeepEqual(renameStage.FieldsRemoved, []string{"_source"}) {
		t.Errorf("expected rename FieldsRemoved=[_source], got %v", renameStage.FieldsRemoved)
	}

	// _source should not be in FieldsOut, origin should be.
	hasSource := false
	hasOrigin := false
	for _, f := range renameStage.FieldsOut {
		if f == "_source" {
			hasSource = true
		}
		if f == "origin" {
			hasOrigin = true
		}
	}
	if hasSource {
		t.Error("expected _source to be removed from FieldsOut")
	}
	if !hasOrigin {
		t.Error("expected origin in FieldsOut")
	}
}

func TestExplain_FieldTracking_TopReplaces(t *testing.T) {
	svc := NewQueryService(planner.New(), nil, config.QueryConfig{})

	result, err := svc.Explain(context.Background(), ExplainRequest{
		Query: `search index=main | top 10 host`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsValid {
		t.Fatal("expected valid query")
	}

	// Find the top stage.
	var topStage *PipelineStage
	for i := range result.Parsed.Pipeline {
		if result.Parsed.Pipeline[i].Command == "top" {
			topStage = &result.Parsed.Pipeline[i]

			break
		}
	}
	if topStage == nil {
		t.Fatal("expected a top stage in the pipeline")
	}

	// Top replaces the field set with: field, count, percent.
	expectedOut := []string{"host", "count", "percent"}
	if !reflect.DeepEqual(topStage.FieldsOut, expectedOut) {
		t.Errorf("expected top FieldsOut=%v, got %v", expectedOut, topStage.FieldsOut)
	}
}

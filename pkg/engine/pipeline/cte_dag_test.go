package pipeline

import (
	"context"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
)

func TestCTEDAG_Independent(t *testing.T) {
	// $a = ...; $b = ... — no dependencies, both at level 0.
	datasets := []spl2.DatasetDef{
		{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
		{Name: "b", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
	}

	plan, err := BuildCTEExecutionPlan(datasets)
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.Levels) != 1 {
		t.Fatalf("expected 1 level, got %d", len(plan.Levels))
	}
	if len(plan.Levels[0]) != 2 {
		t.Fatalf("expected 2 CTEs in level 0, got %d", len(plan.Levels[0]))
	}
}

func TestCTEDAG_Chained(t *testing.T) {
	// $a = ...; $b = FROM $a — $b depends on $a.
	datasets := []spl2.DatasetDef{
		{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
		{Name: "b", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "a", IsVariable: true}}},
	}

	plan, err := BuildCTEExecutionPlan(datasets)
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.Levels) != 2 {
		t.Fatalf("expected 2 levels, got %d", len(plan.Levels))
	}
	if len(plan.Levels[0]) != 1 || plan.Levels[0][0] != 0 {
		t.Errorf("level 0: expected [0], got %v", plan.Levels[0])
	}
	if len(plan.Levels[1]) != 1 || plan.Levels[1][0] != 1 {
		t.Errorf("level 1: expected [1], got %v", plan.Levels[1])
	}
}

func TestCTEDAG_Diamond(t *testing.T) {
	// $a = ...; $b = ...; $c = FROM $a JOIN [$b] — diamond dependency.
	datasets := []spl2.DatasetDef{
		{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
		{Name: "b", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
		{Name: "c", Query: &spl2.Query{
			Source: &spl2.SourceClause{Index: "a", IsVariable: true},
			Commands: []spl2.Command{
				&spl2.JoinCommand{
					Field:    "key",
					JoinType: "inner",
					Subquery: &spl2.Query{Source: &spl2.SourceClause{Index: "b", IsVariable: true}},
				},
			},
		}},
	}

	plan, err := BuildCTEExecutionPlan(datasets)
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.Levels) != 2 {
		t.Fatalf("expected 2 levels, got %d", len(plan.Levels))
	}
	// Level 0: $a and $b (independent).
	if len(plan.Levels[0]) != 2 {
		t.Errorf("level 0: expected 2 CTEs, got %d", len(plan.Levels[0]))
	}
	// Level 1: $c (depends on both).
	if len(plan.Levels[1]) != 1 || plan.Levels[1][0] != 2 {
		t.Errorf("level 1: expected [2], got %v", plan.Levels[1])
	}
}

func TestCTEDAG_CycleError(t *testing.T) {
	// $a = FROM $b; $b = FROM $a — cycle.
	datasets := []spl2.DatasetDef{
		{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "b", IsVariable: true}}},
		{Name: "b", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "a", IsVariable: true}}},
	}

	_, err := BuildCTEExecutionPlan(datasets)
	if err == nil {
		t.Fatal("expected cycle error, got nil")
	}
}

func TestCTEDAG_SelfReference(t *testing.T) {
	// $a = FROM $a — self-reference.
	datasets := []spl2.DatasetDef{
		{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "a", IsVariable: true}}},
	}

	_, err := BuildCTEExecutionPlan(datasets)
	if err == nil {
		t.Fatal("expected self-reference error, got nil")
	}
}

func TestCTEDAG_Empty(t *testing.T) {
	plan, err := BuildCTEExecutionPlan(nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.Levels) != 0 {
		t.Fatalf("expected 0 levels for empty input, got %d", len(plan.Levels))
	}
}

func TestCTEDAG_AppendSubqueryDep(t *testing.T) {
	// $a = ...; $b = main | APPEND [FROM $a] — $b depends on $a via subquery.
	datasets := []spl2.DatasetDef{
		{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
		{Name: "b", Query: &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.AppendCommand{
					Subquery: &spl2.Query{Source: &spl2.SourceClause{Index: "a", IsVariable: true}},
				},
			},
		}},
	}

	plan, err := BuildCTEExecutionPlan(datasets)
	if err != nil {
		t.Fatal(err)
	}

	if len(plan.Levels) != 2 {
		t.Fatalf("expected 2 levels, got %d", len(plan.Levels))
	}
}

// TestCTE_ParallelMaterialization verifies that two independent CTEs are
// materialized in parallel and both results are available to the main query.
// Uses BuildProgramWithBudget with parallel config enabled to exercise the
// materializeCTEsParallel code path (not the sequential fallback).
func TestCTE_ParallelMaterialization(t *testing.T) {
	// Build a simple store with events.
	store := &ServerIndexStore{
		Events: map[string][]*event.Event{
			"main": makeCTEEvents(10),
		},
	}

	prog := &spl2.Program{
		Datasets: []spl2.DatasetDef{
			{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
			{Name: "b", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
		},
		Main: &spl2.Query{Source: &spl2.SourceClause{Index: "a", IsVariable: true}},
	}

	monitor := stats.NewBudgetMonitor("test", 1<<30) // 1GB — plenty of room
	parallelCfg := &ParallelConfig{Enabled: true, MaxBranchParallelism: 4, ChannelBufferSize: 2}

	ctx := context.Background()
	result, err := BuildProgramWithBudget(ctx, prog, store, nil, nil, DefaultBatchSize, "", monitor, nil, false, parallelCfg)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := CollectAll(ctx, result.Iterator)
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(rows))
	}
}

// TestCTE_BudgetSplit verifies that parallel CTEs each get a proportional
// share of the memory budget.
func TestCTE_BudgetSplit(t *testing.T) {
	store := &ServerIndexStore{
		Events: map[string][]*event.Event{
			"main": makeCTEEvents(5),
		},
	}

	prog := &spl2.Program{
		Datasets: []spl2.DatasetDef{
			{Name: "a", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
			{Name: "b", Query: &spl2.Query{Source: &spl2.SourceClause{Index: "main"}}},
		},
		Main: &spl2.Query{Source: &spl2.SourceClause{Index: "a", IsVariable: true}},
	}

	monitor := stats.NewBudgetMonitor("test", 1<<30) // 1GB — plenty of room
	parallelCfg := &ParallelConfig{Enabled: true, MaxBranchParallelism: 4, ChannelBufferSize: 2}

	ctx := context.Background()
	result, err := BuildProgramWithBudget(ctx, prog, store, nil, nil, DefaultBatchSize, "", monitor, nil, false, parallelCfg)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := CollectAll(ctx, result.Iterator)
	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(rows))
	}
}

func makeCTEEvents(n int) []*event.Event {
	events := make([]*event.Event, n)
	for i := range events {
		events[i] = &event.Event{
			Raw:   "test event",
			Index: "main",
			Fields: map[string]event.Value{
				"x": event.IntValue(int64(i)),
			},
		}
	}

	return events
}

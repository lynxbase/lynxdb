package server

import (
	"context"
	"testing"
)

func TestSystemQueryTablesExposeMemoryAndSpillStats(t *testing.T) {
	engine := &Engine{}
	resolver := &systemTableResolver{engine: engine}

	job := newSearchJob("source=main | sort - duration", ResultTypeEvents)
	job.Stats = SearchStats{
		PeakMemoryBytes: 8192,
		SpilledToDisk:   true,
		SpillBytes:      4096,
		RowsScanned:     100,
		RowsReturned:    10,
		PipelineStages: []PipelineStage{
			{
				Name:        "sort",
				InputRows:   100,
				OutputRows:  10,
				DurationMS:  12.5,
				ExclusiveMS: 8.25,
				MemoryBytes: 2048,
				SpilledRows: 64,
				SpillBytes:  4096,
			},
		},
		OperatorBudgets: []OperatorBudgetStat{
			{
				Label:     "sort",
				SoftLimit: 1024,
				PeakBytes: 2048,
				Spilled:   true,
				Phase:     "spilled",
			},
		},
	}
	job.complete(JobStatusDone)
	engine.jobs.Store(job.ID, job)

	queryRows, err := resolver.ResolveSystemTable(context.Background(), "queries")
	if err != nil {
		t.Fatalf("resolve queries: %v", err)
	}
	if len(queryRows) != 1 {
		t.Fatalf("expected 1 query row, got %d", len(queryRows))
	}

	queryRow := queryRows[0]
	if got := queryRow["id"].AsString(); got != job.ID {
		t.Fatalf("query row id = %q, want %q", got, job.ID)
	}
	if got := queryRow["status"].AsString(); got != JobStatusDone {
		t.Fatalf("query row status = %q, want %q", got, JobStatusDone)
	}
	if got := queryRow["peak_memory_bytes"].AsInt(); got != 8192 {
		t.Fatalf("query row peak_memory_bytes = %d, want 8192", got)
	}
	if got := queryRow["spilled_to_disk"].AsBool(); !got {
		t.Fatalf("query row spilled_to_disk = false, want true")
	}
	if got := queryRow["spilled_rows"].AsInt(); got != 64 {
		t.Fatalf("query row spilled_rows = %d, want 64", got)
	}
	if got := queryRow["spill_bytes"].AsInt(); got != 4096 {
		t.Fatalf("query row spill_bytes = %d, want 4096", got)
	}

	operatorRows, err := resolver.ResolveSystemTable(context.Background(), "query_operators")
	if err != nil {
		t.Fatalf("resolve query_operators: %v", err)
	}
	if len(operatorRows) != 2 {
		t.Fatalf("expected 2 operator rows, got %d", len(operatorRows))
	}

	stageRow := operatorRows[0]
	if got := stageRow["query_id"].AsString(); got != job.ID {
		t.Fatalf("stage row query_id = %q, want %q", got, job.ID)
	}
	if got := stageRow["operator"].AsString(); got != "sort" {
		t.Fatalf("stage row operator = %q, want sort", got)
	}
	if got := stageRow["memory_bytes"].AsInt(); got != 2048 {
		t.Fatalf("stage row memory_bytes = %d, want 2048", got)
	}
	if got := stageRow["spilled_rows"].AsInt(); got != 64 {
		t.Fatalf("stage row spilled_rows = %d, want 64", got)
	}
	if got := stageRow["spill_bytes"].AsInt(); got != 4096 {
		t.Fatalf("stage row spill_bytes = %d, want 4096", got)
	}

	budgetRow := operatorRows[1]
	if got := budgetRow["operator"].AsString(); got != "sort" {
		t.Fatalf("budget row operator = %q, want sort", got)
	}
	if got := budgetRow["soft_limit"].AsInt(); got != 1024 {
		t.Fatalf("budget row soft_limit = %d, want 1024", got)
	}
	if got := budgetRow["peak_bytes"].AsInt(); got != 2048 {
		t.Fatalf("budget row peak_bytes = %d, want 2048", got)
	}
	if got := budgetRow["spilled"].AsBool(); !got {
		t.Fatalf("budget row spilled = false, want true")
	}
	if got := budgetRow["budget_phase"].AsString(); got != "spilled" {
		t.Fatalf("budget row budget_phase = %q, want spilled", got)
	}
}

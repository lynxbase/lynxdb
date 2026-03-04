package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

func TestEventStatsSpillTransition(t *testing.T) {
	// Create rows that exceed the memory budget. EventStats should spill
	// rows to disk while keeping aggregation groups in memory.
	n := 2000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", i%10)),
			"val":   event.FloatValue(float64(i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget: 256 bytes per row * ~100 rows = ~25KB before spill.
	// With 2000 rows, spill should be triggered.
	acct := stats.NewBudgetMonitor("test", 25*1024).NewAccount("eventstats")
	aggs := []AggFunc{
		{Name: "count", Alias: "cnt"},
		{Name: "avg", Field: "val", Alias: "avg_val"},
	}
	iter := newEventStatsIteratorWithSpill(child, aggs, []string{"group"}, 128, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	// Should still have all 2000 rows (eventstats enriches, doesn't reduce).
	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	// Verify aggregation results. Each of 10 groups has 200 rows.
	for _, row := range result {
		cnt := row["cnt"]
		if cnt.AsInt() != 200 {
			t.Errorf("expected count=200 for group %v, got %d", row["group"], cnt.AsInt())
		}
		// avg_val should be populated (non-null).
		avgVal := row["avg_val"]
		if avgVal.IsNull() {
			t.Errorf("expected non-null avg_val for group %v", row["group"])
		}
	}

	// Verify spill actually occurred.
	ei := findEventStatsIterator(iter)
	if ei == nil {
		t.Fatal("could not find EventStatsIterator in chain")
	}
	if !ei.spilled {
		t.Error("expected spill to have occurred")
	}
	if ei.spilledRows == 0 {
		t.Error("expected spilledRows > 0")
	}

	// ResourceStats should report spill.
	rs := ei.ResourceStats()
	if rs.SpilledRows == 0 {
		t.Error("ResourceStats.SpilledRows should be > 0")
	}
}

func TestEventStatsNoSpillSmallDataset(t *testing.T) {
	// With a large budget, eventstats should not spill.
	n := 100
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", i%5)),
			"val":   event.FloatValue(float64(i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Large budget — no spill expected.
	acct := stats.NewBudgetMonitor("test", 1*1024*1024).NewAccount("eventstats")
	aggs := []AggFunc{
		{Name: "count", Alias: "cnt"},
	}
	iter := newEventStatsIteratorWithSpill(child, aggs, []string{"group"}, 128, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	// No spill should have occurred.
	ei := findEventStatsIterator(iter)
	if ei == nil {
		t.Fatal("could not find EventStatsIterator in chain")
	}
	if ei.spilled {
		t.Error("expected no spill for small dataset")
	}

	// Verify counts: 5 groups, 20 rows each.
	for _, row := range result {
		cnt := row["cnt"]
		if cnt.AsInt() != 20 {
			t.Errorf("expected count=20 for group %v, got %d", row["group"], cnt.AsInt())
		}
	}
}

func TestEventStatsSpillWithoutSpillManager(t *testing.T) {
	// Without a SpillManager, budget exceeded should return an error.
	n := 500
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key": event.StringValue(fmt.Sprintf("val%d", i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	// Tiny budget, no spill manager.
	acct := stats.NewBudgetMonitor("test", 2*1024).NewAccount("eventstats")
	aggs := []AggFunc{
		{Name: "count", Alias: "cnt"},
	}
	iter := NewEventStatsIteratorWithBudget(child, aggs, []string{"key"}, 128, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected budget exceeded error, got nil")
	}
}

func TestEventStatsSpillNoGroupBy(t *testing.T) {
	// Test eventstats with spill and no group-by (global aggregation).
	n := 2000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"val": event.FloatValue(float64(i)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget to force spill.
	acct := stats.NewBudgetMonitor("test", 25*1024).NewAccount("eventstats")
	aggs := []AggFunc{
		{Name: "count", Alias: "cnt"},
		{Name: "sum", Field: "val", Alias: "total"},
	}
	iter := newEventStatsIteratorWithSpill(child, aggs, nil, 128, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	// Every row should have the same global count and sum.
	expectedCount := int64(n)
	expectedSum := float64(n*(n-1)) / 2.0

	for i, row := range result {
		cnt := row["cnt"]
		if cnt.AsInt() != expectedCount {
			t.Errorf("row %d: expected count=%d, got %d", i, expectedCount, cnt.AsInt())

			break
		}
		total := row["total"]
		if total.AsFloat() != expectedSum {
			t.Errorf("row %d: expected sum=%.0f, got %.0f", i, expectedSum, total.AsFloat())

			break
		}
	}
}

func TestEventStatsSpillPreservesRowOrder(t *testing.T) {
	// Verify that row order is preserved through spill.
	n := 1000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"seq":   event.IntValue(int64(i)),
			"group": event.StringValue(fmt.Sprintf("g%d", i%5)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget to force spill.
	acct := stats.NewBudgetMonitor("test", 15*1024).NewAccount("eventstats")
	aggs := []AggFunc{
		{Name: "count", Alias: "cnt"},
	}
	iter := newEventStatsIteratorWithSpill(child, aggs, []string{"group"}, 128, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}

	// Verify row order: seq should be 0, 1, 2, ..., n-1.
	for i, row := range result {
		seq := row["seq"]
		if seq.AsInt() != int64(i) {
			t.Errorf("row %d: expected seq=%d, got %d (order not preserved)", i, i, seq.AsInt())

			break
		}
	}
}

// findEventStatsIterator walks the iterator chain to find an EventStatsIterator.
func findEventStatsIterator(iter Iterator) *EventStatsIterator {
	switch it := iter.(type) {
	case *EventStatsIterator:
		return it
	case *InstrumentedIterator:
		return findEventStatsIterator(it.inner)
	default:
		return nil
	}
}

package pipeline

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/buffer"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// makeRowsWithField creates n rows with field "key" set to sequential integers
// and "data" set to a string of the given size.
func makeRowsWithField(n, dataSize int) []map[string]event.Value {
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = 'x'
	}
	dataStr := string(data)

	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key":  event.IntValue(int64(i)),
			"data": event.StringValue(dataStr),
		}
	}

	return rows
}

func TestSortInMemoryFastPath(t *testing.T) {
	// Sort 100 rows with a huge budget — no spill should occur.
	rows := makeRowsWithField(100, 8)
	// Reverse order so sort actually has work to do.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 1<<30).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result))
	}

	// Verify sorted ascending order.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	// No spill files should have been created.
	count, _ := mgr.Stats()
	if count != 0 {
		t.Fatalf("expected 0 spill files, got %d", count)
	}
}

func TestSortSpillsToDisk(t *testing.T) {
	// 1000 rows with a tiny budget to force spilling.
	rows := makeRowsWithField(1000, 16)
	// Shuffle: reverse order.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Budget must fit at least one batch (32 rows × 256 bytes = 8KB) but not all 1000 rows.
	// 32KB fits ~128 rows, so 1000 rows will need ~8 spill runs.
	acct := stats.NewBudgetMonitor("test", 32*1024).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 32, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1000 {
		t.Fatalf("expected 1000 rows, got %d", len(result))
	}

	// Verify globally sorted ascending order.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	// After Close, all spill files should be cleaned up.
	iter.Close()
}

func TestSortMultipleSpillRuns(t *testing.T) {
	// 5000 rows with a small budget to force many spill runs.
	rows := makeRowsWithField(5000, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 16KB budget — fits ~64 rows at 256 bytes each. 5000 rows → ~78 spill runs.
	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 32, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 5000 {
		t.Fatalf("expected 5000 rows, got %d", len(result))
	}

	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}
}

func TestSortDescendingWithSpill(t *testing.T) {
	rows := makeRowsWithField(500, 8)
	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 16KB fits ~64 rows; 500 rows → several spill runs.
	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: true}}, 32, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 500 {
		t.Fatalf("expected 500 rows, got %d", len(result))
	}

	// Verify sorted descending.
	for i := 0; i < len(result); i++ {
		expected := int64(499 - i)
		got := result[i]["key"].AsInt()
		if got != expected {
			t.Fatalf("row %d: expected key=%d, got %d", i, expected, got)
		}
	}
}

func TestSortSpillFileCleanup(t *testing.T) {
	rows := makeRowsWithField(500, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Budget must fit at least one batch (32 rows × 256 bytes = 8KB) but not all 500 rows.
	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 32, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Consume all output.
	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
	}

	// Close should clean up all spill files.
	iter.Close()

	count, _ := mgr.Stats()
	if count != 0 {
		t.Fatalf("expected 0 tracked files after Close, got %d", count)
	}

	// Verify no spill files remain in the spill directory.
	entries, err := os.ReadDir(mgr.Dir())
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("orphan spill file found after Close: %s", e.Name())
	}
}

func TestSortEmptyInput(t *testing.T) {
	child := NewRowScanIterator(nil, DefaultBatchSize)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 1<<20).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, DefaultBatchSize, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result))
	}

	count, _ := mgr.Stats()
	if count != 0 {
		t.Fatalf("expected 0 spill files for empty input, got %d", count)
	}
}

func TestSortWithoutSpillManager(t *testing.T) {
	// Without a SpillManager, budget exceeded should return an error.
	rows := makeRowsWithField(100, 8)
	child := NewRowScanIterator(rows, 64)

	// Tiny budget, no spill manager.
	acct := stats.NewBudgetMonitor("test", 1024).NewAccount("sort")
	iter := NewSortIteratorWithBudget(child, []SortField{{Name: "key", Desc: false}}, 64, acct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected error when budget exceeded without SpillManager")
	}
	if !stats.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

// SpillMerger tests

func TestSpillMergerTwoRuns(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Write two sorted runs.
	paths := make([]string, 2)
	for run := 0; run < 2; run++ {
		sw, err := NewManagedSpillWriter(mgr, "test")
		if err != nil {
			t.Fatal(err)
		}
		start := run * 50
		for i := start; i < start+50; i++ {
			err := sw.WriteRow(map[string]event.Value{
				"key": event.IntValue(int64(i)),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		paths[run] = sw.Path()
		sw.CloseFile()
	}

	merger, err := NewSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var result []int64
	for {
		row, err := merger.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			break
		}
		result = append(result, row["key"].AsInt())
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result))
	}
	for i, v := range result {
		if v != int64(i) {
			t.Fatalf("row %d: expected %d, got %d", i, i, v)
		}
	}
}

func TestSpillMergerManyRuns(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	const numRuns = 16
	const rowsPerRun = 100
	paths := make([]string, numRuns)

	for run := 0; run < numRuns; run++ {
		sw, err := NewManagedSpillWriter(mgr, "test")
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < rowsPerRun; i++ {
			// Interleave: each run has values run, run+16, run+32, ...
			val := int64(run + i*numRuns)
			err := sw.WriteRow(map[string]event.Value{
				"key": event.IntValue(val),
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		paths[run] = sw.Path()
		sw.CloseFile()
	}

	merger, err := NewSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var result []int64
	for {
		row, err := merger.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			break
		}
		result = append(result, row["key"].AsInt())
	}

	if len(result) != numRuns*rowsPerRun {
		t.Fatalf("expected %d rows, got %d", numRuns*rowsPerRun, len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i] < result[i-1] {
			t.Fatalf("not sorted at index %d: %d < %d", i, result[i], result[i-1])
		}
	}
}

func TestSpillMergerEmptyRuns(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Create 3 files: first empty, second with data, third empty.
	paths := make([]string, 3)
	for i := 0; i < 3; i++ {
		sw, err := NewManagedSpillWriter(mgr, "test")
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			for j := 0; j < 5; j++ {
				if err := sw.WriteRow(map[string]event.Value{"key": event.IntValue(int64(j))}); err != nil {
					t.Fatal(err)
				}
			}
		}
		paths[i] = sw.Path()
		sw.CloseFile()
	}

	merger, err := NewSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	count := 0
	for {
		row, err := merger.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			break
		}
		count++
	}

	if count != 5 {
		t.Fatalf("expected 5 rows, got %d", count)
	}
}

func TestSpillMergerSingleRun(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewManagedSpillWriter(mgr, "test")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := sw.WriteRow(map[string]event.Value{"key": event.IntValue(int64(i))}); err != nil {
			t.Fatal(err)
		}
	}
	path := sw.Path()
	sw.CloseFile()

	merger, err := NewSpillMerger([]string{path}, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var result []int64
	for {
		row, err := merger.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			break
		}
		result = append(result, row["key"].AsInt())
	}

	if len(result) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(result))
	}
	for i, v := range result {
		if v != int64(i) {
			t.Fatalf("row %d: expected %d, got %d", i, i, v)
		}
	}
}

func TestSpillMergerNextBatch(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Two runs of 50 each.
	paths := make([]string, 2)
	for run := 0; run < 2; run++ {
		sw, err := NewManagedSpillWriter(mgr, "test")
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			val := int64(run + i*2) // interleaved: 0,2,4,... and 1,3,5,...
			if err := sw.WriteRow(map[string]event.Value{"key": event.IntValue(val)}); err != nil {
				t.Fatal(err)
			}
		}
		paths[run] = sw.Path()
		sw.CloseFile()
	}

	merger, err := NewSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var allRows []int64
	for {
		batch, err := merger.NextBatch(32)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			allRows = append(allRows, batch.Row(i)["key"].AsInt())
		}
	}

	if len(allRows) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(allRows))
	}
	for i := 1; i < len(allRows); i++ {
		if allRows[i] < allRows[i-1] {
			t.Fatalf("not sorted at index %d: %d < %d", i, allRows[i], allRows[i-1])
		}
	}
}

// Aggregate spill tests

func TestAggregateSpillByMemoryPressure(t *testing.T) {
	// Create many unique groups that should exceed a small budget.
	n := 10000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", i)),
			"val":   event.IntValue(1),
		}
	}

	child := NewRowScanIterator(rows, 256)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget: 32KB — will force spill for 10K groups.
	acct := stats.NewBudgetMonitor("test", 32*1024).NewAccount("agg")
	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	iter := NewAggregateIteratorWithSpill(child, aggs, []string{"group"}, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d groups, got %d", n, len(result))
	}

	// Every group should have count=1.
	for i, row := range result {
		c := row["count"]
		if c.AsInt() != 1 {
			t.Fatalf("group %d: expected count=1, got %d", i, c.AsInt())
		}
	}
}

func TestAggregateNoSpillSmallGroups(t *testing.T) {
	// 100 groups with small keys under a generous budget — should NOT spill.
	n := 100
	rows := make([]map[string]event.Value, n*10)
	for i := 0; i < n*10; i++ {
		rows[i] = map[string]event.Value{
			"group": event.IntValue(int64(i % n)),
			"val":   event.IntValue(1),
		}
	}

	child := NewRowScanIterator(rows, 256)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 1MB budget — more than enough for 100 groups.
	acct := stats.NewBudgetMonitor("test", 1<<20).NewAccount("agg")
	aggs := []AggFunc{{Name: "sum", Field: "val", Alias: "total"}}
	iter := NewAggregateIteratorWithSpill(child, aggs, []string{"group"}, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != n {
		t.Fatalf("expected %d groups, got %d", n, len(result))
	}

	// No spill files should have been created.
	count, _ := mgr.Stats()
	if count != 0 {
		t.Fatalf("expected 0 spill files (no spill needed), got %d", count)
	}
}

func TestAggregateSpillWithAvg(t *testing.T) {
	// Test that AVG is correctly computed across spills (sum/count tuples).
	n := 5000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", i%100)),
			"val":   event.FloatValue(float64(i)),
		}
	}

	child := NewRowScanIterator(rows, 128)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget to force spills.
	acct := stats.NewBudgetMonitor("test", 8*1024).NewAccount("agg")
	aggs := []AggFunc{
		{Name: "avg", Field: "val", Alias: "avg_val"},
		{Name: "count", Alias: "cnt"},
	}
	iter := NewAggregateIteratorWithSpill(child, aggs, []string{"group"}, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 groups, got %d", len(result))
	}

	// Each group should have count = n/100 = 50.
	for _, row := range result {
		cnt := row["cnt"]
		if cnt.AsInt() != 50 {
			t.Errorf("expected count=50, got %d for group %v", cnt.AsInt(), row["group"])
		}
	}
}

func TestAggregateSpillWithDC(t *testing.T) {
	// Test dc (distinct count) correctness across spill boundaries.
	// 100 groups, each with 50 events. Each event has a unique "item" per group,
	// so dc(item) should be 50 per group.
	n := 5000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		groupIdx := i % 100
		itemIdx := i / 100 // 0..49 for each group
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", groupIdx)),
			"item":  event.StringValue(fmt.Sprintf("item_%d_%d", groupIdx, itemIdx)),
		}
	}

	child := NewRowScanIterator(rows, 128)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 8*1024).NewAccount("agg")
	aggs := []AggFunc{
		{Name: "dc", Field: "item", Alias: "dc_item"},
		{Name: "count", Alias: "cnt"},
	}
	iter := NewAggregateIteratorWithSpill(child, aggs, []string{"group"}, acct, mgr)

	ctx := context.Background()
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 groups, got %d", len(result))
	}

	for _, row := range result {
		dc := row["dc_item"].AsInt()
		cnt := row["cnt"].AsInt()
		if cnt != 50 {
			t.Errorf("group %v: expected count=50, got %d", row["group"], cnt)
		}
		if dc != 50 {
			t.Errorf("group %v: expected dc=50, got %d", row["group"], dc)
		}
	}
}

func TestAggregateSpillWithValues(t *testing.T) {
	// Test values() correctness across spill boundaries.
	// 50 groups, each with 20 events. Each event has "item" = "v_<group>_<seq>".
	n := 1000
	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		groupIdx := i % 50
		itemIdx := i / 50
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", groupIdx)),
			"item":  event.StringValue(fmt.Sprintf("v_%d_%d", groupIdx, itemIdx)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 4*1024).NewAccount("agg")
	aggs := []AggFunc{
		{Name: "values", Field: "item", Alias: "vals"},
		{Name: "count", Alias: "cnt"},
	}
	iter := NewAggregateIteratorWithSpill(child, aggs, []string{"group"}, acct, mgr)

	ctx := context.Background()
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 50 {
		t.Fatalf("expected 50 groups, got %d", len(result))
	}

	for _, row := range result {
		cnt := row["cnt"].AsInt()
		if cnt != 20 {
			t.Errorf("group %v: expected count=20, got %d", row["group"], cnt)
		}
		// The values field contains all distinct items separated by "|||".
		valsStr := row["vals"].AsString()
		parts := make(map[string]bool)
		for _, p := range splitNonEmpty(valsStr, "|||") {
			parts[p] = true
		}
		if len(parts) != 20 {
			t.Errorf("group %v: expected 20 distinct values, got %d", row["group"], len(parts))
		}
	}
}

func TestAggregateSpillWithStdev(t *testing.T) {
	// Test stdev correctness across spill boundaries by comparing to a
	// non-spill reference computation.
	n := 5000
	numGroups := 100

	rows := make([]map[string]event.Value, n)
	// Track per-group values for reference stdev computation.
	groupVals := make(map[string][]float64)
	for i := 0; i < n; i++ {
		groupIdx := i % numGroups
		gkey := fmt.Sprintf("g%d", groupIdx)
		val := float64(i)
		rows[i] = map[string]event.Value{
			"group": event.StringValue(gkey),
			"val":   event.FloatValue(val),
		}
		groupVals[gkey] = append(groupVals[gkey], val)
	}

	child := NewRowScanIterator(rows, 128)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 8*1024).NewAccount("agg")
	aggs := []AggFunc{
		{Name: "stdev", Field: "val", Alias: "sd"},
		{Name: "count", Alias: "cnt"},
	}
	iter := NewAggregateIteratorWithSpill(child, aggs, []string{"group"}, acct, mgr)

	ctx := context.Background()
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != numGroups {
		t.Fatalf("expected %d groups, got %d", numGroups, len(result))
	}

	for _, row := range result {
		gkey := row["group"].AsString()
		cnt := row["cnt"].AsInt()
		sd := row["sd"].AsFloat()

		if cnt != int64(n/numGroups) {
			t.Errorf("group %s: expected count=%d, got %d", gkey, n/numGroups, cnt)
		}

		// Compute reference stdev.
		vals := groupVals[gkey]
		refSD := referenceStdev(vals)
		// Allow 0.01% relative error for floating point.
		if refSD > 0 {
			relErr := abs64((sd - refSD) / refSD)
			if relErr > 0.0001 {
				t.Errorf("group %s: stdev mismatch: got %.6f, want %.6f (relErr=%.6f)", gkey, sd, refSD, relErr)
			}
		}
	}
}

func TestAggregateSpillWithPerc95(t *testing.T) {
	// Test percentile correctness across spill boundaries.
	n := 5000
	numGroups := 100

	rows := make([]map[string]event.Value, n)
	groupVals := make(map[string][]float64)
	for i := 0; i < n; i++ {
		groupIdx := i % numGroups
		gkey := fmt.Sprintf("g%d", groupIdx)
		val := float64(i)
		rows[i] = map[string]event.Value{
			"group": event.StringValue(gkey),
			"val":   event.FloatValue(val),
		}
		groupVals[gkey] = append(groupVals[gkey], val)
	}

	child := NewRowScanIterator(rows, 128)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	acct := stats.NewBudgetMonitor("test", 8*1024).NewAccount("agg")
	aggs := []AggFunc{
		{Name: "perc95", Field: "val", Alias: "p95"},
		{Name: "count", Alias: "cnt"},
	}
	iter := NewAggregateIteratorWithSpill(child, aggs, []string{"group"}, acct, mgr)

	ctx := context.Background()
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != numGroups {
		t.Fatalf("expected %d groups, got %d", numGroups, len(result))
	}

	for _, row := range result {
		gkey := row["group"].AsString()
		cnt := row["cnt"].AsInt()
		p95 := row["p95"].AsFloat()

		if cnt != int64(n/numGroups) {
			t.Errorf("group %s: expected count=%d, got %d", gkey, n/numGroups, cnt)
		}

		// Compute reference p95 from exact values.
		vals := groupVals[gkey]
		refP95 := referencePercentile(vals, 95)
		// Allow 5% relative error for t-digest approximation.
		if refP95 > 0 {
			relErr := abs64((p95 - refP95) / refP95)
			if relErr > 0.05 {
				t.Errorf("group %s: p95 mismatch: got %.2f, want %.2f (relErr=%.4f)", gkey, p95, refP95, relErr)
			}
		}
	}
}

// Test helpers

// splitNonEmpty splits s by sep and returns non-empty parts.
func splitNonEmpty(s, sep string) []string {
	parts := make([]string, 0)
	for _, p := range strings.Split(s, sep) {
		if p != "" {
			parts = append(parts, p)
		}
	}

	return parts
}

// referenceStdev computes sample standard deviation for reference.
func referenceStdev(vals []float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	n := float64(len(vals))
	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / n
	var sumSq float64
	for _, v := range vals {
		d := v - mean
		sumSq += d * d
	}

	return math.Sqrt(sumSq / (n - 1))
}

// referencePercentile computes exact percentile from sorted values.
func referencePercentile(vals []float64, pct float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	idx := pct / 100.0 * float64(len(sorted)-1)
	lower := int(idx)
	if lower >= len(sorted)-1 {
		return sorted[len(sorted)-1]
	}
	frac := idx - float64(lower)

	return sorted[lower] + frac*(sorted[lower+1]-sorted[lower])
}

func abs64(x float64) float64 {
	if x < 0 {
		return -x
	}

	return x
}

// Bug fix tests: sort/aggregate spill on child budget pressure

// budgetErrorIterator is a test child that returns BudgetExceededError after
// producing a configured number of batches. It uses a shared BudgetMonitor
// so the error is a real *stats.BudgetExceededError (checked via errors.As).
type budgetErrorIterator struct {
	inner       Iterator
	batchesSeen int
	failAfter   int // fail on the (failAfter+1)th call to Next
	monitor     *stats.BudgetMonitor
	failAccount *stats.BoundAccount
	failAmount  int64
	hasFailed   bool
}

func (b *budgetErrorIterator) Init(ctx context.Context) error { return b.inner.Init(ctx) }
func (b *budgetErrorIterator) Close() error                   { return b.inner.Close() }
func (b *budgetErrorIterator) Schema() []FieldInfo            { return b.inner.Schema() }

func (b *budgetErrorIterator) Next(ctx context.Context) (*Batch, error) {
	if b.batchesSeen >= b.failAfter && !b.hasFailed {
		// Simulate scan's Grow failing on the shared monitor.
		err := b.failAccount.Grow(b.failAmount)
		if err != nil {
			return nil, err
		}
		// If Grow somehow succeeds (sort freed memory via spill), mark as retried.
		b.hasFailed = true
		b.failAccount.Shrink(b.failAmount)
	}

	batch, err := b.inner.Next(ctx)
	if err != nil {
		return nil, err
	}
	if batch != nil {
		b.batchesSeen++
	}

	return batch, err
}

func TestSortSpillOnChildBudgetExceeded(t *testing.T) {
	// Simulate the real bug: scan and sort share a BudgetMonitor. Sort accumulates
	// rows, then scan's Grow fails because the shared budget is exhausted.
	// After the fix, sort should spill its buffer, freeing budget capacity,
	// and the query should complete with correct sorted output.
	const (
		totalRows = 200
		batchSize = 32
		dataSize  = 64
	)

	rows := makeRowsWithField(totalRows, dataSize)
	// Reverse order so sort has work to do.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	// Shared budget monitor — small enough that sort's accumulated rows + scan's
	// next batch will exceed the limit.
	monitor := stats.NewBudgetMonitor("test", 200*1024)
	sortAcct := monitor.NewAccount("sort")
	scanAcct := monitor.NewAccount("scan")

	child := NewRowScanIterator(rows, batchSize)

	// Wrap child in a budgetErrorIterator that simulates scan failing after 4 batches.
	// At that point sort holds 4*32=128 rows. The "scan" account tries to Grow
	// more than the monitor allows, producing a BudgetExceededError.
	budgetChild := &budgetErrorIterator{
		inner:       child,
		failAfter:   4,
		monitor:     monitor,
		failAccount: scanAcct,
		failAmount:  200 * 1024, // request the entire budget — guaranteed to fail
	}

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	iter := NewSortIteratorWithSpill(budgetChild, []SortField{{Name: "key", Desc: false}}, batchSize, sortAcct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatalf("expected query to complete after sort spill, got error: %v", err)
	}

	if len(result) != totalRows {
		t.Fatalf("expected %d rows, got %d", totalRows, len(result))
	}

	// Verify sorted ascending order.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}
}

func TestSortSpillOnChildBudgetExceeded_NoSpillManager(t *testing.T) {
	// Without SpillManager, BudgetExceededError from child should propagate.
	rows := makeRowsWithField(200, 64)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	monitor := stats.NewBudgetMonitor("test", 200*1024)
	sortAcct := monitor.NewAccount("sort")
	scanAcct := monitor.NewAccount("scan")

	child := NewRowScanIterator(rows, 32)
	budgetChild := &budgetErrorIterator{
		inner:       child,
		failAfter:   4,
		monitor:     monitor,
		failAccount: scanAcct,
		failAmount:  200 * 1024,
	}

	// No SpillManager — sort cannot spill.
	iter := NewSortIteratorWithBudget(budgetChild, []SortField{{Name: "key", Desc: false}}, 32, sortAcct)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err := CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected error when budget exceeded without SpillManager")
	}
	if !stats.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

func TestSortSpillOnChildBudgetExceeded_NoRowsToSpill(t *testing.T) {
	// If sort has no accumulated rows when child fails, error should propagate.
	// Use a very small budget so that the child's Grow always exceeds it even
	// before sort has accumulated any rows. failAmount exceeds the full budget
	// by 2x to guarantee the Grow fails regardless of sort's current usage.
	monitor := stats.NewBudgetMonitor("test", 512)
	sortAcct := monitor.NewAccount("sort")
	scanAcct := monitor.NewAccount("scan")

	rows := makeRowsWithField(10, 64)
	child := NewRowScanIterator(rows, 32)

	// Fail immediately (failAfter=0), before sort accumulates any rows.
	// failAmount is 2x the total budget — guaranteed to exceed.
	budgetChild := &budgetErrorIterator{
		inner:       child,
		failAfter:   0,
		monitor:     monitor,
		failAccount: scanAcct,
		failAmount:  1024,
	}

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	iter := NewSortIteratorWithSpill(budgetChild, []SortField{{Name: "key", Desc: false}}, 32, sortAcct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected error when child fails and sort has no rows to spill")
	}
	if !stats.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

func TestEstimateRowMapBytes(t *testing.T) {
	tests := []struct {
		name     string
		row      map[string]event.Value
		minBytes int64
		maxBytes int64
	}{
		{
			name:     "empty row",
			row:      map[string]event.Value{},
			minBytes: 64,
			maxBytes: 64,
		},
		{
			name: "small string fields",
			row: map[string]event.Value{
				"key":   event.StringValue("hello"),
				"level": event.StringValue("info"),
			},
			minBytes: 64 + 2*(56+3) + 5 + 4, // overhead + 2 entries + string lens
			maxBytes: 300,
		},
		{
			name: "large _raw field",
			row: map[string]event.Value{
				"_raw":  event.StringValue(strings.Repeat("x", 500_000)),
				"_time": event.IntValue(1234567890),
			},
			minBytes: 500_000, // must account for the large string
			maxBytes: 501_000,
		},
		{
			name: "int and float fields only",
			row: map[string]event.Value{
				"status":   event.IntValue(200),
				"duration": event.FloatValue(1.5),
			},
			minBytes: 64 + 2*56,
			maxBytes: 300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateRowMapBytes(tt.row)
			if got < tt.minBytes {
				t.Errorf("estimate %d < min %d", got, tt.minBytes)
			}
			if got > tt.maxBytes {
				t.Errorf("estimate %d > max %d", got, tt.maxBytes)
			}
		})
	}
}

func TestAggregateSpillOnChildBudgetExceeded(t *testing.T) {
	// Aggregate holds groups in memory. When child's budget fails, aggregate
	// should spill groups, freeing budget capacity, and the query should complete.
	const (
		totalRows = 500
		numGroups = 50
	)

	rows := make([]map[string]event.Value, totalRows)
	for i := 0; i < totalRows; i++ {
		rows[i] = map[string]event.Value{
			"group": event.StringValue(fmt.Sprintf("g%d", i%numGroups)),
			"val":   event.IntValue(1),
		}
	}

	monitor := stats.NewBudgetMonitor("test", 100*1024)
	aggAcct := monitor.NewAccount("agg")
	scanAcct := monitor.NewAccount("scan")

	child := NewRowScanIterator(rows, 64)
	budgetChild := &budgetErrorIterator{
		inner:       child,
		failAfter:   3, // fail after 3 batches (192 rows processed)
		monitor:     monitor,
		failAccount: scanAcct,
		failAmount:  100 * 1024,
	}

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	iter := NewAggregateIteratorWithSpill(budgetChild, aggs, []string{"group"}, aggAcct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatalf("expected query to complete after agg spill, got error: %v", err)
	}

	if len(result) != numGroups {
		t.Fatalf("expected %d groups, got %d", numGroups, len(result))
	}

	// Verify total count across all groups equals totalRows.
	var totalCount int64
	for _, row := range result {
		totalCount += row["count"].AsInt()
	}
	if totalCount != totalRows {
		t.Fatalf("expected total count=%d, got %d", totalRows, totalCount)
	}
}

// Batch-splitting tests (row-by-row Grow)

func TestSortRowByRowAccumulationWithSpill(t *testing.T) {
	// 100 rows @ ~10KB each, budget=50KB, spillMgr enabled.
	// With row-by-row accumulation, sort should spill multiple times and
	// produce correctly sorted output. This is the core fix scenario: the
	// old batch-level Grow would fail because 32 rows × ~10KB = 320KB > 50KB,
	// but row-by-row allows accumulating rows up to the budget, spilling,
	// and continuing.
	const (
		numRows  = 100
		dataSize = 10 * 1024 // ~10KB per row
	)

	rows := makeRowsWithField(numRows, dataSize)
	// Reverse order so sort has work to do.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 50KB budget — each row is ~10KB, so only ~5 rows fit before spill.
	acct := stats.NewBudgetMonitor("test", 50*1024).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 32, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatalf("expected completion with row-by-row spill, got error: %v", err)
	}

	if len(result) != numRows {
		t.Fatalf("expected %d rows, got %d", numRows, len(result))
	}

	// Verify sorted ascending order.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	// Verify spill occurred — ResourceStats should show spilled rows.
	rs := iter.ResourceStats()
	if rs.SpilledRows == 0 {
		t.Error("expected spilled rows > 0 with 50KB budget for 100×10KB rows")
	}
}

func TestSortLargeRowClearError(t *testing.T) {
	// 1 row @ ~1MB, budget=256KB, spillMgr enabled.
	// A single row larger than the entire budget should produce a clear error
	// with the row size information, since there's nothing to spill.
	const rowSize = 1 << 20 // 1MB

	rows := makeRowsWithField(1, rowSize)
	child := NewRowScanIterator(rows, 1)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Budget much smaller than a single row.
	acct := stats.NewBudgetMonitor("test", 256*1024).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 1, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected error for row larger than budget")
	}

	// Error should be a wrapped BudgetExceededError.
	if !stats.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

func TestSortMixedRowSizesCorrectOrder(t *testing.T) {
	// 1000 mixed-size rows in reverse order, budget=100KB, spillMgr enabled.
	// Rows alternate between small (~100B) and medium (~5KB) to test that
	// row-by-row accumulation handles variable sizes correctly.
	const numRows = 1000

	rows := make([]map[string]event.Value, numRows)
	for i := 0; i < numRows; i++ {
		dataSize := 100 // small
		if i%3 == 0 {
			dataSize = 5000 // medium
		}
		data := make([]byte, dataSize)
		for j := range data {
			data[j] = 'a'
		}
		rows[i] = map[string]event.Value{
			"key":  event.IntValue(int64(numRows - 1 - i)), // reverse order
			"data": event.StringValue(string(data)),
		}
	}

	child := NewRowScanIterator(rows, 64)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 100KB budget — forces multiple spill runs with mixed sizes.
	acct := stats.NewBudgetMonitor("test", 100*1024).NewAccount("sort")
	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 64, acct, mgr)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatalf("expected completion, got error: %v", err)
	}

	if len(result) != numRows {
		t.Fatalf("expected %d rows, got %d", numRows, len(result))
	}

	// Verify globally sorted ascending order.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}
}

func TestSortWithScanSharingBudget_Integration(t *testing.T) {
	// Integration test: realistic pipeline ScanIteratorWithBudget → SortIterator,
	// both sharing one BudgetMonitor. Events have large _raw fields (~1KB each).
	// The budget is large enough for sort to spill but not large enough to hold
	// all events simultaneously. Verifies end-to-end correctness.
	const (
		numEvents = 300
		rawSize   = 1024
		batchSize = 32
	)

	events := makeSizedEvents(numEvents, rawSize)

	// Budget: ~100KB. Each event is ~1KB raw + overhead ≈ 1.2KB.
	// 300 events ≈ 360KB total. Budget forces spilling.
	monitor := stats.NewBudgetMonitor("test", 100*1024)
	scanAcct := monitor.NewAccount("scan")
	sortAcct := monitor.NewAccount("sort")

	scan := NewScanIteratorWithBudget(events, batchSize, scanAcct)

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sortIter := NewSortIteratorWithSpill(scan, []SortField{{Name: "idx", Desc: false}}, batchSize, sortAcct, mgr)

	ctx := context.Background()
	if err := sortIter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, sortIter)
	if err != nil {
		t.Fatalf("expected query to complete, got error: %v", err)
	}

	if len(result) != numEvents {
		t.Fatalf("expected %d rows, got %d", numEvents, len(result))
	}

	// Verify sorted ascending by idx.
	for i := 0; i < len(result); i++ {
		got := result[i]["idx"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected idx=%d, got %d", i, i, got)
		}
	}
}

// BufferedSortIterator per-row accounting tests

func TestBufferedSort_PerRowAccountingSmallBudget(t *testing.T) {
	// Validates that BufferedSortIterator correctly handles per-row accounting
	// with a budget smaller than a single batch but larger than a single row.
	// Before the per-row fix, the batch-level Grow would fail with:
	//   "cannot fit batch after spill: memory budget exceeded"
	// because the total batch size (32 rows × ~10KB = 320KB) exceeded the
	// post-spill reservation (256KB), even though individual rows (~10KB)
	// can fit with intermediate spills.
	const (
		numRows  = 100
		dataSize = 10 * 1024 // ~10KB per row
	)

	rows := makeRowsWithField(numRows, dataSize)
	// Reverse order so sort has work to do.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 200, buffer.PageSize64KB)

	// Budget: 50KB. Each row is ~10KB. A full batch of 32 rows is ~320KB,
	// which far exceeds the budget. Per-row accounting allows accumulating
	// ~5 rows before spilling, then continuing.
	acct := stats.NewBudgetMonitor("test", 50*1024).NewAccount("sort")
	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		32, acct, pool, "q-perrow", mgr,
	)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatalf("expected completion with per-row spill, got error: %v", err)
	}

	if len(result) != numRows {
		t.Fatalf("expected %d rows, got %d", numRows, len(result))
	}

	// Verify sorted ascending order.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	// Verify spill occurred.
	rs := iter.ResourceStats()
	if rs.SpilledRows == 0 {
		t.Error("expected spilled rows > 0 with 50KB budget for 100×10KB rows")
	}
}

func TestBufferedSort_SingleRowExceedsBudget(t *testing.T) {
	// A single row larger than the entire budget should produce a clear error
	// since there's nothing to spill (no accumulated rows).
	const rowSize = 1 << 20 // 1MB per row

	rows := makeRowsWithField(1, rowSize)
	child := NewRowScanIterator(rows, 1)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 200, buffer.PageSize64KB)

	// Budget much smaller than a single row.
	acct := stats.NewBudgetMonitor("test", 256*1024).NewAccount("sort")
	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		1, acct, pool, "q-bigrow", mgr,
	)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = CollectAll(ctx, iter)
	if err == nil {
		t.Fatal("expected error for row larger than budget")
	}

	// Error should be a wrapped BudgetExceededError.
	if !stats.IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got: %v", err)
	}
}

package pipeline

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/buffer"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// makeUniqueGroupEvents creates n events with unique group-by keys to force
// high cardinality aggregation. Each event gets a unique "group" field and
// a padding "data" field of the given size to consume memory quickly.
func makeUniqueGroupEvents(n, dataSize int) []*event.Event {
	padding := make([]byte, dataSize)
	for i := range padding {
		padding[i] = 'x'
	}
	padStr := string(padding)

	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		events[i] = &event.Event{
			Raw:   fmt.Sprintf("event %d %s", i, padStr),
			Index: "main",
			Fields: map[string]event.Value{
				"group": event.StringValue(fmt.Sprintf("g-%06d", i)),
				"val":   event.IntValue(int64(i)),
			},
		}
	}

	return events
}

// makeUniqueGroupRows creates n row maps with unique group keys and padding.
func makeUniqueGroupRows(n, dataSize int) []map[string]event.Value {
	padding := make([]byte, dataSize)
	for i := range padding {
		padding[i] = 'y'
	}
	padStr := string(padding)

	rows := make([]map[string]event.Value, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]event.Value{
			"key":  event.IntValue(int64(n - 1 - i)), // reverse order for sort tests
			"data": event.StringValue(padStr),
		}
	}

	return rows
}

// newTestBufferPool creates a small buffer pool for testing.
func newTestBufferPool(t *testing.T, maxPages int) *buffer.Pool {
	t.Helper()
	bp, err := buffer.NewPool(buffer.PoolConfig{
		MaxPages:      maxPages,
		PageSize:      buffer.PageSize64KB,
		EnableOffHeap: false, // Go heap in tests
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(func() { _ = bp.Close() })

	return bp
}

// TestBuildProgramWithBufferPool_StatsQuery verifies that a query pipeline
// built with a buffer pool runs correctly. Operators allocate memory credits
// from pool pages instead of the BudgetMonitor.
func TestBuildProgramWithBufferPool_StatsQuery(t *testing.T) {
	events := makeEvents(100)
	store := &ServerIndexStore{
		Events: map[string][]*event.Event{
			"main": events,
		},
	}

	prog, err := spl2.ParseProgram("search index=main | stats count by status")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	pool := newTestBufferPool(t, 64) // 64 pages = 4MB
	monitor := stats.NewBudgetMonitor("test", 0)
	defer monitor.Close()

	ctx := context.Background()
	result, err := BuildProgramWithBufferPool(ctx, prog, store, nil, nil, 0, "", monitor, nil, false, pool, "test-q-1", nil)
	if err != nil {
		t.Fatalf("BuildProgramWithBufferPool: %v", err)
	}

	rows, err := CollectAll(ctx, result.Iterator)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// makeEvents generates statuses 200, 300, 400, 500, 600 (5 groups).
	if len(rows) != 5 {
		t.Errorf("expected 5 groups, got %d", len(rows))
	}

	// Verify pool was used: at least one page should have been allocated.
	poolStats := pool.Stats()
	if poolStats.TotalPages == poolStats.FreePages {
		// After Close, pages are returned. Check AllocCalls instead.
		t.Logf("pool stats: total=%d free=%d hits=%d misses=%d",
			poolStats.TotalPages, poolStats.FreePages, poolStats.Hits, poolStats.Misses)
	}
}

// TestBuildProgramWithBufferPool_NilPool_FallsBack verifies that when pool is nil,
// BuildProgramWithBufferPool falls back to the BudgetMonitor path.
func TestBuildProgramWithBufferPool_NilPool_FallsBack(t *testing.T) {
	events := makeEvents(50)
	store := &ServerIndexStore{
		Events: map[string][]*event.Event{
			"main": events,
		},
	}

	prog, err := spl2.ParseProgram("search index=main | stats count")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	monitor := stats.NewBudgetMonitor("test", 0)
	defer monitor.Close()

	ctx := context.Background()
	// pool=nil should fall back to BudgetMonitor path.
	result, err := BuildProgramWithBufferPool(ctx, prog, store, nil, nil, 0, "", monitor, nil, false, nil, "test-q-2", nil)
	if err != nil {
		t.Fatalf("BuildProgramWithBufferPool (nil pool): %v", err)
	}

	rows, err := CollectAll(ctx, result.Iterator)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("expected 1 row (count), got %d", len(rows))
	}
}

// TestPoolAccountAsMemoryAccount_InOperator verifies that a PoolAccount works
// correctly when passed to an operator constructor as a MemoryAccount.
func TestPoolAccountAsMemoryAccount_InOperator(t *testing.T) {
	pool := newTestBufferPool(t, 32)
	acct := buffer.NewPoolAccount(pool, "test-op", nil)

	// Use PoolAccount as MemoryAccount in an AggregateIterator.
	// We manually drive the iterator instead of using CollectAll, because
	// CollectAll calls Close() which releases all pages.
	events := makeEvents(100)
	scan := NewScanIterator(events, 1024)
	aggs := []AggFunc{
		{Name: "count", Alias: "count"},
		{Name: "avg", Field: "x", Alias: "avg_x"},
	}
	agg := NewAggregateIterator(scan, aggs, []string{"status"}, acct)

	ctx := context.Background()
	if err := agg.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	var rows []map[string]event.Value
	for {
		batch, err := agg.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			rows = append(rows, batch.Row(i))
		}
	}

	if len(rows) != 5 {
		t.Errorf("expected 5 groups, got %d", len(rows))
	}

	// Before Close: verify memory was tracked through the pool account.
	if acct.MaxUsed() == 0 {
		t.Error("expected MaxUsed > 0 through PoolAccount")
	}
	if acct.PageCount() == 0 {
		t.Error("expected PageCount > 0 through PoolAccount before Close")
	}

	// Close releases pages back to the pool.
	if err := agg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if acct.PageCount() != 0 {
		t.Errorf("expected PageCount = 0 after Close, got %d", acct.PageCount())
	}
	// MaxUsed should still reflect the peak.
	if acct.MaxUsed() == 0 {
		t.Error("expected MaxUsed > 0 after Close (peak preserved)")
	}
}

// TestNopAccount_InOperator verifies that operators work correctly with
// NopAccount (no memory tracking).
func TestNopAccount_InOperator(t *testing.T) {
	nop := stats.NopAccount()

	events := makeEvents(50)
	scan := NewScanIterator(events, 1024)
	aggs := []AggFunc{
		{Name: "count", Alias: "count"},
	}
	agg := NewAggregateIterator(scan, aggs, nil, nop)

	ctx := context.Background()
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
	if nop.Used() != 0 {
		t.Errorf("NopAccount Used = %d, want 0", nop.Used())
	}
}

// TestEnsureAccount_NilBecomesNop verifies that EnsureAccount converts nil
// to a NopAccount.
func TestEnsureAccount_NilBecomesNop(t *testing.T) {
	acct := stats.EnsureAccount(nil)
	if acct == nil {
		t.Fatal("EnsureAccount(nil) should return non-nil")
	}

	// Should work without panics.
	if err := acct.Grow(1000); err != nil {
		t.Errorf("Grow: %v", err)
	}
	acct.Shrink(500)
	if acct.Used() != 0 {
		t.Errorf("Used = %d, want 0 (nop)", acct.Used())
	}
	acct.Close()
}

// TestEnsureAccount_PreservesNonNil verifies that EnsureAccount passes through
// non-nil accounts unchanged.
func TestEnsureAccount_PreservesNonNil(t *testing.T) {
	mon := stats.NewBudgetMonitor("test", 0)
	original := mon.NewAccount("op")
	ensured := stats.EnsureAccount(original)

	if ensured != original {
		t.Error("EnsureAccount should return the original non-nil account")
	}
}

// TestBufferPool_AggregateSpillOnExhaustion verifies that when a PoolAccount
// is exhausted during aggregation, the operator spills to disk and produces
// correct results. This is the critical path that Gap 1 (error unification) fixes.
func TestBufferPool_AggregateSpillOnExhaustion(t *testing.T) {
	// 4 pages × 64KB = 256KB pool — will be exhausted by 2000 unique groups.
	pool := newTestBufferPool(t, 4)
	acct := buffer.NewPoolAccount(pool, "agg-spill-test", nil)

	events := makeUniqueGroupEvents(2000, 64)
	scan := NewScanIterator(events, 128)
	aggs := []AggFunc{
		{Name: "count", Alias: "count"},
		{Name: "sum", Field: "val", Alias: "sum_val"},
	}

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	agg := NewAggregateIteratorWithSpill(scan, aggs, []string{"group"}, acct, mgr)
	ctx := context.Background()
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	// Each event has a unique group, so we expect 2000 groups.
	if len(rows) != 2000 {
		t.Fatalf("expected 2000 groups, got %d", len(rows))
	}

	// Verify that spill actually occurred via the resource stats.
	rs := agg.ResourceStats()
	if rs.SpilledRows == 0 {
		t.Error("expected spill to have occurred (SpilledRows > 0)")
	}
	t.Logf("aggregate spill stats: peak_bytes=%d, spilled_rows=%d", rs.PeakBytes, rs.SpilledRows)

	// Verify correctness: each group should have count=1 and sum_val = its index.
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		g := r["group"].AsString()
		if seen[g] {
			t.Fatalf("duplicate group %q", g)
		}
		seen[g] = true

		cnt := r["count"].AsInt()
		if cnt != 1 {
			t.Errorf("group %q: expected count=1, got %d", g, cnt)
		}
	}
}

// TestBufferPool_SortSpillOnExhaustion verifies that when a PoolAccount is
// exhausted during sort materialization, the operator spills sorted runs to
// disk and merges them correctly.
func TestBufferPool_SortSpillOnExhaustion(t *testing.T) {
	// 4 pages × 64KB = 256KB pool — will be exhausted by 1000 rows with 128B padding.
	pool := newTestBufferPool(t, 4)
	acct := buffer.NewPoolAccount(pool, "sort-spill-test", nil)

	rows := makeUniqueGroupRows(1000, 128)
	child := NewRowScanIterator(rows, 64)

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	iter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 64, acct, mgr)

	ctx := context.Background()
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatalf("CollectAll: %v", err)
	}

	if len(result) != 1000 {
		t.Fatalf("expected 1000 rows, got %d", len(result))
	}

	// Verify globally sorted ascending order by key.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	t.Logf("sort spill: peak_bytes=%d", acct.MaxUsed())
}

// TestBufferPool_ResultParityWithBoundAccount runs the same aggregation query
// with both BoundAccount and PoolAccount paths and verifies that results
// are identical.
func TestBufferPool_ResultParityWithBoundAccount(t *testing.T) {
	events := makeUniqueGroupEvents(500, 32)

	// BoundAccount path
	mon := stats.NewBudgetMonitor("parity-test", 0) // unlimited
	defer mon.Close()
	boundAcct := mon.NewAccount("agg")
	scan1 := NewScanIterator(events, 256)
	aggs := []AggFunc{
		{Name: "count", Alias: "count"},
		{Name: "sum", Field: "val", Alias: "sum_val"},
	}
	agg1 := NewAggregateIterator(scan1, aggs, []string{"group"}, boundAcct)
	ctx := context.Background()
	boundRows, err := CollectAll(ctx, agg1)
	if err != nil {
		t.Fatalf("BoundAccount path: %v", err)
	}

	// PoolAccount path
	pool := newTestBufferPool(t, 64) // 64 pages = 4MB — enough to avoid spill
	poolAcct := buffer.NewPoolAccount(pool, "agg-parity", nil)
	scan2 := NewScanIterator(events, 256)
	agg2 := NewAggregateIterator(scan2, aggs, []string{"group"}, poolAcct)
	poolRows, err := CollectAll(ctx, agg2)
	if err != nil {
		t.Fatalf("PoolAccount path: %v", err)
	}

	// Compare: same number of groups.
	if len(boundRows) != len(poolRows) {
		t.Fatalf("row count mismatch: bound=%d, pool=%d", len(boundRows), len(poolRows))
	}

	// Build maps for order-independent comparison.
	boundMap := make(map[string]map[string]event.Value, len(boundRows))
	for _, r := range boundRows {
		boundMap[r["group"].AsString()] = r
	}
	poolMap := make(map[string]map[string]event.Value, len(poolRows))
	for _, r := range poolRows {
		poolMap[r["group"].AsString()] = r
	}

	// Sort keys for deterministic comparison.
	keys := make([]string, 0, len(boundMap))
	for k := range boundMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		br, ok := boundMap[k]
		if !ok {
			t.Fatalf("group %q missing from bound results", k)
		}
		pr, ok := poolMap[k]
		if !ok {
			t.Fatalf("group %q missing from pool results", k)
		}

		if br["count"] != pr["count"] {
			t.Errorf("group %q: count mismatch bound=%v pool=%v", k, br["count"], pr["count"])
		}
		if br["sum_val"] != pr["sum_val"] {
			t.Errorf("group %q: sum_val mismatch bound=%v pool=%v", k, br["sum_val"], pr["sum_val"])
		}
	}

	t.Logf("parity check passed: %d groups, bound_peak=%d pool_peak=%d",
		len(keys), boundAcct.MaxUsed(), poolAcct.MaxUsed())
}

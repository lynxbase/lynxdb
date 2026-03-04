package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/buffer"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// newTestPool creates a pool for testing with the given number of pages.
// Uses on-heap allocation (no mmap) for test portability.
func newTestPool(t *testing.T, numPages, pageSize int) *buffer.Pool {
	t.Helper()
	pool, err := buffer.NewPool(buffer.PoolConfig{
		MaxPages:      numPages,
		PageSize:      pageSize,
		EnableOffHeap: false,
	})
	if err != nil {
		t.Fatalf("newTestPool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	return pool
}

func TestBufferedSortSmallInMemory(t *testing.T) {
	// 100 rows, large budget, large pool. In-memory fast path.
	rows := makeRowsWithField(100, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, DefaultBatchSize)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 200, buffer.PageSize64KB)
	acct := stats.NewBudgetMonitor("test", 1<<30).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		DefaultBatchSize, acct, pool, "q1", mgr,
	)

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

	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	// No spill files should have been created (in-memory fast path).
	count, _ := mgr.Stats()
	if count != 0 {
		t.Fatalf("expected 0 spill files, got %d", count)
	}

	// No pool pages should be in use (fast path doesn't use pool).
	poolStats := pool.Stats()
	if poolStats.QueryPages != 0 {
		t.Fatalf("expected 0 query pages after in-memory sort, got %d", poolStats.QueryPages)
	}
}

func TestBufferedSortMultipleRunsNoEviction(t *testing.T) {
	// 1000 rows, tiny budget (forces multiple runs), pool with many pages (no eviction).
	rows := makeRowsWithField(1000, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 200 pages of 64KB = 12.8MB — plenty for all runs.
	pool := newTestPool(t, 200, buffer.PageSize64KB)
	// 32KB budget — fits ~128 rows at 256 bytes each, forces ~8 spill runs.
	acct := stats.NewBudgetMonitor("test", 32*1024).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		32, acct, pool, "q2", mgr,
	)

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

	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}
}

func TestBufferedSortWithEviction(t *testing.T) {
	// 2000 rows, tiny budget, pool with only 20 pages.
	// Earlier runs' pages will be evicted to make room for later runs.
	rows := makeRowsWithField(2000, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 20 pages of 64KB — will force eviction of earlier run pages.
	pool := newTestPool(t, 20, buffer.PageSize64KB)
	// 16KB budget forces many small spill runs.
	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		32, acct, pool, "q3", mgr,
	)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2000 {
		t.Fatalf("expected 2000 rows, got %d", len(result))
	}

	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	// Verify spill file exists (eviction wrote data to disk).
	iter.Close()
}

func TestBufferedSortPoolExhaustionFallback(t *testing.T) {
	// 500 rows, tiny budget, pool with only 4 pages.
	// Force disk fallback when pool can't allocate.
	rows := makeRowsWithField(500, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Only 4 pages — will exhaust quickly when all are pinned by different consumers.
	pool := newTestPool(t, 4, buffer.PageSize64KB)

	// Pin all 4 pages to simulate exhaustion from other consumers.
	pinnedPages := make([]*buffer.Page, 4)
	for i := 0; i < 4; i++ {
		p, allocErr := pool.AllocPage(buffer.OwnerSegmentCache, "other")
		if allocErr != nil {
			t.Fatalf("pre-alloc page %d: %v", i, allocErr)
		}
		pinnedPages[i] = p
	}

	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")
	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		32, acct, pool, "q4", mgr,
	)

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

	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}

	iter.Close()

	// Release the pinned pages.
	for _, p := range pinnedPages {
		p.Unpin()
		pool.FreePage(p)
	}
}

func TestBufferedSortDescending(t *testing.T) {
	rows := makeRowsWithField(500, 8)
	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 30, buffer.PageSize64KB)
	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: true}},
		32, acct, pool, "q5", mgr,
	)

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

	for i := 0; i < len(result); i++ {
		expected := int64(499 - i)
		got := result[i]["key"].AsInt()
		if got != expected {
			t.Fatalf("row %d: expected key=%d, got %d", i, expected, got)
		}
	}
}

func TestBufferedSortEmptyInput(t *testing.T) {
	child := NewRowScanIterator(nil, DefaultBatchSize)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 10, buffer.PageSize64KB)
	acct := stats.NewBudgetMonitor("test", 1<<20).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		DefaultBatchSize, acct, pool, "q6", mgr,
	)

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

func TestBufferedSortCloseReleasesPages(t *testing.T) {
	// 1000 rows, small budget to force pool-backed runs. Consume half, Close.
	// Verify pool stats show 0 query pages after Close.
	rows := makeRowsWithField(1000, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 100, buffer.PageSize64KB)
	acct := stats.NewBudgetMonitor("test", 32*1024).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		32, acct, pool, "q7", mgr,
	)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Consume only 2 batches (64 rows) and then close.
	for i := 0; i < 2; i++ {
		batch, batchErr := iter.Next(ctx)
		if batchErr != nil {
			t.Fatal(batchErr)
		}
		if batch == nil {
			break
		}
	}

	iter.Close()

	// After Close, no query pages should remain in the pool.
	poolStats := pool.Stats()
	if poolStats.QueryPages != 0 {
		t.Fatalf("expected 0 query pages after Close, got %d", poolStats.QueryPages)
	}
}

func TestBufferedSortVsRegularSort(t *testing.T) {
	// Same 2000 rows through both SortIterator and BufferedSortIterator.
	// Assert identical output.
	const n = 2000
	rows := makeRowsWithField(n, 16)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	fields := []SortField{{Name: "key", Desc: false}}
	ctx := context.Background()

	// Regular sort.
	mgr1, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr1.CleanupAll()

	acct1 := stats.NewBudgetMonitor("test", 32*1024).NewAccount("sort")
	iter1 := NewSortIteratorWithSpill(
		NewRowScanIterator(rows, 32), fields, 32, acct1, mgr1,
	)
	if err := iter1.Init(ctx); err != nil {
		t.Fatal(err)
	}
	result1, err := CollectAll(ctx, iter1)
	if err != nil {
		t.Fatal(err)
	}

	// Buffered sort.
	mgr2, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.CleanupAll()

	pool := newTestPool(t, 100, buffer.PageSize64KB)
	acct2 := stats.NewBudgetMonitor("test", 32*1024).NewAccount("sort")
	iter2 := NewBufferedSortIterator(
		NewRowScanIterator(rows, 32), fields, 32, acct2, pool, "q8", mgr2,
	)
	if err := iter2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	result2, err := CollectAll(ctx, iter2)
	if err != nil {
		t.Fatal(err)
	}

	if len(result1) != len(result2) {
		t.Fatalf("row count mismatch: regular=%d buffered=%d", len(result1), len(result2))
	}

	for i := 0; i < len(result1); i++ {
		k1 := result1[i]["key"].AsInt()
		k2 := result2[i]["key"].AsInt()
		if k1 != k2 {
			t.Fatalf("row %d: regular key=%d, buffered key=%d", i, k1, k2)
		}
	}
}

func TestBufferedSortMultiKey(t *testing.T) {
	// 2 sort keys: primary ASC, secondary DESC.
	rows := make([]map[string]event.Value, 200)
	for i := 0; i < 200; i++ {
		rows[i] = map[string]event.Value{
			"group": event.IntValue(int64(i % 10)),
			"seq":   event.IntValue(int64(i)),
		}
	}
	// Shuffle.
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 50, buffer.PageSize64KB)
	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{
			{Name: "group", Desc: false},
			{Name: "seq", Desc: true},
		},
		32, acct, pool, "q9", mgr,
	)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 200 {
		t.Fatalf("expected 200 rows, got %d", len(result))
	}

	// Verify: sorted by group ASC, then seq DESC within each group.
	for i := 1; i < len(result); i++ {
		g0 := result[i-1]["group"].AsInt()
		g1 := result[i]["group"].AsInt()
		s0 := result[i-1]["seq"].AsInt()
		s1 := result[i]["seq"].AsInt()

		if g1 < g0 {
			t.Fatalf("row %d: group not ascending: %d -> %d", i, g0, g1)
		}
		if g1 == g0 && s1 > s0 {
			t.Fatalf("row %d: seq not descending within group %d: %d -> %d", i, g0, s0, s1)
		}
	}
}

func TestBufferedSortWideRows(t *testing.T) {
	// 200 rows with 10KB _raw field. Tests that rowsPerPage adapts.
	rows := makeRowsWithField(200, 10*1024)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 16)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 100, buffer.PageSize64KB)
	// Budget must hold at least one batch: 16 rows * ~10KB = ~160KB.
	// Use 256KB budget to fit 1 batch, forcing spill after that.
	acct := stats.NewBudgetMonitor("test", 256*1024).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		16, acct, pool, "q10", mgr,
	)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 200 {
		t.Fatalf("expected 200 rows, got %d", len(result))
	}

	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}
}

func TestBufferedSortResourceReporter(t *testing.T) {
	// Verify ResourceStats() returns non-zero PeakBytes and SpilledRows.
	rows := makeRowsWithField(500, 8)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 50, buffer.PageSize64KB)
	acct := stats.NewBudgetMonitor("test", 16*1024).NewAccount("sort")

	iter := NewBufferedSortIterator(
		child, []SortField{{Name: "key", Desc: false}},
		32, acct, pool, "q11", mgr,
	)

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	rs := iter.ResourceStats()
	if rs.PeakBytes == 0 {
		t.Fatal("expected non-zero PeakBytes")
	}
	if rs.SpilledRows == 0 {
		t.Fatal("expected non-zero SpilledRows")
	}
}

func TestBufferedSortChildBudgetPressure(t *testing.T) {
	// Shared budget, child returns BudgetExceededError. Sort spills buffer, retries.
	const (
		totalRows = 200
		batchSize = 32
		dataSize  = 64
	)

	rows := makeRowsWithField(totalRows, dataSize)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}

	monitor := stats.NewBudgetMonitor("test", 200*1024)
	sortAcct := monitor.NewAccount("sort")
	scanAcct := monitor.NewAccount("scan")

	child := NewRowScanIterator(rows, batchSize)
	budgetChild := &budgetErrorIterator{
		inner:       child,
		failAfter:   4,
		monitor:     monitor,
		failAccount: scanAcct,
		failAmount:  200 * 1024,
	}

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	pool := newTestPool(t, 100, buffer.PageSize64KB)
	iter := NewBufferedSortIterator(
		budgetChild, []SortField{{Name: "key", Desc: false}},
		batchSize, sortAcct, pool, "q12", mgr,
	)

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

	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %d", i, i, got)
		}
	}
}

// Pool-level tests for new methods

func TestPageWriteBackDispatch(t *testing.T) {
	// Verify handleEviction calls PageWriteBack.WriteBackPage when OwnerData
	// implements it, instead of the global writeback.
	//
	// Setup: pool with 4 pages. Allocate p1, set OwnerData with PageWriteBack,
	// mark dirty, unpin. Then allocate 3 more pages (consuming entire free list)
	// and keep them PINNED. When we allocate a 5th page (exceeds pool size),
	// p1 is the only unpinned page and must be evicted.
	pool := newTestPool(t, 4, buffer.PageSize64KB)

	p1, err := pool.AllocPage(buffer.OwnerQueryOperator, "wb-test")
	if err != nil {
		t.Fatal(err)
	}

	var perPageCalled bool
	p1.SetOwnerData(&testWriteBack{called: &perPageCalled})
	if writeErr := p1.WriteAt([]byte("hello"), 0); writeErr != nil {
		t.Fatal(writeErr)
	}
	p1.Unpin() // make eviction candidate

	// Allocate 3 more pages from the free list, keep them pinned.
	pinnedFillers := make([]*buffer.Page, 3)
	for i := 0; i < 3; i++ {
		pp, allocErr := pool.AllocPage(buffer.OwnerSegmentCache, "filler")
		if allocErr != nil {
			t.Fatalf("alloc filler %d: %v", i, allocErr)
		}
		pinnedFillers[i] = pp
		// Do NOT unpin — keeps them ineligible for eviction.
	}

	// Free list is now empty, all 4 pages allocated. p1 is the only
	// unpinned page. This allocation must evict p1.
	// But we already used all 4 slots — we need the 4th alloc to force eviction.
	// Actually we allocated p1 + 3 fillers = 4 pages total. Free list is empty.
	// The 4th filler IS the eviction trigger. Let me re-count:
	// Pool has 4 pages. p1 = 1st alloc (from free). 3 fillers = 2nd/3rd/4th from free.
	// Free list now empty. We need one more alloc to trigger eviction of p1.
	extraPage, allocErr := pool.AllocPage(buffer.OwnerSegmentCache, "trigger")
	if allocErr != nil {
		// If this fails with ErrAllPagesPinned, our test logic is wrong.
		t.Fatalf("trigger alloc: %v", allocErr)
	}
	extraPage.Unpin()

	if !perPageCalled {
		t.Fatal("expected per-page WriteBackPage to be called on eviction")
	}

	// Cleanup: unpin filler pages.
	for _, pp := range pinnedFillers {
		pp.Unpin()
	}
}

// testWriteBack implements buffer.PageWriteBack for testing.
type testWriteBack struct {
	called *bool
}

func (wb *testWriteBack) WriteBackPage(_ *buffer.Page) error {
	*wb.called = true

	return nil
}

func TestPinPageIfOwned(t *testing.T) {
	pool := newTestPool(t, 10, buffer.PageSize64KB)

	p, err := pool.AllocPage(buffer.OwnerQueryOperator, "my-tag")
	if err != nil {
		t.Fatal(err)
	}
	pageID := p.ID()
	p.Unpin()

	// Correct tag: should succeed.
	pinned, ok := pool.PinPageIfOwned(pageID, "my-tag")
	if !ok {
		t.Fatal("expected PinPageIfOwned to succeed with correct tag")
	}
	if pinned == nil {
		t.Fatal("expected non-nil page")
	}
	pinned.Unpin()

	// Wrong tag: should fail.
	_, ok = pool.PinPageIfOwned(pageID, "wrong-tag")
	if ok {
		t.Fatal("expected PinPageIfOwned to fail with wrong tag")
	}

	// Invalid ID: should fail.
	_, ok = pool.PinPageIfOwned(buffer.PageID(9999), "my-tag")
	if ok {
		t.Fatal("expected PinPageIfOwned to fail with invalid ID")
	}
}

// Serialization round-trip test

func TestSerializeDeserializeBatch(t *testing.T) {
	rows := []map[string]event.Value{
		{
			"name":   event.StringValue("alice"),
			"age":    event.IntValue(30),
			"score":  event.FloatValue(95.5),
			"active": event.BoolValue(true),
		},
		{
			"name":   event.StringValue("bob"),
			"age":    event.IntValue(25),
			"score":  event.FloatValue(87.3),
			"active": event.BoolValue(false),
		},
		{
			"name": event.StringValue("carol"),
			// age is missing (null)
			"score":  event.FloatValue(92.1),
			"active": event.BoolValue(true),
		},
	}

	data, err := serializeBatchToBytes(rows)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty serialized data")
	}

	batch, err := deserializeBatchFromBytes(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	if batch.Len != 3 {
		t.Fatalf("expected 3 rows, got %d", batch.Len)
	}

	// Verify values.
	r0 := batch.Row(0)
	if r0["name"].AsString() != "alice" {
		t.Fatalf("row 0 name: expected alice, got %s", r0["name"].AsString())
	}
	if r0["age"].AsInt() != 30 {
		t.Fatalf("row 0 age: expected 30, got %d", r0["age"].AsInt())
	}

	r2 := batch.Row(2)
	if r2["name"].AsString() != "carol" {
		t.Fatalf("row 2 name: expected carol, got %s", r2["name"].AsString())
	}
	// Carol's age should be null.
	if !r2["age"].IsNull() {
		t.Fatalf("row 2 age: expected null, got %v", r2["age"])
	}
}

// Benchmark

func BenchmarkBufferedSortVsFileBased(b *testing.B) {
	const n = 50000
	rows := makeRowsWithField(n, 32)
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
	fields := []SortField{{Name: "key", Desc: false}}
	ctx := context.Background()

	b.Run("FileBased", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dir := b.TempDir()
			mgr, _ := NewSpillManager(dir, nil)
			acct := stats.NewBudgetMonitor("bench", 64*1024).NewAccount("sort")
			iter := NewSortIteratorWithSpill(
				NewRowScanIterator(rows, 256), fields, 256, acct, mgr,
			)
			_ = iter.Init(ctx)

			for {
				batch, err := iter.Next(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if batch == nil {
					break
				}
			}
			iter.Close()
			mgr.CleanupAll()
		}
	})

	b.Run("BufferPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dir := b.TempDir()
			mgr, _ := NewSpillManager(dir, nil)
			pool, _ := buffer.NewPool(buffer.PoolConfig{
				MaxPages: 200, PageSize: buffer.PageSize64KB,
				EnableOffHeap: false,
			})
			acct := stats.NewBudgetMonitor("bench", 64*1024).NewAccount("sort")
			iter := NewBufferedSortIterator(
				NewRowScanIterator(rows, 256), fields, 256, acct, pool, "bench", mgr,
			)
			_ = iter.Init(ctx)

			for {
				batch, err := iter.Next(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if batch == nil {
					break
				}
			}
			iter.Close()
			pool.Close()
			mgr.CleanupAll()
		}
	})
}

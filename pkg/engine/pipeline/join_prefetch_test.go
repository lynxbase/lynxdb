package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
)

// TestJoin_Prefetch_SameResults verifies that enabling prefetch produces
// identical results to the non-prefetch path.
func TestJoin_Prefetch_SameResults(t *testing.T) {
	numKeys := 10
	leftRows := makeJoinRows(100, numKeys, "left")
	rightRows := makeJoinRows(50, numKeys, "right")

	for _, joinType := range []string{"inner", "left"} {
		t.Run(joinType, func(t *testing.T) {
			// Run without prefetch.
			left1 := NewRowScanIterator(leftRows, DefaultBatchSize)
			right1 := NewRowScanIterator(rightRows, DefaultBatchSize)
			acct1 := stats.NewBudgetMonitor("test", 1<<30).NewAccount("join")
			iter1 := NewJoinIteratorWithBudget(left1, right1, "key", joinType, acct1)
			iter1.SetPrefetch(false)

			ctx := context.Background()
			result1, err := CollectAll(ctx, iter1)
			if err != nil {
				t.Fatalf("no-prefetch: %v", err)
			}

			// Run with prefetch.
			left2 := NewRowScanIterator(leftRows, DefaultBatchSize)
			right2 := NewRowScanIterator(rightRows, DefaultBatchSize)
			acct2 := stats.NewBudgetMonitor("test", 1<<30).NewAccount("join")
			iter2 := NewJoinIteratorWithBudget(left2, right2, "key", joinType, acct2)
			iter2.SetPrefetch(true)

			result2, err := CollectAll(ctx, iter2)
			if err != nil {
				t.Fatalf("prefetch: %v", err)
			}

			if len(result1) != len(result2) {
				t.Fatalf("result count mismatch: no-prefetch=%d, prefetch=%d",
					len(result1), len(result2))
			}

			// Verify all keys are present in both results.
			keys1 := countByKey(result1, "key")
			keys2 := countByKey(result2, "key")
			for k, v1 := range keys1 {
				if v2, ok := keys2[k]; !ok || v1 != v2 {
					t.Errorf("key %q: no-prefetch=%d, prefetch=%d", k, v1, v2)
				}
			}
		})
	}
}

// TestJoin_Prefetch_ContextCancel verifies that the prefetch goroutine
// exits cleanly when the context is canceled.
func TestJoin_Prefetch_ContextCancel(t *testing.T) {
	// Create a slow left side.
	slowLeft := newCUMockIterator(100, 10)
	slowLeft.delay = 50 * time.Millisecond

	rightRows := makeJoinRows(10, 5, "right")
	right := NewRowScanIterator(rightRows, DefaultBatchSize)

	acct := stats.NewBudgetMonitor("test", 1<<30).NewAccount("join")
	iter := NewJoinIteratorWithBudget(slowLeft, right, "key", "inner", acct)
	iter.SetPrefetch(true)

	ctx, cancel := context.WithCancel(context.Background())
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Read one batch to trigger prefetch start during buildHashTable.
	_, err := iter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Cancel context.
	cancel()

	// Close should complete without hanging (prefetch goroutine should exit).
	done := make(chan struct{})
	go func() {
		iter.Close()
		close(done)
	}()

	select {
	case <-done:
		// OK.
	case <-time.After(5 * time.Second):
		t.Fatal("Close() hung after context cancellation — prefetch goroutine leak")
	}
}

// TestJoin_Prefetch_GraceHashJoin verifies that when prefetch is active
// and the right side exceeds budget (triggering grace hash join), the left
// side is correctly read from the prefetch channel instead of j.left directly.
// Regression test for the data race between prefetch goroutine and graceHashJoin.
func TestJoin_Prefetch_GraceHashJoin(t *testing.T) {
	numKeys := 20
	leftRows := makeJoinRows(200, numKeys, "left")
	rightRows := makeJoinRows(100, numKeys, "right")

	left := NewRowScanIterator(leftRows, 32)
	right := NewRowScanIterator(rightRows, 32)

	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Small budget to force grace hash join.
	acct := stats.NewBudgetMonitor("test", 8*1024).NewAccount("join")
	iter := NewJoinIteratorWithSpill(left, right, "key", "inner", acct, mgr)
	iter.SetPrefetch(true) // Enable prefetch — this is the critical part.

	ctx := context.Background()
	if err := iter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	// Each left row's key matches 5 right rows (100/20).
	expected := 200 * 5
	if len(result) != expected {
		t.Fatalf("expected %d joined rows, got %d", expected, len(result))
	}

	// Verify spill happened.
	rs := iter.ResourceStats()
	if rs.SpilledRows == 0 {
		t.Fatal("expected SpilledRows > 0 for grace join with prefetch")
	}
}

// TestJoin_Prefetch_EmptyLeft verifies prefetch with empty left side.
func TestJoin_Prefetch_EmptyLeft(t *testing.T) {
	var leftRows []map[string]event.Value
	rightRows := makeJoinRows(10, 5, "right")

	left := NewRowScanIterator(leftRows, DefaultBatchSize)
	right := NewRowScanIterator(rightRows, DefaultBatchSize)

	acct := stats.NewBudgetMonitor("test", 1<<30).NewAccount("join")
	iter := NewJoinIteratorWithBudget(left, right, "key", "inner", acct)
	iter.SetPrefetch(true)

	ctx := context.Background()
	result, err := CollectAll(ctx, iter)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 0 {
		t.Fatalf("expected 0 rows for inner join with empty left, got %d", len(result))
	}
}

// countByKey counts rows per distinct value of a given field.
func countByKey(rows []map[string]event.Value, field string) map[string]int {
	counts := make(map[string]int)
	for _, row := range rows {
		key := ""
		if v, ok := row[field]; ok {
			key = v.String()
		}
		counts[key]++
	}

	return counts
}

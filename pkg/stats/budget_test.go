package stats

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestBudgetMonitor_BasicTracking(t *testing.T) {
	mon := NewBudgetMonitor("test", 0) // unlimited
	acct := mon.NewAccount("scan")

	if err := acct.Grow(1000); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if acct.Used() != 1000 {
		t.Fatalf("expected used=1000, got %d", acct.Used())
	}
	if mon.CurAllocated() != 1000 {
		t.Fatalf("expected cur=1000, got %d", mon.CurAllocated())
	}
	if mon.MaxAllocated() != 1000 {
		t.Fatalf("expected max=1000, got %d", mon.MaxAllocated())
	}
}

func TestBudgetMonitor_HighWaterMark(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)
	acct := mon.NewAccount("agg")

	if err := acct.Grow(500); err != nil {
		t.Fatal(err)
	}
	if err := acct.Grow(500); err != nil {
		t.Fatal(err)
	}
	// Peak = 1000.
	acct.Shrink(800)
	// Current = 200, peak still 1000.
	if mon.MaxAllocated() != 1000 {
		t.Fatalf("expected max=1000 after shrink, got %d", mon.MaxAllocated())
	}
	if mon.CurAllocated() != 200 {
		t.Fatalf("expected cur=200 after shrink, got %d", mon.CurAllocated())
	}

	// Grow again but below peak.
	if err := acct.Grow(100); err != nil {
		t.Fatal(err)
	}
	if mon.MaxAllocated() != 1000 {
		t.Fatalf("expected max=1000, got %d", mon.MaxAllocated())
	}

	// Grow past previous peak.
	if err := acct.Grow(900); err != nil {
		t.Fatal(err)
	}
	if mon.MaxAllocated() != 1200 {
		t.Fatalf("expected max=1200, got %d", mon.MaxAllocated())
	}
}

func TestBudgetMonitor_LimitEnforcement(t *testing.T) {
	mon := NewBudgetMonitor("query-1", 1024)
	acct := mon.NewAccount("sort")

	if err := acct.Grow(512); err != nil {
		t.Fatal(err)
	}
	if err := acct.Grow(512); err != nil {
		t.Fatal(err)
	}
	// At limit exactly (1024). Next grow should fail.
	err := acct.Grow(1)
	if err == nil {
		t.Fatal("expected BudgetExceededError, got nil")
	}

	var budgetErr *BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected *BudgetExceededError, got %T: %v", err, err)
	}
	if budgetErr.Monitor != "query-1" {
		t.Fatalf("expected monitor=query-1, got %s", budgetErr.Monitor)
	}
	if budgetErr.Account != "sort" {
		t.Fatalf("expected account=sort, got %s", budgetErr.Account)
	}
	if budgetErr.Requested != 1 {
		t.Fatalf("expected requested=1, got %d", budgetErr.Requested)
	}
	if budgetErr.Current != 1024 {
		t.Fatalf("expected current=1024, got %d", budgetErr.Current)
	}
	if budgetErr.Limit != 1024 {
		t.Fatalf("expected limit=1024, got %d", budgetErr.Limit)
	}
}

func TestBudgetMonitor_MultiAccount(t *testing.T) {
	mon := NewBudgetMonitor("query", 1000)
	scan := mon.NewAccount("scan")
	agg := mon.NewAccount("aggregate")

	if err := scan.Grow(600); err != nil {
		t.Fatal(err)
	}
	if err := agg.Grow(300); err != nil {
		t.Fatal(err)
	}
	// Total = 900. agg tries to grow by 200 → exceeds 1000.
	err := agg.Grow(200)
	if err == nil {
		t.Fatal("expected budget exceeded")
	}
	if !IsBudgetExceeded(err) {
		t.Fatalf("expected IsBudgetExceeded=true, got false")
	}

	// Shrink scan, then agg should succeed.
	scan.Shrink(200) // scan=400, total=700
	if err := agg.Grow(200); err != nil {
		t.Fatalf("expected success after shrink, got: %v", err)
	}
	// Total = 900, scan=400, agg=500.
	if mon.CurAllocated() != 900 {
		t.Fatalf("expected cur=900, got %d", mon.CurAllocated())
	}
}

func TestBudgetMonitor_AccountClose(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)
	acct := mon.NewAccount("join")

	if err := acct.Grow(500); err != nil {
		t.Fatal(err)
	}
	acct.Close()

	if mon.CurAllocated() != 0 {
		t.Fatalf("expected cur=0 after close, got %d", mon.CurAllocated())
	}
	if acct.Used() != 0 {
		t.Fatalf("expected used=0 after close, got %d", acct.Used())
	}
	// Double close is safe.
	acct.Close()
}

func TestBoundAccount_NilSafety(t *testing.T) {
	var acct *BoundAccount
	// All operations on nil account should be no-ops.
	if err := acct.Grow(100); err != nil {
		t.Fatalf("nil.Grow should return nil, got: %v", err)
	}
	acct.Shrink(50)
	acct.Close()
	if acct.Used() != 0 {
		t.Fatalf("nil.Used should return 0, got %d", acct.Used())
	}
}

func TestBudgetMonitor_NilMonitor(t *testing.T) {
	var mon *BudgetMonitor
	acct := mon.NewAccount("test")
	if acct != nil {
		t.Fatal("nil monitor should return nil account")
	}
	if mon.MaxAllocated() != 0 {
		t.Fatal("nil monitor MaxAllocated should be 0")
	}
	if mon.CurAllocated() != 0 {
		t.Fatal("nil monitor CurAllocated should be 0")
	}
}

func TestBoundAccount_GrowZeroOrNegative(t *testing.T) {
	mon := NewBudgetMonitor("test", 100)
	acct := mon.NewAccount("test")

	if err := acct.Grow(0); err != nil {
		t.Fatalf("Grow(0) should succeed, got: %v", err)
	}
	if err := acct.Grow(-10); err != nil {
		t.Fatalf("Grow(-10) should succeed (no-op), got: %v", err)
	}
	if acct.Used() != 0 {
		t.Fatalf("expected used=0 after zero/negative grows, got %d", acct.Used())
	}
}

func TestBoundAccount_ShrinkBeyondUsed(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)
	acct := mon.NewAccount("test")

	if err := acct.Grow(100); err != nil {
		t.Fatal(err)
	}
	// Shrink more than used — should clamp to used.
	acct.Shrink(200)
	if acct.Used() != 0 {
		t.Fatalf("expected used=0 after over-shrink, got %d", acct.Used())
	}
	if mon.CurAllocated() != 0 {
		t.Fatalf("expected cur=0 after over-shrink, got %d", mon.CurAllocated())
	}
}

func TestIsBudgetExceeded(t *testing.T) {
	err := &BudgetExceededError{Monitor: "m", Account: "a", Requested: 1, Current: 0, Limit: 0}
	if !IsBudgetExceeded(err) {
		t.Fatal("expected true for BudgetExceededError")
	}
	if IsBudgetExceeded(errors.New("some other error")) {
		t.Fatal("expected false for non-BudgetExceededError")
	}
	if IsBudgetExceeded(nil) {
		t.Fatal("expected false for nil")
	}
}

// IsMemoryExhausted tests

func TestIsMemoryExhausted_BudgetExceeded(t *testing.T) {
	err := &BudgetExceededError{Monitor: "query", Account: "sort", Requested: 1, Current: 100, Limit: 100}
	if !IsMemoryExhausted(err) {
		t.Fatal("expected true for BudgetExceededError")
	}
}

func TestIsMemoryExhausted_PoolExhausted(t *testing.T) {
	err := &PoolExhaustedError{Pool: "global", Requested: 1, Current: 100, Limit: 100}
	if !IsMemoryExhausted(err) {
		t.Fatal("expected true for PoolExhaustedError")
	}
}

func TestIsMemoryExhausted_WrappedErrors(t *testing.T) {
	// Wrapped BudgetExceededError.
	inner := &BudgetExceededError{Monitor: "buffer-pool", Account: "job-1", Requested: 64, Current: 0, Limit: 128}
	wrapped := fmt.Errorf("operator: %w", inner)
	if !IsMemoryExhausted(wrapped) {
		t.Fatal("expected true for wrapped BudgetExceededError")
	}

	// Wrapped PoolExhaustedError.
	innerPool := &PoolExhaustedError{Pool: "global", Requested: 1, Current: 0, Limit: 0}
	wrappedPool := fmt.Errorf("monitor: %w", innerPool)
	if !IsMemoryExhausted(wrappedPool) {
		t.Fatal("expected true for wrapped PoolExhaustedError")
	}
}

func TestIsMemoryExhausted_Nil(t *testing.T) {
	if IsMemoryExhausted(nil) {
		t.Fatal("expected false for nil")
	}
}

func TestIsMemoryExhausted_GenericError(t *testing.T) {
	if IsMemoryExhausted(errors.New("disk full")) {
		t.Fatal("expected false for generic error")
	}
}

// Parent pool integration tests

func TestBudgetMonitor_WithParent_Amortization(t *testing.T) {
	parent := NewRootMonitor("pool", 0) // unlimited
	mon := NewBudgetMonitorWithParent("query", 0, parent)
	acct := mon.NewAccount("scan")

	// Small grows should be amortized: parent gets one 64KB reservation,
	// not one per-byte.
	for i := 0; i < 100; i++ {
		if err := acct.Grow(100); err != nil {
			t.Fatalf("grow %d: %v", i, err)
		}
	}
	// 100 * 100 = 10,000 bytes used.
	if acct.Used() != 10000 {
		t.Fatalf("expected used=10000, got %d", acct.Used())
	}
	// Parent should have reserved at least 10,000 bytes (likely poolAllocationSize=64KB).
	if parent.CurAllocated() < 10000 {
		t.Fatalf("expected parent cur >= 10000, got %d", parent.CurAllocated())
	}
	// But parent reservation should be amortized — not 100 separate calls.
	// With 64KB chunks and 10KB total, we expect exactly 1 parent reservation.
	if parent.CurAllocated() != poolAllocationSize {
		t.Fatalf("expected parent cur=%d (one chunk), got %d", poolAllocationSize, parent.CurAllocated())
	}
}

func TestBudgetMonitor_WithParent_PoolExhaustion(t *testing.T) {
	parent := NewRootMonitor("pool", 1000)
	mon := NewBudgetMonitorWithParent("query", 0, parent) // no per-query limit
	acct := mon.NewAccount("sort")

	// Grow past the pool limit.
	err := acct.Grow(1001)
	if err == nil {
		t.Fatal("expected PoolExhaustedError, got nil")
	}
	if !IsPoolExhausted(err) {
		t.Fatalf("expected IsPoolExhausted=true, got %T: %v", err, err)
	}
	// Account should be unchanged after failed grow.
	if acct.Used() != 0 {
		t.Fatalf("expected used=0 after failed grow, got %d", acct.Used())
	}
}

func TestBudgetMonitor_WithParent_PerQueryLimitCheckedFirst(t *testing.T) {
	parent := NewRootMonitor("pool", 1<<30) // 1GB pool
	mon := NewBudgetMonitorWithParent("query", 500, parent)
	acct := mon.NewAccount("agg")

	if err := acct.Grow(500); err != nil {
		t.Fatal(err)
	}
	// Per-query limit reached. Should get BudgetExceededError, not PoolExhaustedError.
	err := acct.Grow(1)
	if err == nil {
		t.Fatal("expected error")
	}
	if !IsBudgetExceeded(err) {
		t.Fatalf("expected BudgetExceededError, got %T: %v", err, err)
	}
}

func TestBudgetMonitor_Close_ReturnsToParent(t *testing.T) {
	parent := NewRootMonitor("pool", 0)
	mon := NewBudgetMonitorWithParent("query", 0, parent)
	acct := mon.NewAccount("scan")

	if err := acct.Grow(1000); err != nil {
		t.Fatal(err)
	}
	// Parent has some reservation (at least 1000, probably poolAllocationSize).
	beforeClose := parent.CurAllocated()
	if beforeClose == 0 {
		t.Fatal("expected parent to have reservations")
	}

	mon.Close()

	if parent.CurAllocated() != 0 {
		t.Fatalf("expected parent cur=0 after Close, got %d", parent.CurAllocated())
	}
}

func TestBudgetMonitor_Close_NilParent(t *testing.T) {
	mon := NewBudgetMonitor("query", 0) // no parent
	acct := mon.NewAccount("scan")
	if err := acct.Grow(1000); err != nil {
		t.Fatal(err)
	}
	// Close on nil parent should be a no-op (not panic).
	mon.Close()
}

func TestBudgetMonitor_Close_NilMonitor(t *testing.T) {
	var mon *BudgetMonitor
	// Should not panic.
	mon.Close()
}

func TestBudgetMonitor_MultipleChildrenShareParent(t *testing.T) {
	// Pool must be large enough for amortized 64KB chunks from both children.
	parent := NewRootMonitor("pool", 256*1024)

	m1 := NewBudgetMonitorWithParent("q1", 0, parent)
	m2 := NewBudgetMonitorWithParent("q2", 0, parent)

	a1 := m1.NewAccount("scan")
	a2 := m2.NewAccount("scan")

	if err := a1.Grow(10000); err != nil {
		t.Fatal(err)
	}
	if err := a2.Grow(10000); err != nil {
		t.Fatal(err)
	}
	// Both queries drew from the pool. Exact reservation depends on amortization.

	// Close one child, freeing its share.
	m1.Close()

	// Now q2 should be able to grow further.
	if err := a2.Grow(5000); err != nil {
		t.Fatalf("expected success after m1.Close, got: %v", err)
	}

	m2.Close()

	if parent.CurAllocated() != 0 {
		t.Fatalf("expected parent cur=0 after both close, got %d", parent.CurAllocated())
	}
}

// Resize tests

func TestBoundAccount_Resize_Grow(t *testing.T) {
	mon := NewBudgetMonitor("test", 1000)
	acct := mon.NewAccount("hash")

	if err := acct.Grow(100); err != nil {
		t.Fatal(err)
	}
	// Resize from 100 to 300 — net grow of 200.
	if err := acct.Resize(100, 300); err != nil {
		t.Fatal(err)
	}
	if acct.Used() != 300 {
		t.Fatalf("expected used=300, got %d", acct.Used())
	}
	if mon.CurAllocated() != 300 {
		t.Fatalf("expected cur=300, got %d", mon.CurAllocated())
	}
}

func TestBoundAccount_Resize_Shrink(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)
	acct := mon.NewAccount("hash")

	if err := acct.Grow(500); err != nil {
		t.Fatal(err)
	}
	// Resize from 500 to 200 — net shrink of 300.
	if err := acct.Resize(500, 200); err != nil {
		t.Fatal(err)
	}
	if acct.Used() != 200 {
		t.Fatalf("expected used=200, got %d", acct.Used())
	}
}

func TestBoundAccount_Resize_NoChange(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)
	acct := mon.NewAccount("hash")

	if err := acct.Grow(100); err != nil {
		t.Fatal(err)
	}
	if err := acct.Resize(100, 100); err != nil {
		t.Fatal(err)
	}
	if acct.Used() != 100 {
		t.Fatalf("expected used=100 unchanged, got %d", acct.Used())
	}
}

func TestBoundAccount_Resize_FailPreservesTracking(t *testing.T) {
	mon := NewBudgetMonitor("test", 500)
	acct := mon.NewAccount("hash")

	if err := acct.Grow(400); err != nil {
		t.Fatal(err)
	}
	// Resize from 400 to 600 — would exceed limit.
	err := acct.Resize(400, 600)
	if err == nil {
		t.Fatal("expected error")
	}
	// Tracking should be unchanged.
	if acct.Used() != 400 {
		t.Fatalf("expected used=400 after failed resize, got %d", acct.Used())
	}
	if mon.CurAllocated() != 400 {
		t.Fatalf("expected cur=400 after failed resize, got %d", mon.CurAllocated())
	}
}

func TestBoundAccount_Resize_NilSafety(t *testing.T) {
	var acct *BoundAccount
	if err := acct.Resize(100, 200); err != nil {
		t.Fatalf("nil.Resize should return nil, got: %v", err)
	}
}

// Shrink + parent pool regression tests
// These tests verify the fix for the global memory pool leak where shrink()
// failed to move bytes back to reserved, causing Close() to under-return
// bytes to the parent.

func TestBudgetMonitor_ShrinkReturnsToParentOnClose(t *testing.T) {
	parent := NewRootMonitor("pool", 0) // unlimited
	mon := NewBudgetMonitorWithParent("query", 0, parent)
	acct := mon.NewAccount("scan")

	if err := acct.Grow(10000); err != nil {
		t.Fatal(err)
	}
	// Simulate operator cleanup: acct.Close() calls shrink internally.
	acct.Close()
	// At this point curAllocated=0. The shrunk bytes must still be
	// tracked in reserved so Close() returns them to the parent.
	mon.Close()

	if parent.CurAllocated() != 0 {
		t.Fatalf("expected parent cur=0 after shrink+close, got %d", parent.CurAllocated())
	}
}

func TestBudgetMonitor_SequentialQueriesNoPoolLeak(t *testing.T) {
	parent := NewRootMonitor("pool", 1<<20) // 1MB pool

	for i := 0; i < 100; i++ {
		mon := NewBudgetMonitorWithParent("query", 0, parent)
		scan := mon.NewAccount("scan")
		agg := mon.NewAccount("agg")

		if err := scan.Grow(50000); err != nil {
			t.Fatalf("iter %d scan.Grow: %v", i, err)
		}
		if err := agg.Grow(10000); err != nil {
			t.Fatalf("iter %d agg.Grow: %v", i, err)
		}

		scan.Close() // operator cleanup
		agg.Close()  // operator cleanup
		mon.Close()  // query cleanup
	}

	if parent.CurAllocated() != 0 {
		t.Fatalf("pool leaked after 100 queries: cur=%d", parent.CurAllocated())
	}
}

func TestBudgetMonitor_SpillShrinkNoLeak(t *testing.T) {
	parent := NewRootMonitor("pool", 0)
	mon := NewBudgetMonitorWithParent("query", 0, parent)
	acct := mon.NewAccount("agg")

	// Simulate: grow -> spill -> shrink -> grow again -> close
	if err := acct.Grow(100000); err != nil {
		t.Fatal(err)
	}
	acct.Shrink(acct.Used()) // spill to disk
	if err := acct.Grow(50000); err != nil {
		t.Fatal(err)
	}
	acct.Close()
	mon.Close()

	if parent.CurAllocated() != 0 {
		t.Fatalf("pool leaked after spill pattern: cur=%d", parent.CurAllocated())
	}
}

// ObserveGrow / ObserveShrink tests

func TestBudgetMonitor_ObserveGrow(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)

	mon.ObserveGrow(500)
	if mon.CurAllocated() != 500 {
		t.Fatalf("expected cur=500, got %d", mon.CurAllocated())
	}
	if mon.MaxAllocated() != 500 {
		t.Fatalf("expected max=500, got %d", mon.MaxAllocated())
	}

	mon.ObserveGrow(300)
	if mon.CurAllocated() != 800 {
		t.Fatalf("expected cur=800, got %d", mon.CurAllocated())
	}
	if mon.MaxAllocated() != 800 {
		t.Fatalf("expected max=800, got %d", mon.MaxAllocated())
	}
}

func TestBudgetMonitor_ObserveShrink(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)

	mon.ObserveGrow(1000)
	mon.ObserveShrink(400)
	if mon.CurAllocated() != 600 {
		t.Fatalf("expected cur=600, got %d", mon.CurAllocated())
	}
	// MaxAllocated should still reflect the peak.
	if mon.MaxAllocated() != 1000 {
		t.Fatalf("expected max=1000, got %d", mon.MaxAllocated())
	}
}

func TestBudgetMonitor_ObserveShrinkAll(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)

	mon.ObserveGrow(500)
	mon.ObserveShrink(500)
	if mon.CurAllocated() != 0 {
		t.Fatalf("expected cur=0, got %d", mon.CurAllocated())
	}
	if mon.MaxAllocated() != 500 {
		t.Fatalf("expected max=500, got %d", mon.MaxAllocated())
	}
}

func TestBudgetMonitor_ObserveShrinkPanicsOnOverflow(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)
	mon.ObserveGrow(100)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on ObserveShrink exceeding curAllocated")
		}
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "ObserveShrink") {
			t.Fatalf("unexpected panic message: %s", msg)
		}
	}()
	mon.ObserveShrink(200) // should panic
}

func TestBudgetMonitor_ObserveGrowZeroOrNegative(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)

	mon.ObserveGrow(0)
	mon.ObserveGrow(-5)
	if mon.CurAllocated() != 0 {
		t.Fatalf("expected cur=0, got %d", mon.CurAllocated())
	}
}

func TestBudgetMonitor_ObserveShrinkZeroOrNegative(t *testing.T) {
	mon := NewBudgetMonitor("test", 0)
	mon.ObserveGrow(100)

	mon.ObserveShrink(0)
	mon.ObserveShrink(-5)
	if mon.CurAllocated() != 100 {
		t.Fatalf("expected cur=100, got %d", mon.CurAllocated())
	}
}

func TestBudgetMonitor_ObserveGrowNilSafety(t *testing.T) {
	var mon *BudgetMonitor
	// Should not panic.
	mon.ObserveGrow(100)
	mon.ObserveShrink(50)
}

func TestBudgetMonitor_ObserveDoesNotEnforceLimit(t *testing.T) {
	// ObserveGrow must NOT enforce the per-query limit — the buffer pool
	// handles enforcement. Observe is tracking-only.
	mon := NewBudgetMonitor("test", 500) // limit = 500

	mon.ObserveGrow(1000) // exceeds limit but should not error
	if mon.CurAllocated() != 1000 {
		t.Fatalf("expected cur=1000 (no enforcement), got %d", mon.CurAllocated())
	}
	if mon.MaxAllocated() != 1000 {
		t.Fatalf("expected max=1000, got %d", mon.MaxAllocated())
	}
}

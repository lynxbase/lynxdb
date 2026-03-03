package buffer

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
)

// Verify PoolAccount satisfies stats.MemoryAccount at compile time.
var _ stats.MemoryAccount = (*PoolAccount)(nil)

func TestPoolAccount_Grow(t *testing.T) {
	bp := newTestPool(t, 16)
	acct := NewPoolAccount(bp, "test-query", nil)
	defer acct.Close()

	if err := acct.Grow(100); err != nil {
		t.Fatalf("Grow(100): %v", err)
	}
	if acct.Used() != 100 {
		t.Errorf("Used = %d, want 100", acct.Used())
	}
	if acct.MaxUsed() != 100 {
		t.Errorf("MaxUsed = %d, want 100", acct.MaxUsed())
	}
	// 100 bytes < 64KB page → should have allocated 1 page.
	if acct.PageCount() != 1 {
		t.Errorf("PageCount = %d, want 1", acct.PageCount())
	}
}

func TestPoolAccount_GrowMultiplePages(t *testing.T) {
	bp := newTestPool(t, 16)
	acct := NewPoolAccount(bp, "test-multi", nil)
	defer acct.Close()

	// Grow beyond one page.
	if err := acct.Grow(int64(PageSize64KB) + 1); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	if acct.PageCount() != 2 {
		t.Errorf("PageCount = %d, want 2", acct.PageCount())
	}
}

func TestPoolAccount_GrowExhausted(t *testing.T) {
	// Pool with only 2 pages.
	bp := newTestPool(t, 2)
	acct := NewPoolAccount(bp, "test-exhaust", nil)
	defer acct.Close()

	// Allocate both pages.
	if err := acct.Grow(int64(PageSize64KB) * 2); err != nil {
		t.Fatalf("Grow(2 pages): %v", err)
	}

	// Third page should fail.
	err := acct.Grow(int64(PageSize64KB))
	if err == nil {
		t.Fatal("expected error when pool is exhausted")
	}
}

func TestPoolAccount_GrowExhausted_IsMemoryExhausted(t *testing.T) {
	// Pool with only 2 pages — exhaust it, then verify the error satisfies
	// both IsBudgetExceeded and IsMemoryExhausted. This is the critical fix
	// that enables spill-to-disk when using the buffer pool path.
	bp := newTestPool(t, 2)
	acct := NewPoolAccount(bp, "test-exhaust-type", nil)
	defer acct.Close()

	// Allocate both pages.
	if err := acct.Grow(int64(PageSize64KB) * 2); err != nil {
		t.Fatalf("Grow(2 pages): %v", err)
	}

	// Third page should fail with *BudgetExceededError.
	err := acct.Grow(int64(PageSize64KB))
	if err == nil {
		t.Fatal("expected error when pool is exhausted")
	}

	// Must satisfy IsBudgetExceeded — operators check this for spill decisions.
	if !stats.IsBudgetExceeded(err) {
		t.Fatalf("expected IsBudgetExceeded=true for pool exhaustion, got false; error type: %T, error: %v", err, err)
	}

	// Must satisfy IsMemoryExhausted — the unified check.
	if !stats.IsMemoryExhausted(err) {
		t.Fatalf("expected IsMemoryExhausted=true for pool exhaustion, got false; error type: %T, error: %v", err, err)
	}

	// Verify the error carries useful diagnostic information.
	var budgetErr *stats.BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected errors.As to find *BudgetExceededError, got %T", err)
	}
	if budgetErr.Monitor != "buffer-pool" {
		t.Errorf("expected Monitor=buffer-pool, got %s", budgetErr.Monitor)
	}
	if budgetErr.Limit != int64(2)*int64(PageSize64KB) {
		t.Errorf("expected Limit=%d, got %d", int64(2)*int64(PageSize64KB), budgetErr.Limit)
	}
}

func TestPoolAccount_Shrink(t *testing.T) {
	bp := newTestPool(t, 16)
	acct := NewPoolAccount(bp, "test-shrink", nil)
	defer acct.Close()

	if err := acct.Grow(1000); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	acct.Shrink(600)
	if acct.Used() != 400 {
		t.Errorf("Used = %d, want 400", acct.Used())
	}
	// MaxUsed should still reflect peak.
	if acct.MaxUsed() != 1000 {
		t.Errorf("MaxUsed = %d, want 1000", acct.MaxUsed())
	}
}

func TestPoolAccount_ShrinkOverflow(t *testing.T) {
	bp := newTestPool(t, 8)
	acct := NewPoolAccount(bp, "test-overflow", nil)
	defer acct.Close()

	if err := acct.Grow(500); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	acct.Shrink(9999) // more than used
	if acct.Used() != 0 {
		t.Errorf("Used = %d, want 0", acct.Used())
	}
}

func TestPoolAccount_Close(t *testing.T) {
	bp := newTestPool(t, 8)
	acct := NewPoolAccount(bp, "test-close", nil)

	if err := acct.Grow(int64(PageSize64KB) * 3); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	// Should have allocated 3 pages.
	if acct.PageCount() != 3 {
		t.Errorf("PageCount before close = %d, want 3", acct.PageCount())
	}

	preStats := bp.Stats()
	acct.Close()

	postStats := bp.Stats()
	if acct.Used() != 0 {
		t.Errorf("Used after close = %d, want 0", acct.Used())
	}
	if acct.PageCount() != 0 {
		t.Errorf("PageCount after close = %d, want 0", acct.PageCount())
	}
	// Pages should be returned to the pool.
	if postStats.FreePages <= preStats.FreePages {
		t.Errorf("FreePages did not increase after close: %d -> %d",
			preStats.FreePages, postStats.FreePages)
	}
}

func TestPoolAccount_NilSafe(t *testing.T) {
	var acct *PoolAccount

	// All methods should be no-ops on nil.
	if err := acct.Grow(100); err != nil {
		t.Errorf("nil Grow: %v", err)
	}
	acct.Shrink(100)
	acct.Close()
	if acct.Used() != 0 {
		t.Errorf("nil Used = %d, want 0", acct.Used())
	}
	if acct.MaxUsed() != 0 {
		t.Errorf("nil MaxUsed = %d, want 0", acct.MaxUsed())
	}
	if acct.PageCount() != 0 {
		t.Errorf("nil PageCount = %d, want 0", acct.PageCount())
	}
}

func TestPoolAccount_NilPool(t *testing.T) {
	acct := NewPoolAccount(nil, "test-nil-pool", nil)
	if acct != nil {
		t.Error("NewPoolAccount(nil) should return nil")
	}
}

func TestPoolAccount_GrowZero(t *testing.T) {
	bp := newTestPool(t, 8)
	acct := NewPoolAccount(bp, "test-zero", nil)
	defer acct.Close()

	if err := acct.Grow(0); err != nil {
		t.Errorf("Grow(0): %v", err)
	}
	if err := acct.Grow(-1); err != nil {
		t.Errorf("Grow(-1): %v", err)
	}
	if acct.Used() != 0 {
		t.Errorf("Used = %d, want 0", acct.Used())
	}
	if acct.PageCount() != 0 {
		t.Errorf("PageCount = %d, want 0", acct.PageCount())
	}
}

func TestPoolAccount_AsMemoryAccount(t *testing.T) {
	bp := newTestPool(t, 8)

	// Use through the interface.
	var ma stats.MemoryAccount = NewPoolAccount(bp, "test-iface", nil)
	defer ma.Close()

	if err := ma.Grow(500); err != nil {
		t.Fatalf("Grow via interface: %v", err)
	}
	if ma.Used() != 500 {
		t.Errorf("Used via interface = %d, want 500", ma.Used())
	}
	ma.Shrink(200)
	if ma.Used() != 300 {
		t.Errorf("Used after shrink = %d, want 300", ma.Used())
	}
}

func TestPoolAccount_MonitorTracking(t *testing.T) {
	bp := newTestPool(t, 16)
	mon := stats.NewBudgetMonitor("test", 0)
	acct := NewPoolAccount(bp, "test-tracking", mon)

	// Grow 1000 → monitor tracks 1000
	if err := acct.Grow(1000); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	if mon.MaxAllocated() != 1000 {
		t.Errorf("MaxAllocated = %d, want 1000", mon.MaxAllocated())
	}

	// Grow 500 more → monitor tracks 1500
	if err := acct.Grow(500); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	if mon.MaxAllocated() != 1500 {
		t.Errorf("MaxAllocated = %d, want 1500", mon.MaxAllocated())
	}

	// Shrink 800 → curAllocated=700, but MaxAllocated still 1500
	acct.Shrink(800)
	if mon.MaxAllocated() != 1500 {
		t.Errorf("MaxAllocated after shrink = %d, want 1500", mon.MaxAllocated())
	}
	if mon.CurAllocated() != 700 {
		t.Errorf("CurAllocated = %d, want 700", mon.CurAllocated())
	}

	// Close → curAllocated=0
	acct.Close()
	if mon.CurAllocated() != 0 {
		t.Errorf("CurAllocated after close = %d, want 0", mon.CurAllocated())
	}
	// MaxAllocated unchanged
	if mon.MaxAllocated() != 1500 {
		t.Errorf("MaxAllocated after close = %d, want 1500", mon.MaxAllocated())
	}
}

func TestPoolAccount_ConcurrentMonitorTracking(t *testing.T) {
	bp := newTestPool(t, 64)
	mon := stats.NewBudgetMonitor("concurrent", 0)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			acct := NewPoolAccount(bp, fmt.Sprintf("branch-%d", id), mon)
			defer acct.Close()
			for j := 0; j < 100; j++ {
				if err := acct.Grow(64); err != nil {
					return // pool exhausted, ok
				}
			}
			acct.Shrink(acct.Used())
		}(i)
	}
	wg.Wait()

	if mon.MaxAllocated() <= 0 {
		t.Error("MaxAllocated should be > 0 after concurrent Grow calls")
	}
	if mon.CurAllocated() != 0 {
		t.Errorf("CurAllocated should be 0 after all Close, got %d", mon.CurAllocated())
	}
}

func TestPoolAccount_NilMonitor(t *testing.T) {
	// Verify that passing nil monitor doesn't panic — all ObserveGrow/ObserveShrink
	// calls are no-ops on nil BudgetMonitor.
	bp := newTestPool(t, 8)
	acct := NewPoolAccount(bp, "nil-mon", nil)
	defer acct.Close()

	if err := acct.Grow(500); err != nil {
		t.Fatalf("Grow: %v", err)
	}
	acct.Shrink(200)
	if acct.Used() != 300 {
		t.Errorf("Used = %d, want 300", acct.Used())
	}
}

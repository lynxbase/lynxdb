package memgov

import (
	"errors"
	"testing"
)

// Compile-time interface check: AccountAdapter must implement MemoryAccount.
var _ MemoryAccount = (*AccountAdapter)(nil)

// ---------------------------------------------------------------------------
// AccountAdapter unit tests
// ---------------------------------------------------------------------------

func TestUnit_AccountAdapter_Grow_IncreasesUsedAndMaxUsed(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	if err := acct.Grow(1024); err != nil {
		t.Fatalf("Grow(1024) failed: %v", err)
	}

	if got := acct.Used(); got != 1024 {
		t.Errorf("Used() = %d after Grow(1024), want 1024", got)
	}
	if got := acct.MaxUsed(); got != 1024 {
		t.Errorf("MaxUsed() = %d after Grow(1024), want 1024", got)
	}

	// Second Grow should accumulate.
	if err := acct.Grow(2048); err != nil {
		t.Fatalf("Grow(2048) failed: %v", err)
	}

	if got := acct.Used(); got != 3072 {
		t.Errorf("Used() = %d after Grow(1024)+Grow(2048), want 3072", got)
	}
	if got := acct.MaxUsed(); got != 3072 {
		t.Errorf("MaxUsed() = %d, want 3072", got)
	}
}

func TestUnit_AccountAdapter_Grow_ZeroIsNoOp(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	if err := acct.Grow(0); err != nil {
		t.Fatalf("Grow(0) returned error: %v", err)
	}
	if got := acct.Used(); got != 0 {
		t.Errorf("Used() = %d after Grow(0), want 0", got)
	}
	if got := acct.MaxUsed(); got != 0 {
		t.Errorf("MaxUsed() = %d after Grow(0), want 0", got)
	}

	// Negative should also be a no-op.
	if err := acct.Grow(-100); err != nil {
		t.Fatalf("Grow(-100) returned error: %v", err)
	}
	if got := acct.Used(); got != 0 {
		t.Errorf("Used() = %d after Grow(-100), want 0", got)
	}
}

func TestUnit_AccountAdapter_Shrink_DecreasesUsed(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	if err := acct.Grow(4096); err != nil {
		t.Fatalf("Grow failed: %v", err)
	}

	acct.Shrink(1024)

	if got := acct.Used(); got != 3072 {
		t.Errorf("Used() = %d after Grow(4096) then Shrink(1024), want 3072", got)
	}
	// MaxUsed must not decrease.
	if got := acct.MaxUsed(); got != 4096 {
		t.Errorf("MaxUsed() = %d after Shrink, want 4096 (watermark must not decrease)", got)
	}
}

func TestUnit_AccountAdapter_Shrink_MoreThanUsed_ClampsToZero(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	if err := acct.Grow(100); err != nil {
		t.Fatalf("Grow failed: %v", err)
	}

	// Shrink by more than what was Grown -- should clamp to 0, not go negative.
	acct.Shrink(500)

	if got := acct.Used(); got != 0 {
		t.Errorf("Used() = %d after over-Shrink, want 0 (clamped)", got)
	}
}

func TestUnit_AccountAdapter_Shrink_ReleasesToGovernor(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	if err := acct.Grow(4096); err != nil {
		t.Fatalf("Grow failed: %v", err)
	}

	// Governor should show the allocation.
	if got := gov.TotalUsage().Allocated; got != 4096 {
		t.Fatalf("Governor.Allocated = %d after Grow, want 4096", got)
	}

	// Shrink should release bytes back to the governor immediately.
	acct.Shrink(1024)

	if got := acct.Used(); got != 3072 {
		t.Errorf("Used() = %d after Shrink(1024), want 3072", got)
	}
	if got := gov.TotalUsage().Allocated; got != 3072 {
		t.Errorf("Governor.Allocated = %d after Shrink(1024), want 3072 (bytes released)", got)
	}

	// Shrink the rest.
	acct.Shrink(3072)
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.Allocated = %d after full Shrink, want 0", got)
	}
}

func TestUnit_AccountAdapter_Shrink_ZeroAndNegative_NoOp(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	if err := acct.Grow(500); err != nil {
		t.Fatalf("Grow failed: %v", err)
	}

	acct.Shrink(0)
	if got := acct.Used(); got != 500 {
		t.Errorf("Used() = %d after Shrink(0), want 500 (unchanged)", got)
	}

	acct.Shrink(-100)
	if got := acct.Used(); got != 500 {
		t.Errorf("Used() = %d after Shrink(-100), want 500 (unchanged)", got)
	}
}

func TestUnit_AccountAdapter_MaxUsed_TracksPeak(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	// Grow to 3000, shrink to 1000, grow to 2000.
	// Peak should remain 3000 from the first Grow.
	if err := acct.Grow(3000); err != nil {
		t.Fatalf("Grow failed: %v", err)
	}
	acct.Shrink(2000) // used=1000
	if err := acct.Grow(1000); err != nil {
		t.Fatalf("Grow failed: %v", err)
	} // used=2000

	if got := acct.Used(); got != 2000 {
		t.Errorf("Used() = %d, want 2000", got)
	}
	if got := acct.MaxUsed(); got != 3000 {
		t.Errorf("MaxUsed() = %d, want 3000 (peak from first Grow)", got)
	}

	// Now grow past the old peak.
	if err := acct.Grow(2000); err != nil {
		t.Fatalf("Grow failed: %v", err)
	} // used=4000
	if got := acct.MaxUsed(); got != 4000 {
		t.Errorf("MaxUsed() = %d, want 4000 (new peak)", got)
	}
}

func TestUnit_AccountAdapter_Close_ReleasesViaOperatorMemory(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)

	if err := acct.Grow(4096); err != nil {
		t.Fatalf("Grow failed: %v", err)
	}

	acct.Close()

	// After Close, Used must be 0.
	if got := acct.Used(); got != 0 {
		t.Errorf("Used() = %d after Close, want 0", got)
	}

	// The governor must have released the memory reserved by the OperatorMemory.
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.TotalUsage.Allocated = %d after Close, want 0", got)
	}
}

func TestUnit_AccountAdapter_OperatorMemory_ReturnsUnderlying(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	got := acct.OperatorMemory()
	if got == nil {
		t.Fatal("OperatorMemory() returned nil")
	}
	// They must be the exact same interface value.
	if got != opMem {
		t.Error("OperatorMemory() returned a different OperatorMemory than was passed to constructor")
	}
}

func TestUnit_AccountAdapter_Grow_GovernorLimitExceeded_ReturnsError(t *testing.T) {
	// Governor with a very small limit -- 256 bytes total.
	gov := NewGovernor(GovernorConfig{TotalLimit: 256})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	// First grow should succeed.
	if err := acct.Grow(200); err != nil {
		t.Fatalf("Grow(200) failed: %v", err)
	}

	// Second grow should exceed the 256-byte governor limit.
	err := acct.Grow(100) // 200+100=300 > 256
	if err == nil {
		t.Fatal("Grow should return error when governor limit is exceeded")
	}
	if !errors.Is(err, ErrMemoryPressure) {
		t.Errorf("error should wrap ErrMemoryPressure, got: %v", err)
	}

	// Used must remain at the value before the failed Grow.
	if got := acct.Used(); got != 200 {
		t.Errorf("Used() = %d after failed Grow, want 200 (unchanged)", got)
	}
	// MaxUsed must remain at the watermark before the failed Grow.
	if got := acct.MaxUsed(); got != 200 {
		t.Errorf("MaxUsed() = %d after failed Grow, want 200 (unchanged)", got)
	}
}

func TestUnit_AccountAdapter_Grow_GovernorLimitExactBoundary_Succeeds(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 512})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	// Grow to exactly the limit.
	if err := acct.Grow(512); err != nil {
		t.Fatalf("Grow to exact limit failed: %v", err)
	}
	if got := acct.Used(); got != 512 {
		t.Errorf("Used() = %d, want 512", got)
	}

	// One more byte should fail.
	err := acct.Grow(1)
	if err == nil {
		t.Fatal("Grow beyond exact limit should fail")
	}
}

func TestUnit_AccountAdapter_GrowShrinkGrow_StateConsistency(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")
	acct := NewAccountAdapter(opMem)
	defer acct.Close()

	// Grow, shrink all, grow again -- should work from clean state.
	if err := acct.Grow(1000); err != nil {
		t.Fatal(err)
	}
	acct.Shrink(1000) // back to 0

	if got := acct.Used(); got != 0 {
		t.Errorf("Used() = %d after Grow+Shrink to zero, want 0", got)
	}

	// Second cycle.
	if err := acct.Grow(2000); err != nil {
		t.Fatalf("second Grow failed: %v", err)
	}
	if got := acct.Used(); got != 2000 {
		t.Errorf("Used() = %d after second Grow, want 2000", got)
	}
	if got := acct.MaxUsed(); got != 2000 {
		t.Errorf("MaxUsed() = %d, want 2000 (from second Grow)", got)
	}
}

// ---------------------------------------------------------------------------
// BudgetAdapter unit tests
// ---------------------------------------------------------------------------

func TestUnit_BudgetAdapter_NewAccount_CreatesWorkingAccountAdapter(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	budget := NewQueryBudget(gov, "query-1")
	adapter := NewBudgetAdapter(budget, gov)
	defer adapter.Close()

	acct := adapter.NewAccount("sort")
	if acct == nil {
		t.Fatal("NewAccount returned nil")
	}
	defer acct.Close()

	// The account must be functional: Grow, Used, MaxUsed, Shrink all work.
	if err := acct.Grow(4096); err != nil {
		t.Fatalf("acct.Grow failed: %v", err)
	}
	if got := acct.Used(); got != 4096 {
		t.Errorf("acct.Used() = %d, want 4096", got)
	}
	if got := acct.MaxUsed(); got != 4096 {
		t.Errorf("acct.MaxUsed() = %d, want 4096", got)
	}

	acct.Shrink(1024)
	if got := acct.Used(); got != 3072 {
		t.Errorf("acct.Used() = %d after Shrink(1024), want 3072", got)
	}
}

func TestUnit_BudgetAdapter_MultipleNewAccount_CreatesIndependentAccounts(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	budget := NewQueryBudget(gov, "query-2")
	adapter := NewBudgetAdapter(budget, gov)
	defer adapter.Close()

	acct1 := adapter.NewAccount("sort")
	acct2 := adapter.NewAccount("join")
	defer acct1.Close()
	defer acct2.Close()

	// Grow each independently.
	if err := acct1.Grow(1000); err != nil {
		t.Fatalf("acct1.Grow failed: %v", err)
	}
	if err := acct2.Grow(2000); err != nil {
		t.Fatalf("acct2.Grow failed: %v", err)
	}

	// They must track independently.
	if got := acct1.Used(); got != 1000 {
		t.Errorf("acct1.Used() = %d, want 1000", got)
	}
	if got := acct2.Used(); got != 2000 {
		t.Errorf("acct2.Used() = %d, want 2000", got)
	}

	// Shrinking one must not affect the other.
	acct1.Shrink(500)
	if got := acct1.Used(); got != 500 {
		t.Errorf("acct1.Used() = %d after Shrink(500), want 500", got)
	}
	if got := acct2.Used(); got != 2000 {
		t.Errorf("acct2.Used() = %d after shrinking acct1, want 2000 (independent)", got)
	}
}

func TestUnit_BudgetAdapter_MultipleNewAccount_DistinctOperatorMemories(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	budget := NewQueryBudget(gov, "query-3")
	adapter := NewBudgetAdapter(budget, gov)
	defer adapter.Close()

	acct1 := adapter.NewAccount("sort")
	acct2 := adapter.NewAccount("join")
	defer acct1.Close()
	defer acct2.Close()

	// Each account wraps a different OperatorMemory instance.
	om1 := acct1.OperatorMemory()
	om2 := acct2.OperatorMemory()
	if om1 == om2 {
		t.Error("NewAccount should create distinct OperatorMemory instances, but got the same one")
	}
}

func TestUnit_BudgetAdapter_Close_ReleasesQueryBudget(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	budget := NewQueryBudget(gov, "query-4")
	adapter := NewBudgetAdapter(budget, gov)

	// Borrow some memory through the budget directly to verify Close releases it.
	lease, err := budget.Borrow(ClassSpillable, 8192)
	if err != nil {
		t.Fatalf("budget.Borrow failed: %v", err)
	}
	_ = lease

	// Before Close, governor should have allocated bytes.
	if got := gov.TotalUsage().Allocated; got != 8192 {
		t.Errorf("Governor.Allocated = %d before Close, want 8192", got)
	}

	adapter.Close()

	// After Close, the budget's leases must be released.
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.Allocated = %d after adapter.Close(), want 0", got)
	}
}

func TestUnit_BudgetAdapter_NewAccount_AccountWorksAfterBudgetBorrow(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	budget := NewQueryBudget(gov, "query-5")
	adapter := NewBudgetAdapter(budget, gov)
	defer adapter.Close()

	// Borrow through the budget first.
	_, err := budget.Borrow(ClassSpillable, 1024)
	if err != nil {
		t.Fatalf("budget.Borrow failed: %v", err)
	}

	// Now create an account via the adapter -- should still work.
	acct := adapter.NewAccount("agg")
	defer acct.Close()

	if err := acct.Grow(2048); err != nil {
		t.Fatalf("acct.Grow failed: %v", err)
	}

	if got := acct.Used(); got != 2048 {
		t.Errorf("acct.Used() = %d, want 2048", got)
	}

	// Governor should see both allocations (budget borrow + account grow).
	totalAllocated := gov.TotalUsage().Allocated
	if totalAllocated < 3072 {
		t.Errorf("Governor.Allocated = %d, want at least 3072 (1024 budget + 2048 account)", totalAllocated)
	}
}

func TestUnit_AccountAdapter_UsedAsStatsMemoryAccount(t *testing.T) {
	// Verify the adapter works when used through the MemoryAccount interface,
	// as it would be in production code that accepts the interface type.
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20})
	opMem := NewOperatorMemory(gov, "test-op")

	var acct MemoryAccount = NewAccountAdapter(opMem)
	defer acct.Close()

	if err := acct.Grow(512); err != nil {
		t.Fatalf("Grow failed through interface: %v", err)
	}
	if got := acct.Used(); got != 512 {
		t.Errorf("Used() = %d through interface, want 512", got)
	}
	if got := acct.MaxUsed(); got != 512 {
		t.Errorf("MaxUsed() = %d through interface, want 512", got)
	}

	acct.Shrink(256)
	if got := acct.Used(); got != 256 {
		t.Errorf("Used() = %d after Shrink through interface, want 256", got)
	}
}

package memgov

import (
	"sync"
	"testing"
)

// --- Lease tests ---

func TestUnit_Lease_Release_Idempotent(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})

	if err := gov.Reserve(ClassSpillable, 1024); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	lease := &Lease{
		gov:   gov,
		class: ClassSpillable,
		bytes: 1024,
	}

	lease.Release()
	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 0 {
		t.Errorf("after first Release, Allocated = %d, want 0", got)
	}

	// Second Release should be a no-op, not double-free.
	lease.Release()
	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 0 {
		t.Errorf("after second Release, Allocated = %d, want 0", got)
	}
}

func TestUnit_Lease_Release_NilSafe(t *testing.T) {
	var lease *Lease
	// Must not panic.
	lease.Release()
}

func TestUnit_Lease_Bytes_ReturnsReservedAmount(t *testing.T) {
	lease := &Lease{
		gov:   &NopGovernor{},
		class: ClassMetadata,
		bytes: 4096,
	}

	if got := lease.Bytes(); got != 4096 {
		t.Errorf("Bytes() = %d, want 4096", got)
	}
}

func TestUnit_Lease_Bytes_NilReturnsZero(t *testing.T) {
	var lease *Lease
	if got := lease.Bytes(); got != 0 {
		t.Errorf("nil Lease.Bytes() = %d, want 0", got)
	}
}

func TestUnit_Lease_IsReleased_NilReturnsTrue(t *testing.T) {
	var lease *Lease
	if !lease.IsReleased() {
		t.Error("nil Lease.IsReleased() should return true")
	}
}

func TestUnit_Lease_IsReleased_BeforeAndAfterRelease(t *testing.T) {
	lease := &Lease{
		gov:   &NopGovernor{},
		class: ClassSpillable,
		bytes: 512,
	}

	if lease.IsReleased() {
		t.Error("new lease should not be released")
	}

	lease.Release()

	if !lease.IsReleased() {
		t.Error("lease should be released after Release()")
	}
}

// --- QueryBudget tests ---

func TestUnit_QueryBudget_Borrow_ReservesFromGovernor(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	qb := NewQueryBudget(gov, "test-query")

	lease, err := qb.Borrow(ClassSpillable, 2048)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	if lease == nil {
		t.Fatal("Borrow returned nil lease")
	}

	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 2048 {
		t.Errorf("Governor.ClassUsage.Allocated = %d, want 2048", got)
	}

	if got := lease.Bytes(); got != 2048 {
		t.Errorf("lease.Bytes() = %d, want 2048", got)
	}
}

func TestUnit_QueryBudget_Borrow_ZeroBytes_ReturnsClosedLease(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})
	qb := NewQueryBudget(gov, "test-query")

	lease, err := qb.Borrow(ClassMetadata, 0)
	if err != nil {
		t.Fatalf("Borrow(0) failed: %v", err)
	}
	if lease == nil {
		t.Fatal("Borrow(0) returned nil lease")
	}
	if !lease.IsReleased() {
		t.Error("zero-byte lease should be already closed")
	}
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.Allocated = %d after zero borrow, want 0", got)
	}
}

func TestUnit_QueryBudget_Borrow_NegativeBytes_ReturnsClosedLease(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})
	qb := NewQueryBudget(gov, "test-query")

	lease, err := qb.Borrow(ClassMetadata, -100)
	if err != nil {
		t.Fatalf("Borrow(-100) failed: %v", err)
	}
	if lease == nil {
		t.Fatal("Borrow(-100) returned nil lease")
	}
	if !lease.IsReleased() {
		t.Error("negative-byte lease should be already closed")
	}
}

func TestUnit_QueryBudget_Borrow_GovernorDenies_ReturnsError(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100})
	qb := NewQueryBudget(gov, "test-query")

	_, err := qb.Borrow(ClassSpillable, 200)
	if err == nil {
		t.Fatal("Borrow should fail when governor denies reservation")
	}
}

func TestUnit_QueryBudget_TryBorrow_UnderLimit_Succeeds(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})
	qb := NewQueryBudget(gov, "test-query")

	lease, ok := qb.TryBorrow(ClassRevocable, 1024)
	if !ok {
		t.Fatal("TryBorrow should succeed within budget")
	}
	if lease == nil {
		t.Fatal("TryBorrow returned nil lease on success")
	}
	if got := lease.Bytes(); got != 1024 {
		t.Errorf("lease.Bytes() = %d, want 1024", got)
	}
}

func TestUnit_QueryBudget_TryBorrow_OverLimit_ReturnsNilFalse(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100})
	qb := NewQueryBudget(gov, "test-query")

	lease, ok := qb.TryBorrow(ClassSpillable, 200)
	if ok {
		t.Error("TryBorrow should return false when over limit")
	}
	if lease != nil {
		t.Error("TryBorrow should return nil lease on failure")
	}
}

func TestUnit_QueryBudget_TryBorrow_ZeroBytes_ReturnsClosedLease(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})
	qb := NewQueryBudget(gov, "test-query")

	lease, ok := qb.TryBorrow(ClassMetadata, 0)
	if !ok {
		t.Fatal("TryBorrow(0) should succeed")
	}
	if lease == nil {
		t.Fatal("TryBorrow(0) returned nil lease")
	}
	if !lease.IsReleased() {
		t.Error("zero-byte TryBorrow lease should be already closed")
	}
}

func TestUnit_QueryBudget_PinnedBytes_TracksNonRevocable(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	qb := NewQueryBudget(gov, "test-query")

	if got := qb.PinnedBytes(); got != 0 {
		t.Errorf("PinnedBytes() = %d before any borrow, want 0", got)
	}

	_, err := qb.Borrow(ClassNonRevocable, 1000)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}

	if got := qb.PinnedBytes(); got != 1000 {
		t.Errorf("PinnedBytes() = %d, want 1000", got)
	}

	// Borrow in a different class should not affect PinnedBytes.
	_, err = qb.Borrow(ClassSpillable, 500)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}

	if got := qb.PinnedBytes(); got != 1000 {
		t.Errorf("PinnedBytes() = %d after spillable borrow, want 1000", got)
	}
}

func TestUnit_QueryBudget_RevocableBytes_TracksRevocableAndSpillable(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	qb := NewQueryBudget(gov, "test-query")

	if got := qb.RevocableBytes(); got != 0 {
		t.Errorf("RevocableBytes() = %d before any borrow, want 0", got)
	}

	_, err := qb.Borrow(ClassRevocable, 300)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	_, err = qb.Borrow(ClassSpillable, 700)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}

	if got := qb.RevocableBytes(); got != 1000 {
		t.Errorf("RevocableBytes() = %d, want 1000 (300 revocable + 700 spillable)", got)
	}

	// Non-revocable and other classes should not count.
	_, err = qb.Borrow(ClassNonRevocable, 200)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}
	_, err = qb.Borrow(ClassPageCache, 100)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}

	if got := qb.RevocableBytes(); got != 1000 {
		t.Errorf("RevocableBytes() = %d after non-revocable borrows, want 1000", got)
	}
}

func TestUnit_QueryBudget_Close_ReleasesAllLeases(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	qb := NewQueryBudget(gov, "test-query")

	lease1, _ := qb.Borrow(ClassSpillable, 1000)
	lease2, _ := qb.Borrow(ClassRevocable, 2000)
	lease3, _ := qb.Borrow(ClassNonRevocable, 3000)

	qb.Close()

	// All governor memory should be released.
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.Allocated = %d after Close, want 0", got)
	}

	// All leases should be marked released.
	if !lease1.IsReleased() {
		t.Error("lease1 should be released after Close")
	}
	if !lease2.IsReleased() {
		t.Error("lease2 should be released after Close")
	}
	if !lease3.IsReleased() {
		t.Error("lease3 should be released after Close")
	}

	// PinnedBytes and RevocableBytes should be zero.
	if got := qb.PinnedBytes(); got != 0 {
		t.Errorf("PinnedBytes() = %d after Close, want 0", got)
	}
	if got := qb.RevocableBytes(); got != 0 {
		t.Errorf("RevocableBytes() = %d after Close, want 0", got)
	}
}

func TestUnit_QueryBudget_Close_Idempotent(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	qb := NewQueryBudget(gov, "test-query")

	_, _ = qb.Borrow(ClassSpillable, 1000)

	qb.Close()

	// Second close should not panic or double-free.
	qb.Close()

	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.Allocated = %d after double Close, want 0", got)
	}
}

func TestUnit_QueryBudget_MultipleBorrows_CumulativeAccounting(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	qb := NewQueryBudget(gov, "test-query")

	for i := 0; i < 10; i++ {
		_, err := qb.Borrow(ClassSpillable, 100)
		if err != nil {
			t.Fatalf("Borrow %d failed: %v", i, err)
		}
	}

	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 1000 {
		t.Errorf("Governor.Spillable.Allocated = %d, want 1000", got)
	}

	qb.Close()

	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 0 {
		t.Errorf("after Close, Governor.Spillable.Allocated = %d, want 0", got)
	}
}

func TestConcurrent_QueryBudget_BorrowAndClose_NoRace(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 0}) // unlimited
	qb := NewQueryBudget(gov, "test-query")

	const goroutines = 8
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_, _ = qb.Borrow(ClassSpillable, 64)
				_, _ = qb.TryBorrow(ClassRevocable, 32)
				_ = qb.PinnedBytes()
				_ = qb.RevocableBytes()
			}
		}()
	}

	wg.Wait()
	qb.Close()
}

func TestUnit_QueryBudget_LeaseReleasedIndependently_StillCleanedOnClose(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	qb := NewQueryBudget(gov, "test-query")

	lease, err := qb.Borrow(ClassSpillable, 500)
	if err != nil {
		t.Fatalf("Borrow failed: %v", err)
	}

	// Release the lease before Close.
	lease.Release()

	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Allocated = %d after lease.Release, want 0", got)
	}

	// Close should still be safe (lease already released, idempotent).
	qb.Close()

	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Allocated = %d after Close (lease already released), want 0", got)
	}
}

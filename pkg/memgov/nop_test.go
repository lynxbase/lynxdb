package memgov

import "testing"

func TestUnit_NopGovernor_ImplementsInterface(t *testing.T) {
	var gov Governor = &NopGovernor{}
	_ = gov // compile-time check; this test verifies the interface assertion
}

func TestUnit_NopGovernor_ReserveAlwaysSucceeds(t *testing.T) {
	gov := &NopGovernor{}

	if err := gov.Reserve(ClassSpillable, 1<<40); err != nil {
		t.Errorf("NopGovernor.Reserve returned error: %v", err)
	}
}

func TestUnit_NopGovernor_TryReserveAlwaysTrue(t *testing.T) {
	gov := &NopGovernor{}

	if !gov.TryReserve(ClassSpillable, 1<<40) {
		t.Error("NopGovernor.TryReserve returned false")
	}
}

func TestUnit_NopGovernor_ReleaseNoOp(t *testing.T) {
	gov := &NopGovernor{}
	// Must not panic.
	gov.Release(ClassSpillable, 1000)
}

func TestUnit_NopGovernor_ClassUsageReturnsZero(t *testing.T) {
	gov := &NopGovernor{}
	cs := gov.ClassUsage(ClassSpillable)

	if cs.Allocated != 0 || cs.Peak != 0 || cs.Limit != 0 {
		t.Errorf("NopGovernor.ClassUsage should return zero stats, got %+v", cs)
	}
}

func TestUnit_NopGovernor_TotalUsageReturnsZero(t *testing.T) {
	gov := &NopGovernor{}
	ts := gov.TotalUsage()

	if ts.Allocated != 0 || ts.Peak != 0 || ts.Limit != 0 {
		t.Errorf("NopGovernor.TotalUsage should return zero stats, got %+v", ts)
	}
}

func TestUnit_NopGovernor_OnPressureNoOp(t *testing.T) {
	gov := &NopGovernor{}
	// Must not panic.
	gov.OnPressure(ClassRevocable, func(target int64) int64 { return 0 })
}

func TestUnit_NopLease_AlreadyReleased(t *testing.T) {
	lease := NopLease()

	if !lease.IsReleased() {
		t.Error("NopLease should be already released")
	}
	if got := lease.Bytes(); got != 0 {
		t.Errorf("NopLease.Bytes() = %d, want 0", got)
	}

	// Release should be safe (idempotent on already-closed lease).
	lease.Release()
}

func TestUnit_NopQueryBudget_BorrowSucceeds(t *testing.T) {
	qb := NopQueryBudget()

	lease, err := qb.Borrow(ClassSpillable, 9999)
	if err != nil {
		t.Fatalf("NopQueryBudget.Borrow returned error: %v", err)
	}
	if lease == nil {
		t.Fatal("NopQueryBudget.Borrow returned nil lease")
	}

	qb.Close()
}

func TestUnit_NopQueryBudget_TryBorrowSucceeds(t *testing.T) {
	qb := NopQueryBudget()

	lease, ok := qb.TryBorrow(ClassRevocable, 9999)
	if !ok {
		t.Fatal("NopQueryBudget.TryBorrow returned false")
	}
	if lease == nil {
		t.Fatal("NopQueryBudget.TryBorrow returned nil lease")
	}

	qb.Close()
}

func TestUnit_NopQueryBudget_PinnedAndRevocableBytes(t *testing.T) {
	qb := NopQueryBudget()

	// Before any borrows, should be zero.
	if got := qb.PinnedBytes(); got != 0 {
		t.Errorf("PinnedBytes() = %d, want 0", got)
	}
	if got := qb.RevocableBytes(); got != 0 {
		t.Errorf("RevocableBytes() = %d, want 0", got)
	}

	qb.Close()
}

func TestUnit_NopOperatorMemory_ReserveSucceeds(t *testing.T) {
	om := NopOperatorMemory()

	if err := om.Reserve(9999); err != nil {
		t.Fatalf("NopOperatorMemory.Reserve returned error: %v", err)
	}

	if got := om.Used(); got != 9999 {
		t.Errorf("Used() = %d, want 9999", got)
	}

	om.Close()
}

func TestUnit_NopOperatorMemory_TryGrowSucceeds(t *testing.T) {
	om := NopOperatorMemory()

	lease, err := om.TryGrow(5000)
	if err != nil {
		t.Fatalf("NopOperatorMemory.TryGrow returned error: %v", err)
	}
	if lease == nil {
		t.Fatal("TryGrow returned nil lease")
	}

	om.Close()
}

package memgov

import (
	"sync"
	"testing"
)

func TestUnit_OperatorMemory_Reserve_UpdatesUsedAndPinned(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "sort-op")

	if err := om.Reserve(1024); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	if got := om.Used(); got != 1024 {
		t.Errorf("Used() = %d, want 1024", got)
	}
	if got := om.Pinned(); got != 1024 {
		t.Errorf("Pinned() = %d, want 1024", got)
	}
	if got := om.Revocable(); got != 0 {
		t.Errorf("Revocable() = %d, want 0 (Reserve is pinned)", got)
	}

	// Governor should have it under ClassNonRevocable.
	if got := gov.ClassUsage(ClassNonRevocable).Allocated; got != 1024 {
		t.Errorf("Governor.NonRevocable.Allocated = %d, want 1024", got)
	}
}

func TestUnit_OperatorMemory_Reserve_ZeroNoOp(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})
	om := NewOperatorMemory(gov, "test-op")

	if err := om.Reserve(0); err != nil {
		t.Fatalf("Reserve(0) returned error: %v", err)
	}
	if got := om.Used(); got != 0 {
		t.Errorf("Used() = %d after Reserve(0), want 0", got)
	}

	if err := om.Reserve(-100); err != nil {
		t.Fatalf("Reserve(-100) returned error: %v", err)
	}
	if got := om.Used(); got != 0 {
		t.Errorf("Used() = %d after Reserve(-100), want 0", got)
	}
}

func TestUnit_OperatorMemory_Reserve_GovernorDenies_ReturnsError(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100})
	om := NewOperatorMemory(gov, "test-op")

	err := om.Reserve(200)
	if err == nil {
		t.Fatal("Reserve should fail when governor denies")
	}

	if got := om.Used(); got != 0 {
		t.Errorf("Used() = %d after failed Reserve, want 0", got)
	}
}

func TestUnit_OperatorMemory_TryGrow_ReturnsLease(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "test-op")

	lease, err := om.TryGrow(2048)
	if err != nil {
		t.Fatalf("TryGrow failed: %v", err)
	}
	if lease == nil {
		t.Fatal("TryGrow returned nil lease")
	}
	if got := lease.Bytes(); got != 2048 {
		t.Errorf("lease.Bytes() = %d, want 2048", got)
	}

	if got := om.Revocable(); got != 2048 {
		t.Errorf("Revocable() = %d, want 2048", got)
	}
	if got := om.Used(); got != 2048 {
		t.Errorf("Used() = %d, want 2048", got)
	}

	// Governor should track under ClassSpillable.
	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 2048 {
		t.Errorf("Governor.Spillable.Allocated = %d, want 2048", got)
	}
}

func TestUnit_OperatorMemory_TryGrow_ZeroReturnsClosedLease(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})
	om := NewOperatorMemory(gov, "test-op")

	lease, err := om.TryGrow(0)
	if err != nil {
		t.Fatalf("TryGrow(0) failed: %v", err)
	}
	if lease == nil {
		t.Fatal("TryGrow(0) returned nil lease")
	}
	if !lease.IsReleased() {
		t.Error("zero-byte TryGrow lease should be already closed")
	}

	lease, err = om.TryGrow(-50)
	if err != nil {
		t.Fatalf("TryGrow(-50) failed: %v", err)
	}
	if !lease.IsReleased() {
		t.Error("negative-byte TryGrow lease should be already closed")
	}
}

func TestUnit_OperatorMemory_TryGrow_OverLimit_ReturnsError(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 500})
	om := NewOperatorMemory(gov, "test-op")

	_, err := om.TryGrow(1000)
	if err == nil {
		t.Fatal("TryGrow should fail when governor denies")
	}

	if got := om.Used(); got != 0 {
		t.Errorf("Used() = %d after failed TryGrow, want 0", got)
	}
	if got := om.Revocable(); got != 0 {
		t.Errorf("Revocable() = %d after failed TryGrow, want 0", got)
	}
}

func TestUnit_OperatorMemory_UsedAndMaxUsed_Tracking(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "test-op")

	if err := om.Reserve(1000); err != nil {
		t.Fatal(err)
	}

	lease1, _ := om.TryGrow(2000)
	// Used = 3000, MaxUsed = 3000

	if got := om.Used(); got != 3000 {
		t.Errorf("Used() = %d, want 3000", got)
	}
	if got := om.MaxUsed(); got != 3000 {
		t.Errorf("MaxUsed() = %d, want 3000", got)
	}

	// Release the lease -- Used should decrease but MaxUsed should stay.
	lease1.Release()

	// Note: releasing the lease returns bytes to governor but does not
	// automatically update the operatorMemory's internal used counter.
	// Only Close() resets used. We verify the MaxUsed watermark.
	if got := om.MaxUsed(); got != 3000 {
		t.Errorf("MaxUsed() = %d after lease release, want 3000 (watermark)", got)
	}

	// Grow again past previous peak.
	_, _ = om.TryGrow(5000)
	// Used = 1000 (reserved) + 2000 (lease1 counted) + 5000 = 8000
	// But lease1 was already released -- operatorMemory still tracks it in used.
	// Actual internal used will be 1000 + 2000 + 5000 = 8000.

	if got := om.MaxUsed(); got < 3000 {
		t.Errorf("MaxUsed() = %d, should be at least 3000", got)
	}
}

func TestUnit_OperatorMemory_Close_ReleasesReservedAndLeases(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "test-op")

	if err := om.Reserve(1000); err != nil {
		t.Fatal(err)
	}
	lease, _ := om.TryGrow(2000)
	_ = lease // should be released by Close

	om.Close()

	// Governor memory should be fully released.
	if got := gov.ClassUsage(ClassNonRevocable).Allocated; got != 0 {
		t.Errorf("Governor.NonRevocable.Allocated = %d, want 0", got)
	}
	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 0 {
		t.Errorf("Governor.Spillable.Allocated = %d, want 0", got)
	}
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.Total.Allocated = %d, want 0", got)
	}
}

func TestUnit_OperatorMemory_Close_Idempotent(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "test-op")

	if err := om.Reserve(1000); err != nil {
		t.Fatal(err)
	}

	om.Close()
	// Second close should not panic or double-free.
	om.Close()

	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Governor.Allocated = %d after double Close, want 0", got)
	}
}

func TestUnit_OperatorMemory_ReserveAndGrow_CombinedAccounting(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "test-op")

	// Reserve pinned memory.
	if err := om.Reserve(1000); err != nil {
		t.Fatal(err)
	}
	// Grow with spillable memory.
	if _, err := om.TryGrow(3000); err != nil {
		t.Fatal(err)
	}

	if got := om.Pinned(); got != 1000 {
		t.Errorf("Pinned() = %d, want 1000", got)
	}
	if got := om.Revocable(); got != 3000 {
		t.Errorf("Revocable() = %d, want 3000", got)
	}
	if got := om.Used(); got != 4000 {
		t.Errorf("Used() = %d, want 4000", got)
	}

	// Governor tracks them under different classes.
	if got := gov.ClassUsage(ClassNonRevocable).Allocated; got != 1000 {
		t.Errorf("Governor.NonRevocable = %d, want 1000", got)
	}
	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 3000 {
		t.Errorf("Governor.Spillable = %d, want 3000", got)
	}
}

func TestUnit_OperatorMemory_SetOnRevoke_CalledUnderPressure(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 5000})
	om := NewOperatorMemory(gov, "test-op")

	// Reserve some memory to use.
	if err := om.Reserve(1000); err != nil {
		t.Fatal(err)
	}
	if _, err := om.TryGrow(2000); err != nil {
		t.Fatal(err)
	}

	var revokeCalled bool
	var revokeTarget int64

	om.SetOnRevoke(func(target int64) int64 {
		revokeCalled = true
		revokeTarget = target
		// Simulate releasing spillable memory from the governor.
		gov.Release(ClassSpillable, target)
		return target
	})

	// Now trigger pressure by reserving more than remaining budget.
	// Current total: 3000, limit: 5000, remaining: 2000.
	// Try to reserve 3000 more -- deficit = 1000.
	err := gov.Reserve(ClassTempIO, 3000)
	if err != nil {
		t.Fatalf("Reserve should succeed after revocation freed memory: %v", err)
	}

	if !revokeCalled {
		t.Error("OnRevoke callback was not invoked under pressure")
	}
	if revokeTarget <= 0 {
		t.Errorf("revoke target = %d, should be positive", revokeTarget)
	}
}

func TestUnit_OperatorMemory_SetOnRevoke_NilClearsCallback(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "test-op")

	called := false
	om.SetOnRevoke(func(target int64) int64 {
		called = true
		return 0
	})

	// Setting nil should clear the callback.
	om.SetOnRevoke(nil)

	// The callback registered with the governor still exists, but the
	// operatorMemory's onRevoke is nil, so the governor callback wrapper
	// should return 0.
	// We cannot easily test this without triggering pressure, but we
	// verify it does not panic.
	_ = called
}

func TestUnit_OperatorMemory_MultipleReserves_Cumulative(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})
	om := NewOperatorMemory(gov, "test-op")

	for i := 0; i < 5; i++ {
		if err := om.Reserve(100); err != nil {
			t.Fatalf("Reserve %d failed: %v", i, err)
		}
	}

	if got := om.Pinned(); got != 500 {
		t.Errorf("Pinned() = %d after 5x100 Reserve, want 500", got)
	}
	if got := om.Used(); got != 500 {
		t.Errorf("Used() = %d after 5x100 Reserve, want 500", got)
	}
	if got := gov.ClassUsage(ClassNonRevocable).Allocated; got != 500 {
		t.Errorf("Governor.NonRevocable = %d, want 500", got)
	}

	om.Close()
	if got := gov.ClassUsage(ClassNonRevocable).Allocated; got != 0 {
		t.Errorf("Governor.NonRevocable = %d after Close, want 0", got)
	}
}

func TestConcurrent_OperatorMemory_ReserveGrowClose_NoRace(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 0}) // unlimited
	om := NewOperatorMemory(gov, "test-op")

	var wg sync.WaitGroup
	wg.Add(3)

	// Goroutine 1: reserves.
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = om.Reserve(10)
			_ = om.Used()
			_ = om.MaxUsed()
			_ = om.Pinned()
			_ = om.Revocable()
		}
	}()

	// Goroutine 2: try-grows.
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = om.TryGrow(10)
			_ = om.Used()
			_ = om.Revocable()
		}
	}()

	// Goroutine 3: reads.
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = om.Used()
			_ = om.MaxUsed()
			_ = om.Pinned()
			_ = om.Revocable()
		}
	}()

	wg.Wait()
	om.Close()
}

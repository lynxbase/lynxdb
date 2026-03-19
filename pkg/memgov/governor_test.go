package memgov

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestUnit_Governor_Reserve_Release_BasicAccounting(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 20}) // 1 MiB

	if err := gov.Reserve(ClassSpillable, 4096); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	cs := gov.ClassUsage(ClassSpillable)
	if cs.Allocated != 4096 {
		t.Errorf("ClassUsage.Allocated = %d, want 4096", cs.Allocated)
	}

	ts := gov.TotalUsage()
	if ts.Allocated != 4096 {
		t.Errorf("TotalUsage.Allocated = %d, want 4096", ts.Allocated)
	}

	gov.Release(ClassSpillable, 4096)

	cs = gov.ClassUsage(ClassSpillable)
	if cs.Allocated != 0 {
		t.Errorf("after Release, ClassUsage.Allocated = %d, want 0", cs.Allocated)
	}

	ts = gov.TotalUsage()
	if ts.Allocated != 0 {
		t.Errorf("after Release, TotalUsage.Allocated = %d, want 0", ts.Allocated)
	}
}

func TestUnit_Governor_Reserve_ZeroAndNegative_NoOp(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1024})

	// Zero reservation should succeed without changing state.
	if err := gov.Reserve(ClassMetadata, 0); err != nil {
		t.Fatalf("Reserve(0) returned error: %v", err)
	}
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("after Reserve(0), Allocated = %d, want 0", got)
	}

	// Negative reservation should also be a no-op.
	if err := gov.Reserve(ClassMetadata, -100); err != nil {
		t.Fatalf("Reserve(-100) returned error: %v", err)
	}
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("after Reserve(-100), Allocated = %d, want 0", got)
	}
}

func TestUnit_Governor_Release_ZeroAndNegative_NoOp(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1024})

	if err := gov.Reserve(ClassSpillable, 100); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	gov.Release(ClassSpillable, 0)
	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 100 {
		t.Errorf("Release(0) changed Allocated to %d, want 100", got)
	}

	gov.Release(ClassSpillable, -50)
	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 100 {
		t.Errorf("Release(-50) changed Allocated to %d, want 100", got)
	}
}

func TestUnit_Governor_TryReserve_UnderLimit_Succeeds(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 8192})

	if !gov.TryReserve(ClassRevocable, 4096) {
		t.Fatal("TryReserve returned false for reservation within budget")
	}

	cs := gov.ClassUsage(ClassRevocable)
	if cs.Allocated != 4096 {
		t.Errorf("Allocated = %d, want 4096", cs.Allocated)
	}
}

func TestUnit_Governor_TryReserve_ZeroAndNegative_ReturnsTrue(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1024})

	if !gov.TryReserve(ClassMetadata, 0) {
		t.Error("TryReserve(0) returned false, want true")
	}
	if !gov.TryReserve(ClassMetadata, -10) {
		t.Error("TryReserve(-10) returned false, want true")
	}
	if got := gov.TotalUsage().Allocated; got != 0 {
		t.Errorf("Allocated = %d after zero/negative TryReserve, want 0", got)
	}
}

func TestUnit_Governor_TryReserve_OverTotalLimit_ReturnsFalse(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1000})

	if !gov.TryReserve(ClassSpillable, 800) {
		t.Fatal("first TryReserve should succeed")
	}

	// This would push total to 1200, exceeding limit of 1000.
	if gov.TryReserve(ClassSpillable, 400) {
		t.Error("TryReserve should return false when total limit exceeded")
	}

	// Verify no state change on failed TryReserve.
	if got := gov.TotalUsage().Allocated; got != 800 {
		t.Errorf("Allocated = %d after failed TryReserve, want 800", got)
	}
}

func TestUnit_Governor_TryReserve_OverClassLimit_ReturnsFalse(t *testing.T) {
	var classLimits [numClasses]int64
	classLimits[ClassPageCache] = 500

	gov := NewGovernor(GovernorConfig{
		TotalLimit:  10000,
		ClassLimits: classLimits,
	})

	if !gov.TryReserve(ClassPageCache, 400) {
		t.Fatal("first TryReserve should succeed")
	}

	// This would push ClassPageCache to 600, exceeding class limit of 500.
	if gov.TryReserve(ClassPageCache, 200) {
		t.Error("TryReserve should return false when class limit exceeded")
	}

	// Other classes should still work (total limit not reached).
	if !gov.TryReserve(ClassSpillable, 200) {
		t.Error("TryReserve for different class should succeed")
	}
}

func TestUnit_Governor_Reserve_ClassLimitExceeded_ReturnsError(t *testing.T) {
	var classLimits [numClasses]int64
	classLimits[ClassMetadata] = 256

	gov := NewGovernor(GovernorConfig{
		TotalLimit:  10000,
		ClassLimits: classLimits,
	})

	if err := gov.Reserve(ClassMetadata, 128); err != nil {
		t.Fatalf("first Reserve failed: %v", err)
	}

	err := gov.Reserve(ClassMetadata, 200) // 128+200=328 > 256
	if err == nil {
		t.Fatal("Reserve should return error when class limit exceeded")
	}
	if !errors.Is(err, ErrMemoryPressure) {
		t.Errorf("error should wrap ErrMemoryPressure, got: %v", err)
	}

	// Verify the failed reservation did not change state.
	if got := gov.ClassUsage(ClassMetadata).Allocated; got != 128 {
		t.Errorf("Allocated = %d after failed Reserve, want 128", got)
	}
}

func TestUnit_Governor_Reserve_TotalLimitExceeded_ReturnsError(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1000})

	if err := gov.Reserve(ClassSpillable, 900); err != nil {
		t.Fatalf("first Reserve failed: %v", err)
	}

	err := gov.Reserve(ClassTempIO, 200) // 900+200=1100 > 1000
	if err == nil {
		t.Fatal("Reserve should return error when total limit exceeded")
	}
	if !errors.Is(err, ErrMemoryPressure) {
		t.Errorf("error should wrap ErrMemoryPressure, got: %v", err)
	}
}

func TestUnit_Governor_Reserve_TotalLimitExceeded_InvokesPressureCallbacks(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1000})

	var callbackInvoked atomic.Bool
	var requestedTarget atomic.Int64

	gov.OnPressure(ClassRevocable, func(target int64) int64 {
		callbackInvoked.Store(true)
		requestedTarget.Store(target)
		// Simulate freeing some memory: release from governor to actually free it.
		gov.Release(ClassSpillable, target)
		return target
	})

	// Fill up to the limit.
	if err := gov.Reserve(ClassSpillable, 900); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	// This should trigger pressure callbacks since 900+200 > 1000.
	err := gov.Reserve(ClassTempIO, 200)
	if err != nil {
		t.Fatalf("Reserve should succeed after pressure callback freed memory: %v", err)
	}

	if !callbackInvoked.Load() {
		t.Error("pressure callback was not invoked")
	}

	if got := requestedTarget.Load(); got != 100 {
		// deficit = 900 + 200 - 1000 = 100
		t.Errorf("pressure callback target = %d, want 100", got)
	}
}

func TestUnit_Governor_Reserve_PressureCallbackInsufficientReclamation_ReturnsError(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1000})

	gov.OnPressure(ClassRevocable, func(target int64) int64 {
		// Callback reports freeing but does not actually release from governor.
		// The re-check in Reserve will still find totalAllocated+n > limit.
		return 0
	})

	if err := gov.Reserve(ClassSpillable, 900); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	err := gov.Reserve(ClassTempIO, 200)
	if err == nil {
		t.Fatal("Reserve should fail when pressure callback cannot free enough")
	}
	if !errors.Is(err, ErrMemoryPressure) {
		t.Errorf("error should wrap ErrMemoryPressure, got: %v", err)
	}
}

func TestUnit_Governor_PeakTracking(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})

	if err := gov.Reserve(ClassSpillable, 5000); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	gov.Release(ClassSpillable, 3000) // now at 2000, peak should stay at 5000

	cs := gov.ClassUsage(ClassSpillable)
	if cs.Allocated != 2000 {
		t.Errorf("Allocated = %d, want 2000", cs.Allocated)
	}
	if cs.Peak != 5000 {
		t.Errorf("Peak = %d, want 5000 (should be watermark)", cs.Peak)
	}

	ts := gov.TotalUsage()
	if ts.Peak != 5000 {
		t.Errorf("TotalUsage.Peak = %d, want 5000", ts.Peak)
	}

	// Reserve more than previous peak.
	if err := gov.Reserve(ClassSpillable, 7000); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	// Now at 9000, peak should update.
	cs = gov.ClassUsage(ClassSpillable)
	if cs.Peak != 9000 {
		t.Errorf("Peak = %d, want 9000 after new high", cs.Peak)
	}
}

func TestUnit_Governor_TotalUsage_ByClass_MatchesClassUsage(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})

	amounts := map[MemoryClass]int64{
		ClassNonRevocable: 100,
		ClassRevocable:    200,
		ClassSpillable:    300,
		ClassPageCache:    400,
		ClassMetadata:     500,
		ClassTempIO:       600,
	}

	for class, n := range amounts {
		if err := gov.Reserve(class, n); err != nil {
			t.Fatalf("Reserve(%s, %d) failed: %v", class, n, err)
		}
	}

	ts := gov.TotalUsage()

	var expectedTotal int64
	for class, n := range amounts {
		expectedTotal += n
		byClass := ts.ByClass[class]
		individual := gov.ClassUsage(class)

		if byClass.Allocated != n {
			t.Errorf("ByClass[%s].Allocated = %d, want %d", class, byClass.Allocated, n)
		}
		if individual.Allocated != n {
			t.Errorf("ClassUsage(%s).Allocated = %d, want %d", class, individual.Allocated, n)
		}
		if byClass.Allocated != individual.Allocated {
			t.Errorf("ByClass and ClassUsage disagree for %s: %d vs %d",
				class, byClass.Allocated, individual.Allocated)
		}
	}

	if ts.Allocated != expectedTotal {
		t.Errorf("TotalUsage.Allocated = %d, want %d", ts.Allocated, expectedTotal)
	}
}

func TestUnit_Governor_Release_MoreThanAllocated_ClampsToZero(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})

	if err := gov.Reserve(ClassTempIO, 100); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	// Release more than allocated -- should clamp to zero, not go negative.
	gov.Release(ClassTempIO, 500)

	cs := gov.ClassUsage(ClassTempIO)
	if cs.Allocated != 0 {
		t.Errorf("Allocated = %d after over-release, want 0", cs.Allocated)
	}

	ts := gov.TotalUsage()
	if ts.Allocated < 0 {
		t.Errorf("TotalUsage.Allocated = %d, should not be negative", ts.Allocated)
	}
}

func TestUnit_Governor_Unlimited_NoLimitEnforcement(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 0}) // unlimited

	// Should be able to reserve arbitrary amounts.
	if err := gov.Reserve(ClassSpillable, 1<<40); err != nil {
		t.Fatalf("Reserve failed with unlimited governor: %v", err)
	}

	if !gov.TryReserve(ClassSpillable, 1<<40) {
		t.Fatal("TryReserve failed with unlimited governor")
	}

	ts := gov.TotalUsage()
	expected := int64(1<<40) * 2
	if ts.Allocated != expected {
		t.Errorf("Allocated = %d, want %d", ts.Allocated, expected)
	}
	if ts.Limit != 0 {
		t.Errorf("Limit = %d, want 0 (unlimited)", ts.Limit)
	}
}

func TestUnit_Governor_ClassUsage_ReportsLimit(t *testing.T) {
	var classLimits [numClasses]int64
	classLimits[ClassPageCache] = 4096

	gov := NewGovernor(GovernorConfig{
		TotalLimit:  100000,
		ClassLimits: classLimits,
	})

	cs := gov.ClassUsage(ClassPageCache)
	if cs.Limit != 4096 {
		t.Errorf("ClassUsage.Limit = %d, want 4096", cs.Limit)
	}

	cs = gov.ClassUsage(ClassSpillable)
	if cs.Limit != 0 {
		t.Errorf("ClassUsage.Limit for unlimited class = %d, want 0", cs.Limit)
	}
}

func TestUnit_Governor_MultipleClasses_IndependentAccounting(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 100000})

	if err := gov.Reserve(ClassSpillable, 1000); err != nil {
		t.Fatal(err)
	}
	if err := gov.Reserve(ClassMetadata, 2000); err != nil {
		t.Fatal(err)
	}

	// Release from one class should not affect the other.
	gov.Release(ClassSpillable, 1000)

	if got := gov.ClassUsage(ClassSpillable).Allocated; got != 0 {
		t.Errorf("Spillable.Allocated = %d, want 0", got)
	}
	if got := gov.ClassUsage(ClassMetadata).Allocated; got != 2000 {
		t.Errorf("Metadata.Allocated = %d, want 2000", got)
	}
	if got := gov.TotalUsage().Allocated; got != 2000 {
		t.Errorf("Total.Allocated = %d, want 2000", got)
	}
}

func TestConcurrent_Governor_ReserveRelease_NoRace(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 0}) // unlimited to avoid contention errors

	const goroutines = 16
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		class := MemoryClass(g % int(numClasses))
		go func(class MemoryClass) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if err := gov.Reserve(class, 64); err != nil {
					t.Errorf("Reserve failed: %v", err)
					return
				}
				_ = gov.ClassUsage(class)
				_ = gov.TotalUsage()
				gov.Release(class, 64)
			}
		}(class)
	}

	wg.Wait()

	ts := gov.TotalUsage()
	if ts.Allocated != 0 {
		t.Errorf("after all goroutines done, Total.Allocated = %d, want 0", ts.Allocated)
	}
}

func TestConcurrent_Governor_TryReserveRelease_NoRace(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 1 << 30}) // large limit

	const goroutines = 16
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		class := MemoryClass(g % int(numClasses))
		go func(class MemoryClass) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if gov.TryReserve(class, 128) {
					gov.Release(class, 128)
				}
			}
		}(class)
	}

	wg.Wait()

	ts := gov.TotalUsage()
	if ts.Allocated != 0 {
		t.Errorf("after all goroutines done, Total.Allocated = %d, want 0", ts.Allocated)
	}
}

func TestConcurrent_Governor_OnPressure_NoRace(t *testing.T) {
	gov := NewGovernor(GovernorConfig{TotalLimit: 10000})

	var wg sync.WaitGroup

	// Register callbacks concurrently.
	wg.Add(10)
	for i := 0; i < 10; i++ {
		class := MemoryClass(i % int(numClasses))
		go func(class MemoryClass) {
			defer wg.Done()
			gov.OnPressure(class, func(target int64) int64 {
				return 0
			})
		}(class)
	}
	wg.Wait()

	// Reserve and trigger pressure concurrently.
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()
			_ = gov.Reserve(ClassSpillable, 5000)
			gov.Release(ClassSpillable, 5000)
		}()
	}
	wg.Wait()
}

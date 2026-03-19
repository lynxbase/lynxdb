package memgov

import "testing"

func TestUnit_PressureRegistry_InvocationOrder(t *testing.T) {
	pr := newPressureRegistry()

	var order []MemoryClass

	pr.register(ClassMetadata, func(target int64) int64 {
		order = append(order, ClassMetadata)
		return 0
	})
	pr.register(ClassRevocable, func(target int64) int64 {
		order = append(order, ClassRevocable)
		return 0
	})
	pr.register(ClassPageCache, func(target int64) int64 {
		order = append(order, ClassPageCache)
		return 0
	})
	pr.register(ClassSpillable, func(target int64) int64 {
		order = append(order, ClassSpillable)
		return 0
	})
	pr.register(ClassTempIO, func(target int64) int64 {
		order = append(order, ClassTempIO)
		return 0
	})

	pr.invoke(1000)

	// Expected order per revocationOrder:
	// ClassRevocable, ClassPageCache, ClassSpillable, ClassMetadata, ClassTempIO
	expected := []MemoryClass{
		ClassRevocable,
		ClassPageCache,
		ClassSpillable,
		ClassMetadata,
		ClassTempIO,
	}

	if len(order) != len(expected) {
		t.Fatalf("invoked %d callbacks, want %d", len(order), len(expected))
	}
	for i, got := range order {
		if got != expected[i] {
			t.Errorf("callback[%d] = %s, want %s", i, got, expected[i])
		}
	}
}

func TestUnit_PressureRegistry_StopsWhenTargetMet(t *testing.T) {
	pr := newPressureRegistry()

	callCount := 0

	pr.register(ClassRevocable, func(target int64) int64 {
		callCount++
		return target // claim we freed everything
	})
	pr.register(ClassPageCache, func(target int64) int64 {
		callCount++
		return target
	})
	pr.register(ClassSpillable, func(target int64) int64 {
		callCount++
		return target
	})

	freed := pr.invoke(500)

	if callCount != 1 {
		t.Errorf("callCount = %d, want 1 (should stop after first callback satisfied target)", callCount)
	}
	if freed != 500 {
		t.Errorf("freed = %d, want 500", freed)
	}
}

func TestUnit_PressureRegistry_PartialFreeingContinuesToNextCallback(t *testing.T) {
	pr := newPressureRegistry()

	pr.register(ClassRevocable, func(target int64) int64 {
		return 300 // only frees part
	})
	pr.register(ClassPageCache, func(target int64) int64 {
		return 200 // remaining target = 500 - 300 = 200
	})
	pr.register(ClassSpillable, func(target int64) int64 {
		t.Error("should not be called, target already met")
		return 0
	})

	freed := pr.invoke(500)
	if freed != 500 {
		t.Errorf("freed = %d, want 500", freed)
	}
}

func TestUnit_PressureRegistry_NoCallbacks_ReturnsZero(t *testing.T) {
	pr := newPressureRegistry()

	freed := pr.invoke(1000)
	if freed != 0 {
		t.Errorf("freed = %d with no callbacks, want 0", freed)
	}
}

func TestUnit_PressureRegistry_MultipleCallbacksSameClass(t *testing.T) {
	pr := newPressureRegistry()

	callOrder := []int{}

	pr.register(ClassRevocable, func(target int64) int64 {
		callOrder = append(callOrder, 1)
		return 100
	})
	pr.register(ClassRevocable, func(target int64) int64 {
		callOrder = append(callOrder, 2)
		return 100
	})

	freed := pr.invoke(200)
	if freed != 200 {
		t.Errorf("freed = %d, want 200", freed)
	}
	if len(callOrder) != 2 {
		t.Errorf("callOrder = %v, want 2 entries", callOrder)
	}
	if callOrder[0] != 1 || callOrder[1] != 2 {
		t.Errorf("callOrder = %v, want [1 2]", callOrder)
	}
}

func TestUnit_PressureRegistry_NonRevocable_NeverInvoked(t *testing.T) {
	pr := newPressureRegistry()

	called := false
	pr.register(ClassNonRevocable, func(target int64) int64 {
		called = true
		return target
	})

	pr.invoke(1000)

	if called {
		t.Error("ClassNonRevocable callback should never be invoked (not in revocationOrder)")
	}
}

func TestUnit_PressureRegistry_CallbackReceivesRemainingTarget(t *testing.T) {
	pr := newPressureRegistry()

	var targets []int64

	pr.register(ClassRevocable, func(target int64) int64 {
		targets = append(targets, target)
		return 300
	})
	pr.register(ClassPageCache, func(target int64) int64 {
		targets = append(targets, target)
		return 200
	})

	pr.invoke(500)

	if len(targets) != 2 {
		t.Fatalf("expected 2 callbacks, got %d", len(targets))
	}
	if targets[0] != 500 {
		t.Errorf("first callback target = %d, want 500", targets[0])
	}
	if targets[1] != 200 {
		t.Errorf("second callback target = %d, want 200 (500-300)", targets[1])
	}
}

func TestUnit_PressureRegistry_CallbackReturnsNegative_Ignored(t *testing.T) {
	pr := newPressureRegistry()

	pr.register(ClassRevocable, func(target int64) int64 {
		return -100 // bogus negative return
	})
	pr.register(ClassPageCache, func(target int64) int64 {
		return target // should still get full target
	})

	freed := pr.invoke(500)
	if freed != 500 {
		t.Errorf("freed = %d, want 500 (negative returns ignored)", freed)
	}
}

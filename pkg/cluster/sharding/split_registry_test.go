package sharding

import "testing"

func TestSplitRegistry_Resolve_NoSplit(t *testing.T) {
	sr := NewSplitRegistry()

	// No splits registered — resolve returns the input partition.
	got := sr.Resolve(42, 0xDEADBEEF)
	if got != 42 {
		t.Errorf("expected 42, got %d", got)
	}
}

func TestSplitRegistry_Resolve_SingleSplit(t *testing.T) {
	sr := NewSplitRegistry()
	sr.Register(&SplitInfo{
		ParentPartition: 10,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})

	// Bit 0 = 0 → ChildA.
	got := sr.Resolve(10, 0b1110) // bit 0 = 0
	if got != 1024 {
		t.Errorf("expected ChildA=1024, got %d", got)
	}

	// Bit 0 = 1 → ChildB.
	got = sr.Resolve(10, 0b1111) // bit 0 = 1
	if got != 1025 {
		t.Errorf("expected ChildB=1025, got %d", got)
	}
}

func TestSplitRegistry_Resolve_ChainedSplits(t *testing.T) {
	sr := NewSplitRegistry()

	// First split: partition 10 → (1024, 1025) on bit 0.
	sr.Register(&SplitInfo{
		ParentPartition: 10,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})

	// Second split: partition 1024 → (2048, 2049) on bit 1.
	sr.Register(&SplitInfo{
		ParentPartition: 1024,
		ChildA:          2048,
		ChildB:          2049,
		SplitBit:        1,
	})

	// hash=0b00 → bit0=0 → 1024 → bit1=0 → 2048.
	got := sr.Resolve(10, 0b00)
	if got != 2048 {
		t.Errorf("expected 2048, got %d", got)
	}

	// hash=0b10 → bit0=0 → 1024 → bit1=1 → 2049.
	got = sr.Resolve(10, 0b10)
	if got != 2049 {
		t.Errorf("expected 2049, got %d", got)
	}

	// hash=0b01 → bit0=1 → 1025 (no further split).
	got = sr.Resolve(10, 0b01)
	if got != 1025 {
		t.Errorf("expected 1025, got %d", got)
	}
}

func TestSplitRegistry_Unregister(t *testing.T) {
	sr := NewSplitRegistry()
	sr.Register(&SplitInfo{
		ParentPartition: 10,
		ChildA:          1024,
		ChildB:          1025,
		SplitBit:        0,
	})

	sr.Unregister(10)

	// After unregister, resolve should return the partition itself.
	got := sr.Resolve(10, 0xFF)
	if got != 10 {
		t.Errorf("expected 10 after unregister, got %d", got)
	}

	if sr.Len() != 0 {
		t.Errorf("expected 0 splits, got %d", sr.Len())
	}
}

func TestSplitRegistry_All(t *testing.T) {
	sr := NewSplitRegistry()
	sr.Register(&SplitInfo{ParentPartition: 1, ChildA: 10, ChildB: 11, SplitBit: 0})
	sr.Register(&SplitInfo{ParentPartition: 2, ChildA: 20, ChildB: 21, SplitBit: 0})

	all := sr.All()
	if len(all) != 2 {
		t.Errorf("expected 2 splits, got %d", len(all))
	}
	if all[1] == nil || all[1].ChildA != 10 {
		t.Errorf("missing or wrong split for partition 1")
	}
	if all[2] == nil || all[2].ChildA != 20 {
		t.Errorf("missing or wrong split for partition 2")
	}
}

func TestSplitRegistry_Get(t *testing.T) {
	sr := NewSplitRegistry()
	sr.Register(&SplitInfo{ParentPartition: 5, ChildA: 50, ChildB: 51, SplitBit: 3})

	info := sr.Get(5)
	if info == nil {
		t.Fatal("expected split info for partition 5")
	}
	if info.SplitBit != 3 {
		t.Errorf("expected split_bit=3, got %d", info.SplitBit)
	}

	if sr.Get(99) != nil {
		t.Error("expected nil for non-existent partition")
	}
}

func TestSplitRegistry_ConcurrentAccess(t *testing.T) {
	sr := NewSplitRegistry()

	// Concurrent register + resolve shouldn't race.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := uint32(0); i < 100; i++ {
			sr.Register(&SplitInfo{
				ParentPartition: i,
				ChildA:          i*2 + 1000,
				ChildB:          i*2 + 1001,
				SplitBit:        0,
			})
		}
	}()

	for i := uint32(0); i < 100; i++ {
		sr.Resolve(i, uint64(i))
	}

	<-done
}

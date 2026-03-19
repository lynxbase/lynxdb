package bufmgr

import (
	"fmt"
	"testing"
)

func TestUnit_ResidencyIndex_InsertAndLookup_ReturnsFrameID(t *testing.T) {
	idx := NewResidencyIndex(32)

	ok := idx.Insert(OwnerSegCache, "seg-001", FrameID(7))
	if !ok {
		t.Fatal("Insert returned false; expected true")
	}

	id, found := idx.Lookup(OwnerSegCache, "seg-001")
	if !found {
		t.Fatal("Lookup returned not-found after Insert")
	}
	if id != FrameID(7) {
		t.Fatalf("Lookup returned FrameID %d; want 7", id)
	}
}

func TestUnit_ResidencyIndex_Lookup_MissingKey_ReturnsFalse(t *testing.T) {
	idx := NewResidencyIndex(32)

	_, found := idx.Lookup(OwnerSegCache, "does-not-exist")
	if found {
		t.Fatal("Lookup found a key that was never inserted")
	}
}

func TestUnit_ResidencyIndex_Upsert_SameKey_OverwritesValue(t *testing.T) {
	idx := NewResidencyIndex(32)

	idx.Insert(OwnerQuery, "op-1", FrameID(10))
	idx.Insert(OwnerQuery, "op-1", FrameID(20))

	id, found := idx.Lookup(OwnerQuery, "op-1")
	if !found {
		t.Fatal("Lookup returned not-found after upsert")
	}
	if id != FrameID(20) {
		t.Fatalf("Lookup returned FrameID %d after upsert; want 20", id)
	}

	// Upsert must not double-count.
	if idx.Count() != 1 {
		t.Fatalf("Count after upsert = %d; want 1", idx.Count())
	}
}

func TestUnit_ResidencyIndex_Remove_ThenLookup_ReturnsFalse(t *testing.T) {
	idx := NewResidencyIndex(32)

	idx.Insert(OwnerMemtable, "mt-0", FrameID(5))
	idx.Remove(OwnerMemtable, "mt-0")

	_, found := idx.Lookup(OwnerMemtable, "mt-0")
	if found {
		t.Fatal("Lookup found key after Remove")
	}
}

func TestUnit_ResidencyIndex_Remove_ThenReinsert_Succeeds(t *testing.T) {
	idx := NewResidencyIndex(32)

	idx.Insert(OwnerSegCache, "seg-A", FrameID(1))
	idx.Remove(OwnerSegCache, "seg-A")

	// Reinsert at the same key with a different FrameID.
	ok := idx.Insert(OwnerSegCache, "seg-A", FrameID(99))
	if !ok {
		t.Fatal("Insert after Remove returned false")
	}

	id, found := idx.Lookup(OwnerSegCache, "seg-A")
	if !found {
		t.Fatal("Lookup returned not-found after reinsert")
	}
	if id != FrameID(99) {
		t.Fatalf("Lookup returned FrameID %d; want 99", id)
	}
	if idx.Count() != 1 {
		t.Fatalf("Count after remove+reinsert = %d; want 1", idx.Count())
	}
}

func TestUnit_ResidencyIndex_Count_TracksInsertAndRemove(t *testing.T) {
	idx := NewResidencyIndex(64)

	if idx.Count() != 0 {
		t.Fatalf("initial Count = %d; want 0", idx.Count())
	}

	for i := 0; i < 10; i++ {
		idx.Insert(OwnerSegCache, fmt.Sprintf("seg-%d", i), FrameID(i+1))
	}
	if idx.Count() != 10 {
		t.Fatalf("Count after 10 inserts = %d; want 10", idx.Count())
	}

	for i := 0; i < 5; i++ {
		idx.Remove(OwnerSegCache, fmt.Sprintf("seg-%d", i))
	}
	if idx.Count() != 5 {
		t.Fatalf("Count after 5 removes = %d; want 5", idx.Count())
	}
}

func TestUnit_ResidencyIndex_LoadFactor_RejectsWhenFull(t *testing.T) {
	// Minimum capacity is 16. At 75% load factor, max entries = 12.
	idx := NewResidencyIndex(16)

	inserted := 0
	for i := 0; i < 16; i++ {
		tag := fmt.Sprintf("tag-%d", i)
		ok := idx.Insert(OwnerQuery, tag, FrameID(i+1))
		if ok {
			inserted++
		} else {
			break
		}
	}

	// With capacity 16, the 75% threshold is 12 entries.
	if inserted != 12 {
		t.Fatalf("inserted %d entries before rejection; want 12 (75%% of 16)", inserted)
	}

	// The 13th insert should fail.
	ok := idx.Insert(OwnerQuery, "tag-overflow", FrameID(100))
	if ok {
		t.Fatal("Insert succeeded beyond 75% load factor; expected rejection")
	}
}

func TestUnit_ResidencyIndex_DifferentOwners_SameTag_AreDistinct(t *testing.T) {
	idx := NewResidencyIndex(32)

	idx.Insert(OwnerSegCache, "shared-tag", FrameID(1))
	idx.Insert(OwnerQuery, "shared-tag", FrameID(2))
	idx.Insert(OwnerMemtable, "shared-tag", FrameID(3))

	if idx.Count() != 3 {
		t.Fatalf("Count = %d; want 3 (different owners, same tag)", idx.Count())
	}

	tests := []struct {
		owner FrameOwner
		want  FrameID
	}{
		{OwnerSegCache, FrameID(1)},
		{OwnerQuery, FrameID(2)},
		{OwnerMemtable, FrameID(3)},
	}

	for _, tc := range tests {
		id, found := idx.Lookup(tc.owner, "shared-tag")
		if !found {
			t.Errorf("Lookup(%s, shared-tag): not found", tc.owner)
			continue
		}
		if id != tc.want {
			t.Errorf("Lookup(%s, shared-tag) = %d; want %d", tc.owner, id, tc.want)
		}
	}
}

func TestUnit_ResidencyIndex_Remove_NonexistentKey_IsNoop(t *testing.T) {
	idx := NewResidencyIndex(32)

	idx.Insert(OwnerSegCache, "existing", FrameID(1))

	// Remove a key that was never inserted. This must not panic or corrupt state.
	idx.Remove(OwnerQuery, "ghost")

	if idx.Count() != 1 {
		t.Fatalf("Count after removing nonexistent key = %d; want 1", idx.Count())
	}

	// Original entry is still intact.
	id, found := idx.Lookup(OwnerSegCache, "existing")
	if !found || id != FrameID(1) {
		t.Fatalf("Original entry corrupted after removing nonexistent key")
	}
}

func TestUnit_ResidencyIndex_MinimumCapacity_Is16(t *testing.T) {
	// Even with capacity 1, the index should allow at least a few entries.
	idx := NewResidencyIndex(1)

	// Should be able to insert entries (capacity was rounded up to 16).
	for i := 0; i < 12; i++ {
		ok := idx.Insert(OwnerSegCache, fmt.Sprintf("tag-%d", i), FrameID(i+1))
		if !ok {
			t.Fatalf("Insert %d failed; minimum capacity should allow at least 12 entries (75%% of 16)", i)
		}
	}
}

func TestUnit_ResidencyIndex_LookupAfterCollision_FindsCorrectEntry(t *testing.T) {
	// Insert many entries to force hash collisions via linear probing.
	idx := NewResidencyIndex(32) // capacity rounds to 32

	for i := 0; i < 20; i++ {
		tag := fmt.Sprintf("key-%03d", i)
		ok := idx.Insert(OwnerSegCache, tag, FrameID(i+1))
		if !ok {
			break // hit load factor
		}
	}

	// Every inserted entry must be retrievable.
	for i := 0; i < idx.Count(); i++ {
		tag := fmt.Sprintf("key-%03d", i)
		id, found := idx.Lookup(OwnerSegCache, tag)
		if !found {
			t.Errorf("Lookup(OwnerSegCache, %q): not found", tag)
			continue
		}
		if id != FrameID(i+1) {
			t.Errorf("Lookup(OwnerSegCache, %q) = %d; want %d", tag, id, i+1)
		}
	}
}

func TestUnit_ResidencyIndex_Remove_DoesNotBreakProbeChain(t *testing.T) {
	// Removing an entry must use tombstones (deleted flag), not clear occupied,
	// otherwise entries after the removed slot become unreachable.
	idx := NewResidencyIndex(16)

	// Insert enough entries to likely cause probe chains.
	tags := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	for i, tag := range tags {
		idx.Insert(OwnerSegCache, tag, FrameID(i+1))
	}

	// Remove entries in the middle of likely probe chains.
	idx.Remove(OwnerSegCache, "charlie")
	idx.Remove(OwnerSegCache, "echo")

	// All remaining entries must still be reachable.
	remaining := []struct {
		tag  string
		want FrameID
	}{
		{"alpha", 1}, {"bravo", 2}, {"delta", 4},
		{"foxtrot", 6}, {"golf", 7}, {"hotel", 8},
	}
	for _, tc := range remaining {
		id, found := idx.Lookup(OwnerSegCache, tc.tag)
		if !found {
			t.Errorf("Lookup(%q) not found after removing middle entries", tc.tag)
			continue
		}
		if id != tc.want {
			t.Errorf("Lookup(%q) = %d; want %d", tc.tag, id, tc.want)
		}
	}

	// Removed entries must not be found.
	for _, tag := range []string{"charlie", "echo"} {
		_, found := idx.Lookup(OwnerSegCache, tag)
		if found {
			t.Errorf("Lookup(%q) found after Remove", tag)
		}
	}
}

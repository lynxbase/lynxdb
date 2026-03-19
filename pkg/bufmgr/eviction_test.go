package bufmgr

import (
	"testing"
)

// newTestFrame creates a Frame suitable for eviction queue tests.
// slot must be in [0, capacity).
func newTestFrame(id FrameID, slot int, owner FrameOwner) *Frame {
	f := &Frame{
		ID:    id,
		Owner: owner,
		slot:  slot,
	}
	f.State.Store(int32(StateClean))
	return f
}

func TestUnit_EvictionQueue_EvictOne_EmptyQueue_ReturnsNil(t *testing.T) {
	eq := newEvictionQueue(8)

	got := eq.evictOne()
	if got != nil {
		t.Fatalf("evictOne on empty queue returned frame %d; want nil", got.ID)
	}
}

func TestUnit_EvictionQueue_EvictOne_SingleFrame_ReturnsIt(t *testing.T) {
	eq := newEvictionQueue(8)

	f := newTestFrame(1, 0, OwnerSegCache)
	eq.add(f)
	// Clear the reference bit so the frame is immediately evictable
	// (add sets RefBit=true; the first scan will give it a second chance).
	f.RefBit.Store(false)

	evicted := eq.evictOne()
	if evicted == nil {
		t.Fatal("evictOne returned nil; expected the single frame")
	}
	if evicted.ID != 1 {
		t.Fatalf("evicted frame ID = %d; want 1", evicted.ID)
	}
	if eq.len() != 0 {
		t.Fatalf("queue count after eviction = %d; want 0", eq.len())
	}
}

func TestUnit_EvictionQueue_EvictOne_SecondChance_ClearsRefBitFirst(t *testing.T) {
	eq := newEvictionQueue(4)

	// Frame 0 at slot 0 with refBit=true (set by add).
	f0 := newTestFrame(1, 0, OwnerSegCache)
	eq.add(f0)

	// Frame 1 at slot 1 with refBit manually cleared.
	f1 := newTestFrame(2, 1, OwnerSegCache)
	eq.add(f1)
	f1.RefBit.Store(false)

	// The clock hand starts at 0. It should:
	//   1. See f0 at slot 0 with refBit=true -> clear bit, advance
	//   2. See f1 at slot 1 with refBit=false -> evict it
	evicted := eq.evictOne()
	if evicted == nil {
		t.Fatal("evictOne returned nil; expected frame 2")
	}
	if evicted.ID != 2 {
		t.Fatalf("evicted frame ID = %d; want 2 (second-chance should skip frame 1)", evicted.ID)
	}

	// f0's refBit should have been cleared (it got a second chance).
	if f0.RefBit.Load() {
		t.Fatal("frame 1's refBit still set; second-chance should have cleared it")
	}
}

func TestUnit_EvictionQueue_EvictOne_PinnedFrames_AreSkipped(t *testing.T) {
	eq := newEvictionQueue(4)

	pinned := newTestFrame(1, 0, OwnerSegCache)
	pinned.Pin()
	eq.add(pinned)

	unpinned := newTestFrame(2, 1, OwnerQuery)
	eq.add(unpinned)
	unpinned.RefBit.Store(false)

	evicted := eq.evictOne()
	if evicted == nil {
		t.Fatal("evictOne returned nil; expected the unpinned frame")
	}
	if evicted.ID != 2 {
		t.Fatalf("evicted frame ID = %d; want 2 (pinned frame should be skipped)", evicted.ID)
	}
}

func TestUnit_EvictionQueue_EvictOne_AllPinned_ReturnsNil(t *testing.T) {
	eq := newEvictionQueue(4)

	for i := 0; i < 4; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerSegCache)
		f.Pin()
		eq.add(f)
	}

	evicted := eq.evictOne()
	if evicted != nil {
		t.Fatalf("evictOne returned frame %d; want nil (all frames are pinned)", evicted.ID)
	}
}

func TestUnit_EvictionQueue_EvictBatch_ReturnsUpToN(t *testing.T) {
	eq := newEvictionQueue(8)

	for i := 0; i < 8; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerSegCache)
		eq.add(f)
		f.RefBit.Store(false)
	}

	evicted := eq.evictBatch(3, OwnerFree)
	if len(evicted) != 3 {
		t.Fatalf("evictBatch(3) returned %d frames; want 3", len(evicted))
	}
	if eq.len() != 5 {
		t.Fatalf("queue count after evictBatch(3) = %d; want 5", eq.len())
	}
}

func TestUnit_EvictionQueue_EvictBatch_PrefersOwner(t *testing.T) {
	eq := newEvictionQueue(8)

	// 4 OwnerSegCache frames, 4 OwnerQuery frames.
	for i := 0; i < 4; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerSegCache)
		eq.add(f)
		f.RefBit.Store(false)
	}
	for i := 4; i < 8; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerQuery)
		eq.add(f)
		f.RefBit.Store(false)
	}

	// Request 3, preferring OwnerSegCache.
	evicted := eq.evictBatch(3, OwnerSegCache)
	if len(evicted) != 3 {
		t.Fatalf("evictBatch(3, OwnerSegCache) returned %d frames; want 3", len(evicted))
	}

	for _, f := range evicted {
		if f.Owner != OwnerSegCache {
			t.Errorf("evicted frame %d has owner %s; want seg-cache (preferred owner)", f.ID, f.Owner)
		}
	}
}

func TestUnit_EvictionQueue_EvictBatch_FallsBackToAnyOwner(t *testing.T) {
	eq := newEvictionQueue(8)

	// Only 2 OwnerSegCache frames, 4 OwnerQuery frames.
	for i := 0; i < 2; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerSegCache)
		eq.add(f)
		f.RefBit.Store(false)
	}
	for i := 2; i < 6; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerQuery)
		eq.add(f)
		f.RefBit.Store(false)
	}

	// Request 4, preferring OwnerSegCache. Only 2 are seg-cache, so it should
	// fill the remaining 2 from OwnerQuery.
	evicted := eq.evictBatch(4, OwnerSegCache)
	if len(evicted) != 4 {
		t.Fatalf("evictBatch(4, OwnerSegCache) returned %d frames; want 4", len(evicted))
	}
}

func TestUnit_EvictionQueue_EvictByOwner_ReturnsCorrectOwner(t *testing.T) {
	eq := newEvictionQueue(8)

	for i := 0; i < 4; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerSegCache)
		eq.add(f)
		f.RefBit.Store(false)
	}
	for i := 4; i < 8; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerQuery)
		eq.add(f)
		f.RefBit.Store(false)
	}

	evicted := eq.evictByOwner(OwnerQuery)
	if evicted == nil {
		t.Fatal("evictByOwner(OwnerQuery) returned nil")
	}
	if evicted.Owner != OwnerQuery {
		t.Fatalf("evicted frame owner = %s; want query", evicted.Owner)
	}
}

func TestUnit_EvictionQueue_EvictByOwner_NoMatchingOwner_ReturnsNil(t *testing.T) {
	eq := newEvictionQueue(4)

	for i := 0; i < 4; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerSegCache)
		eq.add(f)
		f.RefBit.Store(false)
	}

	evicted := eq.evictByOwner(OwnerCompaction)
	if evicted != nil {
		t.Fatalf("evictByOwner(OwnerCompaction) returned frame %d; want nil (no compaction frames)", evicted.ID)
	}
}

func TestUnit_EvictionQueue_Add_Remove_CountTracking(t *testing.T) {
	eq := newEvictionQueue(8)

	if eq.len() != 0 {
		t.Fatalf("initial count = %d; want 0", eq.len())
	}

	frames := make([]*Frame, 5)
	for i := 0; i < 5; i++ {
		frames[i] = newTestFrame(FrameID(i+1), i, OwnerSegCache)
		eq.add(frames[i])
	}

	if eq.len() != 5 {
		t.Fatalf("count after 5 adds = %d; want 5", eq.len())
	}

	eq.remove(frames[2])
	eq.remove(frames[4])

	if eq.len() != 3 {
		t.Fatalf("count after 2 removes = %d; want 3", eq.len())
	}

	// Removing the same frame again should not decrement further.
	eq.remove(frames[2])
	if eq.len() != 3 {
		t.Fatalf("count after duplicate remove = %d; want 3", eq.len())
	}
}

func TestUnit_EvictionQueue_EvictBatch_ZeroOrNegativeN_ReturnsNil(t *testing.T) {
	eq := newEvictionQueue(4)

	f := newTestFrame(1, 0, OwnerSegCache)
	eq.add(f)

	if got := eq.evictBatch(0, OwnerFree); got != nil {
		t.Fatalf("evictBatch(0) returned %d frames; want nil", len(got))
	}
	if got := eq.evictBatch(-1, OwnerFree); got != nil {
		t.Fatalf("evictBatch(-1) returned %d frames; want nil", len(got))
	}
}

func TestUnit_EvictionQueue_EvictOne_ForceFallback_AllHaveRefBitAndPinned(t *testing.T) {
	// Scenario: one frame pinned, one with refBit. The second frame should be
	// evicted via the fallback path after the clock clears refBit.
	eq := newEvictionQueue(4)

	pinned := newTestFrame(1, 0, OwnerSegCache)
	pinned.Pin()
	eq.add(pinned)

	refBitSet := newTestFrame(2, 1, OwnerQuery)
	eq.add(refBitSet) // refBit=true from add

	evicted := eq.evictOne()
	if evicted == nil {
		t.Fatal("evictOne returned nil; expected frame 2 to be evicted via fallback")
	}
	if evicted.ID != 2 {
		t.Fatalf("evicted frame ID = %d; want 2", evicted.ID)
	}
}

func TestUnit_EvictionQueue_Add_SameSlot_Replaces(t *testing.T) {
	eq := newEvictionQueue(4)

	f1 := newTestFrame(1, 0, OwnerSegCache)
	eq.add(f1)

	if eq.len() != 1 {
		t.Fatalf("count after first add = %d; want 1", eq.len())
	}

	// Adding a different frame to the same slot replaces it.
	f2 := newTestFrame(2, 0, OwnerQuery)
	eq.add(f2)

	// Count should stay the same (replaced, not added).
	if eq.len() != 1 {
		t.Fatalf("count after replacing slot = %d; want 1", eq.len())
	}
}

func TestUnit_EvictionQueue_EvictBatch_MoreThanAvailable_ReturnsAll(t *testing.T) {
	eq := newEvictionQueue(4)

	for i := 0; i < 3; i++ {
		f := newTestFrame(FrameID(i+1), i, OwnerSegCache)
		eq.add(f)
		f.RefBit.Store(false)
	}

	// Request 10 but only 3 are available.
	evicted := eq.evictBatch(10, OwnerFree)
	if len(evicted) != 3 {
		t.Fatalf("evictBatch(10) with 3 frames returned %d; want 3", len(evicted))
	}
	if eq.len() != 0 {
		t.Fatalf("queue count after evicting all = %d; want 0", eq.len())
	}
}

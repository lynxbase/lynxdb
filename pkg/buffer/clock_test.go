package buffer

import (
	"testing"
)

func newTestPage(id PageID, slot int, owner PageOwner) *Page {
	return &Page{
		id:       id,
		size:     PageSize64KB,
		poolSlot: slot,
		owner:    owner,
	}
}

func TestClockEvictor_AddAndLen(t *testing.T) {
	c := newClockEvictor(4)
	if c.len() != 0 {
		t.Errorf("len = %d, want 0", c.len())
	}

	p := newTestPage(1, 0, OwnerSegmentCache)
	c.add(p)
	if c.len() != 1 {
		t.Errorf("len = %d, want 1", c.len())
	}

	// Adding same slot again should not increase count.
	c.add(p)
	if c.len() != 1 {
		t.Errorf("len after re-add = %d, want 1", c.len())
	}
}

func TestClockEvictor_Remove(t *testing.T) {
	c := newClockEvictor(4)

	p := newTestPage(1, 0, OwnerSegmentCache)
	c.add(p)
	c.remove(p)
	if c.len() != 0 {
		t.Errorf("len after remove = %d, want 0", c.len())
	}

	// Double remove should be safe.
	c.remove(p)
	if c.len() != 0 {
		t.Errorf("len after double remove = %d, want 0", c.len())
	}
}

func TestClockEvictor_Evict_Empty(t *testing.T) {
	c := newClockEvictor(4)

	if p := c.evict(); p != nil {
		t.Errorf("evict on empty evictor returned %v, want nil", p)
	}
}

func TestClockEvictor_Evict_SingleUnpinned(t *testing.T) {
	c := newClockEvictor(4)

	p := newTestPage(1, 0, OwnerSegmentCache)
	p.refBit.Store(false) // no second chance
	c.add(p)

	evicted := c.evict()
	if evicted != p {
		t.Errorf("evicted = %v, want %v", evicted, p)
	}
	if c.len() != 0 {
		t.Errorf("len after evict = %d, want 0", c.len())
	}
}

func TestClockEvictor_Evict_SecondChance(t *testing.T) {
	c := newClockEvictor(4)

	p1 := newTestPage(1, 0, OwnerSegmentCache)
	p2 := newTestPage(2, 1, OwnerSegmentCache)

	// p1 has refBit=true (gets second chance), p2 has refBit=false (evicted first).
	c.add(p1)
	c.add(p2)

	// p1 was added first (refBit=true from add), p2 also refBit=true from add.
	// Clear p2's refBit to make it evictable immediately.
	p2.refBit.Store(false)

	evicted := c.evict()
	// Clock hand starts at 0, sees p1 with refBit=true, clears it, moves to p2
	// which has refBit=false, evicts p2.
	if evicted != p2 {
		t.Errorf("evicted page ID %d, want %d (second chance should protect p1)", evicted.id, p2.id)
	}
}

func TestClockEvictor_Evict_SkipsPinned(t *testing.T) {
	c := newClockEvictor(4)

	p1 := newTestPage(1, 0, OwnerSegmentCache)
	p2 := newTestPage(2, 1, OwnerSegmentCache)

	// Pin p1, leave p2 unpinned.
	p1.Pin()
	p2.refBit.Store(false) // make immediately evictable

	c.add(p1)
	c.add(p2)

	evicted := c.evict()
	if evicted != p2 {
		t.Errorf("evicted page ID %d, want %d (pinned page should be skipped)", evicted.id, p2.id)
	}
	if c.len() != 1 {
		t.Errorf("len = %d, want 1", c.len())
	}
}

func TestClockEvictor_Evict_AllPinned(t *testing.T) {
	c := newClockEvictor(4)

	p1 := newTestPage(1, 0, OwnerSegmentCache)
	p2 := newTestPage(2, 1, OwnerSegmentCache)

	p1.Pin()
	p2.Pin()

	c.add(p1)
	c.add(p2)

	if evicted := c.evict(); evicted != nil {
		t.Errorf("evict should return nil when all pages are pinned, got page %d", evicted.id)
	}
}

func TestClockEvictor_Evict_ForceFallback(t *testing.T) {
	c := newClockEvictor(2)

	p1 := newTestPage(1, 0, OwnerSegmentCache)
	p2 := newTestPage(2, 1, OwnerSegmentCache)

	// Both have refBit=true (from add). Neither is pinned.
	c.add(p1)
	c.add(p2)

	// The main loop (2*capacity sweeps) should clear refBits on first pass
	// and evict on second pass. If somehow that fails, the fallback loop
	// force-evicts any unpinned page.
	evicted := c.evict()
	if evicted == nil {
		t.Fatal("evict should find an unpinned page")
	}
	if evicted.IsPinned() {
		t.Error("evicted page should not be pinned")
	}
}

func TestClockEvictor_EvictByOwner(t *testing.T) {
	c := newClockEvictor(4)

	cache1 := newTestPage(1, 0, OwnerSegmentCache)
	cache2 := newTestPage(2, 1, OwnerSegmentCache)
	query1 := newTestPage(3, 2, OwnerQueryOperator)

	c.add(cache1)
	c.add(cache2)
	c.add(query1)

	// Clear refBit on cache pages for immediate evictability.
	cache1.refBit.Store(false)
	cache2.refBit.Store(false)

	evicted := c.evictByOwner(OwnerSegmentCache)
	if evicted == nil {
		t.Fatal("evictByOwner should find a cache page")
	}
	if evicted.owner != OwnerSegmentCache {
		t.Errorf("evicted owner = %v, want OwnerSegmentCache", evicted.owner)
	}
}

func TestClockEvictor_EvictByOwner_NoMatch(t *testing.T) {
	c := newClockEvictor(4)

	query1 := newTestPage(1, 0, OwnerQueryOperator)
	query1.refBit.Store(false)
	c.add(query1)

	// Request eviction of cache pages, but none exist.
	evicted := c.evictByOwner(OwnerSegmentCache)
	if evicted != nil {
		t.Errorf("evictByOwner should return nil when no matching owner, got page %d", evicted.id)
	}
}

func TestClockEvictor_EvictByOwner_SkipsPinned(t *testing.T) {
	c := newClockEvictor(4)

	cache1 := newTestPage(1, 0, OwnerSegmentCache)
	cache2 := newTestPage(2, 1, OwnerSegmentCache)

	cache1.Pin() // pinned, cannot evict
	cache2.refBit.Store(false)

	c.add(cache1)
	c.add(cache2)

	evicted := c.evictByOwner(OwnerSegmentCache)
	if evicted != cache2 {
		t.Errorf("should evict cache2 (unpinned), got page %v", evicted)
	}
}

func TestClockEvictor_AddOutOfBounds(t *testing.T) {
	c := newClockEvictor(2)

	// Page with slot out of bounds should be silently ignored.
	p := newTestPage(1, 5, OwnerSegmentCache)
	c.add(p) // should not panic
	if c.len() != 0 {
		t.Errorf("len = %d, want 0 (out-of-bounds slot)", c.len())
	}
}

func TestClockEvictor_RemoveOutOfBounds(t *testing.T) {
	c := newClockEvictor(2)

	p := newTestPage(1, 5, OwnerSegmentCache)
	c.remove(p) // should not panic
}

func TestClockEvictor_EvictByOwner_SecondChance(t *testing.T) {
	c := newClockEvictor(4)

	cache1 := newTestPage(1, 0, OwnerSegmentCache)
	cache2 := newTestPage(2, 1, OwnerSegmentCache)

	c.add(cache1) // refBit=true
	c.add(cache2) // refBit=true

	// cache1 gets second chance (refBit cleared), cache2 gets second chance too.
	// On next sweep, cache1 should be evicted (refBit was cleared on first pass).
	evicted := c.evictByOwner(OwnerSegmentCache)
	if evicted == nil {
		t.Fatal("evictByOwner should eventually find an evictable cache page")
	}
	if evicted.owner != OwnerSegmentCache {
		t.Errorf("evicted owner = %v, want OwnerSegmentCache", evicted.owner)
	}
}

func TestClockEvictor_DefaultCapacity(t *testing.T) {
	c := newClockEvictor(0) // should default to 1024
	if c.capacity != 1024 {
		t.Errorf("capacity = %d, want 1024", c.capacity)
	}

	c2 := newClockEvictor(-5) // negative should also default
	if c2.capacity != 1024 {
		t.Errorf("capacity = %d, want 1024", c2.capacity)
	}
}

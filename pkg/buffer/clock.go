package buffer

// clockEvictor implements the clock (second-chance) eviction algorithm.
//
// The clock algorithm provides O(1) amortized eviction with the same eviction
// quality as LRU. It maintains a circular array of page pointers. On eviction:
//  1. Scan clockwise from current hand position
//  2. If page is pinned -> skip
//  3. If page has reference bit set -> clear bit, advance (second chance)
//  4. If page has reference bit cleared -> evict this page
//  5. If dirty -> caller handles write-back before reclaiming
//
// All methods require external synchronization (BufferPool.mu).
type clockEvictor struct {
	pages    []*Page // circular buffer of page pointers (nil = empty slot)
	capacity int
	hand     int // current clock hand position
	count    int // number of non-nil slots
}

// newClockEvictor creates a clock evictor with the given page capacity.
func newClockEvictor(capacity int) *clockEvictor {
	if capacity < 1 {
		capacity = 1024
	}

	return &clockEvictor{
		pages:    make([]*Page, capacity),
		capacity: capacity,
	}
}

// add places a page into the clock buffer at its poolSlot position.
// The page's reference bit is set to true (recently used).
func (c *clockEvictor) add(p *Page) {
	slot := p.poolSlot
	if slot < 0 || slot >= c.capacity {
		return
	}
	if c.pages[slot] == nil {
		c.count++
	}
	c.pages[slot] = p
	p.refBit.Store(true)
}

// remove takes a page out of the clock buffer.
func (c *clockEvictor) remove(p *Page) {
	slot := p.poolSlot
	if slot < 0 || slot >= c.capacity {
		return
	}
	if c.pages[slot] != nil {
		c.pages[slot] = nil
		c.count--
	}
}

// evict finds and returns an unpinned page to evict using the clock algorithm.
// Returns nil if no evictable page is found (all pages are pinned).
//
// The eviction priority order is:
//  1. Unpinned, refBit=0, not dirty, owner=OwnerSegmentCache (cheapest to evict)
//  2. Unpinned, refBit=0, not dirty, owner=OwnerQueryOperator
//  3. Unpinned, refBit=0, dirty (requires write-back)
//  4. Unpinned, refBit=0, owner=OwnerMemtable (last resort — WAL provides durability)
//
// For simplicity and performance, the basic clock sweep does not distinguish
// owners. The owner-based priority is achieved by having memtable pages pin
// themselves during active writes and by the write-back cost naturally
// discouraging dirty page eviction (dirty pages survive an extra sweep because
// their refBit is typically set during write).
func (c *clockEvictor) evict() *Page {
	if c.count == 0 {
		return nil
	}

	// At most 2 full scans: first pass clears refBits, second pass evicts.
	limit := 2 * c.capacity
	for i := 0; i < limit; i++ {
		idx := c.hand
		c.hand = (c.hand + 1) % c.capacity

		p := c.pages[idx]
		if p == nil {
			continue
		}

		// Pinned pages cannot be evicted.
		if p.IsPinned() {
			continue
		}

		// Second-chance: if refBit is set, clear it and move on.
		if p.refBit.CompareAndSwap(true, false) {
			continue
		}

		// refBit is 0 and page is unpinned — evict it.
		c.pages[idx] = nil
		c.count--

		return p
	}

	// Fallback: try to force-evict any unpinned page (all had refBit set).
	for i := 0; i < c.capacity; i++ {
		idx := (c.hand + i) % c.capacity
		p := c.pages[idx]
		if p == nil {
			continue
		}
		if p.IsPinned() {
			continue
		}

		c.pages[idx] = nil
		c.count--
		c.hand = (idx + 1) % c.capacity

		return p
	}

	return nil
}

// evictByOwner finds and evicts a page owned by the specified owner.
// This allows targeted eviction (e.g., evict cache pages first before query pages).
// Returns nil if no evictable page with the given owner exists.
func (c *clockEvictor) evictByOwner(owner PageOwner) *Page {
	if c.count == 0 {
		return nil
	}

	// Scan from hand, looking for an unpinned page with matching owner.
	for i := 0; i < 2*c.capacity; i++ {
		idx := (c.hand + i) % c.capacity
		p := c.pages[idx]
		if p == nil {
			continue
		}
		if p.IsPinned() || p.owner != owner {
			// Clear refBit on non-matching pages as we pass (clock semantics).
			if !p.IsPinned() {
				p.refBit.Store(false)
			}

			continue
		}

		// Found a matching unpinned page.
		if p.refBit.CompareAndSwap(true, false) {
			continue // second chance
		}

		c.pages[idx] = nil
		c.count--
		c.hand = (idx + 1) % c.capacity

		return p
	}

	return nil
}

// len returns the number of pages currently in the clock buffer.
func (c *clockEvictor) len() int {
	return c.count
}

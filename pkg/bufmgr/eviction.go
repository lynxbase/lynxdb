package bufmgr

// evictionQueue implements batched Clock (second-chance) eviction.
//
// The queue maintains a circular buffer of frame pointers. On eviction:
//  1. Scan clockwise from current hand position
//  2. If frame is pinned → skip
//  3. If frame has reference bit set → clear bit, advance (second chance)
//  4. If frame has reference bit cleared → select for eviction
//
// Batched eviction selects up to N frames in one scan, reducing the overhead
// of per-frame eviction. The WritebackScheduler may then writeback dirty
// frames in bulk before recycling them.
//
// All methods require external synchronization (Manager.mu).
type evictionQueue struct {
	frames   []*Frame // circular buffer of frame pointers (nil = empty slot)
	capacity int
	hand     int // current clock hand position
	count    int // number of non-nil slots
}

// newEvictionQueue creates an eviction queue with the given capacity.
func newEvictionQueue(capacity int) *evictionQueue {
	if capacity < 1 {
		capacity = 1024
	}

	return &evictionQueue{
		frames:   make([]*Frame, capacity),
		capacity: capacity,
	}
}

// add places a frame into the eviction queue at its slot position.
func (eq *evictionQueue) add(f *Frame) {
	slot := f.slot
	if slot < 0 || slot >= eq.capacity {
		return
	}
	if eq.frames[slot] == nil {
		eq.count++
	}
	eq.frames[slot] = f
	f.RefBit.Store(true)
}

// remove takes a frame out of the eviction queue.
func (eq *evictionQueue) remove(f *Frame) {
	slot := f.slot
	if slot < 0 || slot >= eq.capacity {
		return
	}
	if eq.frames[slot] != nil {
		eq.frames[slot] = nil
		eq.count--
	}
}

// evictOne finds and returns one unpinned frame to evict using the clock algorithm.
// Returns nil if no evictable frame is found (all frames are pinned).
func (eq *evictionQueue) evictOne() *Frame {
	if eq.count == 0 {
		return nil
	}

	// At most 2 full scans: first pass clears refBits, second pass evicts.
	limit := 2 * eq.capacity
	for i := 0; i < limit; i++ {
		idx := eq.hand
		eq.hand = (eq.hand + 1) % eq.capacity

		f := eq.frames[idx]
		if f == nil {
			continue
		}
		if f.IsPinned() {
			continue
		}
		// Second-chance: if refBit is set, clear it and move on.
		if f.RefBit.CompareAndSwap(true, false) {
			continue
		}
		// refBit is 0 and frame is unpinned — evict it.
		eq.frames[idx] = nil
		eq.count--

		return f
	}

	// Fallback: force-evict any unpinned frame.
	for i := 0; i < eq.capacity; i++ {
		idx := (eq.hand + i) % eq.capacity
		f := eq.frames[idx]
		if f == nil || f.IsPinned() {
			continue
		}
		eq.frames[idx] = nil
		eq.count--
		eq.hand = (idx + 1) % eq.capacity

		return f
	}

	return nil
}

// evictBatch selects up to n frames for eviction. Prefers frames from the
// given owner. Returns the frames selected for eviction (caller must handle
// writeback for dirty frames and recycling).
func (eq *evictionQueue) evictBatch(n int, preferOwner FrameOwner) []*Frame {
	if eq.count == 0 || n <= 0 {
		return nil
	}

	result := make([]*Frame, 0, n)

	// First pass: prefer frames from the specified owner.
	if preferOwner != OwnerFree {
		for i := 0; i < 2*eq.capacity && len(result) < n; i++ {
			idx := (eq.hand + i) % eq.capacity
			f := eq.frames[idx]
			if f == nil || f.IsPinned() || f.Owner != preferOwner {
				continue
			}
			if f.RefBit.CompareAndSwap(true, false) {
				continue
			}
			eq.frames[idx] = nil
			eq.count--
			result = append(result, f)
		}
	}

	// Second pass: any unpinned frame if we haven't filled the batch.
	if len(result) < n {
		for i := 0; i < 2*eq.capacity && len(result) < n; i++ {
			idx := eq.hand
			eq.hand = (eq.hand + 1) % eq.capacity

			f := eq.frames[idx]
			if f == nil || f.IsPinned() {
				continue
			}
			if f.RefBit.CompareAndSwap(true, false) {
				continue
			}
			eq.frames[idx] = nil
			eq.count--
			result = append(result, f)
		}
	}

	// Fallback: force-evict any unpinned frames.
	if len(result) < n {
		for i := 0; i < eq.capacity && len(result) < n; i++ {
			idx := (eq.hand + i) % eq.capacity
			f := eq.frames[idx]
			if f == nil || f.IsPinned() {
				continue
			}
			eq.frames[idx] = nil
			eq.count--
			eq.hand = (idx + 1) % eq.capacity
			result = append(result, f)
		}
	}

	return result
}

// evictByOwner finds and evicts a frame owned by the specified owner.
// Returns nil if no evictable frame with the given owner exists.
func (eq *evictionQueue) evictByOwner(owner FrameOwner) *Frame {
	if eq.count == 0 {
		return nil
	}

	for i := 0; i < 2*eq.capacity; i++ {
		idx := (eq.hand + i) % eq.capacity
		f := eq.frames[idx]
		if f == nil || f.IsPinned() || f.Owner != owner {
			if f != nil && !f.IsPinned() {
				f.RefBit.Store(false)
			}
			continue
		}
		if f.RefBit.CompareAndSwap(true, false) {
			continue
		}
		eq.frames[idx] = nil
		eq.count--
		eq.hand = (idx + 1) % eq.capacity

		return f
	}

	return nil
}

// len returns the number of frames currently in the eviction queue.
func (eq *evictionQueue) len() int {
	return eq.count
}

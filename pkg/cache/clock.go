package cache

import (
	"sync/atomic"
)

// clockEntry is a slot in the CLOCK circular buffer.
type clockEntry struct {
	key     string
	refBit  atomic.Uint32 // 0 or 1
	present bool          // true if slot is occupied
}

// clockBuffer implements the CLOCK page replacement algorithm.
// It provides O(1) amortized eviction.
type clockBuffer struct {
	slots    []clockEntry
	capacity int
	hand     int            // current clock hand position
	count    int            // number of occupied slots
	keyIndex map[string]int // key → slot index for O(1) lookup
}

// newClockBuffer creates a clock buffer with the given capacity.
func newClockBuffer(capacity int) *clockBuffer {
	if capacity < 1 {
		capacity = 1024
	}

	return &clockBuffer{
		slots:    make([]clockEntry, capacity),
		capacity: capacity,
		keyIndex: make(map[string]int, capacity),
	}
}

// access marks a key as recently accessed (sets refBit=1).
// Returns true if the key exists.
func (cb *clockBuffer) access(key string) bool {
	idx, ok := cb.keyIndex[key]
	if !ok {
		return false
	}
	cb.slots[idx].refBit.Store(1)

	return true
}

// insert adds a key to the clock buffer. If full, evicts one entry first.
// Returns the evicted key (empty if no eviction was needed).
func (cb *clockBuffer) insert(key string) string {
	// If already present, just mark accessed.
	if _, ok := cb.keyIndex[key]; ok {
		cb.access(key)

		return ""
	}

	var evictedKey string
	if cb.count >= cb.capacity {
		evictedKey = cb.evict()
	}

	// Find an empty slot.
	for i := 0; i < cb.capacity; i++ {
		idx := (cb.hand + i) % cb.capacity
		if !cb.slots[idx].present {
			cb.slots[idx] = clockEntry{key: key, present: true}
			cb.slots[idx].refBit.Store(1)
			cb.keyIndex[key] = idx
			cb.count++
			cb.hand = (idx + 1) % cb.capacity

			return evictedKey
		}
	}

	// Should not reach here if evict worked correctly.
	return evictedKey
}

// evict removes one entry using the CLOCK algorithm:
// Scan from hand: if refBit=1, set to 0 and move on.
// If refBit=0, evict that entry.
func (cb *clockBuffer) evict() string {
	for i := 0; i < 2*cb.capacity; i++ { // at most 2 full scans
		idx := cb.hand
		cb.hand = (cb.hand + 1) % cb.capacity

		if !cb.slots[idx].present {
			continue
		}

		if cb.slots[idx].refBit.Load() == 1 {
			cb.slots[idx].refBit.Store(0)

			continue
		}

		// refBit=0: evict this entry.
		evictedKey := cb.slots[idx].key
		cb.slots[idx].present = false
		cb.slots[idx].key = ""
		delete(cb.keyIndex, evictedKey)
		cb.count--

		return evictedKey
	}

	// Fallback: evict current hand position.
	idx := cb.hand
	cb.hand = (cb.hand + 1) % cb.capacity
	if cb.slots[idx].present {
		evictedKey := cb.slots[idx].key
		cb.slots[idx].present = false
		cb.slots[idx].key = ""
		delete(cb.keyIndex, evictedKey)
		cb.count--

		return evictedKey
	}

	return ""
}

// remove explicitly removes a key from the clock buffer.
func (cb *clockBuffer) remove(key string) bool {
	idx, ok := cb.keyIndex[key]
	if !ok {
		return false
	}
	cb.slots[idx].present = false
	cb.slots[idx].key = ""
	delete(cb.keyIndex, key)
	cb.count--

	return true
}

// clear removes all entries from the clock buffer.
func (cb *clockBuffer) clear() {
	for i := range cb.slots {
		cb.slots[i].present = false
		cb.slots[i].key = ""
	}
	cb.keyIndex = make(map[string]int, cb.capacity)
	cb.count = 0
	cb.hand = 0
}

package bufmgr

import (
	"hash/fnv"
)

// ResidencyIndex maps (owner, tag) → FrameID for O(1) hit-path lookup.
// Uses open-addressing with linear probing. No per-entry heap allocation.
//
// Thread-safe: external synchronization required (Manager.mu).
type ResidencyIndex interface {
	// Insert registers a frame in the index. Returns false if full.
	Insert(owner FrameOwner, tag string, id FrameID) bool

	// Lookup returns the FrameID for (owner, tag), or (0, false) if not found.
	Lookup(owner FrameOwner, tag string) (FrameID, bool)

	// Remove removes the entry. No-op if not present.
	Remove(owner FrameOwner, tag string)

	// Count returns the number of entries.
	Count() int
}

// residencyEntry is a slot in the open-addressing table.
type residencyEntry struct {
	occupied bool
	deleted  bool
	owner    FrameOwner
	tag      string
	frameID  FrameID
	hash     uint64
}

// openAddressIndex implements ResidencyIndex with open-addressing (linear probing).
type openAddressIndex struct {
	slots    []residencyEntry
	capacity int
	count    int
	mask     uint64 // capacity - 1 (capacity must be power of 2)
}

// NewResidencyIndex creates a residency index with the given capacity.
// Capacity is rounded up to the next power of 2 for efficient masking.
func NewResidencyIndex(capacity int) ResidencyIndex {
	if capacity < 16 {
		capacity = 16
	}
	// Round up to next power of 2.
	capacity = nextPow2(capacity)

	return &openAddressIndex{
		slots:    make([]residencyEntry, capacity),
		capacity: capacity,
		mask:     uint64(capacity - 1),
	}
}

func (idx *openAddressIndex) Insert(owner FrameOwner, tag string, id FrameID) bool {
	// Load factor check: refuse above 75%.
	if idx.count*4 >= idx.capacity*3 {
		return false
	}

	h := hashKey(owner, tag)
	pos := h & idx.mask

	for i := 0; i < idx.capacity; i++ {
		slot := &idx.slots[pos]
		if !slot.occupied || slot.deleted {
			slot.occupied = true
			slot.deleted = false
			slot.owner = owner
			slot.tag = tag
			slot.frameID = id
			slot.hash = h
			idx.count++

			return true
		}
		// Check for existing entry (upsert).
		if slot.hash == h && slot.owner == owner && slot.tag == tag {
			slot.frameID = id

			return true
		}
		pos = (pos + 1) & idx.mask
	}

	return false
}

func (idx *openAddressIndex) Lookup(owner FrameOwner, tag string) (FrameID, bool) {
	h := hashKey(owner, tag)
	pos := h & idx.mask

	for i := 0; i < idx.capacity; i++ {
		slot := &idx.slots[pos]
		if !slot.occupied {
			return 0, false
		}
		if !slot.deleted && slot.hash == h && slot.owner == owner && slot.tag == tag {
			return slot.frameID, true
		}
		pos = (pos + 1) & idx.mask
	}

	return 0, false
}

func (idx *openAddressIndex) Remove(owner FrameOwner, tag string) {
	h := hashKey(owner, tag)
	pos := h & idx.mask

	for i := 0; i < idx.capacity; i++ {
		slot := &idx.slots[pos]
		if !slot.occupied {
			return
		}
		if !slot.deleted && slot.hash == h && slot.owner == owner && slot.tag == tag {
			slot.deleted = true
			slot.tag = ""
			idx.count--

			return
		}
		pos = (pos + 1) & idx.mask
	}
}

func (idx *openAddressIndex) Count() int {
	return idx.count
}

// hashKey computes a hash for (owner, tag).
func hashKey(owner FrameOwner, tag string) uint64 {
	h := fnv.New64a()
	b := [1]byte{byte(owner)}
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(tag))

	return h.Sum64()
}

// nextPow2 returns the smallest power of 2 >= n.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++

	return n
}

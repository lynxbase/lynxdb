package vec

import (
	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

// ByteTable assigns dense ids, in insertion order, to distinct byte keys, so
// per-group state can live in caller-owned slices indexed by id; all of its
// memory comes from an arena and holds no pointers.
type ByteTable struct {
	arena  *mem.Arena
	slots  []uint32 // id + 1, 0 when empty
	hashes []uint64
	ends   []uint32
	data   []byte
}

// NewByteTable returns a table that allocates from a.
func NewByteTable(a *mem.Arena) *ByteTable {
	return &ByteTable{arena: a, slots: mem.Slice[uint32](a, 256, 256)}
}

// Clear removes every key and keeps the table's memory.
func (t *ByteTable) Clear() {
	clear(t.slots)
	t.hashes, t.ends, t.data = t.hashes[:0], t.ends[:0], t.data[:0]
}

// Hash returns the hash ByteTable uses for key.
func Hash(key []byte) uint64 { return xxhash.Sum64(key) }

// Len returns the number of distinct keys.
func (t *ByteTable) Len() int { return len(t.hashes) }

// Key returns the key with the given id.
func (t *ByteTable) Key(id int) []byte {
	start := uint32(0)
	if id > 0 {
		start = t.ends[id-1]
	}

	return t.data[start:t.ends[id]]
}

// KeyHash returns the hash of the key with the given id.
func (t *ByteTable) KeyHash(id int) uint64 { return t.hashes[id] }

// ID returns the id of key, adding it when absent.
func (t *ByteTable) ID(key []byte) int { return t.IDHashed(key, Hash(key)) }

// IDHashed is ID for a caller that already has Hash(key).
func (t *ByteTable) IDHashed(key []byte, h uint64) int {
	mask := uint64(len(t.slots) - 1)
	for i := h & mask; ; i = (i + 1) & mask {
		s := t.slots[i]
		if s == 0 {
			id := len(t.hashes)
			t.slots[i] = uint32(id) + 1
			t.hashes = append(mem.Grow(t.arena, t.hashes, 1), h)
			t.data = append(mem.Grow(t.arena, t.data, len(key)), key...)
			t.ends = append(mem.Grow(t.arena, t.ends, 1), uint32(len(t.data)))
			if 2*len(t.hashes) > len(t.slots) {
				t.rehash()
			}

			return id
		}
		if id := int(s - 1); t.hashes[id] == h && string(t.Key(id)) == string(key) {
			return id
		}
	}
}

func (t *ByteTable) rehash() {
	size := 4 * len(t.slots)
	t.slots = mem.Slice[uint32](t.arena, size, size)
	mask := uint64(size - 1)
	for id, h := range t.hashes {
		i := h & mask
		for t.slots[i] != 0 {
			i = (i + 1) & mask
		}
		t.slots[i] = uint32(id) + 1
	}
}

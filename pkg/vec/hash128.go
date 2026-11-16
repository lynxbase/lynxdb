package vec

import (
	"encoding/binary"
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

// Hash128 returns the 128-bit MurmurHash3 of key, the x64 variant with a
// zero seed.
func Hash128(key []byte) (lo, hi uint64) {
	const (
		c1 = 0x87c37b91114253d5
		c2 = 0x4cf5ad432745937f
	)
	var h1, h2 uint64
	p := key
	for len(p) >= 16 {
		k1 := binary.LittleEndian.Uint64(p)
		k2 := binary.LittleEndian.Uint64(p[8:])
		k1 = bits.RotateLeft64(k1*c1, 31) * c2
		h1 = (bits.RotateLeft64(h1^k1, 27)+h2)*5 + 0x52dce729
		k2 = bits.RotateLeft64(k2*c2, 33) * c1
		h2 = (bits.RotateLeft64(h2^k2, 31)+h1)*5 + 0x38495ab5
		p = p[16:]
	}
	if len(p) > 8 {
		var k2 uint64
		for i := len(p) - 1; i >= 8; i-- {
			k2 = k2<<8 | uint64(p[i])
		}
		h2 ^= bits.RotateLeft64(k2*c2, 33) * c1
	}
	if len(p) > 0 {
		var k1 uint64
		for i := min(len(p), 8) - 1; i >= 0; i-- {
			k1 = k1<<8 | uint64(p[i])
		}
		h1 ^= bits.RotateLeft64(k1*c1, 31) * c2
	}
	h1 ^= uint64(len(key))
	h2 ^= uint64(len(key))
	h1 += h2
	h2 += h1
	h1, h2 = Mix64(h1), Mix64(h2)
	h1 += h2
	h2 += h1

	return h1, h2
}

// Mix64 is a bijection that spreads every input bit over the whole word.
func Mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33

	return x
}

// HashSet128 is a set of 128-bit values that are already hashes: it keeps no
// keys, so two different keys with equal hashes count as one; all of its
// memory comes from an arena and holds no pointers.
type HashSet128 struct {
	arena *mem.Arena
	slots []uint64 // lo, hi pairs; both zero when empty
	n     int
	// zero records the all-zero value, which a slot cannot hold.
	zero bool
}

// NewHashSet128 returns an empty set that allocates from a.
func NewHashSet128(a *mem.Arena) *HashSet128 {
	return &HashSet128{arena: a, slots: mem.Slice[uint64](a, 2*256, 2*256)}
}

// Len returns the number of distinct values.
func (s *HashSet128) Len() int {
	if s.zero {
		return s.n + 1
	}

	return s.n
}

// Clear removes every value and keeps the set's memory.
func (s *HashSet128) Clear() {
	clear(s.slots)
	s.n, s.zero = 0, false
}

// slot128 folds both words into the probe start, so values that share one
// word still spread over the slots.
func slot128(lo, hi uint64) uint64 { return lo + hi*0x9e3779b97f4a7c15 }

// Add inserts the value and reports whether it was absent.
func (s *HashSet128) Add(lo, hi uint64) bool {
	if lo|hi == 0 {
		added := !s.zero
		s.zero = true

		return added
	}
	mask := uint64(len(s.slots)/2 - 1)
	for i := slot128(lo, hi) & mask; ; i = (i + 1) & mask {
		slot := s.slots[2*i : 2*i+2 : 2*i+2]
		if slot[0] == lo && slot[1] == hi {
			return false
		}
		if slot[0]|slot[1] == 0 {
			slot[0], slot[1] = lo, hi
			s.n++
			if 4*s.n > len(s.slots) {
				s.grow()
			}

			return true
		}
	}
}

// grow doubles the slots once half of them are taken.
func (s *HashSet128) grow() {
	old := s.slots
	s.slots = mem.Slice[uint64](s.arena, 2*len(old), 2*len(old))
	mask := uint64(len(s.slots)/2 - 1)
	for j := 0; j < len(old); j += 2 {
		lo, hi := old[j], old[j+1]
		if lo|hi == 0 {
			continue
		}
		i := slot128(lo, hi) & mask
		for s.slots[2*i]|s.slots[2*i+1] != 0 {
			i = (i + 1) & mask
		}
		s.slots[2*i], s.slots[2*i+1] = lo, hi
	}
}

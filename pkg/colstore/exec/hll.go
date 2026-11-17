package exec

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// A distinct-count state holds 64-bit hashes of values. Up to hllExact
// distinct hashes it is the set of them, an open-addressing table that grows
// from 60 bytes to 4092, and its count is exact. Beyond that it is a
// HyperLogLog sketch of 4096 six-bit registers in 3077 bytes, whose count has
// a standard error of 1.04/sqrt(4096), 1.6 percent. Both forms depend on the
// set of hashes alone, not on the order they came in or on how states were
// merged.
//
// The first byte tells the forms apart, and bytes 2 and 3 count the hashes
// of a set, whose slots of eight bytes follow from byte 4; a slot of zero is
// free.
const (
	hllBits      = 12
	hllRegisters = 1 << hllBits
	hllHeader    = 4
	hllDenseSize = hllHeader + hllRegisters*6/8 + 1
	hllFirstSize = 64 - hllHeader
	hllLastSize  = 4096 - hllHeader
	// hllExact is the number of hashes that the largest set holds, three
	// quarters of its slots.
	hllExact = (hllLastSize - hllHeader) / 8 * 3 / 4

	hllSet   = 0
	hllDense = 1
)

func hllSlots(b []byte) int { return (len(b) - hllHeader) / 8 }

// hllFind returns the slot of hash in a set, or the free slot it belongs in.
func hllFind(b []byte, hash uint64) (at int, found bool) {
	slots := hllSlots(b)
	i := int((hash >> 32) * uint64(slots) >> 32)
	for {
		at = hllHeader + 8*i
		switch binary.LittleEndian.Uint64(b[at:]) {
		case hash:
			return at, true
		case 0:
			return at, false
		}
		if i++; i == slots {
			i = 0
		}
	}
}

func hllRegister(b []byte, i int) (at int, shift uint) {
	return hllHeader + i*6/8, uint(i * 6 % 8)
}

func hllDenseAdd(b []byte, hash uint64) {
	// The low bit set bounds the rank at 53, which six bits hold.
	rank := uint16(bits.LeadingZeros64(hash<<hllBits|1<<(hllBits-1))) + 1
	at, shift := hllRegister(b, int(hash>>(64-hllBits)))
	w := binary.LittleEndian.Uint16(b[at:])
	if w>>shift&63 < rank {
		binary.LittleEndian.PutUint16(b[at:], w&^(63<<shift)|rank<<shift)
	}
}

// hllAdd adds a hash to the state at ref, which may move.
func hllAdd(h *stateHeap, ref stateRef, hash uint64) stateRef {
	if hash == 0 {
		hash = 1
	}
	b := h.get(ref)
	if len(b) == 0 {
		ref, b = h.alloc(hllFirstSize)
		clear(b)
	}
	if b[0] == hllDense {
		hllDenseAdd(b, hash)

		return ref
	}
	at, found := hllFind(b, hash)
	if found {
		return ref
	}
	n := int(binary.LittleEndian.Uint16(b[2:]))
	if n+1 > hllSlots(b)*3/4 {
		size, form := 2*len(b)+hllHeader, byte(hllSet)
		if len(b) == hllLastSize {
			size, form = hllDenseSize, hllDense
		}
		grown, nb := h.alloc(size)
		clear(nb)
		nb[0] = form
		for i := hllHeader; i < len(b); i += 8 {
			if v := binary.LittleEndian.Uint64(b[i:]); v != 0 {
				hllPut(nb, v)
			}
		}
		h.release(ref)
		if form == hllDense {
			hllDenseAdd(nb, hash)

			return grown
		}
		ref, b = grown, nb
		at, _ = hllFind(b, hash)
	}
	binary.LittleEndian.PutUint64(b[at:], hash)
	binary.LittleEndian.PutUint16(b[2:], uint16(n+1))

	return ref
}

// hllPut adds a hash to a state that has room for it.
func hllPut(b []byte, hash uint64) {
	if b[0] == hllDense {
		hllDenseAdd(b, hash)

		return
	}
	at, _ := hllFind(b, hash)
	binary.LittleEndian.PutUint64(b[at:], hash)
	binary.LittleEndian.PutUint16(b[2:], binary.LittleEndian.Uint16(b[2:])+1)
}

// hllMerge folds the state src into the state at ref, which may move.
func hllMerge(h *stateHeap, ref stateRef, src []byte) stateRef {
	b := h.get(ref)
	switch {
	case len(b) == 0:
		return h.set(ref, src)
	case src[0] == hllSet:
		for i := hllHeader; i < len(src); i += 8 {
			if v := binary.LittleEndian.Uint64(src[i:]); v != 0 {
				ref = hllAdd(h, ref, v)
			}
		}

		return ref
	case b[0] == hllSet:
		// The sketch takes the place of the set and then its hashes.
		grown, nb := h.alloc(hllDenseSize)
		copy(nb, src)
		for i := hllHeader; i < len(b); i += 8 {
			if v := binary.LittleEndian.Uint64(b[i:]); v != 0 {
				hllDenseAdd(nb, v)
			}
		}
		h.release(ref)

		return grown
	}
	for i := 0; i < hllRegisters; i++ {
		at, shift := hllRegister(b, i)
		w, s := binary.LittleEndian.Uint16(b[at:]), binary.LittleEndian.Uint16(src[at:])
		if rank := s >> shift & 63; w>>shift&63 < rank {
			binary.LittleEndian.PutUint16(b[at:], w&^(63<<shift)|rank<<shift)
		}
	}

	return ref
}

// hllCount returns the number of distinct hashes a state has seen, exact
// for a set and estimated for a sketch.
func hllCount(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	if b[0] == hllSet {
		return int64(binary.LittleEndian.Uint16(b[2:]))
	}
	// The estimator of Ertl, "New cardinality estimation algorithms for
	// HyperLogLog sketches", which needs no correction for small counts.
	const q = 64 - hllBits
	var hist [q + 2]int
	for i := 0; i < hllRegisters; i++ {
		at, shift := hllRegister(b, i)
		hist[binary.LittleEndian.Uint16(b[at:])>>shift&63]++
	}
	m := float64(hllRegisters)
	z := m * hllTau(1-float64(hist[q+1])/m)
	for k := q; k >= 1; k-- {
		z = 0.5 * (z + float64(hist[k]))
	}
	z += m * hllSigma(float64(hist[0])/m)

	return int64(math.Round(m * m / (2 * math.Ln2 * z)))
}

func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x *= x
		prev := z
		z += x * y
		y += y
		if prev == z {
			return z
		}
	}
}

func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		prev := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if prev == z {
			return z / 3
		}
	}
}

package part

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"slices"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

// valueSet gathers the distinct strings of a column and, once sealed,
// numbers them by their place in the order of their bytes.
type valueSet struct {
	data []byte
	ends []uint32
	hash []uint64
	// slots holds the number of a string plus one, zero when empty.
	slots []uint32
	// order holds the strings by place and rank the place of each.
	order, rank []uint32
	keys, spare []dictKey
	// lens counts the strings by length, the last for the longer ones.
	lens [maxDictStride + 2]uint32
	// added counts the strings given to add, repeats included.
	added uint64
}

// maxSetBytes bounds the buffers that a pooled valueSet keeps, and
// maxSetKeys the keys that a set keeps once it is sealed.
const (
	maxSetBytes = 64 << 20
	maxSetKeys  = 1 << 16
)

var valueSets = mem.Pool[valueSet]{Reset: func(v *valueSet) bool {
	v.reset()

	return v.retained() <= maxSetBytes
}}

func (v *valueSet) reset() {
	v.data, v.ends, v.hash, v.order, v.rank = v.data[:0], v.ends[:0], v.hash[:0], v.order[:0], v.rank[:0]
	clear(v.slots)
	v.lens, v.added = [maxDictStride + 2]uint32{}, 0
}

// retained returns the bytes of the buffers that v holds.
func (v *valueSet) retained() int {
	return cap(v.data) + 4*(cap(v.ends)+cap(v.slots)+cap(v.order)+cap(v.rank)) + 8*cap(v.hash) + 16*(cap(v.keys)+cap(v.spare))
}

// sortKeys orders keys by their heads, a byte of the heads at a time from
// the lowest, leaving out the bytes that all heads share; it returns the
// ordered keys and the other of the two buffers it moves them between.
func sortKeys(keys, spare []dictKey) (sorted, other []dictKey) {
	if len(keys) < 256 {
		slices.SortFunc(keys, func(a, b dictKey) int { return cmp.Compare(a.head, b.head) })

		return keys, spare
	}
	if cap(spare) < len(keys) {
		spare = make([]dictKey, len(keys))
	}
	spare = spare[:len(keys)]
	var counts [8][256]uint32
	for _, k := range keys {
		for b := range counts {
			counts[b][byte(k.head>>(8*b))]++
		}
	}
	for b := range counts {
		if counts[b][byte(keys[0].head>>(8*b))] == uint32(len(keys)) {
			continue
		}
		at := uint32(0)
		for c, n := range counts[b] {
			counts[b][c], at = at, at+n
		}
		for _, k := range keys {
			c := byte(k.head >> (8 * b))
			spare[counts[b][c]] = k
			counts[b][c]++
		}
		keys, spare = spare, keys
	}

	return keys, spare
}

// len returns the number of distinct strings.
func (v *valueSet) len() int { return len(v.ends) }

func (v *valueSet) start(i uint32) uint32 {
	if i == 0 {
		return 0
	}

	return v.ends[i-1]
}

// add takes val and reports whether the set has room for it: its strings
// take less than 4 GiB together.
func (v *valueSet) add(val []byte) bool {
	_, ok := v.number(val)

	return ok
}

// number takes val as add does and returns its number among the strings in
// the order they were first given, which place turns into its place in the
// order of bytes once the set is sealed.
func (v *valueSet) number(val []byte) (uint32, bool) {
	v.added++
	if 2*(len(v.ends)+1) > len(v.slots) {
		v.widen()
	}
	h := xxhash.Sum64(val)
	mask := uint64(len(v.slots) - 1)
	slot := h & mask
	for {
		e := v.slots[slot]
		if e == 0 {
			break
		}
		if v.hash[e-1] == h && string(v.data[v.start(e-1):v.ends[e-1]]) == string(val) {
			return e - 1, true
		}
		slot = (slot + 1) & mask
	}
	if len(v.data)+len(val) > math.MaxUint32 {
		return 0, false
	}
	v.data = append(v.data, val...)
	v.ends = append(v.ends, uint32(len(v.data)))
	v.hash = append(v.hash, h)
	v.slots[slot] = uint32(len(v.ends))
	v.lens[min(len(val), len(v.lens)-1)]++

	return uint32(len(v.ends)) - 1, true
}

// place returns the place in the order of bytes of the string with a
// number that number returned.
func (v *valueSet) place(number uint32) uint32 { return v.rank[number] }

// reserve makes room for n strings of size bytes together, so that adding
// that many moves nothing.
func (v *valueSet) reserve(n, size int) {
	if cap(v.data) < size {
		v.data = append(make([]byte, 0, size), v.data...)
	}
	if cap(v.ends) < n {
		v.ends = append(make([]uint32, 0, n), v.ends...)
		v.hash = append(make([]uint64, 0, n), v.hash...)
	}
	slots := max(len(v.slots), 1024)
	for slots < 2*n {
		slots *= 2
	}
	if slots > len(v.slots) {
		v.widenTo(slots)
	}
}

// widen doubles the slots.
func (v *valueSet) widen() { v.widenTo(max(2*len(v.slots), 1024)) }

// widenTo makes the slots size of them, a power of two.
func (v *valueSet) widenTo(size int) {
	if cap(v.slots) < size {
		v.slots = make([]uint32, size)
	} else {
		v.slots = v.slots[:size]
		clear(v.slots)
	}
	mask := uint64(size - 1)
	for i, h := range v.hash {
		slot := h & mask
		for v.slots[slot] != 0 {
			slot = (slot + 1) & mask
		}
		v.slots[slot] = uint32(i) + 1
	}
}

// stride returns the most common length of the strings and how many have
// it; lengths above maxDictStride count as one.
func (v *valueSet) stride() (length int, count uint32) {
	for l, n := range v.lens[:maxDictStride+1] {
		if n > count {
			length, count = l, n
		}
	}

	return length, count
}

// seal orders the strings by their bytes; add may not follow it.
func (v *valueSet) seal() {
	n := len(v.ends)
	v.order, v.rank = grow(v.order, n), grow(v.rank, n)
	if n == 0 {
		return
	}
	value := func(i uint32) []byte { return v.data[v.start(i):v.ends[i]] }
	// The strings are told apart by the eight bytes after those that all of
	// them begin with, as a number, and by the rest where those are equal.
	shared := value(0)
	for i := 1; i < n && len(shared) > 0; i++ {
		s := value(uint32(i))
		k := 0
		for k < len(shared) && k < len(s) && s[k] == shared[k] {
			k++
		}
		shared = shared[:k]
	}
	v.keys = v.keys[:0]
	for i := 0; i < n; i++ {
		var head [8]byte
		copy(head[:], value(uint32(i))[len(shared):])
		v.keys = append(v.keys, dictKey{binary.BigEndian.Uint64(head[:]), uint32(i)})
	}
	v.keys, v.spare = sortKeys(v.keys, v.spare)
	// Strings that agree in those eight bytes are few among identifiers.
	for from := 0; from < n; {
		to := from + 1
		for to < n && v.keys[to].head == v.keys[from].head {
			to++
		}
		if to-from > 1 {
			slices.SortFunc(v.keys[from:to], func(a, b dictKey) int { return bytes.Compare(value(a.entry), value(b.entry)) })
		}
		from = to
	}
	for place, k := range v.keys {
		v.order[place], v.rank[k.entry] = k.entry, uint32(place)
	}
	// The keys of a large set are as large again as its strings.
	if cap(v.keys) > maxSetKeys {
		v.keys, v.spare = nil, nil
	}
}

// numbered returns the string with a number that number returned.
func (v *valueSet) numbered(number uint32) []byte { return v.data[v.start(number):v.ends[number]] }

// value returns the string at place in the order of bytes.
func (v *valueSet) value(place int) []byte {
	i := v.order[place]

	return v.data[v.start(i):v.ends[i]]
}

// find returns the place of val in the order of bytes.
func (v *valueSet) find(val []byte) (place uint32, ok bool) {
	if len(v.slots) == 0 {
		return 0, false
	}
	h := xxhash.Sum64(val)
	mask := uint64(len(v.slots) - 1)
	for slot := h & mask; ; slot = (slot + 1) & mask {
		e := v.slots[slot]
		if e == 0 {
			return 0, false
		}
		if v.hash[e-1] == h && string(v.data[v.start(e-1):v.ends[e-1]]) == string(val) {
			return v.rank[e-1], true
		}
	}
}

// setBudget is the memory that the sets of strings of all merges under way
// may hold together, and setHeld what they hold; a merge that finds the
// budget spent leaves its column in the forms of single blocks.
var setBudget, setHeld atomic.Int64

func init() { setBudget.Store(1 << 30) }

// SetDictionaryMemory sets the memory that the merges of the process may
// hold together to gather the strings of columns whose blocks do not store
// them as codes of a part dictionary yet; 1 GiB unless set. Columns that
// every part being merged stores as codes are merged without it.
func SetDictionaryMemory(bytes int64) { setBudget.Store(bytes) }

// reserve takes n bytes of the budget and reports whether it had them.
func reserve(n int64) bool {
	if setHeld.Add(n) > setBudget.Load() {
		setHeld.Add(-n)

		return false
	}

	return true
}

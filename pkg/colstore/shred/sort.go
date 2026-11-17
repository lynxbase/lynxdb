package shred

import (
	"cmp"
	"math/bits"
	"slices"

	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// sortScratch holds the buffers SortBy reuses between batches.
type sortScratch struct {
	buckets []int64

	perm   []uint32
	inv    []uint32
	packed []uint64
	spare  []uint64
	ranks  [][]uint32
	slot   []uint32
	order  []uint32
	rows   []uint32
	ints   []int64
	floats []float64
	bools  []uint8
	ends   []uint32
	data   []byte
	times  []int64
	ids    []uint32
}

// sortPool shares the buffers of sorting between batches: a batch needs
// them only while it is being sorted, and they are as large as its largest
// column.
var sortPool mem.Pool[sortScratch]

// SortBy reorders the rows of a complete batch by their values at the key
// paths, then by timestamp, keeping the arrival order of equal rows.
//
// At one path a missing value sorts first, then values in kind order (null,
// bool, int, float, string, variant) and by value within a kind.
func (b *Batch) SortBy(keys []string) {
	b.sort = sortPool.Get()
	b.sortBy(nil, keys)
	sortPool.Put(b.sort)
	b.sort = nil
}

// TimeBucket is a run of adjacent rows whose timestamps share one bucket.
type TimeBucket struct {
	ID   int64 // timestamp divided by the bucket width, rounded down
	Rows int
}

// SortByTimeBuckets reorders the rows by the bucket of width nanoseconds
// that their timestamp falls in and, inside a bucket, as SortBy does; it
// appends the buckets, in row order, to dst.
func (b *Batch) SortByTimeBuckets(dst []TimeBucket, width int64, keys []string) []TimeBucket {
	if b.Rows == 0 {
		return dst
	}
	b.sort = sortPool.Get()
	defer func() {
		sortPool.Put(b.sort)
		b.sort = nil
	}()
	s := b.sort
	s.buckets = s.buckets[:0]
	lo, hi := floorDiv(b.Times[0], width), floorDiv(b.Times[0], width)
	for _, t := range b.Times {
		id := floorDiv(t, width)
		lo, hi = min(lo, id), max(hi, id)
		s.buckets = append(s.buckets, id)
	}
	if lo == hi {
		b.sortBy(nil, keys)

		return append(dst, TimeBucket{ID: lo, Rows: b.Rows})
	}
	b.sortBy(s.buckets, keys)
	// The sort moved the timestamps, so the buckets are read from them.
	for from := 0; from < b.Rows; {
		id := floorDiv(b.Times[from], width)
		to := from + 1
		for to < b.Rows && floorDiv(b.Times[to], width) == id {
			to++
		}
		dst = append(dst, TimeBucket{ID: id, Rows: to - from})
		from = to
	}

	return dst
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}

// sortBy orders the rows by buckets, when given, then keys, then time.
func (b *Batch) sortBy(buckets []int64, keys []string) {
	n := b.Rows
	if n < 2 {
		return
	}
	s := b.sort
	for len(s.ranks) < len(keys) {
		s.ranks = append(s.ranks, nil)
	}
	ranks := s.ranks[:len(keys)]
	for k, path := range keys {
		ranks[k] = b.rank(path, ranks[k])
	}
	s.perm = s.perm[:0]
	for i := 0; i < n; i++ {
		s.perm = append(s.perm, uint32(i))
	}
	times := b.Times
	if !b.sortPacked(buckets, ranks) {
		slices.SortFunc(s.perm, func(x, y uint32) int {
			if buckets != nil && buckets[x] != buckets[y] {
				return cmp.Compare(buckets[x], buckets[y])
			}
			for _, r := range ranks {
				if r[x] != r[y] {
					return cmp.Compare(r[x], r[y])
				}
			}
			if times[x] != times[y] {
				return cmp.Compare(times[x], times[y])
			}

			return cmp.Compare(x, y)
		})
	}
	sorted := true
	for i, p := range s.perm {
		if p != uint32(i) {
			sorted = false

			break
		}
	}
	if sorted {
		return
	}
	b.permute()
}

// digitBits is the width of the digits that sortPacked sorts by.
const digitBits = 11

// sortPacked orders perm, which lists the rows as they are, by the bucket
// and the ranks of a row packed into one number, sorted digit by digit,
// then by time. It reports false, with perm untouched, when a number does
// not hold them.
func (b *Batch) sortPacked(buckets []int64, ranks [][]uint32) bool {
	s := b.sort
	n := b.Rows
	var widths [8]uint
	if len(ranks) > len(widths) {
		return false
	}
	total, lo := uint(0), int64(0)
	if buckets != nil {
		hi := buckets[0]
		lo = hi
		for _, v := range buckets {
			lo, hi = min(lo, v), max(hi, v)
		}
		total = uint(bits.Len64(uint64(hi) - uint64(lo)))
	}
	for k, r := range ranks {
		widths[k] = uint(bits.Len32(slices.Max(r)))
		total += widths[k]
	}
	if total > 64 {
		return false
	}
	if cap(s.packed) < n {
		s.packed, s.spare = make([]uint64, n), make([]uint64, n)
	}
	if cap(s.inv) < n {
		s.inv = make([]uint32, n)
	}
	keys, perm := s.packed[:n], s.perm
	for i := range keys {
		key := uint64(0)
		if buckets != nil {
			key = uint64(buckets[i]) - uint64(lo)
		}
		for k, r := range ranks {
			key = key<<widths[k] | uint64(r[i])
		}
		keys[i] = key
	}
	// Each pass keeps the order of rows whose digits are equal, so the
	// last leaves rows of one number in the order they came in.
	otherKeys, otherPerm := s.spare[:n], s.inv[:n]
	var counts [1 << digitBits]uint32
	for shift := uint(0); shift < total; shift += digitBits {
		clear(counts[:])
		for _, key := range keys {
			counts[key>>shift&(1<<digitBits-1)]++
		}
		if counts[keys[0]>>shift&(1<<digitBits-1)] == uint32(n) {
			continue
		}
		at := uint32(0)
		for d, c := range counts {
			counts[d], at = at, at+c
		}
		for i, key := range keys {
			d := key >> shift & (1<<digitBits - 1)
			otherKeys[counts[d]], otherPerm[counts[d]] = key, perm[i]
			counts[d]++
		}
		keys, otherKeys, perm, otherPerm = otherKeys, keys, otherPerm, perm
	}
	s.packed, s.spare, s.perm, s.inv = keys, otherKeys, perm, otherPerm
	times := b.Times
	for from := 0; from < n; {
		to, ordered := from+1, true
		for ; to < n && keys[to] == keys[from]; to++ {
			ordered = ordered && times[perm[to]] >= times[perm[to-1]]
		}
		if !ordered {
			slices.SortFunc(perm[from:to], func(x, y uint32) int {
				if times[x] != times[y] {
					return cmp.Compare(times[x], times[y])
				}

				return cmp.Compare(x, y)
			})
		}
		from = to
	}

	return true
}

// rank assigns every row a number that orders it by its value at path;
// rows with equal values share a number and rows without a value get 0.
func (b *Batch) rank(path string, dst []uint32) []uint32 {
	n := b.Rows
	if cap(dst) < n {
		dst = make([]uint32, n)
	}
	dst = dst[:n]
	clear(dst)
	pc := b.paths[path]
	if pc == nil {
		return dst
	}
	s := b.sort
	base := uint32(1)
	for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
		c := pc.byKind[kind]
		if c == nil || c.Count == 0 {
			continue
		}
		switch kind {
		case vec.KindNull:
			for i := 0; i < c.Count; i++ {
				dst[c.Row(i)] = base
			}
			base++
		case vec.KindBool:
			for i, v := range c.Bools {
				dst[c.Row(i)] = base + uint32(v)
			}
			base += 2
		case vec.KindInt:
			s.ints = append(s.ints[:0], c.Ints...)
			slices.Sort(s.ints)
			s.ints = slices.Compact(s.ints)
			for i, v := range c.Ints {
				at, _ := slices.BinarySearch(s.ints, v)
				dst[c.Row(i)] = base + uint32(at)
			}
			base += uint32(len(s.ints))
		case vec.KindFloat:
			s.floats = append(s.floats[:0], c.Floats...)
			slices.SortFunc(s.floats, cmp.Compare[float64])
			s.floats = slices.CompactFunc(s.floats, func(x, y float64) bool { return cmp.Compare(x, y) == 0 })
			for i, v := range c.Floats {
				at, _ := slices.BinarySearchFunc(s.floats, v, cmp.Compare[float64])
				dst[c.Row(i)] = base + uint32(at)
			}
			base += uint32(len(s.floats))
		default:
			base += b.rankBytes(c, dst, base)
		}
	}

	return dst
}

// rankBytes ranks the values of a string or variant column in byte order
// and returns the number of distinct values.
func (b *Batch) rankBytes(c *Column, dst []uint32, base uint32) uint32 {
	s := b.sort
	arena := mem.NewArena()
	defer arena.Free()
	table := vec.NewByteTable(arena)
	s.ids = s.ids[:0]
	for i := 0; i < c.Count; i++ {
		s.ids = append(s.ids, uint32(table.ID(c.Bytes(i))))
	}
	distinct := table.Len()
	s.order = s.order[:0]
	for i := 0; i < distinct; i++ {
		s.order = append(s.order, uint32(i))
	}
	slices.SortFunc(s.order, func(x, y uint32) int {
		return cmp.Compare(string(table.Key(int(x))), string(table.Key(int(y))))
	})
	if cap(s.slot) < distinct {
		s.slot = make([]uint32, distinct)
	}
	rank := s.slot[:distinct]
	for pos, id := range s.order {
		rank[id] = uint32(pos)
	}
	for i, id := range s.ids {
		dst[c.Row(i)] = base + rank[id]
	}

	return uint32(distinct)
}

// permute moves every row perm[i] to position i.
func (b *Batch) permute() {
	s := b.sort
	n := b.Rows
	if cap(s.inv) < n {
		s.inv = make([]uint32, n)
	}
	s.inv = s.inv[:n]
	for at, row := range s.perm {
		s.inv[row] = uint32(at)
	}
	s.times = append(s.times[:0], b.Times...)
	for at, row := range s.perm {
		b.Times[at] = s.times[row]
	}
	if cap(s.slot) < n {
		s.slot = make([]uint32, n)
	}
	for _, c := range b.cols {
		if c.Count == 0 {
			continue
		}
		// order lists the column's values by their row after the move.
		s.order = s.order[:0]
		if !c.Sparse && c.Count == n {
			s.order = append(s.order, s.perm...)
		} else {
			slot := s.slot[:n]
			clear(slot)
			for i := 0; i < c.Count; i++ {
				slot[s.inv[c.Row(i)]] = uint32(i) + 1
			}
			s.rows = s.rows[:0]
			for at, v := range slot {
				if v != 0 {
					s.order = append(s.order, v-1)
					s.rows = append(s.rows, uint32(at))
				}
			}
			c.Sparse = true
			c.RowIDs = append(c.RowIDs[:0], s.rows...)
		}
		c.lastRow = -1
		// The values are gathered in scratch and copied back, so that every
		// column keeps a buffer of its own size from batch to batch.
		switch c.Kind {
		case vec.KindBool:
			s.bools = s.bools[:0]
			for _, i := range s.order {
				s.bools = append(s.bools, c.Bools[i])
			}
			copy(c.Bools, s.bools)
		case vec.KindInt:
			s.ints = s.ints[:0]
			for _, i := range s.order {
				s.ints = append(s.ints, c.Ints[i])
			}
			copy(c.Ints, s.ints)
		case vec.KindFloat:
			s.floats = s.floats[:0]
			for _, i := range s.order {
				s.floats = append(s.floats, c.Floats[i])
			}
			copy(c.Floats, s.floats)
		case vec.KindString, vec.KindVariant:
			s.data, s.ends = s.data[:0], s.ends[:0]
			for _, i := range s.order {
				s.data = append(s.data, c.Bytes(int(i))...)
				s.ends = append(s.ends, uint32(len(s.data)))
			}
			copy(c.Data, s.data)
			copy(c.Ends, s.ends)
		}
	}
}

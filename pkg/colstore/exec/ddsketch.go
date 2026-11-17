package exec

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// A percentile state is a store of a DDSketch (Masson, Rim and Lee, "DDSketch:
// a fast and fully-mergeable quantile sketch with relative-error
// guarantees"): counts of the values per bucket, bucket k holding the values
// in (gamma^(k-1), gamma^k]. A group has one store for its positive values,
// one for the magnitudes of its negative ones and a count of its zeros.
//
// What it promises: the value reported for a quantile is within ddAlpha, one
// percent, of the value that an exact computation would report, whatever the
// distribution, and merging stores adds their counts, so the answer does not
// depend on how the values were split among workers or in which order states
// were merged. A t-digest promises neither: its error has no bound in the
// value, only one in rank that is observed rather than proven, and its
// centroids depend on the order of insertion and merging.
//
// What it costs: a store is a window of counts over the buckets between the
// least and the greatest value seen, 115 buckets per factor of ten, each
// count one byte wide until a bucket passes 255, then two, four and eight. A
// store never has more than ddMaxBuckets buckets, which span a factor of
// 6e17: values further below the greatest one are counted in the lowest
// bucket kept, where the promise then holds no longer. That bounds a store
// at 16 KB and makes it a few hundred bytes for most groups.
//
// The layout: the width of a count in byte 0, whether low buckets have been
// folded in byte 1, the number of buckets in bytes 2 and 3, the lowest bucket
// in bytes 4 to 7, then the counts.
//
// While a group has no more than ddExactMax distinct numbers other than zero
// it keeps those instead, each with its count, and its percentiles are exact:
// the statuses or the sizes of a small domain come back as they are. The
// list takes the place of the store of positive values, with a zero in byte
// 0, the number of entries in bytes 2 and 3, and from byte 4 on the entries
// in ascending order, a float and a count of eight bytes each. Which form a
// group ends in depends on its numbers alone.
const (
	ddAlpha      = 0.01
	ddMaxBuckets = 2048
	ddHeader     = 8
	// ddExactMax entries fill a block of 1 KB.
	ddExactMax    = 63
	ddExact       = 0
	ddExactHeader = 4
)

// sketchWords are the words of a group that hold a percentile sketch: the
// number of zeros and the refs of the two stores.
type sketchWords struct{ zeros, pos, neg int }

func ddEntry(b []byte, i int) (x float64, count uint64) {
	at := ddExactHeader + 16*i

	return math.Float64frombits(binary.LittleEndian.Uint64(b[at:])), binary.LittleEndian.Uint64(b[at+8:])
}

func ddPutEntry(b []byte, i int, x float64, count uint64) {
	at := ddExactHeader + 16*i
	binary.LittleEndian.PutUint64(b[at:], math.Float64bits(x))
	binary.LittleEndian.PutUint64(b[at+8:], count)
}

// add counts count values of x, which is neither zero nor NaN, in the
// sketch of the group whose words are w.
func (s sketchWords) add(h *stateHeap, w []int64, x float64, count uint64) {
	ref := stateRef(w[s.pos])
	b := h.get(ref)
	if len(b) == 0 && w[s.neg] != 0 || len(b) != 0 && b[0] != ddExact {
		if x < 0 || !ddBump(b, ddKey(x), count) {
			s.bucket(h, w, x, count)
		}

		return
	}
	n := 0
	if len(b) != 0 {
		n = int(binary.LittleEndian.Uint16(b[2:]))
	}
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		if v, _ := ddEntry(b, mid); v < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < n {
		if v, c := ddEntry(b, lo); v == x {
			ddPutEntry(b, lo, x, c+count)

			return
		}
	}
	if n == ddExactMax {
		s.toBuckets(h, w)
		s.bucket(h, w, x, count)

		return
	}
	ref, b = h.resize(ref, ddExactHeader+16*(n+1))
	w[s.pos] = int64(ref)
	at := ddExactHeader + 16*lo
	copy(b[at+16:], b[at:ddExactHeader+16*n])
	b[0], b[1] = ddExact, 0
	binary.LittleEndian.PutUint16(b[2:], uint16(n+1))
	ddPutEntry(b, lo, x, count)
}

// bucket counts count values of x in the stores of a sketch.
func (s sketchWords) bucket(h *stateHeap, w []int64, x float64, count uint64) {
	if x > 0 {
		w[s.pos] = int64(ddAdd(h, stateRef(w[s.pos]), ddKey(x), count))
	} else {
		w[s.neg] = int64(ddAdd(h, stateRef(w[s.neg]), ddKey(-x), count))
	}
}

// toBuckets turns the list of a sketch into its stores.
func (s sketchWords) toBuckets(h *stateHeap, w []int64) {
	ref := stateRef(w[s.pos])
	b := h.get(ref)
	if len(b) == 0 || b[0] != ddExact {
		return
	}
	w[s.pos] = 0
	for i, n := 0, int(binary.LittleEndian.Uint16(b[2:])); i < n; i++ {
		x, count := ddEntry(b, i)
		s.bucket(h, w, x, count)
	}
	h.release(ref)
}

// merge folds a state of another group's sketch, the list or the store of
// positive values when positive is set and the store of negative ones
// otherwise, into the sketch of the group whose words are w.
func (s sketchWords) merge(h *stateHeap, w []int64, src []byte, positive bool) {
	if positive && src[0] == ddExact {
		for i, n := 0, int(binary.LittleEndian.Uint16(src[2:])); i < n; i++ {
			x, count := ddEntry(src, i)
			s.add(h, w, x, count)
		}

		return
	}
	s.toBuckets(h, w)
	at := s.neg
	if positive {
		at = s.pos
	}
	if w[at] == 0 {
		w[at] = int64(h.set(0, src))
	} else {
		w[at] = int64(ddMerge(h, stateRef(w[at]), src))
	}
}

var (
	ddGamma       = (1 + ddAlpha) / (1 - ddAlpha)
	ddInvLogGamma = 1 / math.Log(ddGamma)
)

// ddKey returns the bucket of a positive value.
func ddKey(x float64) int32 {
	if math.IsInf(x, 1) {
		x = math.MaxFloat64
	}

	return int32(math.Ceil(math.Log(x) * ddInvLogGamma))
}

// ddValue returns the value that stands for a bucket, within ddAlpha of
// every value in it.
func ddValue(k int32) float64 { return 2 * math.Pow(ddGamma, float64(k)) / (ddGamma + 1) }

func ddCount(b []byte, width, i int) uint64 {
	at := ddHeader + i*width
	switch width {
	case 1:
		return uint64(b[at])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b[at:]))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b[at:]))
	}

	return binary.LittleEndian.Uint64(b[at:])
}

func ddPut(b []byte, width, i int, v uint64) {
	at := ddHeader + i*width
	switch width {
	case 1:
		b[at] = byte(v)
	case 2:
		binary.LittleEndian.PutUint16(b[at:], uint16(v))
	case 4:
		binary.LittleEndian.PutUint32(b[at:], uint32(v))
	default:
		binary.LittleEndian.PutUint64(b[at:], v)
	}
}

func ddShape(b []byte) (width, n int, lo int32) {
	return int(b[0]), int(binary.LittleEndian.Uint16(b[2:])), int32(binary.LittleEndian.Uint32(b[4:]))
}

// ddAdd counts count values of bucket key in the store at ref, which may
// move.
func ddAdd(h *stateHeap, ref stateRef, key int32, count uint64) stateRef {
	b := h.get(ref)
	if ddBump(b, key, count) {
		return ref
	}

	return ddGrow(h, ref, b, key, count)
}

// ddBump counts count values of bucket key in the store b when its window
// holds the bucket and the count has room, and reports whether it did.
func ddBump(b []byte, key int32, count uint64) bool {
	if len(b) == 0 {
		return false
	}
	width, n, lo := ddShape(b)
	i := int(key) - int(lo)
	if i < 0 && b[1] != 0 {
		i = 0
	}
	if i < 0 || i >= n {
		return false
	}
	v := ddCount(b, width, i) + count
	if width != 8 && v>>(8*width) != 0 {
		return false
	}
	ddPut(b, width, i, v)

	return true
}

// ddGrow rebuilds a store with the window and the width that counting count
// more values of bucket key takes.
func ddGrow(h *stateHeap, ref stateRef, b []byte, key int32, count uint64) stateRef {
	lo, hi := key, key
	width, n, oldLo := 1, 0, int32(0)
	var most, sum uint64
	folded := false
	if len(b) != 0 {
		width, n, oldLo = ddShape(b)
		folded = b[1] != 0
		for i := 0; i < n; i++ {
			if c := ddCount(b, width, i); c != 0 {
				lo, hi = min(lo, oldLo+int32(i)), max(hi, oldLo+int32(i))
				most, sum = max(most, c), sum+c
			}
		}
	}
	// The buckets kept are the highest ones, so that what is folded does not
	// depend on the order the values came in.
	if hi-lo >= ddMaxBuckets {
		lo, folded, most = hi-ddMaxBuckets+1, true, sum
	}
	span := int(hi-lo) + 1
	for most += count; width < 8 && most>>(8*width) != 0; {
		width *= 2
	}
	// The window fills the block it is in, with the spare buckets on the
	// side that grew.
	block := 1 << bits.Len(uint(ddHeader+span*width+heapHeader-1))
	room := min((max(block, 32)-heapHeader-ddHeader)/width, ddMaxBuckets)
	switch {
	case len(b) == 0:
		lo -= int32(room-span) / 2
	case key < oldLo:
		lo -= int32(room - span)
	}
	grown, nb := h.alloc(ddHeader + room*width)
	clear(nb)
	nb[0] = byte(width)
	if folded {
		nb[1] = 1
	}
	binary.LittleEndian.PutUint16(nb[2:], uint16(room))
	binary.LittleEndian.PutUint32(nb[4:], uint32(lo))
	if len(b) != 0 {
		oldWidth := int(b[0])
		for i := 0; i < n; i++ {
			if c := ddCount(b, oldWidth, i); c != 0 {
				at := int(max(oldLo+int32(i), lo) - lo)
				ddPut(nb, width, at, ddCount(nb, width, at)+c)
			}
		}
		h.release(ref)
	}
	at := int(max(key, lo) - lo)
	ddPut(nb, width, at, ddCount(nb, width, at)+count)

	return grown
}

// ddMerge adds the counts of the store src to the store at ref, which may
// move.
func ddMerge(h *stateHeap, ref stateRef, src []byte) stateRef {
	width, n, lo := ddShape(src)
	// The highest bucket goes first, which sets the window before the rest.
	for i := n - 1; i >= 0; i-- {
		if c := ddCount(src, width, i); c != 0 {
			ref = ddAdd(h, ref, lo+int32(i), c)
		}
	}

	return ref
}

func ddTotal(b []byte) (sum uint64) {
	if len(b) == 0 {
		return 0
	}
	width, n, _ := ddShape(b)
	for i := 0; i < n; i++ {
		sum += ddCount(b, width, i)
	}

	return sum
}

// ddQuantile returns the value at quantile q, from 0 to 1, of the values
// counted in a store of negative magnitudes, a number of zeros and a store
// of positive values: the one that has floor(q*n) of the n values before
// it, the greatest for a q of 1. It reports false when there is no value.
func ddQuantile(neg, pos []byte, zeros uint64, q float64) (float64, bool) {
	if len(pos) != 0 && pos[0] == ddExact {
		return ddExactQuantile(pos, zeros, q), true
	}
	total := ddTotal(neg) + zeros + ddTotal(pos)
	if total == 0 {
		return 0, false
	}
	rank := min(uint64(q*float64(total)), total-1)
	var seen uint64
	if len(neg) != 0 {
		width, n, lo := ddShape(neg)
		for i := n - 1; i >= 0; i-- {
			if seen += ddCount(neg, width, i); seen > rank {
				return -ddValue(lo + int32(i)), true
			}
		}
	}
	if seen += zeros; seen > rank {
		return 0, true
	}
	width, n, lo := ddShape(pos)
	for i := 0; i < n; i++ {
		if seen += ddCount(pos, width, i); seen > rank {
			return ddValue(lo + int32(i)), true
		}
	}

	return ddValue(lo + int32(n) - 1), true
}

// ddExactQuantile is ddQuantile for a list of numbers and a number of zeros.
func ddExactQuantile(b []byte, zeros uint64, q float64) float64 {
	n := int(binary.LittleEndian.Uint16(b[2:]))
	total := zeros
	for i := 0; i < n; i++ {
		_, count := ddEntry(b, i)
		total += count
	}
	rank := min(uint64(q*float64(total)), total-1)
	var seen uint64
	for i := 0; i < n; i++ {
		x, count := ddEntry(b, i)
		if x > 0 && zeros != 0 {
			if seen += zeros; seen > rank {
				return 0
			}
			zeros = 0
		}
		if seen += count; seen > rank {
			return x
		}
	}

	return 0
}

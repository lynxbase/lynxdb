package tokenfilter

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// A filter keeps the hashes of its tokens but for their low bits. A filter
// of cells cells, cells a power of two, gives each cell the hashes whose
// high bits name it, and a cell keeps them as a sorted set of numbers below
// a bound, coded after Elias and Fano: a probe of an absent hash meets one
// of the n numbers about n times in the bound. The cells of a filter have
// one size, which each fills by taking the largest bound it can, so that a
// cell with more hashes than others answers a little less precisely.
//
//	count  16 bits, the number of hashes, or cellFull for a cell that holds
//	       too many to tell any apart and that every probe passes
//	high   for the i-th number v, a one at bit v>>low + i, and zeros
//	       elsewhere: count ones and buckets zeros
//	low    the low bits of every number, low bits each
const (
	// BlockBytes is the unit of the size of a cell.
	BlockBytes = 8

	cellCount = 16
	cellFull  = 1<<cellCount - 1
	// maxCellLow bounds the low bits of a number so that one load reads
	// any of them.
	maxCellLow = 32
)

// Target false positive rates. A false answer costs the decoding of what a
// filter guards: the small columns that share one filter are cheap to
// decode, a column with a filter of its own is large.
const (
	SharedFalse = 1.0 / 200
	ColumnFalse = 1.0 / 800
)

// cellShape returns the low bits and buckets of a cell of size bits that
// holds n numbers, those of the largest bound buckets<<low that fits.
func cellShape(n, size int) (low uint, buckets uint64, ok bool) {
	room := size - cellCount - n
	if n >= cellFull || room < 1 {
		return 0, 0, false
	}
	// A bound twice as large costs n bits of buckets.
	l := 0
	if n > 0 {
		l = min(max(room/n-1, 0), maxCellLow)
	}

	return uint(l), uint64(room - n*l), true
}

// cellFalse returns the false positive rate of a cell of size bits that
// holds n numbers.
func cellFalse(n, size int) float64 {
	low, buckets, ok := cellShape(n, size)
	if !ok {
		return 1
	}

	return float64(n) / float64(buckets<<low)
}

// maxCellTokens is the most hashes a cell gets on average, which bounds the
// bits a probe scans.
const maxCellTokens = 128

// Cells returns the cells of a filter of tokens hashes that stripes cut:
// stripes, doubled until a cell gets at most maxCellTokens on average.
func Cells(tokens, stripes int) int {
	cells := stripes
	for tokens > maxCellTokens*cells && cells < 1<<20 {
		cells *= 2
	}

	return cells
}

// Blocks returns the blocks each cell of a filter of cells cells takes to
// hold tokens hashes at a false positive rate of at most rate, expected
// over the numbers of hashes that the cells get.
func Blocks(tokens, cells int, rate float64) int {
	mean := float64(tokens) / float64(cells)
	need := mean*(math.Log2(1/rate)+1) + cellCount
	blocks := max(int(need)/(8*BlockBytes), 1)
	for ; expectedFalse(mean, 8*BlockBytes*blocks) > rate; blocks++ {
	}

	return blocks
}

// expectedFalse returns the false positive rate of cells of size bits whose
// numbers of hashes follow a Poisson law of the mean.
func expectedFalse(mean float64, size int) float64 {
	if mean == 0 {
		return 0
	}
	spread := 8*math.Sqrt(mean) + 8
	from, to := int(max(mean-spread, 0)), int(mean+spread)
	lg, _ := math.Lgamma(float64(from + 1))
	odds := math.Exp(float64(from)*math.Log(mean) - mean - lg)
	rate := 0.0
	for n := from; n <= to; n++ {
		rate += odds * cellFalse(n, size)
		odds *= mean / float64(n+1)
	}

	return rate
}

// Scratch holds what AppendFilter reuses between filters.
type Scratch struct {
	sorted, tmp []uint64
}

// AppendFilter appends a filter of the distinct hashes to dst, cells cells
// of blocks blocks each.
func AppendFilter(dst []byte, hashes []uint64, cells, blocks int, s *Scratch) []byte {
	size := blocks * BlockBytes
	start := len(dst)
	dst = append(dst, make([]byte, cells*size)...)
	filter := dst[start:]
	sorted := s.sort(hashes)
	for c, from := 0, 0; c < cells; c++ {
		to := from
		for to < len(sorted) && Block(sorted[to], cells) == c {
			to++
		}
		writeCell(filter[c*size:(c+1)*size], sorted[from:to], cells)
		from = to
	}

	return dst
}

// sort returns the hashes in order: by their top 16 bits in two passes of
// a radix sort, then by insertion among the few that share them.
func (s *Scratch) sort(hashes []uint64) []uint64 {
	n := len(hashes)
	if cap(s.sorted) < n {
		s.sorted, s.tmp = make([]uint64, n), make([]uint64, n)
	}
	src, dst := hashes, s.tmp[:n]
	for shift := uint(48); shift < 64; shift += 8 {
		var at [257]int
		for _, h := range src {
			at[h>>shift&255+1]++
		}
		for i := 1; i < len(at); i++ {
			at[i] += at[i-1]
		}
		for _, h := range src {
			d := h >> shift & 255
			dst[at[d]] = h
			at[d]++
		}
		src, dst = dst, s.sorted[:n]
	}
	for i := 1; i < n; i++ {
		h, j := src[i], i
		for ; j > 0 && src[j-1] > h; j-- {
			src[j] = src[j-1]
		}
		src[j] = h
	}

	return src
}

// cellNumber maps h to a number below bound from the bits of h below those
// that name its cell among cells cells; it does not decrease as h grows
// within a cell.
func cellNumber(h uint64, cells int, bound uint64) uint64 {
	hi, _ := bits.Mul64(h<<uint(bits.TrailingZeros(uint(cells))), bound)

	return hi
}

// writeCell codes the hashes of one cell, in order, into cell, which is
// zero.
func writeCell(cell []byte, hashes []uint64, cells int) {
	n := len(hashes)
	low, buckets, ok := cellShape(n, 8*len(cell))
	if !ok {
		binary.LittleEndian.PutUint16(cell, cellFull)

		return
	}
	binary.LittleEndian.PutUint16(cell, uint16(n))
	bound, mask := buckets<<low, uint64(1)<<low-1
	lowAt := cellCount + n + int(buckets)
	for i, h := range hashes {
		v := cellNumber(h, cells, bound)
		at := cellCount + int(v>>low) + i
		cell[at>>3] |= 1 << (at & 7)
		putBits(cell, lowAt+i*int(low), v&mask)
	}
}

// putBits ors v, which has at most 57 bits, into the bits of cell from bit
// at on.
func putBits(cell []byte, at int, v uint64) {
	if v == 0 {
		return
	}
	i := at >> 3
	if i+8 <= len(cell) {
		binary.LittleEndian.PutUint64(cell[i:], binary.LittleEndian.Uint64(cell[i:])|v<<(at&7))

		return
	}
	for v <<= at & 7; v != 0; v >>= 8 {
		cell[i] |= byte(v)
		i++
	}
}

// cellBits returns the bits of cell from bit at on, 57 or more of them.
func cellBits(cell []byte, at int) uint64 {
	if i := at >> 3; i+8 <= len(cell) {
		return binary.LittleEndian.Uint64(cell[i:]) >> (at & 7)
	}
	var tail [8]byte
	copy(tail[:], cell[min(at>>3, len(cell)):])

	return binary.LittleEndian.Uint64(tail[:]) >> (at & 7)
}

// MayContain reports whether a filter of cells cells may hold h. It never
// reports false for a hash that was added.
func MayContain(filter []byte, h uint64, cells int) bool {
	size := len(filter) / cells
	c := Block(h, cells)

	return CellMayContain(filter[c*size:(c+1)*size], h, cells)
}

// CellMayContain reports whether a filter of cells cells may hold h, given
// the cell of h. It never reports false for a hash that was added.
func CellMayContain(cell []byte, h uint64, cells int) bool {
	n := int(binary.LittleEndian.Uint16(cell))
	low, buckets, ok := cellShape(n, 8*len(cell))
	if !ok {
		return true
	}
	if n == 0 {
		return false
	}
	v := cellNumber(h, cells, buckets<<low)
	// The ones of bucket q follow the q-th zero of the high bits.
	q := int(v >> low)
	at, end := cellCount, cellCount+n+int(buckets)
	// A cell is whole words, read one after another; the count reads as
	// ones, which the high bits do not count.
	for word, skip, w := 0, q, binary.LittleEndian.Uint64(cell)|cellFull; skip > 0; {
		zeros := ^w
		if left := end - 64*word; left < 64 {
			zeros &= 1<<uint(left) - 1
		}
		if k := bits.OnesCount64(zeros); k < skip {
			if word++; 64*word >= end {
				return false
			}
			skip -= k
			w = binary.LittleEndian.Uint64(cell[8*word:])

			continue
		}
		at = 64*word + SelectBit(zeros, skip-1) + 1

		break
	}
	want, mask := v&(1<<low-1), uint64(1)<<low-1
	for i := at - cellCount - q; at < end && cell[at>>3]>>(at&7)&1 != 0; i, at = i+1, at+1 {
		got := cellBits(cell, end+i*int(low)) & mask
		if got >= want {
			return got == want
		}
	}

	return false
}

// selectInByte holds, at b<<3 | k, the place of the k-th set bit of b.
var selectInByte = func() (t [256 << 3]uint8) {
	for b := 0; b < 256; b++ {
		k := 0
		for i := 0; i < 8; i++ {
			if b>>i&1 != 0 {
				t[b<<3|k] = uint8(i)
				k++
			}
		}
	}

	return t
}()

// SelectBit returns the place of the k-th set bit of x, k from 0, which x
// must have: the byte that holds it from the sums of the set bits of the
// bytes up to each, after Vigna's broadword selection, then the bit within
// it from a table.
func SelectBit(x uint64, k int) int {
	const ones, highs = 0x0101010101010101, 0x8080808080808080
	s := x - x>>1&0x5555555555555555
	s = s&0x3333333333333333 + s>>2&0x3333333333333333
	s = (s + s>>4) & 0x0f0f0f0f0f0f0f0f * ones
	// The bytes whose sum is at most k come first.
	at := bits.OnesCount64(((uint64(k)*ones|highs)-s)&highs) << 3
	before := int(s << 8 >> uint(at) & 0xff)

	return at + int(selectInByte[int(x>>uint(at)&0xff)<<3|(k-before)])
}

// Block returns which of n equal parts of the hashes h falls in: its high
// half scaled to n, so that the part of n that it falls in lies within the
// part of m that it falls in whenever m divides n.
func Block(h uint64, n int) int {
	return int((h >> 32) * uint64(n) >> 32)
}

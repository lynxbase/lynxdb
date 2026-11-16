package vec

import (
	"encoding/binary"
	"math/bits"
)

// A mask selects rows with one byte per row: 0xFF selected, 0x00 not.

// MaskEq8 writes into dst which elements of src equal c.
func MaskEq8(dst, src []uint8, c uint8) {
	dst = dst[:len(src)]
	i := maskEq8Wide(dst, src, c)
	for ; i < len(src); i++ {
		var m uint8
		if src[i] == c {
			m = 0xFF
		}
		dst[i] = m
	}
}

// MaskEq16 writes into dst which elements of src equal c.
func MaskEq16(dst []uint8, src []uint16, c uint16) {
	dst = dst[:len(src)]
	for i, v := range src {
		var m uint8
		if v == c {
			m = 0xFF
		}
		dst[i] = m
	}
}

// MaskRangeInt64 writes into dst which elements of src lie in [lo, hi].
func MaskRangeInt64(dst []uint8, src []int64, lo, hi int64) {
	dst = dst[:len(src)]
	// One unsigned compare covers both bounds.
	span := uint64(hi) - uint64(lo)
	for i, v := range src {
		var m uint8
		if uint64(v)-uint64(lo) <= span {
			m = 0xFF
		}
		dst[i] = m
	}
}

// MaskAnd stores a AND b into dst.
func MaskAnd(dst, a, b []uint8) {
	n := len(a)
	dst, b = dst[:n], b[:n]
	i := maskAndWide(dst, a, b)
	for ; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(dst[i:], binary.LittleEndian.Uint64(a[i:])&binary.LittleEndian.Uint64(b[i:]))
	}
	for ; i < n; i++ {
		dst[i] = a[i] & b[i]
	}
}

// MaskOr stores a OR b into dst.
func MaskOr(dst, a, b []uint8) {
	n := len(a)
	dst, b = dst[:n], b[:n]
	i := 0
	for ; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(dst[i:], binary.LittleEndian.Uint64(a[i:])|binary.LittleEndian.Uint64(b[i:]))
	}
	for ; i < n; i++ {
		dst[i] = a[i] | b[i]
	}
}

// MaskAndBitmap clears the mask bytes of rows whose bit in valid is clear.
func MaskAndBitmap(mask []uint8, valid Bitmap) {
	for i := range mask {
		if !valid.Get(i) {
			mask[i] = 0
		}
	}
}

// MaskCount returns the number of selected rows.
func MaskCount(mask []uint8) int {
	n, i := maskCountWide(mask)
	for ; i+8 <= len(mask); i += 8 {
		n += bits.OnesCount64(binary.LittleEndian.Uint64(mask[i:])) >> 3
	}
	for ; i < len(mask); i++ {
		n += int(mask[i] & 1)
	}

	return n
}

// MaskIndexes appends the positions of the selected rows to dst.
func MaskIndexes(dst []uint32, mask []uint8) []uint32 {
	i := 0
	for ; i+8 <= len(mask); i += 8 {
		w := binary.LittleEndian.Uint64(mask[i:])
		if w == 0 {
			continue
		}
		// Gather the low bit of each byte into one byte.
		b := (w & 0x0101010101010101) * 0x0102040810204080 >> 56
		for b != 0 {
			dst = append(dst, uint32(i+bits.TrailingZeros64(b)))
			b &= b - 1
		}
	}
	for ; i < len(mask); i++ {
		if mask[i] != 0 {
			dst = append(dst, uint32(i))
		}
	}

	return dst
}

// SumInt64 returns the sum of src.
func SumInt64(src []int64) int64 {
	var s0, s1, s2, s3 int64
	i := 0
	for ; i+4 <= len(src); i += 4 {
		s0 += src[i]
		s1 += src[i+1]
		s2 += src[i+2]
		s3 += src[i+3]
	}
	for ; i < len(src); i++ {
		s0 += src[i]
	}

	return s0 + s1 + s2 + s3
}

// MinMaxInt64 returns the smallest and largest element of a non-empty src.
func MinMaxInt64(src []int64) (lo, hi int64) {
	lo, hi = src[0], src[0]
	for _, v := range src[1:] {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}

	return lo, hi
}

// Histogram8 adds the number of occurrences of each code to counts.
func Histogram8(counts *[256]uint32, codes []uint8) {
	// Four tables break the store-to-load dependency between equal codes.
	var t1, t2, t3 [256]uint32
	i := 0
	for ; i+4 <= len(codes); i += 4 {
		counts[codes[i]]++
		t1[codes[i+1]]++
		t2[codes[i+2]]++
		t3[codes[i+3]]++
	}
	for ; i < len(codes); i++ {
		counts[codes[i]]++
	}
	for j := range counts {
		counts[j] += t1[j] + t2[j] + t3[j]
	}
}

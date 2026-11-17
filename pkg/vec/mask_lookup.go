package vec

import "encoding/binary"

// MaskNot stores the inverse of src into dst.
func MaskNot(dst, src []uint8) {
	n := len(src)
	dst = dst[:n]
	i := 0
	for ; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(dst[i:], ^binary.LittleEndian.Uint64(src[i:]))
	}
	for ; i < n; i++ {
		dst[i] = ^src[i]
	}
}

// MaskLookup8 stores table[codes[i]] into dst[i]; table holds a mask byte
// per code and must cover every code.
func MaskLookup8(dst, codes, table []uint8) {
	dst = dst[:len(codes)]
	if len(table) >= 256 {
		t := (*[256]uint8)(table)
		for i, c := range codes {
			dst[i] = t[c]
		}

		return
	}
	for i, c := range codes {
		dst[i] = table[c]
	}
}

// MaskLookup16 is MaskLookup8 for 16-bit codes.
func MaskLookup16(dst []uint8, codes []uint16, table []uint8) {
	dst = dst[:len(codes)]
	for i, c := range codes {
		dst[i] = table[c]
	}
}

// MaskFromBitmap selects in mask the rows whose bit in b is set.
func MaskFromBitmap(mask []uint8, b Bitmap) {
	for i := range mask {
		mask[i] = uint8(0) - uint8(b[i>>6]>>(uint(i)&63)&1)
	}
}

// MaskFromIndexes selects in mask exactly the rows listed in idx.
func MaskFromIndexes(mask []uint8, idx []uint32) {
	clear(mask)
	for _, i := range idx {
		mask[i] = 0xFF
	}
}

// IndexesAndMask keeps, in place, the positions of idx that mask selects.
func IndexesAndMask(idx []uint32, mask []uint8) []uint32 {
	n := 0
	for _, i := range idx {
		idx[n] = i
		n += int(mask[i] & 1)
	}

	return idx[:n]
}

// IndexesAndBitmap keeps, in place, the positions of idx whose bit in b is
// set.
func IndexesAndBitmap(idx []uint32, b Bitmap) []uint32 {
	n := 0
	for _, i := range idx {
		idx[n] = i
		n += int(b[i>>6] >> (i & 63) & 1)
	}

	return idx[:n]
}

// Histogram16 adds the number of occurrences of each code to counts, which
// must cover every code.
func Histogram16(counts []uint32, codes []uint16) {
	for _, c := range codes {
		counts[c]++
	}
}

package part

import (
	"encoding/binary"
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// appendPacked appends rank[code] for every code, each in width bits.
func appendPacked(dst []byte, codes, rank []uint32, width uint) []byte {
	var acc uint64
	var filled uint
	for _, code := range codes {
		v := uint64(rank[code])
		acc |= v << filled
		filled += width
		if filled >= 64 {
			dst = binary.LittleEndian.AppendUint64(dst, acc)
			filled -= 64
			// The part of v that did not fit starts the next word.
			acc = v >> (width - filled)
		}
	}
	for ; filled > 0; filled -= min(filled, 8) {
		dst = append(dst, byte(acc))
		acc >>= 8
	}

	return dst
}

// packedSize returns the bytes that n values of width bits take.
func packedSize(n int, width uint) int { return (n*int(width) + 7) / 8 }

// unpack8 reads len(dst) values of width bits, at most 8, from src, which
// holds exactly packedSize bytes, and reports whether all are below size.
func unpack8(dst []uint8, src []byte, width uint, size int) bool {
	const lanes = 0x0101010101010101
	mask := uint64(1)<<width - 1
	// A value at or above size carries into the top bit of its byte, which
	// works while a byte cannot overflow: for values below 128.
	add := uint64(0x80-min(size, 0x80)) * lanes
	var over uint64
	i, off := 0, 0
	// Eight values take width bytes, so every group starts on a byte.
	for ; i+8 <= len(dst) && off+8 <= len(src); i, off = i+8, off+int(width) {
		word := binary.LittleEndian.Uint64(src[off:])
		out := word & mask
		out |= (word >> width & mask) << 8
		out |= (word >> (2 * width) & mask) << 16
		out |= (word >> (3 * width) & mask) << 24
		out |= (word >> (4 * width) & mask) << 32
		out |= (word >> (5 * width) & mask) << 40
		out |= (word >> (6 * width) & mask) << 48
		out |= (word >> (7 * width) & mask) << 56
		binary.LittleEndian.PutUint64(dst[i:], out)
		over |= out + add
	}
	var tail [24]byte
	copy(tail[:], src[off:])
	for bit := uint(0); i < len(dst); i, bit = i+1, bit+width {
		v := uint64(binary.LittleEndian.Uint16(tail[bit>>3:])) >> (bit & 7) & mask
		dst[i] = uint8(v)
		over |= v + add
	}
	if width < 8 {
		return over&(0x80*lanes) == 0
	}
	if size < 256 {
		for _, v := range dst {
			if int(v) >= size {
				return false
			}
		}
	}

	return true
}

// unpack16 reads len(dst) values of width bits, 9 to 16, from src, which
// holds exactly packedSize bytes, and reports whether all are below size.
func unpack16(dst []uint16, src []byte, width uint, size int) bool {
	mask := uint32(1)<<width - 1
	limit := uint32(size)
	ok := true
	i := 0
	for ; i < len(dst); i++ {
		bit := uint(i) * width
		pos := int(bit >> 3)
		if pos+4 > len(src) {
			break
		}
		v := binary.LittleEndian.Uint32(src[pos:]) >> (bit & 7) & mask
		dst[i] = uint16(v)
		ok = ok && v < limit
	}
	if i == len(dst) {
		return ok
	}
	var tail [16]byte
	origin := (uint(i) * width) >> 3
	copy(tail[:], src[origin:])
	for ; i < len(dst); i++ {
		bit := uint(i)*width - origin*8
		v := binary.LittleEndian.Uint32(tail[bit>>3:]) >> (bit & 7) & mask
		dst[i] = uint16(v)
		ok = ok && v < limit
	}

	return ok
}

// spread copies src, which holds one value per set bit of valid, to the
// positions of those bits in dst.
func spread[T any](dst, src []T, valid vec.Bitmap) {
	k := 0
	for wi, w := range valid {
		base := wi << 6
		if w == ^uint64(0) {
			copy(dst[base:base+64], src[k:k+64])
			k += 64

			continue
		}
		for w != 0 {
			start := bits.TrailingZeros64(w)
			run := bits.TrailingZeros64(^(w >> uint(start)))
			copy(dst[base+start:base+start+run], src[k:k+run])
			k += run
			if start+run == 64 {
				break
			}
			w &^= 1<<uint(start+run) - 1
		}
	}
}

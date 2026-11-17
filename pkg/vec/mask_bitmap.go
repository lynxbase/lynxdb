package vec

import "encoding/binary"

// BitmapFromMask sets bit i of dst for every nonzero byte i of mask and
// clears the other bits of the words that hold them.
func BitmapFromMask(dst Bitmap, mask []uint8) {
	const low = 0x0101010101010101
	words := len(mask) >> 6
	for w := 0; w < words; w++ {
		src := mask[w<<6 : w<<6+64]
		var bits uint64
		for b := 0; b < 8; b++ {
			// A mask byte is 0x00 or 0xFF: its low bits gather into one byte.
			eight := binary.LittleEndian.Uint64(src[b<<3:]) & low
			bits |= (eight * 0x0102040810204080 >> 56) << (b << 3)
		}
		dst[w] = bits
	}
	if rest := mask[words<<6:]; len(rest) > 0 {
		var bits uint64
		for i, m := range rest {
			bits |= uint64(m&1) << uint(i)
		}
		dst[words] = bits
	}
}

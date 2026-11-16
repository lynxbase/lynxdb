package tokenfilter

import "encoding/binary"

const (
	// bitsPerToken sizes filters for a false-positive rate near 0.5%.
	bitsPerToken = 12
	blockBytes   = 32
)

var salts = [8]uint32{
	0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
	0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
}

// Append appends a filter holding the distinct hashes to dst. Each hash
// sets one bit in each of the eight words of a single 256-bit block.
func Append(dst []byte, hashes []uint64) []byte {
	blocks := (len(hashes)*bitsPerToken + 255) / 256
	if blocks == 0 {
		blocks = 1
	}
	start := len(dst)
	dst = append(dst, make([]byte, blocks*blockBytes)...)
	filter := dst[start:]
	for _, h := range hashes {
		if h == 0 {
			h = 1
		}
		block := filter[blockOf(h, blocks):][:blockBytes]
		x := uint32(h)
		for i, salt := range salts {
			word := block[4*i:]
			binary.LittleEndian.PutUint32(word, binary.LittleEndian.Uint32(word)|1<<((x*salt)>>27))
		}
	}

	return dst
}

func blockOf(h uint64, blocks int) int {
	return int((h>>32)*uint64(blocks)>>32) * blockBytes
}

// MayContain reports whether filter may hold h. It never reports false for
// a hash that was added.
func MayContain(filter []byte, h uint64) bool {
	blocks := len(filter) / blockBytes
	if blocks == 0 {
		return true
	}
	if h == 0 {
		h = 1
	}
	block := filter[blockOf(h, blocks):][:blockBytes]
	x := uint32(h)
	for i, salt := range salts {
		if binary.LittleEndian.Uint32(block[4*i:])&(1<<((x*salt)>>27)) == 0 {
			return false
		}
	}

	return true
}

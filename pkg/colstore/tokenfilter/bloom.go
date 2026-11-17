package tokenfilter

import "encoding/binary"

const (
	// bitsPerToken sizes filters for a false-positive rate near 0.5%.
	bitsPerToken = 12
	// BlockBytes is the size of a bloom block, the only bytes of a filter
	// that a probe reads.
	BlockBytes = 32
)

var salts = [8]uint32{
	0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
	0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
}

// Append appends a filter holding the distinct hashes to dst, sized to a
// multiple of unit bloom blocks. Each hash sets one bit in each of the eight
// words of a single 256-bit block.
func Append(dst []byte, hashes []uint64, unit int) []byte {
	blocks := max((len(hashes)*bitsPerToken+255)/256, 1)
	blocks = (blocks + unit - 1) / unit * unit
	start := len(dst)
	dst = append(dst, make([]byte, blocks*BlockBytes)...)
	filter := dst[start:]
	for _, h := range hashes {
		if h == 0 {
			h = 1
		}
		block := filter[Block(h, blocks)*BlockBytes:][:BlockBytes]
		x := uint32(h)
		for i, salt := range salts {
			word := block[4*i:]
			binary.LittleEndian.PutUint32(word, binary.LittleEndian.Uint32(word)|1<<((x*salt)>>27))
		}
	}

	return dst
}

// Block returns which of the blocks bloom blocks of a filter holds h. It is
// the hash's high half scaled to the count, so the n-th part of a filter
// that a hash falls in is the same for every count that n divides.
func Block(h uint64, blocks int) int {
	return int((h >> 32) * uint64(blocks) >> 32)
}

// ProbeOffset returns where in a filter of size bytes the 32 bytes lie that
// MayContain reads for h.
func ProbeOffset(size int, h uint64) int {
	blocks := size / BlockBytes
	if blocks == 0 {
		return 0
	}

	return Block(h, blocks) * BlockBytes
}

// MayContain reports whether filter may hold h. It never reports false for
// a hash that was added.
func MayContain(filter []byte, h uint64) bool {
	blocks := len(filter) / BlockBytes
	if blocks == 0 {
		return true
	}

	return BlockMayContain(filter[Block(h, blocks)*BlockBytes:], h)
}

// BlockMayContain is MayContain on the bloom block of h, which block starts
// with.
func BlockMayContain(block []byte, h uint64) bool {
	if h == 0 {
		h = 1
	}
	block = block[:BlockBytes]
	x := uint32(h)
	for i, salt := range salts {
		if binary.LittleEndian.Uint32(block[4*i:])&(1<<((x*salt)>>27)) == 0 {
			return false
		}
	}

	return true
}

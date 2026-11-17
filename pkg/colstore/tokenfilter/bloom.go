package tokenfilter

import "encoding/binary"

const (
	// SharedBits and ColumnBits are the bits a filter spends per token. A
	// false answer of a filter costs the decoding of what it guards: the
	// small columns that share one filter are cheap to decode, which 12
	// bits and about one false answer in 250 suit; a column with a filter
	// of its own is large, and 16 bits make it about one in 1000.
	SharedBits = 12
	ColumnBits = 16
	// BlockBytes is the size of a bloom block, the only bytes of a filter
	// that a probe reads.
	BlockBytes = 32
)

var salts = [8]uint32{
	0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
	0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
}

// Blocks returns the number of bloom blocks of a filter for tokens distinct
// hashes at bits per token, before it is rounded to a unit.
func Blocks(tokens, bits int) int {
	return max((tokens*bits+255)/256, 1)
}

// Append appends a filter holding the distinct hashes to dst at bits per
// hash, sized to a multiple of unit bloom blocks. Each hash sets one bit in
// each of the eight words of a single 256-bit block.
func Append(dst []byte, hashes []uint64, unit, bits int) []byte {
	blocks := (Blocks(len(hashes), bits) + unit - 1) / unit * unit
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

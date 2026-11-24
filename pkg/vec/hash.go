package vec

import (
	"encoding/binary"
	"math/bits"
)

// The multipliers of wyhash.
const (
	hashP0 = 0xa0761d6478bd642f
	hashP1 = 0xe7037ed1a0b428db
	hashP2 = 0x8ebc6af09c88c6e3
	hashP3 = 0x589965cc75374cc3
)

// hashSeed is what the first step of wyhash makes of a zero seed.
var hashSeed = hashMix(hashP0, hashP1)

// hashMix folds the 128-bit product of two words into one.
func hashMix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)

	return hi ^ lo
}

// Hash returns the hash that a ByteTable and the partitions of its keys go
// by: wyhash with a zero seed, which takes two multiplications for a key of
// up to 16 bytes and one more for every 16 bytes beyond. It is for tables in
// memory only; the value of a key is not part of any format.
func Hash(key []byte) uint64 {
	n := len(key)
	seed := hashSeed
	var a, b uint64
	switch {
	case n > 16:
		p := key
		if n > 48 {
			see1, see2 := seed, seed
			for len(p) > 48 {
				seed = hashMix(binary.LittleEndian.Uint64(p)^hashP1, binary.LittleEndian.Uint64(p[8:])^seed)
				see1 = hashMix(binary.LittleEndian.Uint64(p[16:])^hashP2, binary.LittleEndian.Uint64(p[24:])^see1)
				see2 = hashMix(binary.LittleEndian.Uint64(p[32:])^hashP3, binary.LittleEndian.Uint64(p[40:])^see2)
				p = p[48:]
			}
			seed ^= see1 ^ see2
		}
		for len(p) > 16 {
			seed = hashMix(binary.LittleEndian.Uint64(p)^hashP1, binary.LittleEndian.Uint64(p[8:])^seed)
			p = p[16:]
		}
		// The last 16 bytes of the key, which may reach back into what the
		// loops took.
		a, b = binary.LittleEndian.Uint64(key[n-16:]), binary.LittleEndian.Uint64(key[n-8:])
	case n >= 4:
		half := n >> 3 << 2
		a = uint64(binary.LittleEndian.Uint32(key))<<32 | uint64(binary.LittleEndian.Uint32(key[half:]))
		b = uint64(binary.LittleEndian.Uint32(key[n-4:]))<<32 | uint64(binary.LittleEndian.Uint32(key[n-4-half:]))
	case n > 0:
		a = uint64(key[0])<<16 | uint64(key[n>>1])<<8 | uint64(key[n-1])
	}
	hi, lo := bits.Mul64(a^hashP1, b^seed)

	return hashMix(lo^hashP0^uint64(n), hi^hashP1)
}

package vec

import "math/bits"

// Bitmap is a little-endian bit set; bit i lives in word i/64.
type Bitmap []uint64

// BitmapWords returns the number of words needed for n bits.
func BitmapWords(n int) int { return (n + 63) >> 6 }

// Get reports whether bit i is set.
func (b Bitmap) Get(i int) bool { return b[i>>6]&(1<<(uint(i)&63)) != 0 }

// Set sets bit i.
func (b Bitmap) Set(i int) { b[i>>6] |= 1 << (uint(i) & 63) }

// Count returns the number of set bits.
func (b Bitmap) Count() int {
	n := 0
	for _, w := range b {
		n += bits.OnesCount64(w)
	}

	return n
}

// SetAll sets the first n bits and clears the rest of the last word.
func (b Bitmap) SetAll(n int) {
	full := n >> 6
	for i := 0; i < full; i++ {
		b[i] = ^uint64(0)
	}
	if rem := uint(n) & 63; rem != 0 {
		b[full] = (1 << rem) - 1
	}
}

// Rank returns the number of set bits below i.
func (b Bitmap) Rank(i int) int {
	n := 0
	full := i >> 6
	for _, w := range b[:full] {
		n += bits.OnesCount64(w)
	}
	if rem := uint(i) & 63; rem != 0 {
		n += bits.OnesCount64(b[full] & ((1 << rem) - 1))
	}

	return n
}

// AppendSet appends the positions of the set bits to dst.
func (b Bitmap) AppendSet(dst []uint32) []uint32 {
	for wi, w := range b {
		base := uint32(wi) << 6
		for w != 0 {
			dst = append(dst, base+uint32(bits.TrailingZeros64(w)))
			w &= w - 1
		}
	}

	return dst
}

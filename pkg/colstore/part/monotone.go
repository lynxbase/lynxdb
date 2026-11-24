package part

import (
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// monotone holds numbers that never decrease, appended in order and read by
// position, in two bits a number plus the bits of the mean step between
// them: the low bits of each as they are, and the high bits as one set bit
// per number among one clear bit per step of the high bits.
type monotone struct {
	n, filled int
	low       uint
	lows      []uint64
	highs     []uint64
	// marks holds the bit of highs that every markEvery-th number set.
	marks []uint64
}

const markEvery = 64

// reset readies m for n numbers below bound, reusing its memory.
func (m *monotone) reset(n int, bound uint64) {
	m.n, m.filled, m.low = n, 0, 0
	if n > 0 && bound/uint64(n) > 1 {
		m.low = uint(bits.Len64(bound/uint64(n)) - 1)
	}
	m.lows = grow(m.lows, (n*int(m.low)+63)/64+1)
	m.highs = grow(m.highs, (n+int(bound>>m.low))/64+2)
	m.marks = grow(m.marks, n/markEvery+1)
	clear(m.lows)
	clear(m.highs)
}

// append adds v, which is at least the number before it and below the bound.
func (m *monotone) append(v uint32) {
	i := m.filled
	m.filled++
	if m.low > 0 {
		at := uint(i) * m.low
		lo := uint64(v) & (1<<m.low - 1)
		m.lows[at>>6] |= lo << (at & 63)
		if at&63+m.low > 64 {
			m.lows[at>>6+1] |= lo >> (64 - at&63)
		}
	}
	bit := uint64(v>>m.low) + uint64(i)
	m.highs[bit>>6] |= 1 << (bit & 63)
	if i%markEvery == 0 {
		m.marks[i/markEvery] = bit
	}
}

// get returns number i.
func (m *monotone) get(i int) uint32 {
	bit := m.marks[i/markEvery]
	word := bit >> 6
	w := m.highs[word] &^ (1<<(bit&63) - 1)
	k := i % markEvery
	for {
		c := bits.OnesCount64(w)
		if k < c {
			break
		}
		k -= c
		word++
		w = m.highs[word]
	}
	v := uint32(word<<6+uint64(tokenfilter.SelectBit(w, k))-uint64(i)) << m.low
	if m.low > 0 {
		at := uint(i) * m.low
		lo := m.lows[at>>6] >> (at & 63)
		if at&63+m.low > 64 {
			lo |= m.lows[at>>6+1] << (64 - at&63)
		}
		v |= uint32(lo & (1<<m.low - 1))
	}

	return v
}

// retained returns the bytes of the buffers that m keeps.
func (m *monotone) retained() int { return 8 * (cap(m.lows) + cap(m.highs) + cap(m.marks)) }

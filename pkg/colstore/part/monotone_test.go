package part

import (
	"math/rand/v2"
	"testing"
)

func TestMonotoneReturnsWhatWasAppended(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var m monotone
	for _, tc := range []struct {
		n    int
		step uint32
	}{{0, 1}, {1, 0}, {1, 1 << 30}, {7, 0}, {300, 1}, {1000, 3}, {5000, 1000}, {70000, 40}, {513, 1 << 20}} {
		want := make([]uint32, tc.n)
		v := uint32(0)
		for i := range want {
			if tc.step > 0 {
				v += rng.Uint32N(2 * tc.step)
			}
			want[i] = v
		}
		m.reset(tc.n, uint64(v)+1)
		for _, v := range want {
			m.append(v)
		}
		for i, v := range want {
			if got := m.get(i); got != v {
				t.Fatalf("n=%d step=%d: number %d is %d, want %d", tc.n, tc.step, i, got, v)
			}
		}
	}
}

func TestMonotoneTakesFewBits(t *testing.T) {
	const n = 1 << 16
	var m monotone
	m.reset(n, 8*n)
	for i := range n {
		m.append(uint32(8 * i))
	}
	if got := m.retained(); got > n {
		t.Fatalf("%d numbers eight apart take %d bytes, want at most a byte each", n, got)
	}
}

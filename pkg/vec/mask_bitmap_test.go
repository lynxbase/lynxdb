package vec

import (
	"math/rand"
	"testing"
)

func TestBitmapFromMask(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	for _, n := range []int{0, 1, 7, 63, 64, 65, 128, 200, 1000} {
		mask := make([]uint8, n)
		want := 0
		for i := range mask {
			if rng.Intn(3) == 0 {
				mask[i] = 0xFF
				want++
			}
		}
		bits := make(Bitmap, BitmapWords(n))
		for i := range bits {
			bits[i] = ^uint64(0)
		}
		BitmapFromMask(bits, mask)
		for i, m := range mask {
			if bits.Get(i) != (m != 0) {
				t.Fatalf("%d rows: bit %d is %v for mask byte %#x", n, i, bits.Get(i), m)
			}
		}
		if bits.Count() != want {
			t.Fatalf("%d rows: %d bits set, want %d", n, bits.Count(), want)
		}
	}
}

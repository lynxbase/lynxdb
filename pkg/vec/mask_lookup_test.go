package vec

import (
	"math/rand"
	"slices"
	"testing"
)

func TestMaskLookupKernels(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, n := range []int{0, 1, 7, 8, 9, 63, 64, 65, 1000} {
		mask := make([]uint8, n)
		valid := make(Bitmap, BitmapWords(n))
		codes8 := make([]uint8, n)
		codes16 := make([]uint16, n)
		var want []uint32
		for i := range mask {
			codes8[i] = uint8(rng.Intn(256))
			codes16[i] = uint16(rng.Intn(1000))
			if rng.Intn(3) == 0 {
				mask[i] = 0xFF
				valid.Set(i)
				want = append(want, uint32(i))
			}
		}

		not := make([]uint8, n)
		MaskNot(not, mask)
		for i := range mask {
			if not[i] != ^mask[i] {
				t.Fatalf("n=%d: MaskNot at %d", n, i)
			}
		}

		fromBitmap := make([]uint8, n)
		MaskFromBitmap(fromBitmap, valid)
		if !slices.Equal(fromBitmap, mask) {
			t.Fatalf("n=%d: MaskFromBitmap differs from the mask", n)
		}
		fromIndexes := slices.Repeat([]uint8{0xFF}, n)
		MaskFromIndexes(fromIndexes, want)
		if !slices.Equal(fromIndexes, mask) {
			t.Fatalf("n=%d: MaskFromIndexes differs from the mask", n)
		}

		every := make([]uint32, n)
		for i := range every {
			every[i] = uint32(i)
		}
		if got := IndexesAndMask(slices.Clone(every), mask); !slices.Equal(got, want) {
			t.Fatalf("n=%d: IndexesAndMask got %v, want %v", n, got, want)
		}
		if got := IndexesAndBitmap(slices.Clone(every), valid); !slices.Equal(got, want) {
			t.Fatalf("n=%d: IndexesAndBitmap got %v, want %v", n, got, want)
		}

		for _, size := range []int{256, 300} {
			table := make([]uint8, size)
			for i := range table {
				if i%3 == 0 {
					table[i] = 0xFF
				}
			}
			got := make([]uint8, n)
			MaskLookup8(got, codes8, table)
			for i, c := range codes8 {
				if got[i] != table[c] {
					t.Fatalf("n=%d: MaskLookup8 at %d", n, i)
				}
			}
		}
		table := make([]uint8, 1000)
		counts := make([]uint32, 1000)
		for i := range table {
			table[i] = uint8(0) - uint8(i&1)
		}
		got := make([]uint8, n)
		MaskLookup16(got, codes16, table)
		Histogram16(counts, codes16)
		for i, c := range codes16 {
			if got[i] != table[c] {
				t.Fatalf("n=%d: MaskLookup16 at %d", n, i)
			}
			counts[c]--
		}
		for c, left := range counts {
			if left != 0 {
				t.Fatalf("n=%d: Histogram16 miscounts code %d", n, c)
			}
		}
	}
}

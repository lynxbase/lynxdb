package exec

import (
	"math"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestDistinctStateIsExactThenWithinItsError checks that a state counts
// exactly while it is a set, and that as a sketch its relative error over
// many samples stays near the standard error of 1.04/sqrt(4096) and never
// leaves four of them.
func TestDistinctStateIsExactThenWithinItsError(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	const standard = 1.04 / 64
	for _, n := range []int{1, 5, 6, 47, 48, hllExact, hllExact + 1, 1000, 3000, 10_000, 50_000, 300_000, 2_000_000} {
		samples, squares := 20, 0.0
		for seed := 0; seed < samples; seed++ {
			h.reset()
			var ref stateRef
			for i := 0; i < n; i++ {
				hash := vec.Mix64(uint64(seed)<<40 | uint64(i))
				ref = hllAdd(&h, ref, hash)
				// A value seen again changes nothing.
				if i%3 == 0 {
					ref = hllAdd(&h, ref, hash)
				}
			}
			got := hllCount(h.get(ref))
			if n <= hllExact {
				if got != int64(n) {
					t.Fatalf("%d values: counted %d", n, got)
				}

				continue
			}
			if size := len(h.get(ref)); size != hllDenseSize {
				t.Fatalf("%d values: the state takes %d bytes, want %d", n, size, hllDenseSize)
			}
			rel := float64(got-int64(n)) / float64(n)
			if math.Abs(rel) > 4*standard {
				t.Fatalf("%d values: counted %d, off by %.2f%%", n, got, 100*rel)
			}
			squares += rel * rel
		}
		if rms := math.Sqrt(squares / float64(samples)); rms > 1.5*standard {
			t.Fatalf("%d values: the error over %d samples is %.2f%%, want about %.2f%%", n, samples, 100*rms, 100*standard)
		}
	}
}

func TestDistinctStatesMergeToTheStateOfAllValues(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	for _, n := range []int{0, 3, 200, hllExact, hllExact + 1, 900, 40_000} {
		for _, pieces := range []int{2, 7} {
			var whole, merged stateRef
			parts := make([]stateRef, pieces)
			for i := 0; i < n; i++ {
				hash := vec.Mix64(uint64(i) + 77)
				whole = hllAdd(&h, whole, hash)
				// Most values go to one piece, so that sets meet sketches.
				p := 0
				if i%10 == 0 {
					p = i % pieces
				}
				parts[p] = hllAdd(&h, parts[p], hash)
				if i%5 == 0 {
					parts[(p+1)%pieces] = hllAdd(&h, parts[(p+1)%pieces], hash)
				}
			}
			for _, p := range []int{pieces - 1, 0} {
				if b := h.get(parts[p]); len(b) != 0 {
					merged = hllMerge(&h, merged, b)
				}
			}
			for p := 1; p < pieces-1; p++ {
				if b := h.get(parts[p]); len(b) != 0 {
					merged = hllMerge(&h, merged, b)
				}
			}
			if got, want := hllCount(h.get(merged)), hllCount(h.get(whole)); got != want {
				t.Fatalf("%d values in %d pieces: merged to %d, want %d", n, pieces, got, want)
			}
		}
	}
}

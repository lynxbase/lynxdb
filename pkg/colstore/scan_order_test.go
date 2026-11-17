package colstore

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// TestUnitsMergeAsAStableSortOrdersThem requires the merge of the parts'
// units to give the order of a stable sort by time, both ways, for parts
// in order of time and not, with many units sharing a time.
func TestUnitsMergeAsAStableSortOrdersThem(t *testing.T) {
	rng := rand.New(rand.NewPCG(61, 62))
	for round := 0; round < 400; round++ {
		var units []scanUnit
		parts := 1 + rng.IntN(9)
		ordered := round%4 != 0
		for p := 0; p < parts; p++ {
			at := int64(rng.IntN(50))
			for b, n := 0, rng.IntN(40); b < n; b++ {
				step := int64(rng.IntN(3))
				if !ordered && rng.IntN(5) == 0 {
					step = -int64(rng.IntN(4))
				}
				at += step
				last := at
				if round%3 == 0 {
					last += int64(rng.IntN(2))
				}
				units = append(units, scanUnit{part: int32(p), block: int32(b), first: at, last: last})
			}
		}
		for _, latest := range []bool{false, true} {
			want := sortUnits(slices.Clone(units), latest)
			got := orderUnits(slices.Clone(units), nil, latest)
			if !slices.Equal(got, want) {
				t.Fatalf("round %d, latest %v: %d units merged as\n%v\nwant\n%v", round, latest, len(units), got, want)
			}
		}
	}
}

func BenchmarkOrderUnits(b *testing.B) {
	rng := rand.New(rand.NewPCG(63, 64))
	var units []scanUnit
	for p := 0; p < 9; p++ {
		at := int64(rng.IntN(1000))
		for blk := 0; blk < 800; blk++ {
			at += int64(1 + rng.IntN(5))
			units = append(units, scanUnit{part: int32(p), block: int32(blk), first: at, last: at + 3})
		}
	}
	work, out := make([]scanUnit, len(units)), make([]scanUnit, 0, len(units))
	b.Run("merge", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(work, units)
			out = orderUnits(work, out, true)
		}
	})
	b.Run("sort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			copy(work, units)
			sortUnits(work, true)
		}
	})
}

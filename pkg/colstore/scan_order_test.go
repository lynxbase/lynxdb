package colstore

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// sortUnits orders units as a scan by time takes them.
func sortUnits(units []scanUnit, latest bool) []scanUnit {
	if latest {
		slices.SortStableFunc(units, func(a, b scanUnit) int { return cmp.Compare(b.last, a.last) })
	} else {
		slices.SortStableFunc(units, func(a, b scanUnit) int { return cmp.Compare(a.first, b.first) })
	}

	return units
}

// TestUnitsComeAsAStableSortOrdersThem requires the queue of a scan by time
// to give its units in the order of a stable sort by time, both ways, for
// parts in order of time and not, with many units sharing a time, however
// many units the scan asks for at a time.
func TestUnitsComeAsAStableSortOrdersThem(t *testing.T) {
	rng := rand.New(rand.NewPCG(61, 62))
	var q unitQueue
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
			q.reset(units, latest)
			// Small steps merge the units from their runs, large ones sort
			// those left.
			most := 1 + rng.IntN(1+len(units)/(1+rng.IntN(20)))
			for given := 0; given < len(units); {
				ask := given + 1 + rng.IntN(most)
				if given = q.fill(ask); given < min(ask, len(units)) || given != len(q.out) {
					t.Fatalf("round %d, latest %v: %d units given, asked for %d of %d", round, latest, given, ask, len(units))
				}
				if !slices.Equal(q.out, want[:given]) {
					t.Fatalf("round %d, latest %v: the first %d of %d units come as\n%v\nwant\n%v", round, latest, given, len(units), q.out, want[:given])
				}
			}
		}
	}
}

func BenchmarkUnitQueue(b *testing.B) {
	const day = int64(86400e9)
	rng := rand.New(rand.NewPCG(63, 64))
	// Parts in the order of time, and parts in the order of a sort key
	// whose blocks each cover most of a day.
	var timed, keyed []scanUnit
	for p := 0; p < 490; p++ {
		at := int64(p/12) * day
		for blk := 0; blk < 125; blk++ {
			at += 1 + rng.Int64N(day/250)
			timed = append(timed, scanUnit{part: int32(p), block: int32(blk), first: at, last: at + 3})
			first := int64(p/12)*day + rng.Int64N(day/100)
			keyed = append(keyed, scanUnit{part: int32(p), block: int32(blk), first: first, last: int64(p/12+1)*day - rng.Int64N(day/100)})
		}
	}
	var q unitQueue
	for _, layout := range []struct {
		name  string
		units []scanUnit
	}{{"by time", timed}, {"by key", keyed}} {
		for _, take := range []int{64, 2048, len(layout.units)} {
			b.Run(fmt.Sprintf("%s/first %d of %d", layout.name, take, len(layout.units)), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					q.reset(layout.units, true)
					for given := 0; given < take; given += 64 {
						q.fill(given + 64)
					}
				}
			})
		}
		b.Run(layout.name+"/stable sort", func(b *testing.B) {
			work := make([]scanUnit, len(layout.units))
			for i := 0; i < b.N; i++ {
				copy(work, layout.units)
				sortUnits(work, true)
			}
		})
	}
}

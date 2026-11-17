package colstore

import (
	"cmp"
	"slices"
)

// unitRun is the units of one part that a merge has yet to take: at is the
// next one, and group ends the units that share its time.
type unitRun struct {
	units     []scanUnit
	at, group int
}

// orderUnits returns units in the order a stable sort by time gives them:
// by their first time when latest is unset, by their last time, latest
// first, when it is set. The units of a part follow each other and, in a
// part ordered by time, are in order already, so the parts are merged. The
// result is in out's room and shares none with units.
func orderUnits(units, out []scanUnit, latest bool) []scanUnit {
	key := func(u *scanUnit) int64 {
		if latest {
			return u.last
		}

		return u.first
	}
	var runs []unitRun
	for from := 0; from < len(units); {
		to := from + 1
		for to < len(units) && units[to].part == units[from].part {
			if key(&units[to]) < key(&units[to-1]) {
				return sortUnits(append(out[:0], units...), latest)
			}
			to++
		}
		runs = append(runs, unitRun{units: units[from:to]})
		from = to
	}
	for i := range runs {
		runs[i].start(latest)
	}
	// before reports whether run a gives its next unit before run b does.
	before := func(a, b int32) bool {
		ka, kb := key(&runs[a].units[runs[a].at]), key(&runs[b].units[runs[b].at])
		if ka != kb {
			return (ka < kb) != latest
		}

		return a < b
	}
	heap := make([]int32, 0, len(runs))
	down := func(i int) {
		for {
			least := i
			for c := 2*i + 1; c <= 2*i+2 && c < len(heap); c++ {
				if before(heap[c], heap[least]) {
					least = c
				}
			}
			if least == i {
				return
			}
			heap[i], heap[least] = heap[least], heap[i]
			i = least
		}
	}
	for i := range runs {
		heap = append(heap, int32(i))
	}
	for i := len(heap)/2 - 1; i >= 0; i-- {
		down(i)
	}
	out = out[:0]
	for len(heap) > 0 {
		r := &runs[heap[0]]
		out = append(out, r.units[r.at])
		if !r.next(latest) {
			heap[0] = heap[len(heap)-1]
			heap = heap[:len(heap)-1]
		}
		down(0)
	}

	return out
}

// start puts the run before its first unit in the order of the merge.
func (r *unitRun) start(latest bool) {
	if !latest {
		r.at, r.group = 0, len(r.units)

		return
	}
	r.group = len(r.units)
	r.at = r.groupStart(r.group)
}

// groupStart returns where the units that share the last time of the one
// before end begin.
func (r *unitRun) groupStart(end int) int {
	at := end - 1
	for at > 0 && r.units[at-1].last == r.units[end-1].last {
		at--
	}

	return at
}

// next moves to the unit after the one at hand and reports whether there
// is one. Latest first, the units that share a time keep their order.
func (r *unitRun) next(latest bool) bool {
	if r.at++; r.at < r.group {
		return true
	}
	if !latest {
		return false
	}
	end := r.groupStart(r.group)
	if end == 0 {
		return false
	}
	r.group = end
	r.at = r.groupStart(end)

	return true
}

func sortUnits(units []scanUnit, latest bool) []scanUnit {
	if latest {
		slices.SortStableFunc(units, func(a, b scanUnit) int { return cmp.Compare(b.last, a.last) })
	} else {
		slices.SortStableFunc(units, func(a, b scanUnit) int { return cmp.Compare(a.first, b.first) })
	}

	return units
}

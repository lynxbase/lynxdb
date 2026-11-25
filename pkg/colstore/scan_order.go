package colstore

import (
	"cmp"
	"slices"
)

// unitRun is a stretch of the units of a scan, as they are listed, that are
// in the order of time already or in its reverse: the units from at to
// end, end left out, taken upwards, or downwards when end is below at.
type unitRun struct {
	// key is what orders the unit at at.
	key     int64
	at, end int32
}

// heapShare is one in how many of its units a scan takes from runs that are
// about as many as the units before those left are sorted: taking a unit
// from so many runs costs several times its share of a sort.
const heapShare = 8

// unitQueue gives the units of a scan in the order that a stable sort by
// time gives them: by their first time, or when latest is set by their last
// time, latest first. It orders no more of them than the scan takes, since
// a scan by time often ends after few: the units are merged from the runs
// that are in order as they are listed, which are the parts of a table kept
// in the order of time and a block or two each of one kept in another.
type unitQueue struct {
	units  []scanUnit
	latest bool
	// out holds the units given so far, in order, in room for all.
	out []scanUnit
	// runs is a heap of the runs that hold units not given yet; its root
	// holds the next one. short is set when the runs were about as many as
	// the units.
	runs  []unitRun
	short bool
}

// key returns what orders unit i: its time, turned so that the unit to
// take first has the least.
func (q *unitQueue) key(i int) int64 {
	if q.latest {
		return ^q.units[i].last
	}

	return q.units[i].first
}

// before reports whether run a gives its next unit before run b does;
// units of one time come as they are listed.
func (a *unitRun) before(b *unitRun) bool {
	return a.key < b.key || (a.key == b.key && a.at < b.at)
}

// reset makes q give units, from the first one on.
func (q *unitQueue) reset(units []scanUnit, latest bool) {
	q.units, q.latest = units, latest
	q.out = slices.Grow(q.out[:0], len(units))
	runs := q.runs[:0]
	for i, n := 0, len(units); i < n; {
		j := i + 1
		if j < n && q.key(j) < q.key(i) {
			// Units of one time keep their order, so a run that is taken
			// downwards holds none.
			for j < n && q.key(j) < q.key(j-1) {
				j++
			}
			runs = append(runs, unitRun{key: q.key(j - 1), at: int32(j - 1), end: int32(i - 1)})
		} else {
			for j < n && q.key(j) >= q.key(j-1) {
				j++
			}
			runs = append(runs, unitRun{key: q.key(i), at: int32(i), end: int32(j)})
		}
		i = j
	}
	q.runs, q.short = runs, len(runs) > len(units)/4
	for i := len(runs)/2 - 1; i >= 0; i-- {
		q.down(i)
	}
}

// fill gives units until at least k are given or none is left, and returns
// how many are given.
func (q *unitQueue) fill(k int) int {
	n := len(q.units)
	k = min(k, n)
	if len(q.out) >= k {
		return len(q.out)
	}
	if q.short && k > n/heapShare {
		return q.sortRest()
	}
	for len(q.out) < k {
		r := &q.runs[0]
		q.out = append(q.out, q.units[r.at])
		if r.end > r.at {
			r.at++
		} else {
			r.at--
		}
		if r.at == r.end {
			last := len(q.runs) - 1
			q.runs[0] = q.runs[last]
			q.runs = q.runs[:last]
		} else {
			r.key = q.key(int(r.at))
		}
		q.down(0)
	}

	return len(q.out)
}

// sortRest gives every unit that is left and returns their number in all.
func (q *unitQueue) sortRest() int {
	given := len(q.out)
	for _, r := range q.runs {
		step := int32(1)
		if r.end < r.at {
			step = -1
		}
		for at := r.at; at != r.end; at += step {
			q.out = append(q.out, q.units[at])
		}
	}
	q.runs = q.runs[:0]
	latest := q.latest
	// The units are listed part by part and block by block.
	slices.SortFunc(q.out[given:], func(a, b scanUnit) int {
		ka, kb := a.first, b.first
		if latest {
			ka, kb = b.last, a.last
		}
		if c := cmp.Compare(ka, kb); c != 0 {
			return c
		}
		if c := cmp.Compare(a.part, b.part); c != 0 {
			return c
		}

		return cmp.Compare(a.block, b.block)
	})

	return len(q.out)
}

// down moves the run at i of the heap below the runs that give a unit
// before it does.
func (q *unitQueue) down(i int) {
	runs := q.runs
	if i >= len(runs) {
		return
	}
	run := runs[i]
	for {
		child := 2*i + 1
		if child >= len(runs) {
			break
		}
		if r := child + 1; r < len(runs) && runs[r].before(&runs[child]) {
			child = r
		}
		if !runs[child].before(&run) {
			break
		}
		runs[i] = runs[child]
		i = child
	}
	runs[i] = run
}

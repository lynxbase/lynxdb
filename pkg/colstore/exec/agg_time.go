package exec

import (
	"math"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// timeKey says how a count of rows by buckets of time computes one key of
// the aggregate: from the start of a bucket, for a function of the row time
// whose value is the same over every span of granule nanoseconds that
// starts at a multiple of granule, or from the block, for a column whose
// value is the same in every row of the block.
type timeKey struct {
	op      Op // OpHourOfDay or OpBin, zero for a column
	granule int64
}

// timeMiss says why a block of a count by buckets of time was counted row
// by row, or, as timeCounted, that its record counted it.
type timeMiss uint8

const (
	timeCounted timeMiss = iota
	// missFilter: the filter passes only some of the block's rows.
	missFilter
	// missRecord: the block has no record in buckets that divide the
	// granule of the time keys.
	missRecord
	// missKeys: a column key holds several values in the block.
	missKeys
	// missNulls: a column key holds a value in some of the block's rows
	// only.
	missNulls
	timeMisses
)

// timeScratch holds what byTime reuses: the rows of a block by bucket of
// time, and the values of the time keys of the buckets being added up and
// of the next bucket.
type timeScratch struct {
	counts    []part.TimeCount
	prev, cur []int64
}

// timeKeysOf returns how the keys of the query's aggregate are computed
// from the rows of blocks by bucket of time, or nil when they are not: the
// aggregate counts rows by at least one function of the row time,
// hour(_time) or bin(_time, width), and its other keys are columns.
func timeKeysOf(q *query) []timeKey {
	ap := q.agg
	if ap == nil || !ap.rowsOnly || len(ap.keys) == 0 {
		return nil
	}
	isTime := func(e *sexpr) bool {
		return e.op == OpColumn && e.kind == vec.KindInt && q.paths[e.path] == part.TimePath
	}
	keys := make([]timeKey, len(ap.keys))
	timed := false
	for k, e := range ap.keys {
		switch {
		case e.op == OpColumn && q.paths[e.path] != part.TimePath:
		case e.op == OpHourOfDay && isTime(e.args[0]):
			keys[k], timed = timeKey{op: OpHourOfDay, granule: hourNanos}, true
		case e.op == OpBin && isTime(e.args[0]) && e.args[1].op == OpLiteral && e.args[1].lit.Kind == vec.KindInt && e.args[1].lit.Int > 0:
			keys[k], timed = timeKey{op: OpBin, granule: e.args[1].lit.Int}, true
		default:
			return nil
		}
	}
	if !timed {
		return nil
	}
	for _, k := range keys {
		if k.op != 0 {
			q.timeGranule = gcd(q.timeGranule, k.granule)
		}
	}

	return keys
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}

	return a
}

// timeCut is what a filter asks of the rows of a count by buckets of
// time: times from lo to hi, both included, and untimed, the conjuncts of
// the filter besides those of the time, nil when there are none; path is
// that of the row time.
type timeCut struct {
	lo, hi  int64
	untimed *pred
	path    int
}

// planTimeCut sets q.cut when filter limits the row time, so that a block
// that the range cuts counts the rows of the buckets inside it, and reads
// the row times only for the buckets the range cuts.
func (q *query) planTimeCut(filter *Expr) error {
	cut := &timeCut{lo: math.MinInt64, hi: math.MaxInt64}
	var others []*Expr
	timed := false
	for _, c := range conjuncts(filter, nil) {
		switch c.Op {
		case OpEq, OpLt, OpLe, OpGt, OpGe:
			col, lit, op, ok := columnLiteral(c)
			if ok && lit.Kind == vec.KindInt && col.Path == part.TimePath && (col.Kind == vec.KindInt || col.Kind == KindAny) {
				lo, hi := timeBound(op, lit.Int)
				cut.lo, cut.hi, timed = max(cut.lo, lo), min(cut.hi, hi), true

				continue
			}
		}
		others = append(others, c)
	}
	if !timed {
		return nil
	}
	if len(others) > 0 {
		p, err := q.pred(And(others...), 0)
		if err != nil {
			return err
		}
		cut.untimed = p
	}
	cut.path = q.path(part.TimePath)
	q.cut = cut

	return nil
}

// cuts reports whether the time range of the query cuts through block b of
// p.
func (x *execution) cuts(p *part.Part, b *part.Block) bool {
	cut := x.q.cut
	if cut == nil {
		return false
	}
	_, minT, maxT := p.BlockStats(b.Index())

	return minT < cut.lo || maxT > cut.hi
}

// cutByTime counts the rows of the open block b of p, which the time range
// of the query cuts through, from its rows by bucket of time: every other
// conjunct of the filter must pass every row. It returns timeCounted when
// it did, and why not otherwise.
func (x *execution) cutByTime(w *worker, p *part.Part, b *part.Block) (timeMiss, error) {
	cut := x.q.cut
	if cut.untimed != nil {
		if w.decide(cut.untimed) == some {
			b.Ahead(w.ahead[:w.filtered])
		}
		if v, err := w.eval(cut.untimed, 0); err != nil || v != all {
			return missFilter, err
		}
	}
	rows, miss, err := w.agg.byTime(p, b.Index(), cut.lo, cut.hi)
	if miss != timeCounted || err != nil {
		return miss, err
	}
	w.byTime++
	if rows == 0 {
		x.empty(p, b)

		return timeCounted, nil
	}
	w.blocks++
	w.matched += int64(rows)

	return timeCounted, nil
}

// byTime adds every row of the open block, block i of p, whose time is from
// lo to hi to its group from the block's rows by bucket of time, and
// returns their number; the row times are read only when the range cuts a
// bucket. It returns timeCounted when it could, and why not otherwise: the
// block must count its rows in buckets that divide the granule of every
// time key, and every column key must hold one value in every row of the
// block, or none in any.
func (a *aggWorker) byTime(p *part.Part, i int, lo, hi int64) (uint64, timeMiss, error) {
	s, ts := a.s, &a.s.time
	var width int64
	ts.counts, width = p.TimeCounts(i, a.w.q.timeGranule, ts.counts[:0])
	if width == 0 {
		return 0, missRecord, nil
	}
	keys := a.w.q.timeKeys
	for k, e := range a.plan.keys {
		if keys[k].op != 0 {
			continue
		}
		pt := &s.parts[k]
		if err := a.classifyKey(e, pt, a.w.rows, nil); err != nil {
			return 0, missKeys, err
		}
		switch {
		case pt.nullable:
			return 0, missNulls, nil
		case pt.mode == partNull, pt.mode == partConst, pt.mode == partInts && pt.one:
		default:
			return 0, missKeys, nil
		}
	}
	// Buckets come in order, and those next to one another often give the
	// time keys the same values, whose rows are added at once.
	ts.prev, ts.cur = fit(ts.prev, len(keys)), fit(ts.cur, len(keys))
	rows, total := uint64(0), uint64(0)
	for _, c := range ts.counts {
		start, end := c.Bucket*width, c.Bucket*width+width-1
		if end < lo || start > hi {
			continue
		}
		n := uint64(c.Rows)
		if start < lo || end > hi {
			// The rows of a bucket share their time keys, whatever their time.
			var err error
			if n, err = a.w.timesWithin(max(lo, start), min(hi, end)); err != nil {
				return 0, missRecord, err
			}
			if n == 0 {
				continue
			}
		}
		same := rows > 0
		for k, tk := range keys {
			var v int64
			switch tk.op {
			case OpHourOfDay:
				v = hourOfDay(start)
			case OpBin:
				v = floorTo(start, tk.granule)
			default:
				continue
			}
			ts.cur[k], same = v, same && v == ts.prev[k]
		}
		if !same && rows > 0 {
			a.addRows(a.timeKey(ts.prev), rows)
			rows = 0
		}
		ts.prev, ts.cur = ts.cur, ts.prev
		rows += n
		total += n
	}
	if rows > 0 {
		a.addRows(a.timeKey(ts.prev), rows)
	}
	if a.groups > maxTableGroups {
		a.spill()
	}

	return total, timeCounted, nil
}

// timeKey encodes the key of rows whose time keys take vals, the column
// keys as byTime classified them.
func (a *aggWorker) timeKey(vals []int64) []byte {
	s := a.s
	key := s.key[:0]
	for k, tk := range a.w.q.timeKeys {
		p := &s.parts[k]
		switch {
		case tk.op != 0:
			key = appendKeyInt(key, vals[k])
		case p.mode == partNull:
			key = append(key, p.absent)
		case p.mode == partConst:
			key = appendKeyBytes(key, bytesTag(p.kind), p.val, k == len(s.parts)-1)
		default:
			key = appendKeyInt(key, p.lo)
		}
	}
	s.key = key

	return key
}

// timesWithin returns the number of rows of the open block whose time is
// from lo to hi.
func (w *worker) timesWithin(lo, hi int64) (uint64, error) {
	c, err := w.column(w.q.cut.path, vec.KindInt)
	if err != nil || c == nil {
		return 0, err
	}
	if c.Layout == vec.Const {
		if t := c.Ints[0]; t >= lo && t <= hi {
			return uint64(c.Rows), nil
		}

		return 0, nil
	}
	n := uint64(0)
	for _, t := range c.Ints[:c.Rows] {
		if t >= lo && t <= hi {
			n++
		}
	}

	return n, nil
}

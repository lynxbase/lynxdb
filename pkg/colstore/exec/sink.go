package exec

import (
	"bytes"
	"sort"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// sink receives the rows a plan produces on one goroutine: it keeps the
// best rows when the plan sorts with a limit and every row otherwise.
type sink struct {
	q    *query
	cols []colVec
	top  topK
	// bound is where the sink of a plan that keeps the first rows by time
	// tells the scan how far those reach.
	bound *atomic.Int64
	// raw is set when the sink takes every group as the aggregate leaves
	// it, for what the plan does above the aggregate to run afterwards.
	raw bool
}

func (s *sink) init(q *query) {
	if q.early {
		s.q, s.raw = q, true
		s.cols = make([]colVec, len(q.agg.names))

		return
	}
	s.initFinal(q)
}

// initFinal sets the sink up for the rows of the result.
func (s *sink) initFinal(q *query) {
	s.q = q
	s.cols = make([]colVec, len(q.names))
	if q.limit > 0 && len(q.order) > 0 {
		s.top = topK{k: q.limit, order: q.ranked}
	}
}

func (s *sink) held() int {
	n := 0
	for i := range s.cols {
		n += s.cols[i].held()
	}
	for _, row := range s.top.rows {
		for i := range row {
			n += cap(row[i].Bytes)
		}
	}

	return n
}

// add takes the rows of batch, one colVec per output column.
func (s *sink) add(batch []colVec) {
	n := batch[0].len()
	switch {
	case s.raw:
		for c := range batch {
			s.cols[c].appendAll(&batch[c])
		}
	case s.top.k > 0:
		s.top.offer(batch, n)
		s.tighten()
	case s.q.limit > 0 && s.cols[0].len()+n > s.q.limit:
		take := s.q.limit - s.cols[0].len()
		for c := range batch {
			for i := 0; i < take; i++ {
				s.cols[c].appendFrom(&batch[c], i)
			}
		}
	default:
		for c := range batch {
			s.cols[c].appendAll(&batch[c])
		}
	}
}

// tighten tells the scan the time that a row must reach to be kept, once
// the sink holds as many rows as the plan keeps: the time of the last of
// them, which rows of other goroutines can only move further.
func (s *sink) tighten() {
	if s.bound == nil || len(s.top.rows) < s.top.k {
		return
	}
	at := s.top.rows[0][s.q.timeCol].Int
	for {
		bound := s.bound.Load()
		if (s.q.byTime < 0 && at <= bound) || (s.q.byTime > 0 && at >= bound) || s.bound.CompareAndSwap(bound, at) {
			return
		}
	}
}

// kindRank orders values of different kinds: booleans, numbers, strings,
// variants; a string that holds a number ranks with the numbers.
func kindRank(k vec.Kind) int {
	switch k {
	case vec.KindBool:
		return 1
	case vec.KindInt, vec.KindFloat:
		return 2
	case vec.KindString:
		return 3
	case vec.KindVariant:
		return 4
	}

	return 5
}

func order[T int | int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

// compareValues orders two values that are not null, in the one order of
// every sort, extreme and pick: booleans, false first; then numbers and the
// strings that hold one, by the number, a number before a string of the
// same number and such strings by their bytes; then other strings by their
// bytes; then variants by their bytes. It is a total order.
func compareValues(a, b Value) int {
	if a.Kind == vec.KindInt && b.Kind == vec.KindInt {
		return order(a.Int, b.Int)
	}
	na, nb := a, b
	ra, rb := kindRank(a.Kind), kindRank(b.Kind)
	if a.Kind == vec.KindString {
		if n, ok := numberText(a.Bytes); ok {
			na, ra = n, 2
		}
	}
	if b.Kind == vec.KindString {
		if n, ok := numberText(b.Bytes); ok {
			nb, rb = n, 2
		}
	}
	if ra != rb {
		return order(ra, rb)
	}
	switch ra {
	case 1:
		if a.Bool == b.Bool {
			return 0
		}
		if b.Bool {
			return -1
		}

		return 1
	case 2:
		if c := compareNumbers(na, nb); c != 0 {
			return c
		}
		sa, sb := a.Kind == vec.KindString, b.Kind == vec.KindString
		if sa != sb {
			return order(btoi(sa), btoi(sb))
		}
		if !sa {
			return 0
		}
	}

	return bytes.Compare(a.Bytes, b.Bytes)
}

// compareNumbers orders two numbers; NaN orders after every other number.
func compareNumbers(a, b Value) int {
	if a.Kind == vec.KindInt && b.Kind == vec.KindInt {
		return order(a.Int, b.Int)
	}
	x, y := a.Float, b.Float
	if a.Kind == vec.KindInt {
		x = float64(a.Int)
	}
	if b.Kind == vec.KindInt {
		y = float64(b.Int)
	}
	if x != x || y != y {
		return order(btoi(x != x), btoi(y != y))
	}

	return order(x, y)
}

func btoi(b bool) int {
	if b {
		return 1
	}

	return 0
}

// compareOrdered orders two values by one sort column: nulls at their end,
// then by value in the column's direction.
func compareOrdered(a, b Value, o orderSpec) int {
	an, bn := a.IsNull(), b.IsNull()
	switch {
	case an && bn:
		return 0
	case an != o.nullsFirst && (an || bn):
		return 1
	case an || bn:
		return -1
	}
	c := compareValues(a, b)
	if o.desc {
		return -c
	}

	return c
}

// topK keeps the k rows that sort first, as a heap whose root sorts last.
type topK struct {
	k     int
	order []orderSpec
	rows  [][]Value
}

func (t *topK) compare(a, b []Value) int {
	for _, o := range t.order {
		if c := compareOrdered(a[o.col], b[o.col], o); c != 0 {
			return c
		}
	}

	return 0
}

// before reports whether row i of batch sorts before the kept row.
func (t *topK) before(batch []colVec, i int, row []Value) bool {
	for _, o := range t.order {
		if c := compareOrdered(batch[o.col].value(i), row[o.col], o); c != 0 {
			return c < 0
		}
	}

	return false
}

func (t *topK) offer(batch []colVec, n int) {
	first := t.order[0]
	lead := &batch[first.col]
	for i := 0; i < n; i++ {
		if len(t.rows) < t.k {
			row := make([]Value, len(batch))
			for c := range batch {
				copyValue(&row[c], batch[c].value(i))
			}
			t.rows = append(t.rows, row)
			t.up(len(t.rows) - 1)

			continue
		}
		root := t.rows[0]
		// Most rows lose against the root on the first sort column alone.
		if lead.kind == vec.KindInt && root[first.col].Kind == vec.KindInt {
			if v, r := lead.ints[i], root[first.col].Int; v != r && (v > r) != first.desc {
				continue
			}
		}
		if !t.before(batch, i, root) {
			continue
		}
		for c := range batch {
			copyValue(&root[c], batch[c].value(i))
		}
		t.down(0)
	}
}

// offerRow is offer for a row that another topK kept.
func (t *topK) offerRow(row []Value) {
	if len(t.rows) < t.k {
		t.rows = append(t.rows, row)
		t.up(len(t.rows) - 1)

		return
	}
	if t.compare(row, t.rows[0]) < 0 {
		t.rows[0] = row
		t.down(0)
	}
}

// copyValue stores v in dst, reusing dst's bytes.
func copyValue(dst *Value, v Value) {
	buf := dst.Bytes[:0]
	*dst = v
	dst.Bytes = nil
	if v.Kind == vec.KindString || v.Kind == vec.KindVariant {
		dst.Bytes = append(buf, v.Bytes...)
		if dst.Bytes == nil {
			dst.Bytes = []byte{}
		}
	}
}

func (t *topK) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if t.compare(t.rows[i], t.rows[parent]) <= 0 {
			return
		}
		t.rows[i], t.rows[parent] = t.rows[parent], t.rows[i]
		i = parent
	}
}

func (t *topK) down(i int) {
	for {
		child := 2*i + 1
		if child >= len(t.rows) {
			return
		}
		if r := child + 1; r < len(t.rows) && t.compare(t.rows[r], t.rows[child]) > 0 {
			child = r
		}
		if t.compare(t.rows[child], t.rows[i]) <= 0 {
			return
		}
		t.rows[i], t.rows[child] = t.rows[child], t.rows[i]
		i = child
	}
}

// columns turns the rows that gather returned into the columns of the
// result.
// gather returns the rows of the result that the sinks hold, in the plan's
// order and within its limit.
func gather(q *query, sinks []*sink) ([]colVec, int) {
	out := make([]colVec, len(q.names))
	switch {
	case q.limit > 0 && len(q.order) > 0:
		top := topK{k: q.limit, order: q.ranked}
		for _, s := range sinks {
			for _, row := range s.top.rows {
				top.offerRow(row)
			}
		}
		sort.Slice(top.rows, func(i, j int) bool { return top.compare(top.rows[i], top.rows[j]) < 0 })
		for _, row := range top.rows {
			for c := range out {
				out[c].appendValue(row[c])
			}
		}
	case len(sinks) == 1 && len(q.order) == 0:
		out = sinks[0].cols
	default:
		reserveAll(out, sinks)
		for _, s := range sinks {
			for c := range out {
				out[c].appendAll(&s.cols[c])
			}
		}
	}
	if len(q.order) > 0 && q.limit == 0 {
		out = sorted(q, out)
	}
	n := 0
	if len(out) > 0 {
		n = out[0].len()
	}
	if q.limit > 0 && n > q.limit {
		n = q.limit
		for c := range out {
			out[c].truncate(n)
		}
	}

	return out, n
}

// sorted returns the rows of cols in the query's order.
func sorted(q *query, cols []colVec) []colVec {
	n := cols[0].len()
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	sort.SliceStable(perm, func(i, j int) bool {
		a, b := perm[i], perm[j]
		for _, o := range q.order {
			if c := compareOrdered(cols[o.col].value(a), cols[o.col].value(b), o); c != 0 {
				return c < 0
			}
		}

		return false
	})
	out := make([]colVec, len(cols))
	for c := range cols {
		for _, i := range perm {
			out[c].appendFrom(&cols[c], i)
		}
	}

	return out
}

package exec

import (
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

const (
	slotUnknown uint8 = iota
	slotAbsent
	slotPresent
	slotDecoded
)

// selection is the rows of a block that a filter node selects, marked in
// mask or, when listed is set, listed in ascending order in list.
type selection struct {
	listed bool
	mask   []uint8
	list   []uint32
}

// scratch is the memory a worker keeps between queries.
type scratch struct {
	pathID []uint32
	pathOK []bool
	refs   []*part.ColumnRef
	state  []uint8
	cols   []vec.Column
	// used marks the columns that the query decoded.
	used []bool
	// views show the rows of a window of a shared block, a view per column
	// slot that viewed marks as set for the open window.
	views  []vec.Window
	viewed []bool

	sels   []selection
	ints   [][]int64
	valids [][]uint8
	table  []uint8
	idx    []uint32

	agg aggScratch
	row rowScratch
}

// worker is the state one scan goroutine owns for one query.
type worker struct {
	*scratch
	q     *query
	x     *execution
	arena *mem.Arena

	p    *part.Part
	b    *part.Block
	rows int
	// shared, when set, is the block of a range scan that columns come from
	// instead of b: its rows from from on, or all of them when whole is set.
	shared *colstore.SharedBlock
	from   int
	whole  bool
	// decoded records that the block had a column decoded.
	decoded bool

	blocks, matched, undecoded int64

	agg  aggWorker
	sink sink
}

var scratchPool mem.Pool[scratch]

func newWorker(x *execution) *worker {
	q := x.q
	s := scratchPool.Get()
	slots := len(q.paths) * kindSlots
	s.pathID = fit(s.pathID, len(q.paths))
	s.pathOK = fit(s.pathOK, len(q.paths))
	s.refs = fit(s.refs, slots)
	s.state = fit(s.state, slots)
	s.cols = fit(s.cols, slots)
	s.used = fit(s.used, slots)
	clear(s.used)
	s.sels = fit(s.sels, q.depth+3)
	s.ints = fit(s.ints, q.depth+3)
	s.valids = fit(s.valids, q.depth+3)
	// Emptied buffers tell held which of them the query went on to use.
	for i := range s.sels {
		s.sels[i].mask, s.sels[i].list = s.sels[i].mask[:0], s.sels[i].list[:0]
		s.ints[i], s.valids[i] = s.ints[i][:0], s.valids[i][:0]
	}
	s.table, s.idx = s.table[:0], s.idx[:0]
	s.agg.empty()
	for i := range s.row.batch {
		s.row.batch[i].reset()
	}
	w := &worker{scratch: s, q: q, x: x, arena: mem.NewArena()}
	w.sink.init(q)
	if q.agg != nil {
		w.agg.init(w)
	}

	return w
}

// release returns the worker's memory; nothing it produced may be used
// afterwards.
func (w *worker) release() {
	w.arena.Free()
	clear(w.refs)
	for i := range w.views {
		w.views[i].Drop()
	}
	scratchPool.Put(w.scratch)
	w.scratch = nil
}

// fit returns s with length n, keeping its elements and their buffers.
func fit[T any](s []T, n int) []T {
	if cap(s) < n {
		out := make([]T, n)
		copy(out, s[:cap(s)])

		return out
	}

	return s[:n]
}

// sized returns s with length n and unspecified contents.
func sized[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n, n+n/4)
	}

	return s[:n]
}

// used returns the capacity of a buffer that holds something, which a
// buffer emptied when the query began does only if the query filled it.
func used[T any](s []T) int {
	if len(s) == 0 {
		return 0
	}

	return cap(s)
}

// held returns the bytes of the buffers the query used.
func (w *worker) held() int {
	n := 0
	for i := range w.cols {
		if w.used[i] {
			n += w.cols[i].Retained()
		}
	}
	for i := range w.views {
		n += w.views[i].Retained()
	}
	for i := range w.sels {
		n += used(w.sels[i].mask) + 4*used(w.sels[i].list)
	}
	for i := range w.ints {
		n += 8*used(w.ints[i]) + used(w.valids[i])
	}

	return n + used(w.table) + 4*used(w.idx) + w.scratch.agg.held() + w.row.held()
}

// open points the worker at a block.
func (w *worker) open(p *part.Part, b *part.Block) {
	if p != w.p {
		w.p = p
		for i, name := range w.q.paths {
			w.pathID[i], w.pathOK[i] = p.PathID(name)
		}
	}
	w.b, w.rows, w.decoded = b, b.Rows, false
	clear(w.state)
}

// ref returns the block's column of kind at path, or nil when no row holds
// such a value.
func (w *worker) ref(path int, kind vec.Kind) *part.ColumnRef {
	s := path*kindSlots + int(kind) - 1
	if w.state[s] == slotUnknown {
		w.state[s] = slotAbsent
		if w.pathOK[path] {
			if ref, ok := w.b.Column(w.pathID[path], kind); ok && ref.Count > 0 {
				w.refs[s], w.state[s] = ref, slotPresent
			}
		}
	}
	if w.state[s] == slotAbsent {
		return nil
	}

	return w.refs[s]
}

// column decodes the block's column of kind at path once, and returns nil
// when no row holds such a value.
func (w *worker) column(path int, kind vec.Kind) (*vec.Column, error) {
	if w.shared != nil {
		return w.sharedColumn(path, kind), nil
	}
	ref := w.ref(path, kind)
	if ref == nil {
		return nil, nil
	}
	s := path*kindSlots + int(kind) - 1
	c := &w.cols[s]
	if w.state[s] != slotDecoded {
		if err := w.b.Decode(ref, c); err != nil {
			return nil, err
		}
		w.state[s], w.used[s], w.decoded = slotDecoded, true, true
	}

	return c, nil
}

// has reports whether a row of the open block holds a value of kind at path.
func (w *worker) has(path int, kind vec.Kind) bool {
	if w.shared != nil {
		return w.sharedColumn(path, kind) != nil
	}

	return w.ref(path, kind) != nil
}

// mask returns the mask buffer of a filter depth, one byte per row.
func (w *worker) mask(depth int) []uint8 {
	s := &w.sels[depth]
	s.mask = sized(s.mask, w.rows)
	s.listed = false

	return s.mask
}

// toMask makes s mark its rows in its mask.
func (w *worker) toMask(s *selection) {
	if !s.listed {
		return
	}
	s.mask = sized(s.mask, w.rows)
	vec.MaskFromIndexes(s.mask, s.list)
	s.listed = false
}

// hourNanos is the length of an hour in the unit of the row time.
const hourNanos = 3_600_000_000_000

func hourOfDay(t int64) int64 {
	if t >= 0 {
		return t / hourNanos % 24
	}
	h := t / hourNanos
	if t%hourNanos != 0 {
		h--
	}

	return (h%24 + 24) % 24
}

// evalInt computes an integer expression for n selected rows, the rows
// listed in idx or the first n when idx is nil. It returns one value per
// selected row, which may alias a decoded column, and a mask of the rows that
// have a value, nil when all do.
func (w *worker) evalInt(e *sexpr, n int, idx []uint32, depth int) (vals []int64, valid []uint8, err error) {
	switch e.op {
	case OpLiteral:
		out := sized(w.ints[depth], n)
		w.ints[depth] = out
		for i := range out {
			out[i] = e.lit.Int
		}

		return out, nil, nil
	case OpColumn:
		return w.intColumn(e, n, idx, depth)
	case OpHourOfDay:
		a, valid, err := w.evalInt(e.args[0], n, idx, depth+1)
		if err != nil {
			return nil, nil, err
		}
		out := sized(w.ints[depth], n)
		w.ints[depth] = out
		for i, t := range a[:n] {
			out[i] = hourOfDay(t)
		}

		return out, valid, nil
	}
	a, va, err := w.evalInt(e.args[0], n, idx, depth+1)
	if err != nil {
		return nil, nil, err
	}
	b, vb, err := w.evalInt(e.args[1], n, idx, depth+2)
	if err != nil {
		return nil, nil, err
	}
	out := sized(w.ints[depth], n)
	w.ints[depth] = out
	a, b = a[:n], b[:n]
	switch {
	case va != nil && vb != nil:
		valid = sized(w.valids[depth], n)
		w.valids[depth] = valid
		vec.MaskAnd(valid, va, vb)
	case va != nil:
		valid = va
	default:
		valid = vb
	}
	switch e.op {
	case OpAdd:
		for i := range out {
			out[i] = a[i] + b[i]
		}
	case OpSub:
		for i := range out {
			out[i] = a[i] - b[i]
		}
	case OpMul:
		for i := range out {
			out[i] = a[i] * b[i]
		}
	case OpDiv:
		valid = w.divide(out, a, b, valid, depth)
	}

	return out, valid, nil
}

// divide stores a/b into out and returns valid without the rows that divide
// by zero.
func (w *worker) divide(out, a, b []int64, valid []uint8, depth int) []uint8 {
	zero := false
	for i := range out {
		d := b[i]
		if d == 0 {
			zero = true
			out[i] = 0

			continue
		}
		// The quotient of the smallest integer and -1 does not fit.
		if d == -1 {
			out[i] = -a[i]

			continue
		}
		out[i] = a[i] / d
	}
	if !zero {
		return valid
	}
	n := len(out)
	own := sized(w.valids[depth], n)
	w.valids[depth] = own
	for i := range own {
		m := uint8(0xFF)
		if b[i] == 0 || (valid != nil && valid[i] == 0) {
			m = 0
		}
		own[i] = m
	}

	return own
}

func (w *worker) intColumn(e *sexpr, n int, idx []uint32, depth int) (vals []int64, valid []uint8, err error) {
	c, err := w.column(e.path, vec.KindInt)
	if err != nil {
		return nil, nil, err
	}
	if c == nil {
		out := sized(w.ints[depth], n)
		w.ints[depth] = out
		valid = sized(w.valids[depth], n)
		w.valids[depth] = valid
		clear(out)
		clear(valid)

		return out, valid, nil
	}
	if c.Valid != nil {
		valid = sized(w.valids[depth], n)
		w.valids[depth] = valid
		if idx == nil {
			vec.MaskFromBitmap(valid, c.Valid)
		} else {
			for i, r := range idx {
				valid[i] = uint8(0) - uint8(c.Valid[r>>6]>>(r&63)&1)
			}
		}
	}
	switch {
	case c.Layout == vec.Const:
		out := sized(w.ints[depth], n)
		w.ints[depth] = out
		for i := range out {
			out[i] = c.Ints[0]
		}

		return out, valid, nil
	case idx == nil:
		return c.Ints[:n], valid, nil
	}
	out := sized(w.ints[depth], n)
	w.ints[depth] = out
	src := c.Ints
	for i, r := range idx {
		out[i] = src[r]
	}

	return out, valid, nil
}

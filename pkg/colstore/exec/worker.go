package exec

import (
	"math/bits"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
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
	// ahead holds the path ids in the open part of the paths that the
	// filter reads, first, and of the others; filtered counts the first.
	ahead    []uint32
	filtered int
	refs     []*part.ColumnRef
	state    []uint8
	cols     []vec.Column
	// used marks the columns that the query decoded.
	used []bool
	// seen holds, per column slot, which rows of the open block hold a value
	// when that was decoded without the values, as seenOK marks.
	seen   []vec.Column
	seenOK []bool
	// codes holds, per path, the codes that the open block stores of the
	// string column at the path where it stores codes of the dictionary of
	// its part, one per row, as codesOK marks; packed holds the codes of a
	// column as the block has them, one per value.
	codes   [][]uint32
	codesOK []bool
	packed  []uint32
	// fresh holds the codes of a block whose strings a predicate has not
	// tested yet, odd those of them that their dictionary keeps as they
	// are, and spelled the strings that it tests.
	fresh   []uint32
	odd     []uint32
	spelled vec.Column
	// views show the rows of a window of a shared block, a view per column
	// slot that viewed marks as set for the open window.
	views  []vec.Window
	viewed []bool

	sels   []selection
	ints   [][]int64
	valids [][]uint8
	table  []uint8
	idx    []uint32
	// anyCol and idx2 serve a match over every string column of a block;
	// anyMay marks the columns whose token filter admits its pattern, and
	// anyRows holds the rows of the values of a column with gaps.
	anyCol  vec.Column
	idx2    []uint32
	anyMay  vec.Bitmap
	anyRows []uint32
	// anyString tests one string inside a variant against anyPat and
	// notes a match in anyHit; it is made once, so a value costs no
	// closure.
	anyString func(s []byte)
	anyPat    *tokenfilter.Pattern
	anyHit    bool

	// outs holds the buffers of the computed nodes of scalar expressions,
	// opers the operands of the nodes being computed and vals the values of
	// one row of them.
	outs  []scalarOut
	opers []operand
	vals  []Value

	agg aggScratch
	row rowScratch
	ord orderedScratch
}

// worker is the state one scan goroutine owns for one query.
type worker struct {
	// fns holds the functions of the query's OpFunc nodes, made for this
	// goroutine when it first computes them.
	fns []Func
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
	// place is the place in the table of the first row that the worker sees
	// of the open block, for plans that locate rows.
	place int64

	blocks, matched, undecoded int64
	partialCount               bool
	// searched and unsearched count the columns that matches over every
	// string column decoded and those that a token filter spared them.
	searched, unsearched int64
	// judged holds what the predicates of the query give for the strings of
	// the part dictionaries that the worker met; judgedColumns counts the
	// columns of blocks that it filtered by their codes, and ruledOut those
	// that the layout of their dictionary spared it.
	judged                  map[verdictKey]*verdicts
	judgedColumns, ruledOut int64
	// byTime counts the blocks whose rows were counted from their rows by
	// bucket of time, and timeMissed by reason those of a count by buckets
	// of time that were counted row by row.
	byTime     int64
	timeMissed [timeMisses]int64

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
	s.seen, s.seenOK = fit(s.seen, slots), fit(s.seenOK, slots)
	clear(s.seenOK)
	s.codes, s.codesOK, s.packed = fit(s.codes, len(q.paths)), fit(s.codesOK, len(q.paths)), s.packed[:0]
	clear(s.codesOK)
	for i := range s.codes {
		s.codes[i] = s.codes[i][:0]
	}
	s.outs, s.opers, s.vals = fit(s.outs, q.slots), s.opers[:0], s.vals[:0]
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
	if s.anyString == nil {
		s.anyString = func(v []byte) { s.anyHit = s.anyHit || s.anyPat.Match(v) }
	}
	w.sink.init(q)
	if x.first != nil {
		w.sink.first, w.sink.top.bound = x.first, &x.first.bound
	}
	if q.agg != nil {
		w.agg.init(w)
	}

	return w
}

// release returns the worker's memory; nothing it produced may be used
// afterwards.
func (w *worker) release() {
	if w.agg.table != nil {
		w.agg.table.Release()
	}
	w.agg.coded.release()
	w.arena.Free()
	clear(w.refs)
	for i := range w.views {
		w.views[i].Drop()
	}
	w.ord.drop()
	for i := range w.outs {
		w.outs[i].entries, w.outs[i].dict, w.outs[i].lit.Data = vec.Column{}, vec.Column{}, nil
	}
	clear(w.opers[:cap(w.opers)])
	clear(w.vals[:cap(w.vals)])
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
	for i := range w.seen {
		n += w.seen[i].Retained()
	}
	for i := range w.codes {
		n += 4 * used(w.codes[i])
	}
	n += 4*used(w.packed) + 4*cap(w.fresh) + 4*cap(w.odd) + w.spelled.Retained()
	for i := range w.outs {
		n += w.outs[i].held()
	}
	for i := range w.sels {
		n += used(w.sels[i].mask) + 4*used(w.sels[i].list)
	}
	for i := range w.ints {
		n += 8*used(w.ints[i]) + used(w.valids[i])
	}

	return n + used(w.table) + 4*used(w.idx) + w.anyCol.Retained() + 4*cap(w.idx2) + 8*cap(w.anyMay) + 4*cap(w.anyRows) + w.scratch.agg.held() + w.row.held()
}

// open points the worker at a block.
func (w *worker) open(p *part.Part, b *part.Block) {
	if p != w.p {
		w.p = p
		w.ahead, w.filtered = w.ahead[:0], 0
		for i, name := range w.q.paths {
			w.pathID[i], w.pathOK[i] = p.PathID(name)
			if w.pathOK[i] {
				w.ahead = append(w.ahead, w.pathID[i])
				if i < w.q.filtered {
					w.filtered++
				}
			}
		}
	}
	if w.x.ords != nil {
		w.place = int64(w.x.ords[p])<<44 | int64(b.Index())<<16
	}
	w.b, w.rows, w.decoded = b, b.Rows, false
	clear(w.state)
	clear(w.seenOK)
	clear(w.codesOK)
}

// dictionary returns the dictionary of the open part whose codes the open
// block stores in place of the strings at path, or nil when the block has
// the strings themselves or none.
func (w *worker) dictionary(path int) (*part.Dictionary, *part.ColumnRef) {
	if w.shared != nil {
		return nil, nil
	}
	ref := w.ref(path, vec.KindString)
	if ref == nil || !ref.Shared() {
		return nil, nil
	}
	d, ok := w.p.Dictionary(ref.PathID)
	if !ok {
		return nil, nil
	}

	return d, ref
}

// rowCodes returns the code that every row of the open block has in d, the
// dictionary of the string column at path, and which rows hold a value, nil
// when all do. A row without one has the code d.Len(), which no string has.
// It reads no string.
func (w *worker) rowCodes(path int, ref *part.ColumnRef, d *part.Dictionary) ([]uint32, vec.Bitmap, error) {
	s := path*kindSlots + int(vec.KindString) - 1
	if w.codesOK[path] {
		return w.codes[path], w.seen[s].Valid, nil
	}
	var err error
	if w.packed, err = w.b.DecodeShared(ref, &w.seen[s], w.packed); err != nil {
		return nil, nil, err
	}
	w.seenOK[s], w.decoded = true, true
	valid := w.seen[s].Valid
	if valid == nil {
		w.codes[path], w.packed = w.packed, w.codes[path]
	} else {
		out := sized(w.codes[path], w.rows)
		w.codes[path] = out
		none := uint32(d.Len())
		for i := range out {
			out[i] = none
		}
		v := 0
		for i, word := range valid {
			for ; word != 0 && v < len(w.packed); word &= word - 1 {
				if r := i<<6 | bits.TrailingZeros64(word); r < len(out) {
					out[r] = w.packed[v]
				}
				v++
			}
		}
	}
	w.codesOK[path] = true

	return w.codes[path], valid, nil
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

// presence returns which rows of the open block hold a value of kind at
// path, some of which do and some do not, without decoding the values.
func (w *worker) presence(path int, kind vec.Kind) (vec.Bitmap, error) {
	s := path*kindSlots + int(kind) - 1
	if w.state[s] == slotDecoded {
		return w.cols[s].Valid, nil
	}
	if !w.seenOK[s] {
		if err := w.b.DecodePresence(w.refs[s], &w.seen[s]); err != nil {
			return nil, err
		}
		w.seenOK[s], w.decoded = true, true
	}

	return w.seen[s].Valid, nil
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

		return out, w.ownValid(valid, depth), nil
	}
	a, va, err := w.evalInt(e.args[0], n, idx, depth+1)
	if err != nil {
		return nil, nil, err
	}
	// The second argument is computed in the buffers below the first one's,
	// which hold the mask the first got from an argument of its own.
	va = w.ownValid(va, depth+1)
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
		valid = w.divide(out, a, b, valid, depth, false)
	case OpMod:
		valid = w.divide(out, a, b, valid, depth, true)
	}

	return out, w.ownValid(valid, depth), nil
}

// ownValid returns valid in the buffer of depth, which the expressions
// computed afterwards at the same or a greater depth of another branch
// leave alone.
func (w *worker) ownValid(valid []uint8, depth int) []uint8 {
	if len(valid) == 0 || unsafe.SliceData(valid) == unsafe.SliceData(w.valids[depth]) {
		return valid
	}
	own := sized(w.valids[depth], len(valid))
	w.valids[depth] = own
	copy(own, valid)

	return own
}

// divide stores a/b into out, or its remainder when mod is set, and returns
// valid without the rows that divide by zero.
func (w *worker) divide(out, a, b []int64, valid []uint8, depth int, mod bool) []uint8 {
	zero := false
	for i := range out {
		d := b[i]
		switch {
		case d == 0:
			zero = true
			out[i] = 0
		case d == -1 && mod:
			out[i] = 0
		case d == -1:
			// The quotient of the smallest integer and -1 does not fit.
			out[i] = -a[i]
		case mod:
			out[i] = a[i] % d
		default:
			out[i] = a[i] / d
		}
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

package exec

import (
	"bytes"
	"context"
	"errors"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Ordered aggregation serves a plan whose filter holds string equalities on
// the leading sort key paths and whose group keys, followed by the argument
// of a distinct count if there is one, are the rest of the sort key. Whatever
// else the filter asks is evaluated on the rows of each block that the token
// filters do not rule out. The rows of a key range then arrive ordered by
// group, one stream per sorted run, so a merge of the streams finishes one
// group after another and holds no more than the group it is in.
//
// Parts that form one stream are read in ranges of whole blocks. For a plan
// that only counts rows and distinct values, the part index then tells how
// many groups and distinct values a block holds and whether its first row
// continues the row before it, so a block inside one group is counted without
// being read.

// errUnordered reports rows that the ordered aggregation cannot place: a
// group key or distinct argument that is missing or not a string.
var errUnordered = errors.New("exec: rows do not allow ordered aggregation")

const (
	// rangeRows is the number of rows a key range is cut to hold.
	rangeRows = 1 << 16
	// orderedBatch is the number of finished groups handed on at once.
	orderedBatch = 4096
)

// orderedChunk is the number of rows of a block that are split into groups
// at once, which bounds the states a cursor holds.
var orderedChunk = 4096

// orderedPlan says how a query's aggregation maps onto the sort key.
type orderedPlan struct {
	key *colstore.KeyPlan
	// parts lists the sort key positions the merge orders by: the group
	// keys in sort key order, then the distinct argument if any; groups is
	// the number of group keys among them.
	parts  []int
	groups int
	// keyOf maps each group key of the plan to its position in parts.
	keyOf   []int
	columns []colstore.ColumnID
	// slotKey and slotCol map a column slot to its sort key position or its
	// position in columns, -1 when it has none; rowsOnly marks the slots of
	// columns read for which rows hold a value, without the values.
	slotKey, slotCol []int16
	rowsOnly         []bool
	floats           []bool
	skipped          int64
	// estimated is the argument of an ApproxDistinct that follows the group
	// keys in the sort key, whose distinct values the order of the rows
	// then counts exactly; -1 without one.
	estimated int
	// residual is what the filter asks beyond the key prefix, or nil, and
	// tokens what a block with a row passing it must hold.
	residual *pred
	tokens   []colstore.ColumnTokens
	// prefix is the number of sort key columns the filter fixes.
	prefix int
	// blocks is set when the parts form one stream, which is read in ranges
	// of whole blocks: no block is then shared by readers that wait for one
	// another, and the token filters rule a block out before it is decoded.
	// byIndex is set when counts can also come from the part index.
	blocks, byIndex bool
}

// columns calls need for every column a filter node reads.
func (p *pred) columns(need func(path int, kinds uint8), exprs func(e *sexpr)) {
	switch p.kind {
	case predStrEq, predStr:
		need(p.path, kindBit(vec.KindString))
	case predNum:
		if p.onInt {
			need(p.path, kindBit(vec.KindInt))
		}
		if p.onFloat {
			need(p.path, kindBit(vec.KindFloat))
		}
	case predBool:
		need(p.path, kindBit(vec.KindBool))
	case predInts:
		exprs(p.left)
		exprs(p.right)
	case predExpr:
		exprs(p.left)
	case predHas:
		need(p.path, p.kinds)
	}
	for _, a := range p.args {
		a.columns(need, exprs)
	}
}

// anyColumn reports whether a filter node matches over every column.
func (p *pred) anyColumn() bool {
	for _, a := range p.args {
		if a.anyColumn() {
			return true
		}
	}

	return p.kind == predAny
}

// planOrdered returns how to aggregate in key order, or nil when the plan or
// the snapshot does not allow it.
func (q *query) planOrdered(snap *colstore.Snapshot, filter *Expr) *orderedPlan {
	ap := q.agg
	if snap == nil || filter == nil || len(snap.Parts()) == 0 {
		return nil
	}
	first := snap.Parts()[0]
	if !first.Ordered() {
		return nil
	}
	sortKey := first.SortKey()
	position := func(path string) int {
		for i, p := range sortKey {
			if p == path {
				return i
			}
		}

		return -1
	}
	equal := map[string][]byte{}
	source := map[string]*Expr{}
	var rest []*Expr
	for _, c := range conjuncts(filter, nil) {
		col, lit, op, ok := columnLiteral(c)
		if !ok || c.Op != OpEq || op != OpEq || lit.Kind != vec.KindString || (col.Kind != vec.KindString && col.Kind != KindAny) {
			rest = append(rest, c)

			continue
		}
		if _, dup := equal[col.Path]; dup {
			rest = append(rest, c)

			continue
		}
		equal[col.Path], source[col.Path] = lit.Bytes, c
	}
	prefix := 0
	filterKey := colstore.KeyFilter{}
	for prefix < len(sortKey) {
		v, ok := equal[sortKey[prefix]]
		if !ok {
			break
		}
		filterKey.Paths, filterKey.Values = append(filterKey.Paths, sortKey[prefix]), append(filterKey.Values, v)
		delete(source, sortKey[prefix])
		prefix++
	}
	if prefix == 0 {
		return nil
	}
	for _, c := range source {
		rest = append(rest, c)
	}
	var residual *pred
	var tokens []colstore.ColumnTokens
	if len(rest) != 0 {
		for _, c := range rest {
			tokens = conjunctTokens(c, tokens)
		}
		var err error
		// A match over every column needs the block's directory, which a
		// range scan does not hand out.
		if residual, err = q.pred(And(rest...), 0); err != nil || residual.anyColumn() {
			return nil
		}
	}

	o := &orderedPlan{groups: len(ap.keys), keyOf: make([]int, len(ap.keys)), floats: make([]bool, len(ap.args)),
		residual: residual, tokens: tokens, prefix: prefix, estimated: -1}
	for i, k := range ap.keys {
		if k.op != OpColumn || (k.kind != vec.KindString && k.kind != KindAny) {
			return nil
		}
		pos := position(q.paths[k.path])
		if pos < prefix || pos >= prefix+len(ap.keys) {
			return nil
		}
		o.keyOf[i] = pos - prefix
	}
	for i := 0; i < len(ap.keys); i++ {
		o.parts = append(o.parts, prefix+i)
	}
	seen := make([]bool, len(ap.keys))
	for _, k := range o.keyOf {
		if seen[k] {
			return nil
		}
		seen[k] = true
	}
	distinct := -1
	for i := range ap.args {
		arg := &ap.args[i]
		// Values that are not strings end the run in key order.
		next := arg.expr.op == OpColumn && (arg.expr.kind == vec.KindString || arg.expr.kind == KindAny) &&
			position(q.paths[arg.expr.path]) == prefix+len(ap.keys)
		if len(arg.distinct) != 0 && (distinct >= 0 || !next) {
			return nil
		}
		if sketched := ap.hllWord(i) >= 0; next && distinct < 0 && (sketched || len(arg.distinct) != 0) {
			distinct = i
			o.parts = append(o.parts, prefix+len(ap.keys))
			if sketched {
				o.estimated = i
			}
		}
		// The kind of a numeric aggregate must not depend on rows that a
		// group handed on earlier has not seen.
		for _, w := range arg.words {
			// A pick keeps the place of a row, whatever the row holds.
			if op := ap.words[w].op; op >= wordFirst && op <= wordArgMax {
				continue
			}
			if ap.words[w].op != wordCount && arg.kinds != kindBit(vec.KindInt) && arg.kinds != kindBit(vec.KindFloat) {
				return nil
			}
		}
		o.floats[i] = arg.kinds == kindBit(vec.KindFloat)
	}
	// Reading the whole sort key pays only when the plan uses all of it.
	if len(o.parts) == 0 || prefix+len(o.parts) != len(sortKey) || (len(ap.keys) == 0 && distinct < 0) {
		return nil
	}

	o.slotKey = make([]int16, len(q.paths)*kindSlots)
	o.slotCol = make([]int16, len(q.paths)*kindSlots)
	for s := range o.slotKey {
		o.slotKey[s], o.slotCol[s] = int16(position(q.paths[s/kindSlots])), -1
	}
	slot := func(path int, kinds uint8) {
		for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
			s := path*kindSlots + int(kind) - 1
			if kinds&kindBit(kind) == 0 || o.slotKey[s] >= 0 || o.slotCol[s] >= 0 {
				continue
			}
			o.slotCol[s] = int16(len(o.columns))
			o.columns = append(o.columns, colstore.ColumnID{Path: q.paths[path], Kind: kind})
		}
	}
	var need func(e *sexpr)
	need = func(e *sexpr) {
		if e.op == OpColumn {
			slot(e.path, e.kinds)
		}
		for _, a := range e.args {
			need(a)
		}
	}
	if residual != nil {
		residual.columns(slot, need)
	}
	for i := range ap.args {
		if arg := &ap.args[i]; (len(arg.words) != 0 || len(arg.states) != 0) && !arg.placed {
			need(arg.expr)
		}
	}
	for i := range ap.orders {
		need(ap.orders[i].expr)
	}
	if ap.timePath >= 0 {
		slot(ap.timePath, kindBit(vec.KindInt))
	}
	// A pick and a count need the rows that hold a value, not the values.
	o.rowsOnly = make([]bool, len(o.slotCol))
	for i := range ap.args {
		arg := &ap.args[i]
		for kind := vec.KindBool; arg.placed && kind <= vec.KindVariant; kind++ {
			s := arg.expr.path*kindSlots + int(kind) - 1
			if arg.kinds&kindBit(kind) == 0 || o.slotKey[s] >= 0 || o.slotCol[s] >= 0 {
				continue
			}
			o.slotCol[s], o.rowsOnly[s] = int16(len(o.columns)), true
			o.columns = append(o.columns, colstore.ColumnID{Path: q.paths[arg.expr.path], Kind: kind, Presence: true})
		}
	}

	group := make([]string, len(o.parts))
	for i, pos := range o.parts {
		group[i] = sortKey[pos]
	}
	key, ok := snap.PlanKeyRanges(filterKey, group)
	if !ok || tokenShare(key, tokens) < orderedTokenShare {
		return nil
	}
	o.key = key
	o.byIndex = residual == nil && key.Streams() == 1
	for i, w := range ap.words {
		o.byIndex = o.byIndex && (w.op == wordRows || w.op == wordGroup || (o.estimated >= 0 && i == ap.hllWord(o.estimated)))
	}
	o.blocks = key.Streams() == 1
	for _, h := range snap.Parts() {
		o.skipped += int64(h.NumBlocks())
	}
	o.skipped -= int64(key.Blocks)

	return o
}

// orderedTokenShare is the least share of a key range's blocks that the
// token filters must admit for the aggregation to run in key order; below it
// a hashing scan runs. Measured on 1m events in 66 blocks: when the tokens
// rule out no block the two take the same time within 5%, and when 2 blocks
// are left the hashing scan is 2.4 times faster, because it decodes the
// filter's columns of a block before any key column. The crossover between
// those two points is not measured.
const orderedTokenShare = 0.5

// tokenShare estimates, from a sample of token filters, the share of the
// plan's blocks that may hold every token.
func tokenShare(key *colstore.KeyPlan, tokens []colstore.ColumnTokens) float64 {
	if len(tokens) == 0 || key.Blocks == 0 {
		return 1
	}
	const sample = 64
	step := max(1, key.Blocks/sample)
	probed, kept, skip := 0, 0, 0
	for _, s := range key.Spans() {
		b := s.First + skip
		for ; b < s.End; b += step {
			probed++
			// A filter that cannot be read fails the query when its block is.
			if ok, err := mayHold(s.Part, b, tokens); ok || err != nil {
				kept++
			}
		}
		skip = b - s.End
	}
	if probed == 0 {
		return 1
	}

	return float64(kept) / float64(probed)
}

// mayHold reports whether the token filters of block i of p admit every
// set of tokens in its column.
func mayHold(p *part.Part, i int, tokens []colstore.ColumnTokens) (bool, error) {
	return colstore.BlockMayContain(p, i, tokens)
}

// window points the worker at rows from to to of a block of a range scan,
// which the filter and the aggregates then see as rows 0 to to-from.
func (w *worker) window(sb *colstore.SharedBlock, from, to int) {
	w.shared, w.rows, w.from, w.whole = sb, to-from, from, from == 0 && to == sb.Rows
	w.views, w.viewed = fit(w.views, len(w.state)), fit(w.viewed, len(w.state))
	clear(w.viewed)
}

// sharedColumn returns the rows of the open window that a column slot holds
// in the block of a range scan.
func (w *worker) sharedColumn(path int, kind vec.Kind) *vec.Column {
	o := w.q.ordered
	s := path*kindSlots + int(kind) - 1
	var c *vec.Column
	if k := o.slotKey[s]; k >= 0 {
		c = w.shared.Keys.Column(int(k), kind)
	} else if i := o.slotCol[s]; i >= 0 {
		c = w.shared.Column(int(i))
	}
	if c == nil || w.whole {
		return c
	}
	v := &w.views[s]
	if !w.viewed[s] {
		if o.rowsOnly[s] {
			v.SetPresence(c, w.from, w.from+w.rows)
		} else {
			v.Set(c, w.from, w.from+w.rows)
		}
		w.viewed[s] = true
	}

	return &v.Column
}

// orderedGroup is a group of the merge: its key values, its state and, for a
// distinct count, the number of values and the first and last of them.
type orderedGroup struct {
	set         bool
	key         [][]byte
	words       []int64
	users       int64
	first, last []byte
}

func (g *orderedGroup) copyFrom(src *orderedGroup) {
	g.set, g.users = src.set, src.users
	g.key = fit(g.key, len(src.key))
	for i := range src.key {
		g.key[i] = append(g.key[i][:0], src.key[i]...)
	}
	g.words = append(g.words[:0], src.words...)
	g.first, g.last = append(g.first[:0], src.first...), append(g.last[:0], src.last...)
}

// orderedEdges holds the first and the last group of a key range, which may
// continue in the ranges next to it.
type orderedEdges struct {
	// lo is where the range starts and seq its number among the ranges that
	// start there, which order the ranges of a scan.
	lo          []byte
	seq         int
	first, last orderedGroup
	// heap holds the states of variable size of the two groups.
	heap *stateHeap
}

// orderedCursor walks one stream of a key range run by run.
type orderedCursor struct {
	r    colstore.RangeReader
	cols []*vec.Column
	// starts holds the first row of each run of rows with one key, group the
	// run of rows with one group key that each belongs to, and words the
	// states of those.
	starts []uint32
	group  []uint32
	words  []int64
	i, n   int
	cur    [][]byte
	// at and to bound the rows of the block that are yet to be loaded.
	at, to int
	// repeat tells that the first row loaded continues the distinct value
	// of the row before it in the stream.
	repeat bool
	// place is the place in the table of the first row of the block, for
	// plans that locate rows.
	place int64
	// counted is set for a block taken from the part index as one run with
	// the group in key and values distinct values.
	counted bool
	key     [][]byte
	values  int64
}

func (c *orderedCursor) load() {
	row := int(c.starts[c.i])
	for p, col := range c.cols {
		c.cur[p] = col.Bytes(row)
	}
}

// orderedScratch is the memory a worker keeps between queries for
// aggregation in key order.
type orderedScratch struct {
	cursors []*orderedCursor
	heap    []*orderedCursor

	// open is the group the merge is in and pending the one before it,
	// which is the last of the range unless another group follows.
	open, pending *orderedGroup

	// The finished groups not yet handed to the sink.
	states []int64
	users  []int64
	count  int
	batch  []colVec
	post   postEval
	having havingEval

	bound []uint8
	gid   []uint32
	// edges holds the first and last group of every range the worker read,
	// ranges of them in this query.
	edges  []*orderedEdges
	ranges int
}

func (s *orderedScratch) held() int {
	n := 8*(cap(s.states)+cap(s.users)) + cap(s.bound) + 4*cap(s.gid) + s.post.held() + s.having.held()
	for _, c := range s.cursors {
		n += 4*(cap(c.starts)+cap(c.group)) + 8*cap(c.words)
		for _, k := range c.key {
			n += cap(k)
		}
	}
	for i := range s.batch {
		n += s.batch[i].held()
	}

	return n
}

// drop forgets the blocks and ranges of the scan the scratch last served.
func (s *orderedScratch) drop() {
	for _, c := range s.cursors {
		c.r = colstore.RangeReader{}
		clear(c.cols)
		clear(c.cur)
	}
	clear(s.heap[:cap(s.heap)])
	for _, e := range s.edges {
		e.lo = nil
	}
}

// orderedWorker is the state one goroutine owns while it merges key ranges.
type orderedWorker struct {
	*orderedScratch
	w    *worker
	o    *orderedPlan
	scan *colstore.RangeScan
	turn int
	// byIndex and byToken count the blocks counted from the part index and
	// the ones the token filters ruled out.
	byIndex, byToken int64
}

func newOrderedWorker(w *worker, o *orderedPlan, scan *colstore.RangeScan, turn int) *orderedWorker {
	s := &w.ord
	if s.open == nil {
		s.open, s.pending = &orderedGroup{}, &orderedGroup{}
	}
	s.states, s.users, s.count, s.ranges = s.states[:0], s.users[:0], 0, 0
	s.batch = fit(s.batch, len(w.q.agg.names))
	for i := range s.batch {
		s.batch[i].reset()
	}

	return &orderedWorker{orderedScratch: s, w: w, o: o, scan: scan, turn: turn}
}

// edgesOf returns the edges to fill for the range kr, which the worker reads
// next.
func (ow *orderedWorker) edgesOf(kr *colstore.KeyRange, seq int) *orderedEdges {
	if ow.ranges == len(ow.edges) {
		ow.edges = append(ow.edges, &orderedEdges{})
	}
	e := ow.edges[ow.ranges]
	ow.ranges++
	e.lo, e.seq, e.first.set, e.last.set = kr.Lo, seq, false, false
	e.heap = &ow.w.agg.heap

	return e
}

// markChanges sets bound[j] for every row from+j, j above zero, whose value
// in col differs from the row before it.
func markChanges(bound []uint8, col *vec.Column, from int) {
	switch {
	case col.Layout == vec.Const:
	case col.Layout == vec.Dict && len(col.Codes8) != 0:
		codes := col.Codes8[from : from+len(bound)]
		for j := 1; j < len(codes); j++ {
			if codes[j] != codes[j-1] {
				bound[j] = 1
			}
		}
	case col.Layout == vec.Dict:
		codes := col.Codes16[from : from+len(bound)]
		for j := 1; j < len(codes); j++ {
			if codes[j] != codes[j-1] {
				bound[j] = 1
			}
		}
	default:
		offs, data := col.Offsets[from:from+len(bound)+1], col.Data
		prev := data[offs[0]:offs[1]]
		for j := 1; j < len(bound); j++ {
			v := data[offs[j]:offs[j+1]]
			if string(v) != string(prev) {
				bound[j] = 1
			}
			prev = v
		}
	}
}

// markChangesAt is markChanges for the rows from plus each of rows.
func markChangesAt(bound []uint8, col *vec.Column, from int, rows []uint32) {
	switch col.Layout {
	case vec.Const:
	case vec.Dict:
		for j := 1; j < len(rows); j++ {
			if col.Code(from+int(rows[j])) != col.Code(from+int(rows[j-1])) {
				bound[j] = 1
			}
		}
	default:
		for j := 1; j < len(rows); j++ {
			if string(col.Bytes(from+int(rows[j]))) != string(col.Bytes(from+int(rows[j-1]))) {
				bound[j] = 1
			}
		}
	}
}

// selected evaluates the rest of the filter on the open window and returns
// the rows that pass it, counted from the start of the window; every is set
// when all of them do.
func (ow *orderedWorker) selected() (rows []uint32, every bool, err error) {
	w := ow.w
	v, err := w.eval(ow.o.residual, 0)
	if err != nil || v == none {
		return nil, false, err
	}
	if v == all {
		return nil, true, nil
	}
	s := &w.sels[0]
	rows = s.list
	if !s.listed {
		w.idx = vec.MaskIndexes(w.idx[:0], s.mask)
		rows = w.idx
	}

	return rows, len(rows) == w.rows, nil
}

// fromIndex takes block i of p, every row of which is inside the range, from
// the part index as one run, and reports false unless the index proves that
// the block holds one group with strings for its keys and distinct values.
func (ow *orderedWorker) fromIndex(c *orderedCursor, p *part.Part, i int) bool {
	w, o := ow.w, ow.o
	depth := o.prefix + o.groups
	if p.KeyRuns(i, depth) != 1 {
		return false
	}
	first := p.FirstKey(i)
	for pos := o.prefix; pos < depth; pos++ {
		if part.KeyKind(first, pos) != vec.KindString {
			return false
		}
	}
	c.values = 0
	if len(o.parts) > o.groups {
		_, last := p.KeyRange()
		if i+1 < p.NumBlocks() {
			last = p.FirstKey(i + 1)
		}
		// The values between two strings of one group are strings.
		if part.KeyKind(first, depth) != vec.KindString || part.KeyKind(last, depth) != vec.KindString ||
			part.SharedKeyColumns(first, last) < depth {
			return false
		}
		c.values = int64(p.KeyRuns(i, depth+1))
		if c.repeat {
			c.values--
		}
	}
	c.key = fit(c.key, o.groups)
	for g := 0; g < o.groups; g++ {
		c.key[g], _ = part.KeyString(c.key[g][:0], first, o.prefix+g)
		c.cur[g] = c.key[g]
	}
	rows, _, _ := p.BlockStats(i)
	c.words = append(c.words[:0], w.q.agg.init...)
	c.words[w.q.agg.rows] = int64(rows)
	c.starts, c.group = append(c.starts[:0], 0), append(c.group[:0], 0)
	c.i, c.n, c.counted = 0, 1, true
	w.blocks++
	w.matched += int64(rows)
	ow.byIndex++

	return true
}

// next loads the next rows of c that are in the range and pass the filter,
// at most orderedChunk of a block at once, splits them into runs and
// computes the states of their groups; it reports false at the end of the
// stream.
func (ow *orderedWorker) next(c *orderedCursor, b *part.Block) (bool, error) {
	o := ow.o
	for {
		if c.at < c.to {
			if ok, err := ow.chunk(c); ok || err != nil {
				return ok, err
			}

			continue
		}
		p, i, whole, ok := c.r.Peek()
		if !ok {
			return false, nil
		}
		if o.byIndex {
			c.repeat = c.r.Shared() >= o.prefix+len(o.parts)
			if whole && ow.fromIndex(c, p, i) {
				c.r.Skip()

				return true, nil
			}
		}
		if len(o.tokens) != 0 {
			may, err := mayHold(p, i, o.tokens)
			if err != nil {
				return false, err
			}
			if !may {
				ow.byToken++
				c.r.Skip()

				continue
			}
		}
		if ok, err := c.r.Read(b); err != nil {
			return false, err
		} else if !ok {
			continue
		}
		from, to := c.r.From, c.r.To
		for p, pos := range o.parts {
			col := c.r.Block.Keys.Column(pos, vec.KindString)
			if col == nil || (col.Valid != nil && col.Valid.Rank(to)-col.Valid.Rank(from) != to-from) {
				return false, errUnordered
			}
			c.cols[p] = col
		}
		c.at, c.to = from, to
		c.counted, c.repeat = false, c.repeat && from == 0
		if ords := ow.w.x.ords; ords != nil {
			c.place = int64(ords[p])<<44 | int64(i)<<16
		}
		ow.w.blocks++
	}
}

// sameKey reports whether rows a and b of c's block agree in every key
// column the merge orders by.
func (c *orderedCursor) sameKey(a, b int) bool {
	for _, col := range c.cols {
		switch col.Layout {
		case vec.Const:
		case vec.Dict:
			if col.Code(a) != col.Code(b) {
				return false
			}
		default:
			if string(col.Bytes(a)) != string(col.Bytes(b)) {
				return false
			}
		}
	}

	return true
}

// chunk loads the next rows of the block c is in, and reports false when
// none of them passes the filter.
func (ow *orderedWorker) chunk(c *orderedCursor) (bool, error) {
	w, o := ow.w, ow.o
	stride := w.agg.stride
	from, to := c.at, min(c.to, c.at+orderedChunk)
	// The index speaks for the first row of a block; a later chunk looks at
	// the row before it.
	if o.byIndex && from > c.r.From {
		c.repeat = c.sameKey(from-1, from)
	}
	c.at = to
	n := to - from
	w.window(c.r.Block, from, to)
	w.place = c.place + int64(from)
	var picked []uint32
	if o.residual != nil {
		rows, every, err := ow.selected()
		if err != nil || (!every && len(rows) == 0) {
			return false, err
		}
		if !every {
			picked, n = rows, len(rows)
		}
	}
	ow.bound = sized(ow.bound, n)
	bound := ow.bound
	clear(bound)
	bound[0] = 1
	for p := 0; p < o.groups; p++ {
		if picked != nil {
			markChangesAt(bound, c.cols[p], from, picked)
		} else {
			markChanges(bound, c.cols[p], from)
		}
	}
	ow.gid = sized(ow.gid, n)
	gid := ow.gid
	g := uint32(0)
	for j, m := range bound {
		g += uint32(m)
		gid[j] = g - 1
	}
	if len(o.parts) > o.groups {
		if picked != nil {
			markChangesAt(bound, c.cols[o.groups], from, picked)
		} else {
			markChanges(bound, c.cols[o.groups], from)
		}
	}
	c.starts, c.group = c.starts[:0], c.group[:0]
	for j, m := range bound {
		if m == 0 {
			continue
		}
		row := uint32(from + j)
		if picked != nil {
			row = uint32(from) + picked[j]
		}
		c.starts = append(c.starts, row)
		c.group = append(c.group, gid[j])
	}
	c.words = sized(c.words, int(g)*stride)
	for i := 0; i < int(g); i++ {
		copy(c.words[i*stride:], w.q.agg.init)
	}
	w.matched += int64(n)
	if err := w.agg.feed(&target{words: c.words, gid: gid}, n, picked); err != nil {
		return false, err
	}
	c.i, c.n = 0, len(c.starts)
	c.load()

	return true, nil
}

func (ow *orderedWorker) less(a, b *orderedCursor) bool {
	for p := range a.cur {
		if c := bytes.Compare(a.cur[p], b.cur[p]); c != 0 {
			return c < 0
		}
	}

	return false
}

func (ow *orderedWorker) down(i int) {
	h := ow.heap
	for {
		l := 2*i + 1
		if l >= len(h) {
			return
		}
		if r := l + 1; r < len(h) && ow.less(h[r], h[l]) {
			l = r
		}
		if !ow.less(h[l], h[i]) {
			return
		}
		h[i], h[l] = h[l], h[i]
		i = l
	}
}

// take adds the current run of c to the open group, closing that one first
// when the run belongs to another group.
func (ow *orderedWorker) take(c *orderedCursor, edges *orderedEdges) {
	o, plan := ow.o, ow.w.q.agg
	stride := ow.w.agg.stride
	run := c.group[c.i]
	// Only the first run of a group in a block can start a group or carry
	// its state.
	fresh := c.i == 0 || c.group[c.i-1] != run
	open := ow.open
	if fresh {
		same := open.set
		for p := 0; same && p < o.groups; p++ {
			same = string(c.cur[p]) == string(open.key[p])
		}
		state := c.words[int(run)*stride : (int(run)+1)*stride]
		if same {
			heap := &ow.w.agg.heap
			plan.merge(heap, open.words, heap, state)
			plan.releaseStates(heap, state)
		} else {
			if open.set {
				ow.close(edges)
				open = ow.open
			}
			open.set, open.users = true, 0
			open.key = fit(open.key, o.groups)
			for p := 0; p < o.groups; p++ {
				open.key[p] = append(open.key[p][:0], c.cur[p]...)
			}
			open.words = append(open.words[:0], state...)
			open.first, open.last = open.first[:0], open.last[:0]
		}
	}
	switch {
	case len(o.parts) == o.groups:
		return
	case c.counted:
		open.users += c.values

		return
	case o.byIndex:
		// The index says whether a block starts with a value already counted;
		// every later run of a stream in key order brings another.
		if c.i > 0 || !c.repeat {
			open.users++
		}

		return
	}
	v := c.cur[o.groups]
	if open.users == 0 {
		open.users, open.first = 1, append(open.first[:0], v...)
		open.last = append(open.last[:0], v...)
	} else if string(v) != string(open.last) {
		open.users++
		open.last = append(open.last[:0], v...)
	}
}

// close ends the open group: the first group of a range is kept as its
// edge, and each later one waits until another group shows that it was not
// the last.
func (ow *orderedWorker) close(edges *orderedEdges) {
	switch {
	case !edges.first.set:
		edges.first.copyFrom(ow.open)
	default:
		if ow.pending.set {
			ow.emit(ow.pending)
		}
		ow.pending, ow.open = ow.open, ow.pending
	}
	ow.open.set = false
}

// emitRuns queues runs from to end of c, each of which is a whole group.
func (ow *orderedWorker) emitRuns(c *orderedCursor, from, end int) {
	stride := ow.w.agg.stride
	for from < end {
		to := min(end, from+orderedBatch-ow.count)
		for i, k := range ow.o.keyOf {
			col, dst := c.cols[k], &ow.batch[i]
			for run := from; run < to; run++ {
				dst.appendBytes(vec.KindString, col.Bytes(int(c.starts[run])))
			}
		}
		ow.states = append(ow.states, c.words[from*stride:to*stride]...)
		if ow.count += to - from; ow.count >= orderedBatch {
			ow.flush()
		}
		from = to
	}
}

// emit queues a finished group for the sink.
func (ow *orderedWorker) emit(g *orderedGroup) {
	plan := ow.w.q.agg
	for i, k := range ow.o.keyOf {
		ow.batch[i].appendBytes(vec.KindString, g.key[k])
	}
	ow.states = append(ow.states, g.words...)
	if plan.group >= 0 {
		ow.states[len(ow.states)-len(g.words)+plan.group] = int64(ow.count)
	}
	ow.users = append(ow.users, g.users)
	if ow.count++; ow.count >= orderedBatch {
		ow.flush()
	}
}

// flush computes the columns of the queued groups and hands them on.
func (ow *orderedWorker) flush() {
	if ow.count == 0 {
		return
	}
	q := ow.w.q
	plan := q.agg
	keys := len(plan.keys)
	distinct := make([][]int64, plan.distinct)
	for i := range distinct {
		distinct[i] = ow.users
	}
	for i := range plan.outs {
		if out := &plan.outs[i]; out.fn == ApproxDistinct && out.arg == ow.o.estimated {
			copy(ow.batch[keys+i].bulkInts(ow.count), ow.users)

			continue
		}
		plan.column(&plan.outs[i], ow.states, &ow.w.agg.heap, ow.count, ow.o.floats, distinct, &ow.batch[keys+i])
	}
	out, n := ow.batch, ow.count
	if q.having != nil && !q.early {
		out = ow.having.run(q, out, n)
		n = out[0].len()
	}
	if q.post != nil && !q.early {
		out = ow.post.run(q, out, n)
	}
	ow.w.sink.add(out)
	if len(plan.states) != 0 {
		stride := len(plan.words)
		for g := 0; g < ow.count; g++ {
			plan.releaseStates(&ow.w.agg.heap, ow.states[g*stride:(g+1)*stride])
		}
	}
	for i := range ow.batch[:keys] {
		ow.batch[i].reset()
	}
	ow.states, ow.users, ow.count = ow.states[:0], ow.users[:0], 0
}

// merge aggregates one key range, leaving its first and last group in edges.
func (ow *orderedWorker) merge(kr *colstore.KeyRange, edges *orderedEdges, b *part.Block) error {
	streams := len(kr.Streams)
	for len(ow.cursors) < streams {
		ow.cursors = append(ow.cursors, &orderedCursor{})
	}
	ow.heap = ow.heap[:0]
	ow.open.set, ow.pending.set = false, false
	// With one stream left and no distinct count, every run but the first
	// and the last of a block is a whole group.
	whole := len(ow.o.parts) == ow.o.groups
	// Workers begin at different streams, so that those whose ranges share
	// blocks decode different ones instead of waiting for one another.
	for s := 0; s < streams; s++ {
		c := ow.cursors[(s+ow.turn)%streams]
		c.cols, c.cur = fit(c.cols, len(ow.o.parts)), fit(c.cur, len(ow.o.parts))
		c.r.Reset(ow.scan, kr, (s+ow.turn)%streams)
		c.at, c.to = 0, 0
		ok, err := ow.next(c, b)
		if err != nil {
			return err
		}
		if ok {
			ow.heap = append(ow.heap, c)
		}
	}
	ow.turn++
	for i := len(ow.heap)/2 - 1; i >= 0; i-- {
		ow.down(i)
	}
	for len(ow.heap) > 0 {
		c := ow.heap[0]
		ow.take(c, edges)
		if whole && len(ow.heap) == 1 && c.i == 0 && c.n > 2 {
			ow.close(edges)
			ow.emitRuns(c, 1, c.n-1)
			c.i = c.n - 2
		}
		if c.i++; c.i < c.n {
			c.load()
		} else {
			ok, err := ow.next(c, b)
			if err != nil {
				return err
			}
			if !ok {
				last := len(ow.heap) - 1
				ow.heap[0] = ow.heap[last]
				ow.heap = ow.heap[:last]
			}
		}
		if len(ow.heap) > 1 {
			ow.down(0)
		}
	}
	if ow.open.set {
		ow.close(edges)
	}
	if ow.pending.set {
		edges.last.copyFrom(ow.pending)
	}
	ow.flush()

	return nil
}

// runOrdered executes an aggregation in key order; it fails with errUnordered
// when the rows turn out not to allow it.
func (x *execution) runOrdered(ctx context.Context, snap *colstore.Snapshot) (*Result, error) {
	q := x.q
	o := q.ordered
	workers := len(x.workers)
	ranges := max(4*workers, o.key.Rows/rangeRows)
	var scan *colstore.RangeScan
	// Ranges of whole blocks all start at the same key.
	var seq map[*colstore.KeyRange]int
	if o.blocks {
		// Blocks that are read cost alike, so each is a task of its own;
		// blocks counted from the index cost next to nothing.
		if !o.byIndex {
			ranges = o.key.Blocks
		}
		scan = o.key.BlockRanges(ranges, o.columns...)
		seq = make(map[*colstore.KeyRange]int, len(scan.Ranges))
		for i := range scan.Ranges {
			seq[&scan.Ranges[i]] = i
		}
	} else {
		scan = o.key.Ranges(ranges, o.columns...)
	}
	ows := make([]*orderedWorker, workers)
	err := scan.Run(ctx, workers, func(n int, kr *colstore.KeyRange, b *part.Block) error {
		ow := ows[n]
		if ow == nil {
			x.workers[n] = newWorker(x)
			ow = newOrderedWorker(x.workers[n], o, scan, n)
			ows[n] = ow
		}

		return ow.merge(kr, ow.edgesOf(kr, seq[kr]), b)
	})
	if err != nil {
		return nil, err
	}

	// A group that ends one range may go on in the next: neighbors with one
	// key are one group.
	last := ows[0]
	if last == nil {
		x.workers[0] = newWorker(x)
		last = newOrderedWorker(x.workers[0], o, scan, 0)
		ows[0] = last
	}
	plan := q.agg
	var edges []*orderedEdges
	for _, ow := range ows {
		if ow != nil {
			edges = append(edges, ow.edges[:ow.ranges]...)
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if c := bytes.Compare(edges[i].lo, edges[j].lo); c != 0 {
			return c < 0
		}

		return edges[i].seq < edges[j].seq
	})
	var carry *orderedGroup
	var carried *stateHeap
	// A group is emitted from the heap of the worker that emits it.
	emit := func() {
		if carried != &last.w.agg.heap {
			for i := range plan.states {
				at := plan.states[i].word
				ref := stateRef(0)
				if b := carried.get(stateRef(carry.words[at])); len(b) != 0 {
					ref = last.w.agg.heap.set(0, b)
				}
				carry.words[at] = int64(ref)
			}
		}
		last.emit(carry)
	}
	for i := range edges {
		for _, e := range []*orderedGroup{&edges[i].first, &edges[i].last} {
			if !e.set {
				continue
			}
			same := carry != nil
			for p := 0; same && p < o.groups; p++ {
				same = bytes.Equal(carry.key[p], e.key[p])
			}
			if !same {
				if carry != nil {
					emit()
				}
				carry, carried = e, edges[i].heap

				continue
			}
			plan.merge(carried, carry.words, edges[i].heap, e.words)
			carry.users += e.users
			// Counts taken with the index leave out a value that the row
			// before the range has.
			if !o.byIndex && e.users > 0 && bytes.Equal(carry.last, e.first) {
				carry.users--
			}
			if e.users > 0 {
				carry.last = append(carry.last[:0], e.last...)
			}
		}
	}
	switch {
	case carry != nil:
		emit()
	case len(plan.keys) == 0:
		last.emit(&orderedGroup{words: append([]int64(nil), plan.init...)})
	}
	last.flush()

	res := &Result{}
	var sinks []*sink
	held := scan.Retained()
	for i, w := range x.workers {
		if w == nil {
			continue
		}
		sinks = append(sinks, &w.sink)
		res.Stats.BlocksMatched += w.blocks
		res.Stats.RowsMatched += w.matched
		held += w.arena.Size() + w.held() + w.sink.held() + ows[i].held()
		res.Stats.CountedByIndex += ows[i].byIndex
		res.Stats.SkippedByToken += ows[i].byToken
	}
	if res.Columns, res.Rows, err = x.finish(snap, sinks); err != nil {
		return nil, err
	}
	res.Stats.Blocks, res.Stats.SkippedByKey, res.Stats.HeldBytes = scan.Decoded.Load(), o.skipped, int64(held)
	res.Stats.Ordered = true

	return res, nil
}

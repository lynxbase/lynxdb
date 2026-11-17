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

// Ordered aggregation serves a plan whose filter is an And of string
// equalities on the leading sort key paths and whose group keys, followed by
// the argument of a distinct count if there is one, are the rest of the sort
// key. The rows of a key range then arrive ordered by group, one stream per
// sorted run, so a merge of the streams finishes one group after another and
// holds no more than the group it is in.

// errUnordered reports rows that the ordered aggregation cannot place: a
// group key or distinct argument that is missing or not a string.
var errUnordered = errors.New("exec: rows do not allow ordered aggregation")

const (
	// rangeRows is the number of rows a key range is cut to hold.
	rangeRows = 1 << 16
	// orderedBatch is the number of finished groups handed on at once.
	orderedBatch = 4096
)

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
	// position in columns, -1 when it has none.
	slotKey, slotCol []int16
	floats           []bool
	skipped          int64
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
	for _, c := range conjuncts(filter, nil) {
		col, lit, op, ok := columnLiteral(c)
		if !ok || c.Op != OpEq || op != OpEq || lit.Kind != vec.KindString || (col.Kind != vec.KindString && col.Kind != KindAny) {
			return nil
		}
		if old, dup := equal[col.Path]; dup && !bytes.Equal(old, lit.Bytes) {
			return nil
		}
		equal[col.Path] = lit.Bytes
	}
	prefix := len(equal)
	if prefix > len(sortKey) {
		return nil
	}
	filterKey := colstore.KeyFilter{Paths: sortKey[:prefix]}
	for _, path := range sortKey[:prefix] {
		v, ok := equal[path]
		if !ok {
			return nil
		}
		filterKey.Values = append(filterKey.Values, v)
	}

	o := &orderedPlan{groups: len(ap.keys), keyOf: make([]int, len(ap.keys)), floats: make([]bool, len(ap.args))}
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
		if len(arg.distinct) != 0 {
			if distinct >= 0 || arg.expr.op != OpColumn || arg.expr.kind != vec.KindString ||
				position(q.paths[arg.expr.path]) != prefix+len(ap.keys) {
				return nil
			}
			distinct = i
			o.parts = append(o.parts, prefix+len(ap.keys))
		}
		// The kind of a numeric aggregate must not depend on rows that a
		// group handed on earlier has not seen.
		for _, w := range arg.words {
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
	var need func(e *sexpr, kinds uint8)
	need = func(e *sexpr, kinds uint8) {
		if e.op != OpColumn {
			for _, a := range e.args {
				need(a, kindBit(vec.KindInt))
			}

			return
		}
		for kind := vec.KindBool; kind <= vec.KindVariant; kind++ {
			s := e.path*kindSlots + int(kind) - 1
			if kinds&kindBit(kind) == 0 || o.slotKey[s] >= 0 || o.slotCol[s] >= 0 {
				continue
			}
			o.slotCol[s] = int16(len(o.columns))
			o.columns = append(o.columns, colstore.ColumnID{Path: q.paths[e.path], Kind: kind})
		}
	}
	for i := range ap.args {
		if len(ap.args[i].words) != 0 {
			need(ap.args[i].expr, ap.args[i].kinds)
		}
	}

	group := make([]string, len(o.parts))
	for i, pos := range o.parts {
		group[i] = sortKey[pos]
	}
	key, ok := snap.PlanKeyRanges(filterKey, group)
	if !ok {
		return nil
	}
	o.key = key
	for _, h := range snap.Parts() {
		o.skipped += int64(h.NumBlocks())
	}
	o.skipped -= int64(key.Blocks)

	return o
}

// sharedColumn returns the column of a slot from the block of a range scan.
func (w *worker) sharedColumn(path int, kind vec.Kind) *vec.Column {
	o := w.q.ordered
	s := path*kindSlots + int(kind) - 1
	if k := o.slotKey[s]; k >= 0 {
		return w.shared.Keys.Column(int(k), kind)
	}
	if c := o.slotCol[s]; c >= 0 {
		return w.shared.Column(int(c))
	}

	return nil
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
	// lo is where the range starts, which orders the ranges of a scan.
	lo          []byte
	first, last orderedGroup
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
}

func (c *orderedCursor) load() {
	row := int(c.starts[c.i])
	for p, col := range c.cols {
		c.cur[p] = col.Bytes(row)
	}
}

// orderedWorker is the state one goroutine owns while it merges key ranges.
type orderedWorker struct {
	w       *worker
	o       *orderedPlan
	scan    *colstore.RangeScan
	cursors []*orderedCursor
	heap    []*orderedCursor
	turn    int

	// open is the group the merge is in and pending the one before it,
	// which is the last of the range unless another group follows.
	open, pending *orderedGroup

	// The finished groups not yet handed to the sink.
	states []int64
	users  []int64
	count  int
	batch  []colVec
	post   postEval

	bound []uint8
	gid   []uint32
	idx   []uint32
	// edges holds the first and last group of every range the worker read.
	edges []*orderedEdges
}

func (ow *orderedWorker) held() int {
	n := 8*(cap(ow.states)+cap(ow.users)) + cap(ow.bound) + 4*(cap(ow.gid)+cap(ow.idx)) + ow.post.held()
	for _, c := range ow.cursors {
		n += 4*(cap(c.starts)+cap(c.group)) + 8*cap(c.words)
	}
	for i := range ow.batch {
		n += ow.batch[i].held()
	}

	return n
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

// next loads the next block of c that has rows in the range, splits them
// into runs and computes the states of their groups; it reports false at the
// end of the stream.
func (ow *orderedWorker) next(c *orderedCursor, b *part.Block) (bool, error) {
	w, o := ow.w, ow.o
	stride := w.agg.stride
	ok, err := c.r.Next(b)
	if !ok || err != nil {
		return false, err
	}
	sb := c.r.Block
	from, to := c.r.From, c.r.To
	n := to - from
	for p, pos := range o.parts {
		col := sb.Keys.Column(pos, vec.KindString)
		if col == nil || (col.Valid != nil && col.Valid.Rank(to)-col.Valid.Rank(from) != n) {
			return false, errUnordered
		}
		c.cols[p] = col
	}
	ow.bound = sized(ow.bound, n)
	bound := ow.bound
	clear(bound)
	bound[0] = 1
	for p := 0; p < o.groups; p++ {
		markChanges(bound, c.cols[p], from)
	}
	ow.gid = sized(ow.gid, n)
	gid := ow.gid
	g := uint32(0)
	for j, m := range bound {
		g += uint32(m)
		gid[j] = g - 1
	}
	if len(o.parts) > o.groups {
		markChanges(bound, c.cols[o.groups], from)
	}
	c.starts, c.group = c.starts[:0], c.group[:0]
	for j, m := range bound {
		if m != 0 {
			c.starts = append(c.starts, uint32(from+j))
			c.group = append(c.group, gid[j])
		}
	}
	c.words = sized(c.words, int(g)*stride)
	for i := 0; i < int(g); i++ {
		copy(c.words[i*stride:], w.q.agg.init)
	}
	var idx []uint32
	if from != 0 || to != sb.Rows {
		ow.idx = sized(ow.idx, n)
		idx = ow.idx
		for j := range idx {
			idx[j] = uint32(from + j)
		}
	}
	w.shared, w.rows = sb, sb.Rows
	w.blocks++
	w.matched += int64(n)
	if err := w.agg.feed(&target{words: c.words, gid: gid}, n, idx); err != nil {
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
			plan.mergeWords(open.words, state)
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
	if len(o.parts) == o.groups {
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
	plan := ow.w.q.agg
	if ow.batch == nil {
		ow.batch = make([]colVec, len(plan.names))
	}
	stride := ow.w.agg.stride
	for i, k := range ow.o.keyOf {
		col, dst := c.cols[k], &ow.batch[i]
		for run := from; run < end; run++ {
			dst.appendBytes(vec.KindString, col.Bytes(int(c.starts[run])))
		}
	}
	ow.states = append(ow.states, c.words[from*stride:end*stride]...)
	if ow.count += end - from; ow.count >= orderedBatch {
		ow.flush()
	}
}

// emit queues a finished group for the sink.
func (ow *orderedWorker) emit(g *orderedGroup) {
	plan := ow.w.q.agg
	if ow.batch == nil {
		ow.batch = make([]colVec, len(plan.names))
	}
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
		plan.column(&plan.outs[i], ow.states, ow.count, ow.o.floats, distinct, &ow.batch[keys+i])
	}
	out := ow.batch
	if q.post != nil {
		out = ow.post.run(q, ow.batch, ow.count)
	}
	ow.w.sink.add(out)
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
	if ow.open == nil {
		ow.open, ow.pending = &orderedGroup{}, &orderedGroup{}
	}
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
func (x *execution) runOrdered(ctx context.Context) (*Result, error) {
	q := x.q
	o := q.ordered
	workers := len(x.workers)
	scan := o.key.Ranges(max(4*workers, o.key.Rows/rangeRows), o.columns...)
	ows := make([]*orderedWorker, workers)
	err := scan.Run(ctx, workers, func(n int, kr *colstore.KeyRange, b *part.Block) error {
		ow := ows[n]
		if ow == nil {
			x.workers[n] = newWorker(x)
			ow = &orderedWorker{w: x.workers[n], o: o, scan: scan, turn: n}
			ows[n] = ow
		}
		edges := &orderedEdges{lo: kr.Lo}
		ow.edges = append(ow.edges, edges)

		return ow.merge(kr, edges, b)
	})
	if err != nil {
		return nil, err
	}

	// A group that ends one range may go on in the next: neighbors with one
	// key are one group.
	last := ows[0]
	if last == nil {
		x.workers[0] = newWorker(x)
		last = &orderedWorker{w: x.workers[0], o: o, scan: scan}
		ows[0] = last
	}
	plan := q.agg
	var edges []*orderedEdges
	for _, ow := range ows {
		if ow != nil {
			edges = append(edges, ow.edges...)
		}
	}
	sort.Slice(edges, func(i, j int) bool { return bytes.Compare(edges[i].lo, edges[j].lo) < 0 })
	var carry *orderedGroup
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
					last.emit(carry)
				}
				carry = e

				continue
			}
			plan.mergeWords(carry.words, e.words)
			carry.users += e.users
			if e.users > 0 && bytes.Equal(carry.last, e.first) {
				carry.users--
			}
			if e.users > 0 {
				carry.last = e.last
			}
		}
	}
	switch {
	case carry != nil:
		last.emit(carry)
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
	}
	res.Columns, res.Rows = collect(q, sinks)
	res.Stats.Blocks, res.Stats.SkippedByKey, res.Stats.HeldBytes = scan.Decoded.Load(), o.skipped, int64(held)
	res.Stats.Ordered = true

	return res, nil
}

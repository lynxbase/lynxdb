package part

import (
	"bytes"
	"errors"
	"math/bits"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// ErrSortKey reports a merge whose sources are not ordered the way the
// destination is.
var ErrSortKey = errors.New("part: source is not ordered by the destination's sort key")

const defaultBlockRows = 16384

// Merge appends every row of srcs to w in the order of w's sort key, then
// timestamp, then source position, cutting blocks of blockRows rows.
func Merge(w *Writer, srcs []*Part, blockRows int) error {
	return MergeRange(w, srcs, blockRows, nil, nil)
}

// MergeRange is Merge restricted to the rows whose encoded sort key is at
// least lo and below hi; a nil bound is open.
func MergeRange(w *Writer, srcs []*Part, blockRows int, lo, hi []byte) error {
	return MergeRangeLimit(w, srcs, blockRows, lo, hi, nil)
}

// RangeLimit lets another goroutine take the end of the range of a merge
// under way: Lower moves the merge's upper bound down to a key that it has
// not reached, and the caller merges the rest of the range itself.
type RangeLimit struct {
	rows    atomic.Int64
	pending atomic.Bool
	mu      sync.Mutex
	offer   []byte
	answer  chan bool
	ended   bool
}

// Rows returns the rows that the merge has written so far.
func (l *RangeLimit) Rows() int64 { return l.rows.Load() }

// Lower asks the merge to end below key and reports whether it will; it
// refuses once it has passed key or has ended.
func (l *RangeLimit) Lower(key []byte) bool {
	answer := make(chan bool, 1)
	l.mu.Lock()
	if l.ended || l.offer != nil {
		l.mu.Unlock()

		return false
	}
	l.offer, l.answer = key, answer
	l.pending.Store(true)
	l.mu.Unlock()

	return <-answer
}

// take answers the offer with what lower makes of it.
func (l *RangeLimit) take(lower func(key []byte) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.offer != nil {
		l.answer <- lower(l.offer)
		l.offer, l.answer = nil, nil
	}
	l.pending.Store(false)
}

// end refuses every offer from now on.
func (l *RangeLimit) end() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ended = true
	if l.offer != nil {
		l.answer <- false
		l.offer, l.answer = nil, nil
	}
	l.pending.Store(false)
}

// MergeRangeLimit is MergeRange whose upper bound limit, when not nil, can
// lower while it runs.
func MergeRangeLimit(w *Writer, srcs []*Part, blockRows int, lo, hi []byte, limit *RangeLimit) error {
	if limit != nil {
		defer limit.end()
	}
	if blockRows <= 0 || blockRows > shred.MaxRows {
		blockRows = defaultBlockRows
	}
	if !w.opts.Ordered {
		return ErrSortKey
	}
	for _, p := range srcs {
		if !p.Ordered() || !slices.Equal(p.SortKey(), w.opts.SortKey) {
			return ErrSortKey
		}
	}
	m := mergerPool.Get()
	defer mergerPool.Put(m)
	m.w, m.lo, m.hi, m.blockRows, m.limit = w, lo, hi, blockRows, limit
	m.out = shred.Get(shred.Options{})
	for i, p := range srcs {
		// A part whose keys all lie outside the range is not read at all.
		if first, last := p.KeyRange(); (hi != nil && bytes.Compare(first, hi) >= 0) || (lo != nil && bytes.Compare(last, lo) < 0) {
			continue
		}
		c := cursorPool.Get()
		c.src, c.part, c.block, c.taken = i, p, -1, 0
		c.pathMap = grow(c.pathMap, len(p.paths))
		for j := range c.pathMap {
			c.pathMap[j] = noPath
		}
		c.slots = grow(c.slots, len(p.paths)*kindSlots)
		clear(c.slots)
		c.keyIDs = c.keyIDs[:0]
		for _, path := range w.opts.SortKey {
			id, ok := p.PathID(path)
			if !ok {
				id = noPath
			}
			c.keyIDs = append(c.keyIDs, id)
		}
		if lo != nil {
			// The block before the first one starting at or after lo can
			// end with rows of the range.
			c.block = max(0, sort.Search(len(p.firstKeys), func(b int) bool { return bytes.Compare(p.firstKeys[b], lo) >= 0 })-1) - 1
		}
		c.advance(m)
		m.cursors = append(m.cursors, c)
	}

	return m.run()
}

// shortShare is how many blocks a merge writes for every block under half
// of its row limit that it writes before its last.
const shortShare = 4

// kindSlots is the number of value kinds a path can hold, rounded up.
const kindSlots = 8

const noPath = ^uint32(0)

// merger is the state of one merge, reused between merges.
type merger struct {
	w         *Writer
	lo, hi    []byte
	limit     *RangeLimit
	blockRows int
	out       *shred.Batch
	cursors   []*mergeCursor
	// tree is a tournament over the cursors: tree[0] is the cursor whose
	// row comes first and tree[n] the loser of the match played at node n,
	// with cursor i entering at node len(cursors)+i.
	tree    []int32
	winners []int32
	// blocks and short count the blocks written and those of them under
	// half of blockRows.
	blocks, short int
	// copied is the source of the last block written when it was copied as
	// stored, and nil otherwise.
	copied      *Part
	copiedBlock int
	keyBuf      []byte
	times       []int64
	// counts and unsorted serve the sort of a cursor's events.
	counts   []uint32
	unsorted []uint64
	last     Block
	lastCols []vec.Column
}

var mergerPool = mem.Pool[merger]{Reset: func(m *merger) bool {
	for i, c := range m.cursors {
		if c != exhausted {
			cursorPool.Put(c)
		}
		m.cursors[i] = nil
	}
	if m.out != nil {
		shred.Put(m.out)
	}
	*m = merger{
		cursors: m.cursors[:0], tree: m.tree[:0], winners: m.winners[:0],
		keyBuf: m.keyBuf[:0], times: m.times[:0], lastCols: m.lastCols,
		counts: m.counts[:0], unsorted: m.unsorted[:0],
	}

	return true
}}

// How far the current block of a cursor is decoded.
const (
	cursorBound = iota // nothing: key and time bound the block from below
	cursorKeys         // the sort key and time columns
	cursorRows         // every column
)

// mergeCursor walks the rows of one source in order.
type mergeCursor struct {
	src   int
	part  *Part
	block int
	state uint8
	done  bool
	// key and time order the cursor's row at pos; before the block is
	// decoded they are its first key and its lowest time, which no row of
	// the block comes before.
	key  []byte
	time int64
	pos  int
	end  int // rows of the block inside the merged range
	// taken counts the rows of the part written to the output.
	taken uint64

	blk  Block
	refs []ColumnRef
	// cols holds the decoded column of each column of the block, taken
	// from bufs, which keeps apart the columns of each group of kinds.
	cols    []*mergeColumn
	bufs    [kindGroups][]*mergeColumn
	times   *vec.Column
	keyIDs  []uint32
	isKey   []bool
	keyCols [][]int // for each key path, the block's columns holding it
	// buf holds the encoded key of the row at pos; its first constLen bytes
	// encode the key paths before varKey, which hold one value in the block.
	buf      []byte
	constLen int
	varKey   int

	// dense lists the columns with a value in every row, and events the
	// values of the others inside the range as row<<32 | column, by row.
	dense     []densePlan
	events    []uint64
	nextEvent int
	// outs holds the output column of each column of the block, slots the
	// one of each path and kind of the part.
	outs  []*shred.Column
	slots []*shred.Column
	// next is the slot of the next value of each column decoded by value.
	next []uint32
	// pathMap gives the destination's id of each path of the part.
	pathMap []uint32
}

// densePlan is how the rows of a column without gaps reach the output.
type densePlan struct {
	col  *vec.Column
	out  *shred.Column
	mode uint8
}

const (
	denseValues = iota // one value at a time
	denseInts
	denseFloats
	denseBytes
)

// exhausted stands in for a cursor that gave its buffers back.
var exhausted = &mergeCursor{done: true}

// maxCursorBytes bounds the decoded columns a pooled cursor keeps.
const maxCursorBytes = 64 << 20

var cursorPool = mem.Pool[mergeCursor]{Reset: func(c *mergeCursor) bool {
	retained := 8 * cap(c.events)
	for _, bufs := range c.bufs {
		for _, col := range bufs {
			col.release()
			retained += col.Retained()
		}
	}
	if retained > maxCursorBytes {
		return false
	}
	c.part, c.key, c.times, c.refs, c.done, c.state = nil, nil, nil, nil, false, cursorBound
	clear(c.cols)
	c.blk.part, c.blk.data, c.blk.dir, c.blk.stats = nil, nil, nil, nil
	clear(c.outs)
	clear(c.slots)
	for i := range c.dense {
		c.dense[i] = densePlan{}
	}
	c.dense = c.dense[:0]

	return true
}}

// upperKey returns an encoded sort key that no row of block i comes after.
func (p *Part) upperKey(i int) []byte {
	if i+1 < len(p.firstKeys) {
		return p.firstKeys[i+1]
	}

	return p.lastKey
}

// advance moves the cursor to its next block without decoding it.
func (c *mergeCursor) advance(m *merger) {
	c.block++
	p := c.part
	if c.block >= p.NumBlocks() || (m.hi != nil && bytes.Compare(p.firstKeys[c.block], m.hi) >= 0) {
		c.done = true

		return
	}
	c.state = cursorBound
	c.key = p.firstKeys[c.block]
	_, c.time, _ = p.BlockStats(c.block)
}

// mergeColumn is a decoded column whose string data lives in a pooled
// buffer.
type mergeColumn struct {
	vec.Column
	data *byteBuf
}

// fit makes Data a pooled buffer with room for n bytes and not much more.
func (c *mergeColumn) fit(n int) {
	if c.data != nil {
		if class := bufClass(cap(c.data.b)); cap(c.data.b) >= n && class <= bufClass(n)+2 {
			c.Data = c.data.b[:0]

			return
		}
		putBuf(c.data)
	}
	c.data = getBuf(n)
	c.Data = c.data.b[:0]
}

// release gives the pooled buffer back.
func (c *mergeColumn) release() {
	if c.data != nil {
		putBuf(c.data)
		c.data, c.Data = nil, nil
	}
}

// kindGroups is the number of groups of value kinds whose decoded columns
// fill the same buffers.
const kindGroups = 4

func kindGroup(kind vec.Kind) int {
	switch kind {
	case vec.KindString, vec.KindVariant:
		return 0
	case vec.KindInt:
		return 1
	case vec.KindFloat:
		return 2
	}

	return 3
}

// assign gives every column of the block a column to decode into that last
// held one of the same group of kinds, so that its buffers fit.
func (c *mergeCursor) assign() {
	c.cols = grow(c.cols, len(c.refs))
	var used [kindGroups]int
	for j := range c.refs {
		group := kindGroup(c.refs[j].Kind)
		if used[group] == len(c.bufs[group]) {
			c.bufs[group] = append(c.bufs[group], new(mergeColumn))
		}
		c.cols[j] = c.bufs[group][used[group]]
		used[group]++
	}
	// Columns the block has no use for hold no pooled buffer.
	for group, n := range used {
		for _, col := range c.bufs[group][n:] {
			col.release()
		}
	}
}

// decode reads column j of the block.
func (c *mergeCursor) decode(j int) error {
	ref, col := &c.refs[j], c.cols[j]
	if n := c.blk.dataSize(ref); n > 0 {
		col.fit(int(n))
	}
	// Key columns are read by row; the others only in row order.
	var err error
	if c.isKey[j] {
		err = c.blk.Decode(ref, &col.Column)
	} else {
		err = c.blk.DecodeValues(ref, &col.Column)
	}
	if col.data != nil && cap(col.Data) != cap(col.data.b) {
		// Decode had to replace the buffer, which is kept for the pool's sake.
		col.Data = append(col.data.b[:0], col.Data...)
	}

	return err
}

// decodeKeys reads the columns that order the rows of the block and places
// the cursor on its first row inside the merged range, if it has one.
func (c *mergeCursor) decodeKeys(m *merger) error {
	p := c.part
	if err := p.OpenBlock(c.block, &c.blk); err != nil {
		return err
	}
	c.refs = c.blk.Columns()
	n := len(c.refs)
	c.assign()
	c.isKey = grow(c.isKey, n)
	for len(c.keyCols) < len(c.keyIDs) {
		c.keyCols = append(c.keyCols, nil)
	}
	for k := range c.keyCols {
		c.keyCols[k] = c.keyCols[k][:0]
	}
	c.times = nil
	for j := range c.refs {
		ref := &c.refs[j]
		c.isKey[j] = ref.PathID == 0 && ref.Kind == vec.KindInt
		if c.isKey[j] {
			c.times = &c.cols[j].Column
		}
		for k, id := range c.keyIDs {
			if id == ref.PathID {
				c.keyCols[k] = append(c.keyCols[k], j)
				c.isKey[j] = true
			}
		}
		if c.isKey[j] {
			if err := c.decode(j); err != nil {
				return err
			}
		}
	}
	if c.times == nil {
		return ErrCorrupt
	}

	c.buf, c.varKey = c.buf[:0], 0
	for k := range c.keyIDs {
		cols := c.keyCols[k]
		if len(cols) == 0 {
			c.buf = append(c.buf, 0)
		} else if col := &c.cols[cols[0]].Column; len(cols) == 1 && col.Valid == nil && col.Layout == vec.Const {
			c.buf = appendDecodedKey(c.buf, col, 0)
		} else {
			break
		}
		c.varKey = k + 1
	}
	c.constLen = len(c.buf)

	c.state = cursorKeys
	c.pos, c.end = 0, c.blk.Rows
	if m.lo != nil && bytes.Compare(p.firstKeys[c.block], m.lo) < 0 {
		c.pos = c.search(m, m.lo)
	}
	if m.hi != nil && bytes.Compare(p.upperKey(c.block), m.hi) >= 0 {
		c.end = c.search(m, m.hi)
	}
	if c.pos < c.end {
		c.place()
	}

	return nil
}

// place makes key and time those of the row at pos.
func (c *mergeCursor) place() {
	c.buf = c.appendRowKey(c.buf[:c.constLen], c.pos)
	c.key, c.time = c.buf, c.times.Int(c.pos)
}

// appendRowKey appends the encoding of the key paths from varKey on at row.
func (c *mergeCursor) appendRowKey(dst []byte, row int) []byte {
	for k := c.varKey; k < len(c.keyIDs); k++ {
		dst = appendDecodedKey(dst, c.keyColumn(k, row), row)
	}

	return dst
}

// rowKey returns the encoded sort key of row, held in m.keyBuf.
func (c *mergeCursor) rowKey(m *merger, row int) []byte {
	m.keyBuf = c.appendRowKey(append(m.keyBuf[:0], c.buf[:c.constLen]...), row)

	return m.keyBuf
}

// keyColumn returns the column holding key k at row, or nil when the row
// has no value there; columns of one path are ordered by kind.
func (c *mergeCursor) keyColumn(k, row int) *vec.Column {
	for _, j := range c.keyCols[k] {
		if col := &c.cols[j].Column; col.IsValid(row) {
			return col
		}
	}

	return nil
}

// search returns the first row of the block whose sort key is not below
// bound.
func (c *mergeCursor) search(m *merger, bound []byte) int {
	return sort.Search(c.blk.Rows, func(row int) bool {
		return bytes.Compare(c.rowKey(m, row), bound) >= 0
	})
}

// decodeRows reads the columns that decodeKeys left out and plans how their
// values inside the merged range reach out.
func (c *mergeCursor) decodeRows(m *merger) error {
	out := m.out
	n := len(c.refs)
	c.outs = grow(c.outs, n)
	clear(c.outs)
	c.next = grow(c.next, n)
	for i := range c.dense {
		c.dense[i] = densePlan{}
	}
	c.dense = c.dense[:0]
	unsorted := m.unsorted[:0]
	counts := grow(m.counts, c.blk.Rows+1)
	clear(counts)
	for j := range c.refs {
		ref := &c.refs[j]
		if ref.PathID == 0 {
			continue
		}
		col := &c.cols[j].Column
		if !c.isKey[j] {
			if err := c.decode(j); err != nil {
				return err
			}
		}
		slot := int(ref.PathID)*kindSlots + int(ref.Kind)
		oc := c.slots[slot]
		if oc == nil {
			oc = out.Col(c.part.paths[ref.PathID], ref.Kind)
			c.slots[slot] = oc
		}
		c.outs[j] = oc
		if col.Valid == nil {
			plan := densePlan{col: col, out: oc}
			switch {
			case col.Layout != vec.Flat:
			case col.Kind == vec.KindInt:
				plan.mode = denseInts
			case col.Kind == vec.KindFloat:
				plan.mode = denseFloats
			case col.Kind == vec.KindString || col.Kind == vec.KindVariant:
				plan.mode = denseBytes
			}
			c.dense = append(c.dense, plan)

			continue
		}
		unsorted = appendValid(unsorted, col.Valid, c.pos, c.end-c.pos, uint64(j))
		if !c.isKey[j] {
			c.next[j] = uint32(col.Valid.Rank(c.pos))
		}
	}
	// A counting sort by row keeps the values of one column in row order.
	for _, e := range unsorted {
		counts[e>>32]++
	}
	at := uint32(0)
	for r := c.pos; r < c.end; r++ {
		at, counts[r] = at+counts[r], at
	}
	c.events = grow(c.events, len(unsorted))
	for _, e := range unsorted {
		c.events[counts[e>>32]] = e
		counts[e>>32]++
	}
	m.unsorted, m.counts = unsorted, counts
	c.nextEvent = 0
	c.state = cursorRows

	return nil
}

// appendValid appends row<<32 | tag for the rows in [a, a+n) whose bit is
// set.
func appendValid(dst []uint64, valid vec.Bitmap, a, n int, tag uint64) []uint64 {
	end := a + n
	for word := a >> 6; word<<6 < end; word++ {
		w := valid[word]
		if base := word << 6; base < a {
			w &= ^uint64(0) << (uint(a) & 63)
		}
		if limit := (word + 1) << 6; limit > end {
			w &= ^uint64(0) >> (uint(limit-end) & 63)
		}
		for ; w != 0; w &= w - 1 {
			dst = append(dst, uint64(word<<6+bits.TrailingZeros64(w))<<32|tag)
		}
	}

	return dst
}

// raw returns the input size of the next n rows the cursor writes, the
// share of its part's that they are of its rows.
func (c *mergeCursor) raw(n int) int {
	p := c.part
	share := func(rows uint64) uint64 {
		hi, lo := bits.Mul64(p.RawBytes, min(rows, p.Rows))
		q, _ := bits.Div64(hi, lo, p.Rows)

		return q
	}
	before := share(c.taken)
	c.taken += uint64(n)

	return int(share(c.taken) - before)
}

// emit appends the next n rows of the cursor to the output block.
func (c *mergeCursor) emit(m *merger, n int) error {
	out := m.out
	if c.state != cursorRows {
		if err := c.decodeRows(m); err != nil {
			return err
		}
	}
	a, o := c.pos, out.Rows
	for i := range c.dense {
		p := &c.dense[i]
		col, oc := p.col, p.out
		switch p.mode {
		case denseInts:
			oc.AppendInts(o, col.Ints[a:a+n])
		case denseFloats:
			oc.AppendFloats(o, col.Floats[a:a+n])
		case denseBytes:
			oc.AppendByteRun(o, col.Data, col.Offsets[a:a+n+1])
		default:
			for i := 0; i < n; i++ {
				copyValue(oc, col, a+i, o+i)
			}
		}
	}
	shift := o - a
	for end := uint64(a+n) << 32; c.nextEvent < len(c.events) && c.events[c.nextEvent] < end; c.nextEvent++ {
		e := c.events[c.nextEvent]
		row, j := int(e>>32), int(uint32(e))
		at := row
		if !c.isKey[j] {
			at = int(c.next[j])
			c.next[j]++
		}
		copyValue(c.outs[j], &c.cols[j].Column, at, row+shift)
	}
	if c.times.Layout == vec.Flat {
		out.AppendRows(c.times.Ints[a:a+n], c.raw(n))
	} else {
		m.times = m.times[:0]
		for i := 0; i < n; i++ {
			m.times = append(m.times, c.times.Ints[0])
		}
		out.AppendRows(m.times, c.raw(n))
	}

	return nil
}

func copyValue(oc *shred.Column, col *vec.Column, row, to int) {
	switch col.Kind {
	case vec.KindNull:
		oc.AppendNull(to)
	case vec.KindBool:
		oc.AppendBool(to, col.Bool(row))
	case vec.KindInt:
		oc.AppendInt(to, col.Int(row))
	case vec.KindFloat:
		oc.AppendFloat(to, col.Float(row))
	default:
		oc.AppendBytes(to, col.Bytes(row))
	}
}

// less orders the rows two cursors stand on by sort key, time and source,
// with exhausted cursors last; it matches the order shred.Batch.SortBy
// gives rows of one batch.
func (m *merger) less(a, b int32) bool {
	ca, cb := m.cursors[a], m.cursors[b]
	if ca.done || cb.done {
		return !ca.done
	}
	if d := bytes.Compare(ca.key, cb.key); d != 0 {
		return d < 0
	}
	if ca.time != cb.time {
		return ca.time < cb.time
	}

	return ca.src < cb.src
}

// build plays the whole tournament.
func (m *merger) build() {
	k := len(m.cursors)
	m.tree = grow(m.tree, max(k, 1))
	m.winners = grow(m.winners, 2*k)
	for i := 0; i < k; i++ {
		m.winners[k+i] = int32(i)
	}
	for n := k - 1; n >= 1; n-- {
		a, b := m.winners[2*n], m.winners[2*n+1]
		if m.less(b, a) {
			a, b = b, a
		}
		m.winners[n], m.tree[n] = a, b
	}
	m.tree[0] = 0
	if k > 1 {
		m.tree[0] = m.winners[1]
	}
}

// replay plays the matches of cursor i again after its row changed.
func (m *merger) replay(i int32) {
	winner := i
	for n := (int(i) + len(m.cursors)) / 2; n >= 1; n /= 2 {
		if m.less(m.tree[n], winner) {
			m.tree[n], winner = winner, m.tree[n]
		}
	}
	m.tree[0] = winner
}

// challenger returns the cursor that comes first among the others when
// cursor i holds the first row, or nil when the others are exhausted.
func (m *merger) challenger(i int32) *mergeCursor {
	best := int32(-1)
	for n := (int(i) + len(m.cursors)) / 2; n >= 1; n /= 2 {
		if l := m.tree[n]; !m.cursors[l].done && (best < 0 || m.less(l, best)) {
			best = l
		}
	}
	if best < 0 {
		return nil
	}

	return m.cursors[best]
}

// runBefore returns how many of the next limit rows of c come before the
// row other stands on, given that the first one does.
func (c *mergeCursor) runBefore(m *merger, other *mergeCursor, limit int) int {
	before := func(i int) bool {
		row := c.pos + i
		if d := bytes.Compare(c.rowKey(m, row), other.key); d != 0 {
			return d < 0
		}
		if t := c.times.Int(row); t != other.time {
			return t < other.time
		}

		return c.src < other.src
	}
	// Rows up to lo come before other; the row at hi, if it has one, does not.
	lo, hi := 0, 1
	for hi < limit && before(hi) {
		lo, hi = hi, 2*hi+1
	}
	hi = min(hi, limit)
	for lo+1 < hi {
		if mid := int(uint(lo+hi) >> 1); before(mid) {
			lo = mid
		} else {
			hi = mid
		}
	}

	return hi
}

// advance moves cursor i to its next block and lets go of it after its last.
func (m *merger) advance(i int32, c *mergeCursor) {
	c.advance(m)
	if c.done {
		m.cursors[i] = exhausted
		cursorPool.Put(c)
	}
}

func (m *merger) run() error {
	m.build()
	streak := int32(-1)
	for len(m.cursors) > 0 {
		if m.limit != nil && m.limit.pending.Load() {
			m.limit.take(m.lower)
			streak = -1
		}
		wi := m.tree[0]
		c := m.cursors[wi]
		if c.done {
			break
		}
		if c.state == cursorBound {
			streak = -1
			copied, err := m.copyBlock(wi, c)
			if err != nil {
				return err
			}
			if !copied {
				if err := c.decodeKeys(m); err != nil {
					return err
				}
			}
			if copied || c.pos == c.end {
				m.advance(wi, c)
			}
			m.replay(wi)

			continue
		}
		n := 1
		// A cursor that comes first twice in a row is likely on a run.
		if limit := min(c.end-c.pos, m.blockRows-m.out.Rows); wi == streak && limit > 1 {
			if other := m.challenger(wi); other != nil {
				n = c.runBefore(m, other, limit)
			} else {
				n = limit
			}
		}
		streak = wi
		if err := c.emit(m, n); err != nil {
			return err
		}
		c.pos += n
		if m.out.Rows == m.blockRows {
			if err := m.flush(); err != nil {
				return err
			}
		}
		if c.pos == c.end {
			m.advance(wi, c)
		} else {
			c.place()
		}
		m.replay(wi)
	}
	if m.out.Rows > 0 {
		return m.flush()
	}

	return m.settleLastKey()
}

// flush writes the rows gathered so far as one block.
func (m *merger) flush() error {
	if m.out.Rows < m.blockRows/2 {
		m.short++
	}
	m.blocks++
	// The key statistics of the block compare its first row with the last
	// row written, which a copied block left as a bound.
	if err := m.settleLastKey(); err != nil {
		return err
	}
	m.copied = nil
	err := m.w.WriteBatch(m.out)
	if m.limit != nil {
		m.limit.rows.Add(int64(m.out.Rows))
	}
	m.out.Reset()

	return err
}

// lower makes key the upper bound of the merge when no row at or above it
// has been taken yet, and reports whether it did.
func (m *merger) lower(key []byte) bool {
	if (m.hi != nil && bytes.Compare(key, m.hi) >= 0) || (m.lo != nil && bytes.Compare(key, m.lo) <= 0) {
		return false
	}
	// A copied block leaves a bound on its last key, which is as good.
	last := m.w.lastKey
	if m.out.Rows > 0 {
		last = AppendRowKey(nil, m.out, m.w.opts.SortKey, m.out.Rows-1)
	}
	if (m.out.Rows > 0 || m.w.rows > 0) && bytes.Compare(last, key) >= 0 {
		return false
	}
	m.hi = key
	for i, c := range m.cursors {
		if c.done {
			continue
		}
		if c.state != cursorBound && bytes.Compare(c.part.upperKey(c.block), key) >= 0 {
			// Every row before pos is below key, so the end stays at or
			// after it.
			c.end = max(c.pos, c.search(m, key))
		}
		if (c.state == cursorBound && bytes.Compare(c.key, key) >= 0) || (c.state != cursorBound && c.pos == c.end) {
			m.advance(int32(i), c)
		}
	}
	m.build()

	return true
}

// copyBlock writes the block a cursor stands before as it is stored, when
// every row of it lies inside the merged range and before the rows of the
// other cursors, and reports whether it did.
func (m *merger) copyBlock(wi int32, c *mergeCursor) (bool, error) {
	p, b := c.part, c.block
	rows, _, maxTime := p.BlockStats(b)
	if rows > m.blockRows || !m.w.copies(p, b) {
		return false, nil
	}
	first, upper := p.firstKeys[b], p.upperKey(b)
	if (m.lo != nil && bytes.Compare(first, m.lo) < 0) || (m.hi != nil && bytes.Compare(upper, m.hi) >= 0) {
		return false, nil
	}
	if other := m.challenger(wi); other != nil {
		switch d := bytes.Compare(upper, other.key); {
		case d > 0:
			return false, nil
		case d == 0:
			// Rows that share the key of the other cursor's row precede it
			// only by time and source.
			if !bytes.Equal(first, upper) || maxTime > other.time || (maxTime == other.time && c.src > other.src) {
				return false, nil
			}
		}
	}
	// Short blocks stay under one in four, so that copying cannot cut a
	// part into slivers; the rows before the first copied block do not
	// count, since a range begins inside a block of most sources.
	half, short, written := m.blockRows/2, 0, 1
	if rows < half {
		short++
	}
	if m.out.Rows > 0 {
		written++
		if m.out.Rows < half && m.blocks > 0 {
			short++
		}
	}
	if short > 0 && shortShare*(m.short+short) > m.blocks+written {
		return false, nil
	}
	if m.out.Rows > 0 {
		if err := m.flush(); err != nil {
			return false, err
		}
	}
	follows := m.copied == p && m.copiedBlock == b-1
	if !follows {
		if err := m.settleLastKey(); err != nil {
			return false, err
		}
	}
	if err := m.w.copyBlock(p, b, c.pathMap, c.raw(rows), follows); err != nil {
		return false, err
	}
	m.blocks++
	if rows < half {
		m.short++
	}
	if m.limit != nil {
		m.limit.rows.Add(int64(rows))
	}
	m.copied, m.copiedBlock = p, b

	return true, nil
}

// settleLastKey gives the writer the sort key of its last row when the last
// block was copied without being decoded.
func (m *merger) settleLastKey() error {
	p, b := m.copied, m.copiedBlock
	if p == nil {
		return nil
	}
	if b == p.NumBlocks()-1 {
		m.w.lastKey = append(m.w.lastKey[:0], p.lastKey...)

		return nil
	}
	if err := p.OpenBlock(b, &m.last); err != nil {
		return err
	}
	key := m.w.lastKey[:0]
	row := m.last.Rows - 1
	for _, path := range m.w.opts.SortKey {
		var found *vec.Column
		if id, ok := p.PathID(path); ok {
			refs := m.last.Path(id)
			for len(m.lastCols) < len(refs) {
				m.lastCols = append(m.lastCols, vec.Column{})
			}
			for j := range refs {
				col := &m.lastCols[j]
				if err := m.last.Decode(&refs[j], col); err != nil {
					return err
				}
				if col.IsValid(row) {
					found = col

					break
				}
			}
		}
		key = appendDecodedKey(key, found, row)
	}
	m.w.lastKey = key

	return nil
}

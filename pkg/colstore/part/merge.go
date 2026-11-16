package part

import (
	"bytes"
	"cmp"
	"errors"
	"math/bits"
	"slices"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
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
	if blockRows <= 0 || blockRows > shred.MaxRows {
		blockRows = defaultBlockRows
	}
	if !w.opts.Ordered {
		return ErrSortKey
	}
	m := merger{keys: w.opts.SortKey, lo: lo, hi: hi}
	var size uint64
	for i, p := range srcs {
		if !p.Ordered() || !slices.Equal(p.SortKey(), m.keys) {
			return ErrSortKey
		}
		size += p.RawBytes
		c := &mergeCursor{src: i, part: p, out: make([]*shred.Column, len(p.paths)*kindSlots)}
		for _, path := range m.keys {
			id, ok := p.PathID(path)
			if !ok {
				id = ^uint32(0)
			}
			c.keyIDs = append(c.keyIDs, id)
		}
		if lo != nil {
			// The block before the first one starting at or after lo can
			// end with rows of the range.
			c.next = max(0, sort.Search(len(p.firstKeys), func(b int) bool { return bytes.Compare(p.firstKeys[b], lo) >= 0 })-1)
		}
		ok, err := c.load(&m)
		if err != nil {
			return err
		}
		if ok {
			m.heap = append(m.heap, c)
		}
	}
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.down(i)
	}

	out := shred.Get(shred.Options{})
	defer shred.Put(out)
	for len(m.heap) > 0 {
		c := m.heap[0]
		n := min(c.end-c.pos, blockRows-out.Rows)
		if len(m.heap) > 1 {
			// The smallest of the other cursors is a child of the root.
			other := m.heap[1]
			if len(m.heap) > 2 && m.less(m.heap[2], other) {
				other = m.heap[2]
			}
			run := 1
			for run < n && m.compare(c, c.pos+run, other, other.pos) < 0 {
				run++
			}
			n = run
		}
		c.emit(out, n, int(size), &m)
		size = 0
		c.pos += n
		if out.Rows == blockRows {
			if err := w.WriteBatch(out); err != nil {
				return err
			}
			out.Reset()
		}
		if c.pos == c.end {
			ok, err := c.load(&m)
			if err != nil {
				return err
			}
			if !ok {
				last := len(m.heap) - 1
				m.heap[0] = m.heap[last]
				m.heap = m.heap[:last]
			}
		}
		m.down(0)
	}

	return w.WriteBatch(out)
}

// kindSlots is the number of value kinds a path can hold, rounded up.
const kindSlots = 8

type merger struct {
	keys   []string
	lo, hi []byte
	heap   []*mergeCursor
	times  []int64
	valid  []uint32
	keyBuf []byte
}

// mergeCursor walks the rows of one source in order.
type mergeCursor struct {
	src    int
	part   *Part
	next   int
	blk    Block
	cols   []vec.Column
	time   *vec.Column
	pos    int
	end    int // rows of the block inside the merged range
	seeked bool
	keyIDs []uint32
	// keyCols lists, for each key path, the block's columns holding it.
	keyCols [][]int
	out     []*shred.Column
}

// load decodes the next block that has rows inside the merged range and
// reports whether there was one.
func (c *mergeCursor) load(m *merger) (bool, error) {
	for {
		if c.next >= c.part.NumBlocks() ||
			(m.hi != nil && bytes.Compare(c.part.firstKeys[c.next], m.hi) >= 0) {
			return false, nil
		}
		if err := c.decode(); err != nil {
			return false, err
		}
		c.pos, c.end = 0, c.blk.Rows
		if m.lo != nil && !c.seeked {
			c.pos = c.search(m, m.lo)
		}
		if m.hi != nil {
			c.end = c.search(m, m.hi)
		}
		c.seeked = true
		if c.pos < c.end {
			return true, nil
		}
	}
}

// search returns the first row of the block whose sort key is not below
// bound.
func (c *mergeCursor) search(m *merger, bound []byte) int {
	return sort.Search(c.blk.Rows, func(row int) bool {
		m.keyBuf = m.keyBuf[:0]
		for k := range c.keyIDs {
			m.keyBuf = appendDecodedKey(m.keyBuf, c.key(k, row), row)
		}

		return bytes.Compare(m.keyBuf, bound) >= 0
	})
}

// decode reads every column of the next block.
func (c *mergeCursor) decode() error {
	if err := c.part.OpenBlock(c.next, &c.blk); err != nil {
		return err
	}
	c.next++
	refs := c.blk.Columns()
	for len(c.cols) < len(refs) {
		c.cols = append(c.cols, vec.Column{})
	}
	for len(c.keyCols) < len(c.keyIDs) {
		c.keyCols = append(c.keyCols, nil)
	}
	for k := range c.keyCols {
		c.keyCols[k] = c.keyCols[k][:0]
	}
	c.time = nil
	for j := range refs {
		if err := c.blk.Decode(&refs[j], &c.cols[j]); err != nil {
			return err
		}
		if refs[j].PathID == 0 && refs[j].Kind == vec.KindInt {
			c.time = &c.cols[j]
		}
		for k, id := range c.keyIDs {
			if id == refs[j].PathID {
				c.keyCols[k] = append(c.keyCols[k], j)
			}
		}
	}
	if c.time == nil {
		return ErrCorrupt
	}

	return nil
}

// key returns the column holding key k at row, or nil when the row has no
// value there; columns of one path are ordered by kind.
func (c *mergeCursor) key(k, row int) *vec.Column {
	for _, j := range c.keyCols[k] {
		if col := &c.cols[j]; col.IsValid(row) {
			return col
		}
	}

	return nil
}

// compare orders row ra of a against row rb of b; it matches the order
// shred.Batch.SortBy gives rows of one batch.
func (m *merger) compare(a *mergeCursor, ra int, b *mergeCursor, rb int) int {
	for k := range m.keys {
		ca, cb := a.key(k, ra), b.key(k, rb)
		var ka, kb vec.Kind
		if ca != nil {
			ka = ca.Kind
		}
		if cb != nil {
			kb = cb.Kind
		}
		if ka != kb {
			return cmp.Compare(ka, kb)
		}
		var d int
		switch ka {
		case vec.KindBool:
			va, vb := ca.Bool(ra), cb.Bool(rb)
			if va != vb {
				d = 1
				if vb {
					d = -1
				}
			}
		case vec.KindInt:
			d = cmp.Compare(ca.Int(ra), cb.Int(rb))
		case vec.KindFloat:
			d = cmp.Compare(ca.Float(ra), cb.Float(rb))
		case vec.KindString, vec.KindVariant:
			d = bytes.Compare(ca.Bytes(ra), cb.Bytes(rb))
		}
		if d != 0 {
			return d
		}
	}
	if d := cmp.Compare(a.time.Int(ra), b.time.Int(rb)); d != 0 {
		return d
	}

	return cmp.Compare(a.src, b.src)
}

func (m *merger) less(a, b *mergeCursor) bool { return m.compare(a, a.pos, b, b.pos) < 0 }

func (m *merger) down(i int) {
	for {
		child := 2*i + 1
		if child >= len(m.heap) {
			return
		}
		if child+1 < len(m.heap) && m.less(m.heap[child+1], m.heap[child]) {
			child++
		}
		if !m.less(m.heap[child], m.heap[i]) {
			return
		}
		m.heap[i], m.heap[child] = m.heap[child], m.heap[i]
		i = child
	}
}

// emit appends the next n rows of the cursor to out.
func (c *mergeCursor) emit(out *shred.Batch, n, size int, m *merger) {
	a, o := c.pos, out.Rows
	refs := c.blk.Columns()
	for j := range refs {
		ref := &refs[j]
		if ref.PathID == 0 {
			continue
		}
		slot := int(ref.PathID)*kindSlots + int(ref.Kind)
		oc := c.out[slot]
		if oc == nil {
			oc = out.Col(c.part.paths[ref.PathID], ref.Kind)
			c.out[slot] = oc
		}
		col := &c.cols[j]
		if col.Valid == nil {
			appendDense(oc, col, a, n, o)

			continue
		}
		m.valid = appendValid(m.valid[:0], col.Valid, a, n)
		appendRows(oc, col, m.valid, a, o)
	}
	m.times = m.times[:0]
	for i := 0; i < n; i++ {
		m.times = append(m.times, c.time.Int(a+i))
	}
	out.AppendRows(m.times, size)
}

// appendValid appends the rows in [a, a+n) whose bit is set.
func appendValid(dst []uint32, valid vec.Bitmap, a, n int) []uint32 {
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
			dst = append(dst, uint32(word<<6+bits.TrailingZeros64(w)))
		}
	}

	return dst
}

// appendDense copies rows a to a+n-1 of a column without gaps to output
// rows starting at o.
func appendDense(oc *shred.Column, col *vec.Column, a, n, o int) {
	switch {
	case col.Kind == vec.KindInt && col.Layout == vec.Flat:
		oc.AppendInts(o, col.Ints[a:a+n])
	case col.Kind == vec.KindFloat && col.Layout == vec.Flat:
		oc.AppendFloats(o, col.Floats[a:a+n])
	case (col.Kind == vec.KindString || col.Kind == vec.KindVariant) && col.Layout == vec.Flat:
		oc.AppendByteRun(o, col.Data, col.Offsets[a:a+n+1])
	default:
		for i := 0; i < n; i++ {
			copyValue(oc, col, a+i, o+i)
		}
	}
}

// appendRows copies the listed rows of a column; row r lands on output row
// o + r - a.
func appendRows(oc *shred.Column, col *vec.Column, rows []uint32, a, o int) {
	for _, r := range rows {
		copyValue(oc, col, int(r), o+int(r)-a)
	}
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

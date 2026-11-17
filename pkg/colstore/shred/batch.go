package shred

import (
	"errors"
	"time"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

const (
	// RawPath holds the text of records that are not JSON objects.
	RawPath = "_raw"
	// RestPath holds the members that did not get a column of their own.
	RestPath = "_rest"

	// MaxRows is the most rows a block of a part takes, which keeps row
	// numbers inside it addressable with 16 bits.
	MaxRows = 1 << 16

	defaultMaxColumns = 2048
	maxDepth          = 64
	indexThreshold    = 16
)

// Options configures how documents become rows.
type Options struct {
	// TimeField is the path whose value becomes the row timestamp.
	TimeField string
	// MaxColumns bounds the columns of one batch; members beyond it are
	// kept in the RestPath column.
	MaxColumns int
}

// Batch is an append-only columnar buffer; one written as a block of a part
// holds at most MaxRows rows.
type Batch struct {
	Rows  int
	Times []int64 // unix nanoseconds per row
	Bytes int     // input bytes consumed

	opts  Options
	cols  []*Column
	paths map[string]*pathCols
	root  *node

	src []byte
	pos int

	timeCols *pathCols
	sort     *sortScratch // held only while the batch is being sorted
	seq      uint64
	rest     []byte
	restMark int
	scratch  []byte
}

type pathCols struct {
	byKind [vec.KindVariant + 1]*Column
	// last is the column that received this path's value in document seq.
	last *Column
	seq  uint64
}

type node struct {
	key      string
	path     string
	ord      int
	cols     *pathCols
	children []*node
	index    map[string]*node
}

// NewBatch returns an empty batch.
func NewBatch(opts Options) *Batch {
	if opts.MaxColumns <= 0 {
		opts.MaxColumns = defaultMaxColumns
	}
	b := &Batch{opts: opts}
	b.init()

	return b
}

func (b *Batch) init() {
	b.cols = b.cols[:0]
	b.paths = make(map[string]*pathCols, 64)
	b.root = &node{}
	b.timeCols = nil
}

// Columns returns every column, including ones without values in this batch.
func (b *Batch) Columns() []*Column { return b.cols }

// Full reports whether the batch cannot take another row.
func (b *Batch) Full() bool { return b.Rows >= MaxRows }

// Reset drops the rows and keeps the learned paths and buffers.
func (b *Batch) Reset() {
	for _, c := range b.cols {
		c.reset()
	}
	b.Rows = 0
	b.Times = b.Times[:0]
	b.Bytes = 0
}

const maxRetainedBytes = 256 << 20

var batchPool = mem.Pool[Batch]{Reset: func(b *Batch) bool {
	retained := cap(b.Times) * 8
	for _, c := range b.cols {
		retained += c.retained()
	}
	if retained > maxRetainedBytes || len(b.cols) > 2*b.opts.MaxColumns {
		return false
	}
	b.Reset()
	b.src = nil

	return true
}}

// Get returns a pooled batch configured with opts.
func Get(opts Options) *Batch {
	if opts.MaxColumns <= 0 {
		opts.MaxColumns = defaultMaxColumns
	}
	b := batchPool.Get()
	if b.root == nil || b.opts != opts {
		b.opts = opts
		b.init()
	}

	return b
}

// Put returns b to the pool; the caller must not use it afterwards.
func Put(b *Batch) { batchPool.Put(b) }

// put returns the column for a value of kind at path, placed on the current
// row, dropping an earlier value of the same path in this document.
func (b *Batch) put(pc *pathCols, path string, kind vec.Kind) *Column {
	c := pc.byKind[kind]
	if c == nil {
		c = &Column{Path: path, Kind: kind, lastRow: -1}
		pc.byKind[kind] = c
		b.cols = append(b.cols, c)
	}
	if pc.seq == b.seq && pc.last != c && pc.last.lastRow == int32(b.Rows) {
		pc.last.pop()
	}
	pc.seq, pc.last = b.seq, c
	c.place(b.Rows)

	return c
}

func (b *Batch) pathCols(path string) *pathCols {
	pc := b.paths[path]
	if pc == nil {
		pc = &pathCols{}
		b.paths[path] = pc
	}

	return pc
}

// child returns the node for key under n, or nil when the batch has no room
// for another path.
func (b *Batch) child(n *node, key []byte, hint *int) *node {
	ch := n.children
	// Documents of one source list their keys in the same order.
	if h := *hint; h < len(ch) && string(key) == ch[h].key {
		*hint = h + 1

		return ch[h]
	}
	if n.index != nil {
		if c, ok := n.index[string(key)]; ok {
			*hint = c.ord + 1

			return c
		}
	} else {
		for i, c := range ch {
			if string(key) == c.key {
				*hint = i + 1

				return c
			}
		}
	}
	if len(b.paths) >= b.opts.MaxColumns {
		return nil
	}
	c := &node{key: string(key), ord: len(ch)}
	if n.path == "" && n != b.root {
		c.path = "." + c.key
	} else if n == b.root {
		c.path = c.key
	} else {
		c.path = n.path + "." + c.key
	}
	c.cols = b.pathCols(c.path)
	n.children = append(n.children, c)
	if n.index != nil {
		n.index[c.key] = c
	} else if len(n.children) > indexThreshold {
		n.index = make(map[string]*node, 2*len(n.children))
		for _, x := range n.children {
			n.index[x.key] = x
		}
	}
	*hint = c.ord + 1

	return c
}

// AddText appends a row holding text in the RawPath column.
func (b *Batch) AddText(text []byte, ts int64) {
	b.seq++
	c := b.put(b.pathCols(RawPath), RawPath, vec.KindString)
	c.Data = append(c.Data, text...)
	c.Ends = append(c.Ends, uint32(len(c.Data)))
	b.Times = append(b.Times, ts)
	b.Bytes += len(text)
	b.Rows++
}

// ErrNotObject reports a document whose top-level value is not an object.
var ErrNotObject = errors.New("shred: document is not a JSON object")

// AddJSON appends one JSON object as a row timestamped ts, or by
// Options.TimeField when the document carries it, and leaves the batch
// unchanged on error.
func (b *Batch) AddJSON(doc []byte, ts int64) error {
	b.src, b.pos = doc, 0
	b.seq++
	b.skipSpace()
	if b.pos >= len(doc) || doc[b.pos] != '{' {
		return ErrNotObject
	}
	b.pos++
	err := b.object(b.root, 0)
	if err == nil {
		b.skipSpace()
		if b.pos != len(doc) {
			err = b.syntax("data after the document")
		}
	}
	if err != nil {
		b.rollback()

		return err
	}
	b.endRow(ts, len(doc))

	return nil
}

// endRow completes the row whose values were just appended.
func (b *Batch) endRow(ts int64, size int) {
	if b.restMark != 0 {
		b.rest = variant.End(b.rest, b.restMark)
		c := b.put(b.pathCols(RestPath), RestPath, vec.KindVariant)
		c.Data = append(c.Data, b.rest...)
		c.Ends = append(c.Ends, uint32(len(c.Data)))
		b.rest, b.restMark = b.rest[:0], 0
	}
	if b.opts.TimeField != "" {
		if t, ok := b.rowTime(); ok {
			ts = t
		}
	}
	b.Times = append(b.Times, ts)
	b.Bytes += size
	b.Rows++
}

func (b *Batch) rollback() {
	row := int32(b.Rows)
	for _, c := range b.cols {
		if c.lastRow == row {
			c.pop()
		}
	}
	b.rest, b.restMark = b.rest[:0], 0
}

func (b *Batch) rowTime() (int64, bool) {
	if b.timeCols == nil {
		b.timeCols = b.paths[b.opts.TimeField]
		if b.timeCols == nil {
			return 0, false
		}
	}
	row := int32(b.Rows)
	if c := b.timeCols.byKind[vec.KindInt]; c != nil && c.lastRow == row {
		return EpochNanos(c.Ints[c.Count-1]), true
	}
	if c := b.timeCols.byKind[vec.KindString]; c != nil && c.lastRow == row {
		s := c.Bytes(c.Count - 1)
		if t, err := time.Parse(time.RFC3339Nano, unsafe.String(unsafe.SliceData(s), len(s))); err == nil {
			return t.UnixNano(), true
		}
	}

	return 0, false
}

// EpochNanos converts an epoch in seconds, milliseconds, microseconds or
// nanoseconds to nanoseconds, telling them apart by magnitude.
func EpochNanos(v int64) int64 {
	abs := v
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs < 1e11:
		return v * 1e9
	case abs < 1e14:
		return v * 1e6
	case abs < 1e17:
		return v * 1e3
	}

	return v
}

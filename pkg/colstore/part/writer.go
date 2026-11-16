package part

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"sort"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Options configures a Writer.
type Options struct {
	Level Level
	// Codec compresses the bytes of string and variant values that are not
	// dictionary encoded; everything else is small and stays on zstd.
	Codec Codec
	// NoTokenFilter leaves blocks without a token filter.
	NoTokenFilter bool
	// Ordered declares that the rows arrive ordered by their values at the
	// SortKey paths and then by timestamp; the writer records it unchecked.
	Ordered bool
	SortKey []string
}

// Writer encodes batches into a part, one block per batch.
type Writer struct {
	w    io.Writer
	opts Options
	off  uint64
	err  error

	paths []string
	ids   map[string]uint32
	index []byte
	keys  []byte // first sort key of each block, length-prefixed
	// lastKey is the sort key of the last row written.
	lastKey []byte
	rows    uint64
	raw     uint64
	minT    int64
	maxT    int64

	block     []byte
	stats     []byte
	ents      []dirEntry
	blobs     blobCodec // every section but the values of flat columns
	values    blobCodec
	e         encodeScratch
	ints      []int64
	dictBytes []byte // the integer codec owns e.tmp
	dict      dictBuilder
	tokens    tokenfilter.Set
	hashes    []uint64
	addStr    func(s []byte)
}

type dirEntry struct {
	pathID uint32
	kind   vec.Kind
	enc    uint8
	flags  uint8
	count  int
	off    uint32
	length uint32
	crc    uint32
	minI   int64
	maxI   int64
	minF   float64
	maxF   float64
	// constVal aliases batch memory, which outlives the block being built.
	constVal  []byte
	constBits uint64
}

// NewWriter starts a part on w.
func NewWriter(w io.Writer, opts Options) *Writer {
	pw := &Writer{w: w, opts: opts, ids: make(map[string]uint32, 64)}
	pw.blobs = blobCodec{codec: CodecZstd, level: opts.Level}
	pw.values = blobCodec{codec: opts.Codec, level: opts.Level}
	pw.addStr = func(s []byte) {
		pw.hashes = tokenfilter.AppendHashes(pw.hashes[:0], s)
		pw.tokens.Add(pw.hashes)
	}
	pw.pathID(TimePath)
	for _, path := range opts.SortKey {
		pw.pathID(path)
	}
	var head [headerSize]byte
	binary.LittleEndian.PutUint32(head[0:], fileMagic)
	binary.LittleEndian.PutUint32(head[4:], formatVersion)
	pw.write(head[:])

	return pw
}

func (w *Writer) write(p []byte) {
	if w.err != nil {
		return
	}
	n, err := w.w.Write(p)
	w.off += uint64(n)
	w.err = err
}

func (w *Writer) pathID(path string) uint32 {
	id, ok := w.ids[path]
	if !ok {
		id = uint32(len(w.paths))
		w.paths = append(w.paths, path)
		w.ids[path] = id
	}

	return id
}

// WriteBatch appends the rows of b as one block.
func (w *Writer) WriteBatch(b *shred.Batch) error {
	if b.Rows == 0 || w.err != nil {
		return w.err
	}
	rows := b.Rows
	w.block = w.block[:0]
	w.ents = w.ents[:0]
	w.tokens.Reset()

	minT, maxT := vec.MinMaxInt64(b.Times)
	ent := dirEntry{pathID: 0, kind: vec.KindInt, count: rows}
	w.encodeInts(&ent, b.Times)
	w.finish(&ent, 0)
	for _, c := range b.Columns() {
		if c.Count == 0 {
			continue
		}
		w.encodeColumn(c, rows)
	}
	sort.Slice(w.ents, func(i, j int) bool {
		a, b := &w.ents[i], &w.ents[j]
		if a.pathID != b.pathID {
			return a.pathID < b.pathID
		}

		return a.kind < b.kind
	})

	filterOff := len(w.block)
	if !w.opts.NoTokenFilter {
		w.block = tokenfilter.Append(w.block, w.tokens.Keys())
		for at, end := filterOff, len(w.block); at < end; at += filterPage {
			sum := crc32.Checksum(w.block[at:min(at+filterPage, end)], castagnoli)
			w.block = binary.LittleEndian.AppendUint32(w.block, sum)
		}
	}
	dirOff := len(w.block)
	w.appendDirectory()

	var entry [indexEntrySize]byte
	binary.LittleEndian.PutUint32(entry[0:], uint32(rows))
	binary.LittleEndian.PutUint64(entry[4:], uint64(minT))
	binary.LittleEndian.PutUint64(entry[12:], uint64(maxT))
	binary.LittleEndian.PutUint64(entry[20:], w.off)
	binary.LittleEndian.PutUint32(entry[28:], uint32(len(w.block)))
	binary.LittleEndian.PutUint32(entry[32:], uint32(filterOff))
	binary.LittleEndian.PutUint32(entry[36:], uint32(dirOff-filterOff))
	binary.LittleEndian.PutUint32(entry[40:], uint32(dirOff))
	binary.LittleEndian.PutUint32(entry[44:], uint32(len(w.block)-dirOff))
	binary.LittleEndian.PutUint32(entry[48:], crc32.Checksum(w.block[dirOff:], castagnoli))
	w.index = append(w.index, entry[:]...)
	if w.opts.Ordered {
		w.e.tmp = appendEdgeKey(w.e.tmp[:0], b, w.opts.SortKey, false)
		w.keys = binary.AppendUvarint(w.keys, uint64(len(w.e.tmp)))
		w.keys = append(w.keys, w.e.tmp...)
		w.lastKey = appendEdgeKey(w.lastKey[:0], b, w.opts.SortKey, true)
	}

	if w.rows == 0 || minT < w.minT {
		w.minT = minT
	}
	if w.rows == 0 || maxT > w.maxT {
		w.maxT = maxT
	}
	w.rows += uint64(rows)
	w.raw += uint64(b.Bytes)
	w.write(w.block)

	return w.err
}

func (w *Writer) finish(ent *dirEntry, start int) {
	ent.off = uint32(start)
	ent.length = uint32(len(w.block) - start)
	ent.crc = crc32.Checksum(w.block[start:], castagnoli)
	w.ents = append(w.ents, *ent)
}

func (w *Writer) encodeColumn(c *shred.Column, rows int) {
	ent := dirEntry{pathID: w.pathID(c.Path), kind: c.Kind, count: c.Count}
	start := len(w.block)
	if c.Count != rows {
		ent.flags |= flagSparse
		w.appendPresence(c, rows)
	}
	switch c.Kind {
	case vec.KindNull:
		ent.enc = encNone
	case vec.KindBool:
		w.encodeBools(&ent, c.Bools)
	case vec.KindInt:
		w.encodeInts(&ent, c.Ints)
	case vec.KindFloat:
		w.encodeFloats(&ent, c.Floats)
	case vec.KindString, vec.KindVariant:
		w.encodeBytes(&ent, c)
	}
	w.finish(&ent, start)
}

func (w *Writer) appendPresence(c *shred.Column, rows int) {
	w.e.tmp = w.e.tmp[:0]
	switch absent := rows - c.Count; {
	case 2*c.Count < (rows+7)/8:
		for i := 0; i < c.Count; i++ {
			r := c.Row(i)
			w.e.tmp = append(w.e.tmp, byte(r), byte(r>>8))
		}
		w.block = append(w.block, presenceList)
	case 2*absent < (rows+7)/8:
		next := 0
		for i := 0; i <= c.Count; i++ {
			r := rows
			if i < c.Count {
				r = c.Row(i)
			}
			for ; next < r; next++ {
				w.e.tmp = append(w.e.tmp, byte(next), byte(next>>8))
			}
			next = r + 1
		}
		w.block = append(w.block, presenceHoles)
	default:
		w.e.tmp = append(w.e.tmp, make([]byte, (rows+7)/8)...)
		for i := 0; i < c.Count; i++ {
			r := c.Row(i)
			w.e.tmp[r>>3] |= 1 << (uint(r) & 7)
		}
		w.block = append(w.block, presenceBitmap)
	}
	w.block = w.blobs.appendBlob(w.block, w.e.tmp, &w.e.zbuf)
}

func (w *Writer) encodeBools(ent *dirEntry, vals []uint8) {
	same := true
	for _, v := range vals[1:] {
		if v != vals[0] {
			same = false

			break
		}
	}
	if same {
		ent.enc = encConst
		ent.constVal = vals[:1]

		return
	}
	ent.enc = encFlat
	w.e.tmp = append(w.e.tmp[:0], make([]byte, (len(vals)+7)/8)...)
	for i, v := range vals {
		w.e.tmp[i>>3] |= v << (uint(i) & 7)
	}
	w.block = w.blobs.appendBlob(w.block, w.e.tmp, &w.e.zbuf)
}

func (w *Writer) encodeInts(ent *dirEntry, vals []int64) {
	ent.minI, ent.maxI = vec.MinMaxInt64(vals)
	if ent.minI == ent.maxI {
		ent.enc = encConst

		return
	}
	ent.enc = encFlat
	w.block = appendInts(w.block, vals, &w.blobs, &w.e)
}

func (w *Writer) encodeFloats(ent *dirEntry, vals []float64) {
	same := true
	ent.minF, ent.maxF = math.Inf(1), math.Inf(-1)
	first := math.Float64bits(vals[0])
	for _, v := range vals {
		if math.Float64bits(v) != first {
			same = false
		}
		// NaN fails both comparisons and stays out of the range.
		if v < ent.minF {
			ent.minF = v
		}
		if v > ent.maxF {
			ent.maxF = v
		}
	}
	if same {
		ent.enc = encConst
		ent.constBits = first

		return
	}
	ent.enc = encFlat
	w.e.tmp = w.e.tmp[:0]
	for _, v := range vals {
		w.e.tmp = binary.LittleEndian.AppendUint64(w.e.tmp, math.Float64bits(v))
	}
	w.block = w.blobs.appendBlob(w.block, w.e.tmp, &w.e.zbuf)
}

func (w *Writer) tokenize(kind vec.Kind, val []byte) {
	if w.opts.NoTokenFilter {
		return
	}
	if kind == vec.KindVariant {
		variant.ForEachString(val, w.addStr)

		return
	}
	w.hashes = tokenfilter.AppendHashes(w.hashes[:0], val)
	w.tokens.Add(w.hashes)
}

func (w *Writer) encodeBytes(ent *dirEntry, c *shred.Column) {
	distinct, ok := w.dict.build(c)
	if ok && distinct == 1 {
		ent.enc = encConst
		ent.constVal = c.Bytes(0)
		w.tokenize(c.Kind, ent.constVal)

		return
	}
	if !ok {
		ent.enc = encFlat
		w.ints = w.ints[:0]
		for i := 0; i < c.Count; i++ {
			val := c.Bytes(i)
			w.ints = append(w.ints, int64(len(val)))
			w.tokenize(c.Kind, val)
		}
		w.block = appendInts(w.block, w.ints, &w.blobs, &w.e)
		w.block = w.values.appendBlob(w.block, c.Data[:c.Ends[c.Count-1]], &w.e.zbuf)

		return
	}

	ent.enc = encDict
	d := &w.dict
	w.block = binary.AppendUvarint(w.block, uint64(distinct))
	w.ints = w.ints[:0]
	w.dictBytes = w.dictBytes[:0]
	for _, entry := range d.order {
		val := c.Bytes(int(d.first[entry]))
		w.ints = append(w.ints, int64(len(val)))
		w.dictBytes = append(w.dictBytes, val...)
		w.tokenize(c.Kind, val)
	}
	w.block = appendInts(w.block, w.ints, &w.blobs, &w.e)
	w.block = w.blobs.appendBlob(w.block, w.dictBytes, &w.e.zbuf)

	// Codes stay out of the general codec so that reading them costs a few
	// shifts per row; runs take over when they halve the packed size.
	width := uint(bits.Len(uint(distinct - 1)))
	limit := packedSize(len(d.codes), width) / 2
	w.e.tmp = w.e.tmp[:0]
	for i := 0; i < len(d.codes) && len(w.e.tmp) < limit; {
		run := 1
		for i+run < len(d.codes) && d.codes[i+run] == d.codes[i] {
			run++
		}
		w.e.tmp = binary.AppendUvarint(w.e.tmp, uint64(d.rank[d.codes[i]]))
		w.e.tmp = binary.AppendUvarint(w.e.tmp, uint64(run))
		i += run
	}
	if len(w.e.tmp) < limit {
		w.block = append(append(w.block, codesRuns), w.e.tmp...)

		return
	}
	w.block = appendPacked(append(w.block, codesPacked), d.codes, d.rank, width)
}

// appendDirectory writes the entries at a fixed size so that a reader can
// find one column without decoding the others, then what varies in size.
func (w *Writer) appendDirectory() {
	w.block = binary.AppendUvarint(w.block, uint64(len(w.ents)))
	w.stats = w.stats[:0]
	for i := range w.ents {
		e := &w.ents[i]
		w.block = binary.LittleEndian.AppendUint32(w.block, e.pathID)
		w.block = append(w.block, byte(e.kind), e.enc, e.flags, 0)
		w.block = binary.LittleEndian.AppendUint32(w.block, uint32(e.count))
		w.block = binary.LittleEndian.AppendUint32(w.block, e.off)
		w.block = binary.LittleEndian.AppendUint32(w.block, e.length)
		w.block = binary.LittleEndian.AppendUint32(w.block, e.crc)
		w.block = binary.LittleEndian.AppendUint32(w.block, uint32(len(w.stats)))
		switch e.kind {
		case vec.KindInt:
			w.stats = binary.AppendUvarint(w.stats, zigzag(e.minI))
			w.stats = binary.AppendUvarint(w.stats, zigzag(e.maxI))
		case vec.KindFloat:
			w.stats = binary.LittleEndian.AppendUint64(w.stats, math.Float64bits(e.minF))
			w.stats = binary.LittleEndian.AppendUint64(w.stats, math.Float64bits(e.maxF))
		}
		switch {
		case e.enc != encConst || e.kind == vec.KindInt:
		case e.kind == vec.KindFloat:
			w.stats = binary.AppendUvarint(w.stats, 8)
			w.stats = binary.LittleEndian.AppendUint64(w.stats, e.constBits)
		default:
			w.stats = binary.AppendUvarint(w.stats, uint64(len(e.constVal)))
			w.stats = append(w.stats, e.constVal...)
		}
	}
	w.block = append(w.block, w.stats...)
}

// Close writes the footer; the part is complete once it returns nil.
func (w *Writer) Close() error {
	footer := binary.AppendUvarint(nil, uint64(len(w.paths)))
	for _, p := range w.paths {
		footer = binary.AppendUvarint(footer, uint64(len(p)))
		footer = append(footer, p...)
	}
	// 0 marks an unordered part; otherwise the count of key paths plus one.
	if !w.opts.Ordered {
		footer = binary.AppendUvarint(footer, 0)
	} else {
		footer = binary.AppendUvarint(footer, uint64(len(w.opts.SortKey))+1)
		for _, path := range w.opts.SortKey {
			footer = binary.AppendUvarint(footer, uint64(w.ids[path]))
		}
	}
	footer = binary.AppendUvarint(footer, uint64(len(w.index)/indexEntrySize))
	footer = append(footer, w.index...)
	footer = append(footer, w.keys...)
	if w.opts.Ordered {
		footer = binary.AppendUvarint(footer, uint64(len(w.lastKey)))
		footer = append(footer, w.lastKey...)
	}
	footer = binary.AppendUvarint(footer, w.rows)
	footer = binary.AppendUvarint(footer, w.raw)
	footer = binary.AppendUvarint(footer, zigzag(w.minT))
	footer = binary.AppendUvarint(footer, zigzag(w.maxT))

	footerOff := w.off
	w.write(footer)
	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint64(trailer[0:], footerOff)
	binary.LittleEndian.PutUint64(trailer[8:], uint64(len(footer)))
	binary.LittleEndian.PutUint32(trailer[16:], crc32.Checksum(footer, castagnoli))
	binary.LittleEndian.PutUint32(trailer[20:], formatVersion)
	binary.LittleEndian.PutUint32(trailer[24:], fileMagic)
	w.write(trailer[:])

	return w.err
}

// dictBuilder finds the distinct values of a byte column.
type dictBuilder struct {
	slots []uint32 // entry number + 1, 0 when empty
	hash  []uint64
	first []uint32 // value number of each entry's first occurrence
	codes []uint32 // entry number of every value
	order []uint32 // entry numbers sorted by value
	rank  []uint32 // position of each entry in order
	col   *shred.Column
}

func (d *dictBuilder) Len() int      { return len(d.order) }
func (d *dictBuilder) Swap(i, j int) { d.order[i], d.order[j] = d.order[j], d.order[i] }
func (d *dictBuilder) Less(i, j int) bool {
	return string(d.col.Bytes(int(d.first[d.order[i]]))) < string(d.col.Bytes(int(d.first[d.order[j]])))
}

// build reports the number of distinct values of c and whether a dictionary
// pays off: every value equal, or at most half as many entries as values.
func (d *dictBuilder) build(c *shred.Column) (distinct int, ok bool) {
	n := c.Count
	limit := min(n/2, maxDictSize)
	if n < 8 {
		limit = 1
	}
	if n >= 1024 && d.sampleAllDistinct(c) {
		return 0, false
	}
	size := 64
	for size < 2*(limit+1) {
		size *= 2
	}
	if cap(d.slots) < size {
		d.slots = make([]uint32, size)
	}
	d.slots = d.slots[:size]
	clear(d.slots)
	d.hash, d.first, d.codes = d.hash[:0], d.first[:0], d.codes[:0]
	mask := uint64(size - 1)
	for i := 0; i < n; i++ {
		val := c.Bytes(i)
		h := xxhash.Sum64(val)
		slot := h & mask
		for {
			e := d.slots[slot]
			if e == 0 {
				if len(d.first) == limit {
					return 0, false
				}
				d.slots[slot] = uint32(len(d.first)) + 1
				d.codes = append(d.codes, uint32(len(d.first)))
				d.hash = append(d.hash, h)
				d.first = append(d.first, uint32(i))

				break
			}
			if d.hash[e-1] == h && string(c.Bytes(int(d.first[e-1]))) == string(val) {
				d.codes = append(d.codes, e-1)

				break
			}
			slot = (slot + 1) & mask
		}
	}
	distinct = len(d.first)
	d.order = d.order[:0]
	for i := 0; i < distinct; i++ {
		d.order = append(d.order, uint32(i))
	}
	d.col = c
	sort.Sort(d)
	d.col = nil
	if cap(d.rank) < distinct {
		d.rank = make([]uint32, distinct)
	}
	d.rank = d.rank[:distinct]
	for pos, entry := range d.order {
		d.rank[entry] = uint32(pos)
	}

	return distinct, true
}

// sampleAllDistinct reports whether 64 evenly spaced values are all
// different, which rules a dictionary out without hashing the column.
func (d *dictBuilder) sampleAllDistinct(c *shred.Column) bool {
	var seen [64]uint64
	step := c.Count / len(seen)
	for i := range seen {
		h := xxhash.Sum64(c.Bytes(i * step))
		for _, prev := range seen[:i] {
			if prev == h {
				return false
			}
		}
		seen[i] = h
	}

	return true
}

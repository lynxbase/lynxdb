package part

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"sort"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/mem"
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
	// NoPacking leaves identifier-like strings to the general codec
	// instead of storing them at a few bits per character.
	NoPacking bool
	// ValueSlack, when positive, lets long string values take a codec that
	// is read several times faster where it makes them larger by at most
	// this many percent.
	ValueSlack int
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
	dirs  []byte // the column directory of every block
	keys  []byte // first sort key of each block, length-prefixed
	// lastKey is the sort key of the last row written.
	lastKey []byte
	// keyStats holds, per block, the leading key columns its first row
	// shares with the row before it, then the number of distinct key
	// prefixes of every length.
	keyStats []byte
	ks       keyStatScratch
	rows     uint64
	raw      uint64
	minT     int64
	maxT     int64
	// times holds the row times of the block being written.
	times []int64
	// copied counts the blocks taken over from other parts as stored.
	copied int
	// groups holds the offset, length and stripe count of every filter
	// group written.
	groups []byte
	// filters holds an entry for every filter of the part, the last
	// groupFilters of them for the group being built. That group has
	// grouped blocks, whose filters take stripe bloom blocks in every
	// stripe, and stripes stripes; it closes at groupBlocks blocks or at
	// the share of groupBytes of filters that its stripes are of
	// filterStripes. fixedStripes, when set, is the number of stripes of
	// every group that a block does not need another number for.
	filters      []byte
	groupFilters int
	grouped      int
	stripe       uint32
	groupBlocks  int
	groupBytes   int
	stripes      int
	fixedStripes int
	// columnBytes is the encoded size from which a column of a block gets
	// a filter of its own.
	columnBytes int
	// verified marks the filter pages of group verifiedGroup, a group of
	// a part being merged, that copyBlock has checked.
	verified      vec.Bitmap
	verifiedGroup *filterGroup
	// trials holds, per path and kind, how the values of a flat column are
	// encoded.
	trials []trial

	blobs  blobCodec // every section but the values of flat columns
	values blobCodec
	addStr func(s []byte)
	// Close hands the buffers on to the next writer.
	*writerBuffers
}

// trial is the outcome of encoding one column's values in several ways.
type trial struct {
	// left counts the blocks until the ways are compared again.
	left   uint8
	choice uint8
}

// The ways the values of a flat column are encoded.
const (
	valuesFast = iota
	valuesFastest
	valuesLZ4
)

// longValue is the mean length from which values count as text that a scan
// reads in bulk.
const longValue = 32

const (
	// trialEvery is how many blocks pass between two comparisons.
	trialEvery = 16
	// trialMinSize is the size under which values are not worth comparing.
	trialMinSize = 4096
)

// writerBuffers holds what a writer reuses from block to block.
type writerBuffers struct {
	block     []byte
	stats     []byte
	ents      []dirEntry
	e         encodeScratch
	ints      []int64
	dictBytes []byte // the integer codec owns e.tmp
	dict      dictBuilder
	// column holds the tokens of the column being encoded, and tokens
	// those of the block's columns without a filter of their own. The
	// columns of the block that get one have their tokens in tokenKeys, where
	// owned locates them, until the block's filters are built.
	column    tokenfilter.Set
	tokens    tokenfilter.Set
	hashes    []uint64
	tokenKeys []uint64
	owned     []ownedTokens
	// blooms holds the filters of the group being built, end to end, and
	// pages the group as it is written.
	blooms []byte
	pages  []byte
}

// ownedTokens names a column that gets a filter of its own and the end of
// its tokens among those held for the block.
type ownedTokens struct {
	path uint32
	kind vec.Kind
	end  int
}

// maxWriterBytes bounds the block and compression buffers a pooled
// writerBuffers keeps.
const maxWriterBytes = 64 << 20

var writerPool = mem.Pool[writerBuffers]{Reset: func(b *writerBuffers) bool {
	// Directory entries alias the values of the last batch.
	clear(b.ents[:cap(b.ents)])

	return cap(b.block)+cap(b.e.tmp)+cap(b.e.zbuf)+cap(b.dictBytes)+cap(b.blooms)+cap(b.pages)+8*cap(b.tokenKeys) <= maxWriterBytes
}}

// errClosed reports a write to a writer after Close.
var errClosed = errors.New("part: writer is closed")

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
	pw := &Writer{ids: make(map[string]uint32, 64)}
	pw.addStr = func(s []byte) {
		pw.hashes = tokenfilter.AppendHashes(pw.hashes[:0], s)
		pw.column.Add(pw.hashes)
	}
	pw.Reset(w, opts)

	return pw
}

// Reset starts another part on w.
func (w *Writer) Reset(out io.Writer, opts Options) {
	if w.writerBuffers == nil {
		w.writerBuffers = writerPool.Get()
	}
	w.w, w.opts, w.off, w.err, w.copied = out, opts, 0, nil, 0
	clear(w.trials)
	w.paths = w.paths[:0]
	clear(w.ids)
	w.index, w.dirs, w.keys, w.lastKey, w.keyStats = w.index[:0], w.dirs[:0], w.keys[:0], w.lastKey[:0], w.keyStats[:0]
	w.rows, w.raw, w.minT, w.maxT = 0, 0, 0, 0
	w.groups, w.filters, w.blooms = w.groups[:0], w.filters[:0], w.blooms[:0]
	w.groupFilters, w.grouped, w.stripe, w.verifiedGroup = 0, 0, 0, nil
	w.groupBlocks, w.groupBytes, w.stripes, w.fixedStripes = filterGroupBlocks, filterGroupBytes, 0, 0
	w.columnBytes = filterColumnBytes
	w.blobs.codec, w.blobs.level = CodecZstd, opts.Level
	w.values.codec, w.values.level = opts.Codec, opts.Level
	w.pathID(TimePath)
	for _, path := range opts.SortKey {
		w.pathID(path)
	}
	var head [headerSize]byte
	binary.LittleEndian.PutUint32(head[0:], fileMagic)
	binary.LittleEndian.PutUint32(head[4:], formatVersion)
	w.write(head[:])
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
	w.tokenKeys, w.owned = w.tokenKeys[:0], w.owned[:0]

	minT, maxT := vec.MinMaxInt64(b.Times)
	ent := dirEntry{pathID: 0, kind: vec.KindInt, count: rows}
	w.encodeInts(&ent, b.Times)
	w.finish(&ent, 0)
	w.times = b.Times
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

	// The directory is built at the end of the block buffer and moved to
	// the part's directory section, which is written after the last block.
	blockLen := len(w.block)
	w.appendDirectory()
	dir := w.block[blockLen:]

	var entry [indexEntrySize]byte
	binary.LittleEndian.PutUint32(entry[0:], uint32(rows))
	binary.LittleEndian.PutUint64(entry[4:], uint64(minT))
	binary.LittleEndian.PutUint64(entry[12:], uint64(maxT))
	binary.LittleEndian.PutUint32(entry[28:], uint32(blockLen))
	if !w.opts.NoTokenFilter {
		w.filterBlock(entry[:])
	}
	// The block lies behind the group that its filters may have closed.
	binary.LittleEndian.PutUint64(entry[20:], w.off)
	binary.LittleEndian.PutUint32(entry[40:], uint32(len(w.dirs)))
	binary.LittleEndian.PutUint32(entry[44:], uint32(len(dir)))
	binary.LittleEndian.PutUint32(entry[48:], crc32.Checksum(dir, castagnoli))
	w.index = append(w.index, entry[:]...)
	w.dirs = append(w.dirs, dir...)
	w.block = w.block[:blockLen]
	if w.opts.Ordered {
		w.e.tmp = appendEdgeKey(w.e.tmp[:0], b, w.opts.SortKey, false)
		w.keys = binary.AppendUvarint(w.keys, uint64(len(w.e.tmp)))
		w.keys = append(w.keys, w.e.tmp...)
		w.appendKeyStats(b, w.e.tmp)
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
	w.closeGroup(false)

	return w.err
}

// startFilters names in a block's index entry the group being built and the
// first of the block's filters.
func (w *Writer) startFilters(entry []byte, first int) {
	binary.LittleEndian.PutUint32(entry[32:], uint32(len(w.groups)/groupEntrySize))
	binary.LittleEndian.PutUint32(entry[36:], uint32(first))
	w.grouped++
}

// filterColumn gives the column whose tokens w.column holds, encoded in
// encoded bytes, a filter of its own when it is large, and leaves them to the
// filter of the block's other columns when it is small.
func (w *Writer) filterColumn(path uint32, kind vec.Kind, encoded int) {
	keys := w.column.Keys()
	if encoded < w.columnBytes {
		w.tokens.Add(keys)

		return
	}
	w.tokenKeys = append(w.tokenKeys, keys...)
	w.owned = append(w.owned, ownedTokens{path: path, kind: kind, end: len(w.tokenKeys)})
}

// filterBlock builds the filters of the block being written from the tokens
// of its columns, in the group being built or, when the block's filters suit
// its stripes badly, in another that it starts, and names them in the
// block's index entry.
func (w *Writer) filterBlock(entry []byte) {
	shared := w.tokens.Keys()
	// A block without tokens has an empty filter, so that it has one.
	rest := len(shared) > 0 || len(w.owned) == 0
	total, start := 0, 0
	if rest {
		total = tokenfilter.Blocks(len(shared), tokenfilter.SharedBits)
	}
	for _, o := range w.owned {
		total += tokenfilter.Blocks(o.end-start, tokenfilter.ColumnBits)
		start = o.end
	}
	stripes := w.fixedStripes
	if stripes == 0 {
		stripes = min(1<<(bits.Len(uint(total/filterRounding|1))-1), filterStripes)
	}
	if w.grouped > 0 && (stripes > 2*w.stripes || w.stripes > 2*stripes) {
		w.closeGroup(true)
	}
	if w.grouped == 0 {
		w.stripes = stripes
	}
	w.startFilters(entry, len(w.filters)/filterEntrySize)
	start = 0
	for _, o := range w.owned {
		w.appendFilter(o.path, o.kind, w.tokenKeys[start:o.end])
		start = o.end
	}
	if rest {
		w.appendFilter(noPath, 0, shared)
	}
}

// appendFilter adds the filter of keys to the group being built, as that of
// a column of the block being written or with noPath as that of its columns
// that have none.
func (w *Writer) appendFilter(path uint32, kind vec.Kind, keys []uint64) {
	start := len(w.blooms)
	bits := tokenfilter.ColumnBits
	if path == noPath {
		bits = tokenfilter.SharedBits
	}
	w.blooms = tokenfilter.Append(w.blooms, keys, w.stripes, bits)
	w.addFilter(path, kind, (len(w.blooms)-start)/tokenfilter.BlockBytes)
}

// addFilter records the filter of blocks bloom blocks that was appended to
// w.blooms for the block being written.
func (w *Writer) addFilter(path uint32, kind vec.Kind, blocks int) {
	pieces := uint32(blocks / w.stripes)
	w.filters = binary.LittleEndian.AppendUint32(w.filters, path)
	w.filters = append(w.filters, byte(kind), 0, 0, 0)
	w.filters = binary.LittleEndian.AppendUint32(w.filters, pieces)
	w.filters = binary.LittleEndian.AppendUint32(w.filters, w.stripe)
	w.stripe += pieces
	w.groupFilters++
}

// groupFlush is the size from which closeGroup hands pages on to the file.
const groupFlush = 1 << 20

var filterPad [filterPage]byte

// closeGroup writes the filters of the group being built, striped, once the
// group is full or, with last set, whatever it holds.
func (w *Writer) closeGroup(last bool) {
	if w.grouped == 0 || (!last && w.grouped < w.groupBlocks && len(w.blooms) < w.groupBytes/filterStripes*w.stripes) {
		return
	}
	blocks := int(w.stripe) * w.stripes
	size := (blocks + (blocks+pageBlocks-1)/pageBlocks) * tokenfilter.BlockBytes
	if size >= filterAligned {
		w.write(filterPad[:-w.off&(filterPage-1)])
	}
	w.groups = binary.LittleEndian.AppendUint64(w.groups, w.off)
	w.groups = binary.LittleEndian.AppendUint64(w.groups, uint64(size))
	w.groups = binary.LittleEndian.AppendUint32(w.groups, uint32(w.stripes))

	entries := w.filters[len(w.filters)-w.groupFilters*filterEntrySize:]
	pages, placed := w.pages[:0], 0
	for k := 0; k < w.stripes; k++ {
		at := 0
		for e := entries; len(e) > 0; e = e[filterEntrySize:] {
			piece := int(binary.LittleEndian.Uint32(e[8:])) * tokenfilter.BlockBytes
			src := w.blooms[at+k*piece : at+(k+1)*piece]
			at += w.stripes * piece
			for len(src) > 0 {
				if placed%pageBlocks == 0 {
					if len(pages) >= groupFlush {
						w.writePages(pages)
						pages = pages[:0]
					}
					pages = append(pages, filterPad[:tokenfilter.BlockBytes]...)
				}
				n := min(len(src), (pageBlocks-placed%pageBlocks)*tokenfilter.BlockBytes)
				pages = append(pages, src[:n]...)
				src, placed = src[n:], placed+n/tokenfilter.BlockBytes
			}
		}
	}
	w.writePages(pages)
	w.pages, w.blooms, w.groupFilters, w.grouped, w.stripe = pages[:0], w.blooms[:0], 0, 0, 0
}

// writePages writes filter pages after putting the checksum of each at its
// start.
func (w *Writer) writePages(pages []byte) {
	for at := 0; at < len(pages); at += filterPage {
		page := pages[at:min(at+filterPage, len(pages))]
		binary.LittleEndian.PutUint32(page, crc32.Checksum(page[4:], castagnoli))
	}
	w.write(pages)
}

// copies reports whether block i of src can become a block of w as it is
// stored. The level and codec of its sections are not recorded, so a writer
// that asks for the compact level or another codec than the default one
// encodes every block itself.
func (w *Writer) copies(src *Part, i int) bool {
	return w.opts.Level != LevelCompact && w.opts.Codec == CodecZstd &&
		(w.opts.NoTokenFilter || len(src.groups) > 0)
}

// joinGroup makes the group being built one that the filters first to end
// of group g of src can be copied into: one whose stripes cut every filter
// at bloom block boundaries, into pieces not much larger than they need be.
func (w *Writer) joinGroup(src *Part, g *filterGroup, first, end int) {
	common := 0
	for f := first; f < end; f++ {
		a := src.filterBlocks(g, f)
		for common != 0 {
			a, common = common, a%common
		}
		common = a
	}
	stripes := w.fixedStripes
	if stripes == 0 || common%stripes != 0 {
		for stripes = min(common, filterStripes); common%stripes != 0; stripes-- {
		}
	}
	if w.grouped > 0 && (common%w.stripes != 0 || stripes > 2*w.stripes) {
		w.closeGroup(true)
	}
	if w.grouped == 0 {
		w.stripes = stripes
	}
}

// copyBlock appends block i of src without decoding its columns. pathMap
// holds the id in w of every path of src met so far and noPath for the
// others, and raw is the input size accounted to the block. follows says
// that the block before it in w is block i-1 of src; otherwise the writer's
// last key must be that of its last row, not a bound on it.
func (w *Writer) copyBlock(src *Part, i int, pathMap []uint32, raw int, follows bool) error {
	if w.err != nil {
		return w.err
	}
	e := src.index[i*indexEntrySize:]
	rows, minT, maxT := src.BlockStats(i)
	start := binary.LittleEndian.Uint64(e[20:])
	data := src.blocks[start : start+uint64(binary.LittleEndian.Uint32(e[28:]))]
	if src.readAhead != nil {
		src.readAhead(int(start), len(data))
	}
	dirOff := binary.LittleEndian.Uint32(e[40:])
	dir := src.directories()[dirOff : dirOff+binary.LittleEndian.Uint32(e[44:])]
	if crc32.Checksum(dir, castagnoli) != binary.LittleEndian.Uint32(e[48:]) {
		return ErrCorrupt
	}
	n, size := binary.Uvarint(dir)
	if size <= 0 || n > uint64(len(dir)-size)/dirEntrySize {
		return ErrCorrupt
	}
	entries := size + int(n)*dirEntrySize

	w.block = append(w.block[:0], data...)
	newDir := len(w.block)
	w.block = append(w.block, dir[:entries]...)
	// Path ids are per part, and entries stay ordered by id and kind.
	ents := w.block[newDir+size:]
	sorted, prev := true, uint64(0)
	for at := 0; at < len(ents); at += dirEntrySize {
		id := binary.LittleEndian.Uint32(ents[at:])
		if uint64(id) >= uint64(len(pathMap)) {
			return ErrCorrupt
		}
		if pathMap[id] == noPath {
			pathMap[id] = w.pathID(src.paths[id])
		}
		binary.LittleEndian.PutUint32(ents[at:], pathMap[id])
		key := uint64(pathMap[id])<<8 | uint64(ents[at+4])
		sorted = sorted && key >= prev
		prev = key
	}
	if !sorted {
		w.e.tmp = sortEntries(w.e.tmp[:0], ents)
		copy(ents, w.e.tmp)
	}
	w.block = append(w.block, dir[entries:]...)

	var entry [indexEntrySize]byte
	if !w.opts.NoTokenFilter {
		g, first, end := src.blockFilters(i)
		w.joinGroup(src, g, first, end)
		// The pages of the source are checked here because the group
		// being built gets checksums of its own.
		if w.verifiedGroup != g {
			w.verifiedGroup = g
			w.verified = grow(w.verified, vec.BitmapWords(g.pages()))
			clear(w.verified)
		}
		start := len(w.blooms)
		for f := first; f < end; f++ {
			path, ok := binary.LittleEndian.Uint32(src.filters[f*filterEntrySize:]), false
			if w.blooms, ok = src.appendFilter(w.blooms, g, f, w.verified); !ok ||
				(path != noPath && (uint64(path) >= uint64(len(pathMap)) || pathMap[path] == noPath)) {
				w.blooms = w.blooms[:start]

				return ErrCorrupt
			}
		}
		w.startFilters(entry[:], len(w.filters)/filterEntrySize)
		for f := first; f < end; f++ {
			path := binary.LittleEndian.Uint32(src.filters[f*filterEntrySize:])
			if path != noPath {
				path = pathMap[path]
			}
			w.addFilter(path, vec.Kind(src.filters[f*filterEntrySize+4]), src.filterBlocks(g, f))
		}
	}
	copy(entry[:20], e[:20])
	binary.LittleEndian.PutUint64(entry[20:], w.off)
	binary.LittleEndian.PutUint32(entry[28:], uint32(newDir))
	binary.LittleEndian.PutUint32(entry[40:], uint32(len(w.dirs)))
	binary.LittleEndian.PutUint32(entry[44:], uint32(len(w.block)-newDir))
	binary.LittleEndian.PutUint32(entry[48:], crc32.Checksum(w.block[newDir:], castagnoli))
	w.index = append(w.index, entry[:]...)
	w.dirs = append(w.dirs, w.block[newDir:]...)
	w.block = w.block[:newDir]
	w.keys = binary.AppendUvarint(w.keys, uint64(len(src.firstKeys[i])))
	w.keys = append(w.keys, src.firstKeys[i]...)
	if key := w.opts.SortKey; len(key) > 0 {
		// The statistics of the rows are the block's own; what its first
		// row shares with the row before depends on where it lands.
		shared := 0
		switch {
		case follows:
			shared = src.KeyShared(i)
		case w.rows > 0:
			shared = SharedKeyColumns(w.lastKey, src.firstKeys[i])
		}
		w.keyStats = binary.AppendUvarint(w.keyStats, uint64(shared))
		for d := 1; d <= len(key); d++ {
			w.keyStats = binary.AppendUvarint(w.keyStats, uint64(src.KeyRuns(i, d)))
		}
	}
	w.lastKey = append(w.lastKey[:0], src.upperKey(i)...)

	if w.rows == 0 || minT < w.minT {
		w.minT = minT
	}
	if w.rows == 0 || maxT > w.maxT {
		w.maxT = maxT
	}
	w.rows += uint64(rows)
	w.raw += uint64(raw)
	w.copied++
	w.write(w.block)
	w.closeGroup(false)

	return w.err
}

// sortEntries appends the directory entries in ents to dst ordered by path
// id and kind.
func sortEntries(dst, ents []byte) []byte {
	n := len(ents) / dirEntrySize
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	key := func(i int) uint64 {
		return uint64(binary.LittleEndian.Uint32(ents[i*dirEntrySize:]))<<8 | uint64(ents[i*dirEntrySize+4])
	}
	sort.SliceStable(order, func(a, b int) bool { return key(order[a]) < key(order[b]) })
	for _, i := range order {
		dst = append(dst, ents[i*dirEntrySize:(i+1)*dirEntrySize]...)
	}

	return dst
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
		if power, ok := timeUnit(c.Ints, w.times); ok {
			// The member that the row times were taken from.
			ent.enc = encTime
			ent.minI, ent.maxI = vec.MinMaxInt64(c.Ints)
			w.block = append(w.block, power)
		} else {
			w.encodeInts(&ent, c.Ints)
		}
	case vec.KindFloat:
		w.encodeFloats(&ent, c.Floats)
	case vec.KindString, vec.KindVariant:
		w.column.Reset()
		w.encodeBytes(&ent, c)
		if !w.opts.NoTokenFilter {
			w.filterColumn(ent.pathID, c.Kind, len(w.block)-start)
		}
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

// timeUnits holds the nanoseconds of the units that a time is given in, by
// their power of ten.
var timeUnits = [...]int64{0: 1, 3: 1e3, 6: 1e6, 9: 1e9}

// timeUnit reports whether vals, one for every row, are the times of the
// rows in one coarser unit, and that unit's power of ten in nanoseconds.
// Columns that all hold one value stay constants.
func timeUnit(vals, times []int64) (power byte, ok bool) {
	if len(vals) != len(times) || vals[0] == 0 || times[0]%vals[0] != 0 {
		return 0, false
	}
	unit := times[0] / vals[0]
	for power = 0; int(power) < len(timeUnits) && timeUnits[power] != unit; power++ {
	}
	if int(power) == len(timeUnits) || unit == 0 {
		return 0, false
	}
	same := true
	for i, v := range vals {
		if times[i]%unit != 0 || times[i]/unit != v {
			return 0, false
		}
		same = same && v == vals[0]
	}

	return power, !same
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
	w.column.Add(w.hashes)
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
		stride := len(c.Bytes(0))
		for i := 0; i < c.Count; i++ {
			val := c.Bytes(i)
			if len(val) != stride {
				stride = 0
			}
			w.ints = append(w.ints, int64(len(val)))
			w.tokenize(c.Kind, val)
		}
		w.block = appendInts(w.block, w.ints, &w.blobs, &w.e)
		data := c.Data[:c.Ends[c.Count-1]]
		// Values that repeat are left to the general codec, which finds
		// the repeats; packing cannot.
		if stride > 0 && w.packs(c.Kind) && w.dict.fewRepeats(c) {
			if out, packed := w.values.appendPackedBlob(w.block, data, stride, &w.e.zbuf); packed {
				w.block = out

				return
			}
		}
		w.appendValues(int(ent.pathID)*kindSlots+int(c.Kind), data, c.Count)

		return
	}

	ent.enc = encDict
	d := &w.dict
	w.block = binary.AppendUvarint(w.block, uint64(distinct))
	w.ints = w.ints[:0]
	w.dictBytes = w.dictBytes[:0]
	stride := len(c.Bytes(int(d.first[d.order[0]])))
	for _, entry := range d.order {
		val := c.Bytes(int(d.first[entry]))
		if len(val) != stride {
			stride = 0
		}
		w.ints = append(w.ints, int64(len(val)))
		w.dictBytes = append(w.dictBytes, val...)
		w.tokenize(c.Kind, val)
	}
	w.block = appendInts(w.block, w.ints, &w.blobs, &w.e)
	out, packed := w.block, false
	if w.packs(c.Kind) {
		out, packed = w.blobs.appendPackedBlob(w.block, w.dictBytes, stride, &w.e.zbuf)
	}
	if !packed {
		out = w.blobs.appendBlob(w.block, w.dictBytes, &w.e.zbuf)
	}
	w.block = out

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

// packs reports whether strings of kind may be stored as packed sections.
func (w *Writer) packs(kind vec.Kind) bool {
	return kind == vec.KindString && !w.opts.NoPacking && w.opts.Codec == CodecZstd
}

// appendValues appends the count values of the flat column at slot, laid
// end to end in data, as a section. At LevelFast a column is encoded at
// LevelFastest for as long as that gave it no larger a section the last
// time both were tried: values with little to match, such as hashes, come
// out smaller and in half the time. With ValueSlack set, long values that
// the general codec shrinks little go to the codec that reads several times
// faster, when that costs at most the given share of their size.
func (w *Writer) appendValues(slot int, data []byte, count int) {
	bc := &w.values
	if bc.codec != CodecZstd || bc.level != LevelFast || len(data) < trialMinSize {
		w.block = bc.appendBlob(w.block, data, &w.e.zbuf)

		return
	}
	for len(w.trials) <= slot {
		w.trials = append(w.trials, trial{})
	}
	tr := &w.trials[slot]
	start := len(w.block)
	if tr.left == 0 {
		w.block = bc.appendBlob(w.block, data, &w.e.zbuf)
		fast := len(w.block)
		bc.level = LevelFastest
		w.block = bc.appendBlob(w.block, data, &w.e.zbuf)
		bc.level = LevelFast
		fastest := len(w.block)
		tr.left, tr.choice = trialEvery, valuesFast
		from, to := start, fast
		if fastest-fast <= fast-start {
			tr.choice, from, to = valuesFastest, fast, fastest
		}
		if slack := w.opts.ValueSlack; slack > 0 && len(data) >= longValue*count {
			bc.codec = CodecLZ4
			w.block = bc.appendBlob(w.block, data, &w.e.zbuf)
			bc.codec = CodecZstd
			if lz := len(w.block) - fastest; lz*100 <= (to-from)*(100+slack) {
				tr.choice, from, to = valuesLZ4, fastest, len(w.block)
			}
		}
		w.block = w.block[:start+copy(w.block[start:], w.block[from:to])]

		return
	}
	tr.left--
	switch tr.choice {
	case valuesFastest:
		bc.level = LevelFastest
	case valuesLZ4:
		bc.codec = CodecLZ4
	}
	w.block = bc.appendBlob(w.block, data, &w.e.zbuf)
	bc.codec, bc.level = CodecZstd, LevelFast
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

// keyStatScratch resolves, for every row of a block and every sort key path,
// the kind of the row's value and its position in the column of that kind.
type keyStatScratch struct {
	kinds []uint8
	vals  []uint32
	cols  []*shred.Column
	diff  []int
}

// same reports whether rows r-1 and r of a block of rows rows hold equal
// values at key path d.
func (s *keyStatScratch) same(d, rows, r int) bool {
	at := d*rows + r
	kind := vec.Kind(s.kinds[at])
	if kind != vec.Kind(s.kinds[at-1]) {
		return false
	}
	if kind <= vec.KindNull {
		return true
	}
	c := s.cols[d*int(vec.KindVariant)+int(vec.KindVariant-kind)]
	i, j := int(s.vals[at-1]), int(s.vals[at])
	switch kind {
	case vec.KindBool:
		return c.Bools[i] == c.Bools[j]
	case vec.KindInt:
		return c.Ints[i] == c.Ints[j]
	case vec.KindFloat:
		return keyFloat(c.Floats[i]) == keyFloat(c.Floats[j])
	}

	return string(c.Bytes(i)) == string(c.Bytes(j))
}

// appendKeyStats records the key statistics of the block of b, whose first
// row has the encoded sort key first.
func (w *Writer) appendKeyStats(b *shred.Batch, first []byte) {
	key := w.opts.SortKey
	if len(key) == 0 {
		return
	}
	shared := 0
	if w.rows > 0 {
		shared = SharedKeyColumns(w.lastKey, first)
	}
	w.keyStats = binary.AppendUvarint(w.keyStats, uint64(shared))
	s, rows := &w.ks, b.Rows
	s.kinds, s.vals = grow(s.kinds, len(key)*rows), grow(s.vals, len(key)*rows)
	clear(s.kinds)
	s.cols = s.cols[:0]
	for d, path := range key {
		kinds, vals := s.kinds[d*rows:(d+1)*rows], s.vals[d*rows:(d+1)*rows]
		// A row's key value is the one of the lowest kind it holds at the
		// path, so lower kinds overwrite higher ones.
		for kind := vec.KindVariant; kind >= vec.KindNull; kind-- {
			c := b.Lookup(path, kind)
			s.cols = append(s.cols, c)
			if c == nil {
				continue
			}
			for i := 0; i < c.Count; i++ {
				r := c.Row(i)
				kinds[r], vals[r] = uint8(kind), uint32(i)
			}
		}
	}
	// diff[d] counts the rows whose key first differs from the row before
	// at column d.
	s.diff = grow(s.diff, len(key)+1)
	clear(s.diff)
	for r := 1; r < rows; r++ {
		d := 0
		for d < len(key) && s.same(d, rows, r) {
			d++
		}
		s.diff[d]++
	}
	runs := 1
	for d := range key {
		runs += s.diff[d]
		w.keyStats = binary.AppendUvarint(w.keyStats, uint64(runs))
	}
}

// Close writes the footer; the part is complete once it returns nil.
func (w *Writer) Close() error {
	if w.writerBuffers == nil {
		return w.err
	}
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
	w.closeGroup(true)
	footer = binary.AppendUvarint(footer, uint64(len(w.index)/indexEntrySize))
	footer = append(footer, w.index...)
	footer = binary.AppendUvarint(footer, uint64(len(w.filters)/filterEntrySize))
	footer = binary.AppendUvarint(footer, uint64(len(w.groups)/groupEntrySize))
	footer = append(footer, w.filters...)
	footer = append(footer, w.groups...)
	footer = binary.AppendUvarint(footer, w.off)
	footer = binary.AppendUvarint(footer, uint64(len(w.dirs)))
	w.write(w.dirs)
	footer = append(footer, w.keys...)
	if w.opts.Ordered {
		footer = binary.AppendUvarint(footer, uint64(len(w.lastKey)))
		footer = append(footer, w.lastKey...)
		footer = append(footer, w.keyStats...)
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
	writerPool.Put(w.writerBuffers)
	w.writerBuffers, w.verifiedGroup = nil, nil
	err := w.err
	if err == nil {
		w.err = errClosed
	}

	return err
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
	seen  []uint64 // hashes of the values fewRepeats has met
}

// maxRepeats is the share of values, one in this many, that may repeat an
// earlier one before a column counts as having repeats worth finding.
const maxRepeats = 32

// fewRepeats reports whether nearly every value of c differs from all
// values before it.
func (d *dictBuilder) fewRepeats(c *shred.Column) bool {
	size := 64
	for size < 2*c.Count {
		size *= 2
	}
	if cap(d.seen) < size {
		d.seen = make([]uint64, size)
	}
	seen := d.seen[:size]
	clear(seen)
	mask := uint64(size - 1)
	repeats, limit := 0, c.Count/maxRepeats
	for i := 0; i < c.Count; i++ {
		// Zero marks an empty slot.
		h := xxhash.Sum64(c.Bytes(i)) | 1
		slot := h & mask
		for seen[slot] != 0 && seen[slot] != h {
			slot = (slot + 1) & mask
		}
		if seen[slot] == h {
			if repeats++; repeats > limit {
				return false
			}

			continue
		}
		seen[slot] = h
	}

	return true
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

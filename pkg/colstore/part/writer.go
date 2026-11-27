package part

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"math/bits"
	"slices"
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
	// Stored, when set, tells whether the blocks of a source part of a merge
	// are encoded at this writer's level, so that the merge may take them
	// over as they are stored; unset, those of every part are.
	Stored func(src *Part) bool
	// Dictionary lets the part hold a dictionary of its own for the string
	// columns whose values repeat across its blocks, which then store codes
	// of it: Share and a merge give it one where that pays. Unset, the part
	// has none, and a merge turns the codes of the parts it reads back into
	// strings.
	Dictionary bool
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
	// timeCounts holds the records of every block's rows by bucket of
	// time, length-prefixed; timeRecord and timeBuckets are its scratch.
	timeCounts  []byte
	timeRecord  []byte
	timeBuckets bucketScratch
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
	// grouped blocks, whose filters take stripe filter blocks in every
	// stripe, and stripes stripes; it closes at groupBlocks blocks or at
	// the share of groupBytes of filters that its stripes are of
	// filterStripes. fixedStripes, when set, is the number of stripes of
	// every group that a block does not need another number for.
	filters      []byte
	groupFilters int
	grouped      int
	stripe       uint32
	// order and starts serve closeGroup: the filters of the group by
	// column, and where each begins in filterData.
	order        []int32
	starts       []int
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
	// plans holds, by path, the plan of every string column that the part
	// shares, planned of them. dry is set while a column is encoded only to
	// learn its size, which leaves the sums and the tokens as they are.
	plans   map[string]*sharedPlan
	planned int
	dry     bool
	// shareSkip counts, by path, the parts that the writer writes before it
	// thinks of sharing the column again; shareSeen and shareRuns serve
	// Share.
	shareSkip map[string]uint8
	shareSeen map[string]struct{}
	shareRuns []valueRun
	// tried holds, by path, the values of a column that a merge found not
	// worth sharing, or did not look at again since a merge before it had:
	// the part says so, and merges leave the column alone until it holds
	// twice as many.
	tried map[string]uint64

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
	// wide tells whether identifiers that repeat go into a dictionary of
	// more than half as many entries, and wideLeft counts the blocks until
	// that is compared again with storing them as they are.
	wideLeft uint8
	wide     bool
	// untimed tells that the values could not go into a timed chunk at the
	// last trial and flatSize what they took a value as flat values, in
	// parts of flatScale of a byte; timedLeft counts the blocks until the
	// next trial.
	timedLeft uint8
	untimed   bool
	flatSize  uint16
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
	// wideDictShare bounds what a dictionary of identifiers may cost over
	// the general codec: one part in this many of its size.
	wideDictShare = 16
)

// writerBuffers holds what a writer reuses from block to block.
type writerBuffers struct {
	block     []byte
	stats     []byte
	ents      []dirEntry
	e         encodeScratch
	ints      []int64
	dictBytes []byte // the integer codec owns e.tmp
	// counted is set when the dictionary last written holds the rows of
	// its entries, which dictRows then holds too; dictEnd is where its codes
	// begin in the block.
	counted  bool
	dictEnd  int
	dictRows [maxCounted]uint32
	// sums holds, by path, the rows of every string value that all blocks
	// so far count; the copied fields read those of a block copied as stored.
	sums        []pathSum
	copiedBlock Block
	copiedDict  vec.Column
	copiedRows  []uint32
	dict        dictBuilder
	// column holds the tokens of the column being encoded, and tokens
	// those of the block's columns without a filter of their own. The
	// columns of the block that get one have their tokens in tokenKeys, where
	// owned locates them, until the block's filters are built.
	column    tokenfilter.Set
	tokens    tokenfilter.Set
	hashes    []uint64
	tokenKeys []uint64
	owned     []ownedTokens
	// filterData holds the filters of the group being built, end to end, and
	// pages the group as it is written.
	filterData []byte
	pages      []byte
	cells      tokenfilter.Scratch
	// timedBuf serves the timed chunks of a block, timedRefs holds the
	// columns of the block that timed columns may follow, and anchorCost
	// the bits of each following each other.
	timedBuf   timedScratch
	timedRefs  []timedRef
	anchorCost []int
	// codes32 and locals32 hold the codes of a shared column of the block,
	// own the columns of the block that are not shared, recoded the chunks
	// of a block copied with other codes; dictw and dictFooter write the
	// dictionaries.
	codes32    []uint32
	locals32   []uint32
	own        []*shred.Column
	recoded    []recodedChunk
	dictw      dictWriter
	dictFooter []byte
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

	clear(b.own[:cap(b.own)])
	b.dictw.w, b.dictw.l = nil, nil

	return cap(b.block)+cap(b.e.tmp)+cap(b.e.zbuf)+cap(b.dictBytes)+cap(b.filterData)+cap(b.pages)+8*cap(b.tokenKeys)+
		cap(b.dictw.pending)+cap(b.dictw.otherData) <= maxWriterBytes
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
	// head and headCRC are, for a dictionary column, the bytes of its chunk
	// up to the codes and their checksum, crc then that of the codes;
	// dictEnd is where the codes begin in the block being built.
	head, headCRC uint32
	dictEnd       int
	minI          int64
	maxI          int64
	minF          float64
	maxF          float64
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
	w.dropPlans()
	w.dry = false
	clear(w.tried)
	clear(w.trials)
	w.paths = w.paths[:0]
	clear(w.ids)
	w.index, w.dirs, w.keys, w.lastKey, w.keyStats = w.index[:0], w.dirs[:0], w.keys[:0], w.lastKey[:0], w.keyStats[:0]
	w.timeCounts = w.timeCounts[:0]
	w.rows, w.raw, w.minT, w.maxT = 0, 0, 0, 0
	clear(w.sums)
	w.sums = w.sums[:0]
	w.groups, w.filters, w.filterData = w.groups[:0], w.filters[:0], w.filterData[:0]
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
	binary.LittleEndian.PutUint32(head[4:], w.version())
	w.write(head[:])
}

// version returns the format version of the part: the one with dictionaries
// when the writer may give it any.
func (w *Writer) version() uint32 {
	if w.opts.Dictionary {
		return formatVersion
	}

	return plainVersion
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
	w.timedRefs = w.timedRefs[:0]

	minT, maxT := vec.MinMaxInt64(b.Times)
	ent := dirEntry{pathID: 0, kind: vec.KindInt, count: rows}
	w.encodeInts(&ent, b.Times)
	w.finish(&ent, 0)
	w.times = b.Times
	w.findAnchors(w.ownColumns(b), b.Times)
	for _, c := range b.Columns() {
		if c.Count == 0 {
			continue
		}
		w.encodeColumn(c, rows)
	}
	// The row times stay ahead of a member at the time path, which lookups
	// of the path would otherwise find instead.
	sort.SliceStable(w.ents, func(i, j int) bool {
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
	w.addTimeCounts(b.Times, minT, maxT)

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
		total = singleStripe(len(shared), tokenfilter.SharedFalse)
	}
	for _, o := range w.owned {
		total += singleStripe(o.end-start, tokenfilter.ColumnFalse)
		start = o.end
	}
	// The cells of a filter are a power of two, and so are the stripes
	// that cut them.
	stripes := min(1<<(bits.Len(uint(total/filterRounding|1))-1), filterStripes)
	if w.fixedStripes > 0 {
		stripes = 1 << (bits.Len(uint(w.fixedStripes)) - 1)
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

// singleStripe returns the filter blocks of a filter of tokens hashes at
// rate in a group of one stripe.
func singleStripe(tokens int, rate float64) int {
	cells := tokenfilter.Cells(tokens, 1)

	return cells * tokenfilter.Blocks(tokens, cells, rate)
}

// appendFilter adds the filter of keys to the group being built, as that of
// a column of the block being written or with noPath as that of its columns
// that have none.
func (w *Writer) appendFilter(path uint32, kind vec.Kind, keys []uint64) {
	rate := tokenfilter.ColumnFalse
	if path == noPath {
		rate = tokenfilter.SharedFalse
	}
	cells := tokenfilter.Cells(len(keys), w.stripes)
	per := tokenfilter.Blocks(len(keys), cells, rate)
	w.filterData = tokenfilter.AppendFilter(w.filterData, keys, cells, per, &w.cells)
	w.addFilter(path, kind, uint8(bits.TrailingZeros(uint(cells))), cells*per)
}

// addFilter records the filter of 1<<cells cells and blocks filter blocks
// that was appended to w.filterData for the block being written.
func (w *Writer) addFilter(path uint32, kind vec.Kind, cells uint8, blocks int) {
	pieces := uint32(blocks / w.stripes)
	w.filters = binary.LittleEndian.AppendUint32(w.filters, path)
	w.filters = append(w.filters, byte(kind), cells, 0, 0)
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
	if w.grouped == 0 || (!last && w.grouped < w.groupBlocks && len(w.filterData) < w.groupBytes/filterStripes*w.stripes) {
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
	// In every stripe the pieces of one column's filters lie side by side,
	// block after block, so that looking a token up in a column reads one
	// place of the group and not one per block.
	order, starts := w.order[:0], w.starts[:0]
	for i, at := 0, 0; i < w.groupFilters; i++ {
		order, starts = append(order, int32(i)), append(starts, at)
		at += w.stripes * int(binary.LittleEndian.Uint32(entries[i*filterEntrySize+8:])) * tokenfilter.BlockBytes
	}
	slices.SortStableFunc(order, func(a, b int32) int {
		x, y := entries[int(a)*filterEntrySize:], entries[int(b)*filterEntrySize:]

		return cmp.Or(cmp.Compare(binary.LittleEndian.Uint32(x), binary.LittleEndian.Uint32(y)), cmp.Compare(x[4], y[4]))
	})
	before := uint32(0)
	for _, i := range order {
		e := entries[int(i)*filterEntrySize:]
		binary.LittleEndian.PutUint32(e[12:], before)
		before += binary.LittleEndian.Uint32(e[8:])
	}
	w.order, w.starts = order, starts
	pages, placed := w.pages[:0], 0
	for k := 0; k < w.stripes; k++ {
		for _, i := range order {
			piece := int(binary.LittleEndian.Uint32(entries[int(i)*filterEntrySize+8:])) * tokenfilter.BlockBytes
			src := w.filterData[starts[i]+k*piece : starts[i]+(k+1)*piece]
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
	w.pages, w.filterData, w.groupFilters, w.grouped, w.stripe = pages[:0], w.filterData[:0], 0, 0, 0
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
// encodes every block itself, and so does one told that src was written at
// another level. A block that stores columns as codes of the dictionaries of
// src is copied with the codes of w's, when w shares those columns too.
func (w *Writer) copies(src *Part, i int) bool {
	return w.opts.Level != LevelCompact && w.opts.Codec == CodecZstd &&
		(w.opts.NoTokenFilter || len(src.groups) > 0) && (w.opts.Stored == nil || w.opts.Stored(src)) &&
		((w.planned == 0 && src.dicts == nil) || w.sharesAlike(src, i))
}

// joinGroup makes the group being built one that the filters first to end
// of src can be copied into: one whose stripes cut every filter at cell
// boundaries, into pieces not much larger than they need be.
func (w *Writer) joinGroup(src *Part, first, end int) {
	common := 0
	for f := first; f < end; f++ {
		a := src.filterCells(f)
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
	dir, err := src.directory(i, e)
	if err != nil {
		return err
	}
	n, size := binary.Uvarint(dir)
	if size <= 0 || n > uint64(len(dir)-size)/dirEntrySize {
		return ErrCorrupt
	}
	entries := size + int(n)*dirEntrySize

	if err := w.copyChunks(src, i, data, dir[size:entries]); err != nil {
		return err
	}
	newDir := len(w.block)
	w.block = append(w.block, dir[:entries]...)
	// Path ids are per part, and entries stay ordered by id and kind.
	ents := w.block[newDir+size:]
	stats := w.patchCopied(ents, dir[entries:])
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
	w.block = append(w.block, stats...)
	if err := w.sumCopied(src, i, pathMap); err != nil {
		return err
	}

	var entry [indexEntrySize]byte
	if !w.opts.NoTokenFilter {
		g, first, end := src.blockFilters(i)
		w.joinGroup(src, first, end)
		// The pages of the source are checked here because the group
		// being built gets checksums of its own.
		if w.verifiedGroup != g {
			w.verifiedGroup = g
			w.verified = grow(w.verified, vec.BitmapWords(g.pages()))
			clear(w.verified)
		}
		start := len(w.filterData)
		for f := first; f < end; f++ {
			path, ok := binary.LittleEndian.Uint32(src.filters[f*filterEntrySize:]), false
			if w.filterData, ok = src.appendFilter(w.filterData, g, f, w.verified); !ok ||
				(path != noPath && (uint64(path) >= uint64(len(pathMap)) || pathMap[path] == noPath)) {
				w.filterData = w.filterData[:start]

				return ErrCorrupt
			}
		}
		w.startFilters(entry[:], len(w.filters)/filterEntrySize)
		for f := first; f < end; f++ {
			path := binary.LittleEndian.Uint32(src.filters[f*filterEntrySize:])
			if path != noPath {
				path = pathMap[path]
			}
			w.addFilter(path, vec.Kind(src.filters[f*filterEntrySize+4]), src.filters[f*filterEntrySize+5], src.filterBlocks(g, f))
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
	w.copyTimeCounts(src, i)

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
	codes := start
	if ent.enc == encDict {
		codes = ent.dictEnd
		ent.head, ent.headCRC = uint32(codes-start), crc32.Checksum(w.block[start:codes], castagnoli)
	}
	ent.crc = crc32.Checksum(w.block[codes:], castagnoli)
	w.ents = append(w.ents, *ent)
}

func (w *Writer) encodeColumn(c *shred.Column, rows int) {
	ent := dirEntry{pathID: w.pathID(c.Path), kind: c.Kind, count: c.Count}
	start := len(w.block)
	if c.Count != rows {
		ent.flags |= flagSparse
		w.appendPresence(c, rows)
	}
	w.pathSum(ent.pathID).values += uint64(c.Count)
	if c.Kind != vec.KindString && c.Kind != vec.KindVariant {
		w.noSum(ent.pathID)
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
		size := 0
		if sp := w.sharedPlan(c.Path); sp == nil || c.Kind != vec.KindString || !w.encodeShared(&ent, c, sp) {
			w.encodeBytes(&ent, c)
		} else {
			// A false answer of the filter costs the strings of the
			// column, which lie all over the dictionary of the part, not
			// the codes that the block holds of them: the column gets a
			// filter of its own where the strings in the block would.
			size = c.Count * sp.layout.stride
		}
		if !w.opts.NoTokenFilter {
			w.filterColumn(ent.pathID, c.Kind, max(size, len(w.block)-start))
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
	if w.opts.NoTokenFilter || w.dry {
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
	distinct, ok := w.dict.build(c, false)
	if ok && distinct == 1 {
		ent.enc = encConst
		ent.constVal = c.Bytes(0)
		w.tokenize(c.Kind, ent.constVal)
		if c.Kind == vec.KindString {
			w.sum(ent.pathID, ent.constVal, uint64(c.Count))
		} else {
			w.noSum(ent.pathID)
		}

		return
	}
	if ok && w.encodeDict(c, distinct) {
		w.asDict(ent, c)
		w.tokenizeEntries(c)

		return
	}
	// Identifiers, strings of one length, pack: as they are when few of
	// them repeat, and as a dictionary when more do and that costs little
	// more than the general codec, which reads several times slower.
	stride := 0
	if w.packs(c.Kind) && c.Count >= minPackedStrings {
		stride = len(c.Bytes(0))
		for i := 1; i < c.Count && stride > 0; i++ {
			if int(c.Ends[i]-c.Ends[i-1]) != stride {
				stride = 0
			}
		}
	}
	slot := int(ent.pathID)*kindSlots + int(c.Kind)
	lone := stride > 0 && w.dict.fewRepeats(c)
	if stride > 0 && !lone {
		for len(w.trials) <= slot {
			w.trials = append(w.trials, trial{})
		}
		tr := &w.trials[slot]
		if tr.wideLeft == 0 {
			tr.wideLeft, tr.wide = trialEvery, false
			start := len(w.block)
			if distinct, ok := w.dict.build(c, true); ok && w.encodeDict(c, distinct) {
				dict := len(w.block)
				w.appendFlat(slot, c, 0)
				if flat := len(w.block) - dict; (dict-start)*wideDictShare <= flat*(wideDictShare+1) {
					tr.wide, w.block = true, w.block[:dict]
					w.asDict(ent, c)
				} else {
					ent.enc, w.block = encFlat, w.block[:start+copy(w.block[start:], w.block[dict:])]
					w.sumEach(ent.pathID, c)
				}
				// The entries are all the values there are.
				w.tokenizeEntries(c)

				return
			}
		} else if tr.wideLeft--; tr.wide {
			if distinct, ok := w.dict.build(c, true); ok && w.encodeDict(c, distinct) {
				w.asDict(ent, c)
				w.tokenizeEntries(c)

				return
			}
		}
	}
	w.sumEach(ent.pathID, c)
	ent.enc = encFlat
	for i := 0; i < c.Count; i++ {
		w.tokenize(c.Kind, c.Bytes(i))
	}
	if !lone {
		stride = 0
	}
	if w.appendTimedOrFlat(slot, c, stride) {
		ent.enc = encTimed
	}
}

// appendTimedOrFlat appends the values of the column as a timed chunk when
// that is the smaller form and as flat values otherwise, and reports which.
// The forms are compared every trialEvery blocks, and in between the one
// that won is taken when it can be.
func (w *Writer) appendTimedOrFlat(slot int, c *shred.Column, stride int) bool {
	if !w.packs(c.Kind) || c.Count < minTimed {
		w.appendFlat(slot, c, stride)

		return false
	}
	for len(w.trials) <= slot {
		w.trials = append(w.trials, trial{})
	}
	tr := &w.trials[slot]
	start := len(w.block)
	if tr.timedLeft > 0 {
		// Between trials a timed chunk is kept when it takes fewer bytes a
		// value than the flat values did at the last trial.
		tr.timedLeft--
		if !tr.untimed && w.appendTimed(c, w.times) {
			if size := len(w.block) - start; size*flatScale < c.Count*int(tr.flatSize) {
				return true
			}
			w.block = w.block[:start]
		}
		w.appendFlat(slot, c, stride)

		return false
	}
	tr.timedLeft, tr.untimed = trialEvery, false
	if !w.appendTimed(c, w.times) {
		tr.untimed = true
		w.appendFlat(slot, c, stride)

		return false
	}
	timed := len(w.block)
	w.appendFlat(slot, c, stride)
	flat := len(w.block) - timed
	tr.flatSize = uint16(min(flat*flatScale/c.Count+1, 1<<16-1))
	if timed-start <= flat {
		w.block = w.block[:timed]

		return true
	}
	w.block = w.block[:start+copy(w.block[start:], w.block[timed:])]

	return false
}

// flatScale is the unit of trial.flatSize, a part of a byte.
const flatScale = 64

// appendFlat appends the values of the column one after another, packed
// when stride, their length, is set and packing suits them.
func (w *Writer) appendFlat(slot int, c *shred.Column, stride int) {
	w.ints = w.ints[:0]
	for i := 0; i < c.Count; i++ {
		w.ints = append(w.ints, int64(len(c.Bytes(i))))
	}
	w.block = appendInts(w.block, w.ints, &w.blobs, &w.e)
	data := c.Data[:c.Ends[c.Count-1]]
	// Values that repeat are left to the general codec, which finds the
	// repeats; packing cannot.
	if stride > 0 {
		if out, packed := w.values.appendPackedBlob(w.block, data, stride, &w.e.zbuf); packed {
			w.block = out

			return
		}
	}
	w.appendValues(slot, data, c.Count)
}

// asDict marks the column of ent as the dictionary that encodeDict wrote.
func (w *Writer) asDict(ent *dirEntry, c *shred.Column) {
	ent.enc, ent.dictEnd = encDict, w.dictEnd
	if w.counted {
		ent.flags |= flagCounts
	}
	if !w.counted || c.Kind != vec.KindString {
		w.noSum(ent.pathID)

		return
	}
	for at, entry := range w.dict.order {
		w.sum(ent.pathID, c.Bytes(int(w.dict.first[entry])), uint64(w.dictRows[at]))
	}
}

// pathSum counts the rows of each string value at one path: held of the
// path's values are counted, and off is set once some cannot be.
type pathSum struct {
	off          bool
	index        map[string]int
	rows         []uint64
	bytes        int
	held, values uint64
}

// maxCountedBytes bounds the values of one path that a footer holds.
const maxCountedBytes = 1 << 16

func (w *Writer) pathSum(pathID uint32) *pathSum {
	for len(w.sums) <= int(pathID) {
		w.sums = append(w.sums, pathSum{})
	}

	return &w.sums[pathID]
}

// sumCopied adds what block i of src counts to the sums of its paths.
func (w *Writer) sumCopied(src *Part, i int, pathMap []uint32) error {
	b := &w.copiedBlock
	if err := src.OpenBlock(i, b); err != nil {
		return err
	}
	refs := b.Columns()
	for j := range refs {
		ref := &refs[j]
		id := pathMap[ref.PathID]
		w.pathSum(id).values += uint64(ref.Count)
		switch {
		case ref.Kind != vec.KindString:
			w.noSum(id)
		case ref.Const():
			w.sum(id, ref.ConstBytes(), uint64(ref.Count))
		case ref.Counted() && !w.pathSum(id).off:
			rows, err := b.DecodeCounts(ref, &w.copiedDict, w.copiedRows[:0])
			if err != nil {
				return err
			}
			w.copiedRows = rows
			for entry, n := range rows {
				w.sum(id, w.copiedDict.DictValue(entry), uint64(n))
			}
		case (ref.enc == encFlat || ref.enc == encTimed) && !w.pathSum(id).off:
			col := &w.copiedDict
			if err := b.Decode(ref, col); err != nil {
				return err
			}
			for row := 0; row < col.Rows && !w.sums[id].off; row++ {
				if col.IsValid(row) {
					w.sum(id, col.Bytes(row), 1)
				}
			}
		default:
			w.noSum(id)
		}
	}

	return nil
}

// sumEach adds the values of a column that stores each of them.
func (w *Writer) sumEach(pathID uint32, c *shred.Column) {
	if w.dry {
		return
	}
	if c.Kind != vec.KindString {
		w.noSum(pathID)

		return
	}
	w.pathSum(pathID)
	for i := 0; i < c.Count && !w.sums[pathID].off; i++ {
		w.sum(pathID, c.Bytes(i), 1)
	}
}

// sum adds rows that hold value at a path.
func (w *Writer) sum(pathID uint32, value []byte, rows uint64) {
	s := w.pathSum(pathID)
	if s.off || w.dry {
		return
	}
	at, ok := s.index[string(value)]
	if !ok {
		if s.bytes += len(value); len(s.rows) == maxCounted || s.bytes > maxCountedBytes {
			w.noSum(pathID)

			return
		}
		if s.index == nil {
			s.index = make(map[string]int)
		}
		at = len(s.rows)
		s.index[string(value)] = at
		s.rows = append(s.rows, 0)
	}
	s.rows[at] += rows
	s.held += rows
}

// noSum tells that a block holds values at a path that are not counted.
func (w *Writer) noSum(pathID uint32) {
	if w.dry {
		return
	}
	s := w.pathSum(pathID)
	s.off, s.index, s.rows = true, nil, nil
}

// counts reports whether the sums of a path cover every value it holds.
func (s *pathSum) counts() bool {
	return !s.off && len(s.rows) > 0 && s.held == s.values
}

// tokenizeEntries adds the tokens of the entries of the dictionary that
// the builder holds.
func (w *Writer) tokenizeEntries(c *shred.Column) {
	for _, entry := range w.dict.order {
		w.tokenize(c.Kind, c.Bytes(int(w.dict.first[entry])))
	}
}

// encodeDict appends the column as the dictionary that the builder holds,
// and reports whether it did: a dictionary of more than half as many
// entries as values pays off only packed.
func (w *Writer) encodeDict(c *shred.Column, distinct int) bool {
	d := &w.dict
	mark := len(w.block)
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
	}
	w.block = appendInts(w.block, w.ints, &w.blobs, &w.e)
	out, packed := w.block, false
	if w.packs(c.Kind) {
		out, packed = w.blobs.appendPackedBlob(w.block, w.dictBytes, stride, &w.e.zbuf)
	}
	if !packed {
		if distinct > c.Count/2 {
			w.block = w.block[:mark]

			return false
		}
		out = w.blobs.appendBlob(w.block, w.dictBytes, &w.e.zbuf)
	}
	w.block = out
	// A dictionary of few entries says how many rows hold each, which
	// answers a count by the column without its codes.
	if w.counted = distinct <= maxCounted; w.counted {
		rows := w.dictRows[:distinct]
		clear(rows)
		for _, code := range d.codes {
			rows[d.rank[code]]++
		}
		for _, n := range rows {
			w.block = binary.AppendUvarint(w.block, uint64(n))
		}
	}
	w.dictEnd = len(w.block)

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

		return true
	}
	w.block = appendPacked(append(w.block, codesPacked), d.codes, d.rank, width)

	return true
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
		if e.enc == encDict {
			w.stats = binary.AppendUvarint(w.stats, uint64(e.head))
			w.stats = binary.LittleEndian.AppendUint32(w.stats, e.headCRC)
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
	var dicts []byte
	if w.opts.Dictionary {
		dicts = w.writeDictionaries()
	}
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
	footer = w.appendSums(footer)
	footer = binary.AppendUvarint(footer, uint64(len(w.timeCounts)))
	footer = append(footer, w.timeCounts...)
	footer = append(footer, dicts...)

	footerOff := w.off
	w.write(footer)
	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint64(trailer[0:], footerOff)
	binary.LittleEndian.PutUint64(trailer[8:], uint64(len(footer)))
	binary.LittleEndian.PutUint32(trailer[16:], crc32.Checksum(footer, castagnoli))
	binary.LittleEndian.PutUint32(trailer[20:], w.version())
	binary.LittleEndian.PutUint32(trailer[24:], fileMagic)
	w.write(trailer[:])
	w.dropPlans()
	writerPool.Put(w.writerBuffers)
	w.writerBuffers, w.verifiedGroup, w.w = nil, nil, nil
	err := w.err
	if err == nil {
		w.err = errClosed
	}

	return err
}

// appendSums appends the counted paths: their number, then for each its
// id, the number of its values, and the values in order with their rows.
func (w *Writer) appendSums(dst []byte) []byte {
	n := 0
	for i := range w.sums {
		if w.sums[i].counts() {
			n++
		}
	}
	dst = binary.AppendUvarint(dst, uint64(n))
	var values []string
	for id := range w.sums {
		s := &w.sums[id]
		if !s.counts() {
			continue
		}
		values = values[:0]
		for v := range s.index {
			values = append(values, v)
		}
		slices.Sort(values)
		dst = binary.AppendUvarint(dst, uint64(id))
		dst = binary.AppendUvarint(dst, uint64(len(values)))
		for _, v := range values {
			dst = binary.AppendUvarint(dst, uint64(len(v)))
			dst = append(dst, v...)
			dst = binary.AppendUvarint(dst, s.rows[s.index[v]])
		}
	}

	return dst
}

// dictBuilder finds the distinct values of a byte column.
type dictBuilder struct {
	slots []uint32 // entry number + 1, 0 when empty
	hash  []uint64
	first []uint32 // value number of each entry's first occurrence
	codes []uint32 // entry number of every value
	order []uint32 // entry numbers sorted by value
	rank  []uint32 // position of each entry in order
	keys  []dictKey
	seen  []uint64 // hashes of the values fewRepeats has met
}

// maxRepeats is the share of values, one in this many, that may repeat an
// earlier one before a column counts as having repeats worth finding.
const maxRepeats = 32

// fewRepeats reports whether nearly every value of c differs from all
// values before it. When none of the first eighth of the values repeats, it
// takes the rest to repeat as little: a column at the limit would show
// repeats there but for a chance of about e^-(limit/64).
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
	sample := max(c.Count/8, 1)
	for i := 0; i < c.Count; i++ {
		if i == sample && repeats == 0 && limit >= 64 {
			return true
		}
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

// dictKey is an entry of a dictionary with the bytes of its value that
// most often tell it from the others: the eight after the bytes that all
// the values begin with, as a number that orders as they do.
type dictKey struct {
	head  uint64
	entry uint32
}

// sortEntries puts the entries in order by their values.
func (d *dictBuilder) sortEntries(c *shred.Column) {
	value := func(entry uint32) []byte { return c.Bytes(int(d.first[entry])) }
	shared := value(0)
	for entry := 1; entry < len(d.first) && len(shared) > 0; entry++ {
		v := value(uint32(entry))
		n := 0
		for n < len(shared) && n < len(v) && v[n] == shared[n] {
			n++
		}
		shared = shared[:n]
	}
	d.keys = d.keys[:0]
	for entry := range d.first {
		v := value(uint32(entry))[len(shared):]
		var head [8]byte
		copy(head[:], v)
		d.keys = append(d.keys, dictKey{binary.BigEndian.Uint64(head[:]), uint32(entry)})
	}
	slices.SortFunc(d.keys, func(a, b dictKey) int {
		if a.head != b.head {
			return cmp.Compare(a.head, b.head)
		}

		return bytes.Compare(value(a.entry), value(b.entry))
	})
	for i, k := range d.keys {
		d.order[i] = k.entry
	}
}

// build reports the number of distinct values of c and whether a dictionary
// pays off: every value equal, or at most half as many entries as values,
// or seven in eight when wide is set, for entries that pack. A column whose
// values repeat too seldom for the wider one is not given to it.
func (d *dictBuilder) build(c *shred.Column, wide bool) (distinct int, ok bool) {
	n := c.Count
	limit := min(n/2, maxDictSize)
	if wide {
		limit = min(n-n/8, maxDictSize)
	}
	if n < 8 {
		limit = 1
	}
	if !wide && n >= 1024 && d.sampleAllDistinct(c) {
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
	d.sortEntries(c)
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

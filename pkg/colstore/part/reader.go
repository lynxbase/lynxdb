package part

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/bits"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Part is an opened, immutable part that reads only the bytes it was opened
// on, which must stay valid and unchanged while it is in use.
type Part struct {
	data []byte
	// blocks holds the bytes of data; the column chunks are read from it.
	blocks []byte
	paths  []string
	ids    map[string]uint32
	index  []byte
	// dirs holds the column directories of all blocks, side by side, so
	// that opening blocks reads one region of the part and not one place
	// per block.
	dirs    []byte
	dirsOff int
	// detached is set once the index is held apart from data. The
	// directory of a block then moves to dirHeap, at its place in dirs,
	// when the block is first opened: dirHeld has a bit per block that
	// did, and dirMu guards the moving.
	detached bool
	// counts holds, by path id, the rows of the values of counted paths.
	counts map[uint32]*Counts
	// sound has a bit for every column of every block, at the place of its
	// directory entry, that is set once the bytes of the column were seen
	// to match their checksum, until Recheck clears it; advised has one
	// that is set once the kernel was asked for the bytes ahead of a read,
	// until the pages of the part are let go of.
	sound   []atomic.Uint64
	advised []atomic.Uint64
	dirMu   sync.Mutex
	dirHeap []byte
	dirHeld []atomic.Uint64
	// filters holds an entry for every token filter, block after block,
	// and groups locates them; every block names its group and its first
	// filter, and a part without filters has neither.
	filters []byte
	groups  []filterGroup
	// readAhead, when set, is told which bytes of data are about to be
	// read.
	readAhead func(off, n int)
	// objects holds the paths of the objects whose members have columns,
	// once objectsOnce has listed them.
	objectsOnce sync.Once
	objects     map[string]struct{}
	// misses holds, by condition, the blocks found to hold no row passing
	// it; missClock orders the uses of the conditions.
	missMu    sync.RWMutex
	misses    map[string]*Misses
	missClock atomic.Uint64

	ordered bool
	sortKey []string
	// firstKeys holds the encoded sort key of each block's first row.
	firstKeys [][]byte
	// lastKey holds the encoded sort key of the last row.
	lastKey []byte
	// keyStats holds, per block, the leading key columns its first row
	// shares with the row before it, then the number of distinct key
	// prefixes of every length.
	keyStats []uint32

	// Rows is the number of rows in the part.
	Rows uint64
	// RawBytes is the size of the ingested input the part was built from.
	RawBytes uint64
	// MinTime and MaxTime bound the row timestamps.
	MinTime int64
	MaxTime int64
}

// filterGroup locates the token filters of a run of blocks: size bytes at
// off, which hold stripes stripes of stripe filter blocks each.
type filterGroup struct {
	off     uint64
	size    uint64
	stripes uint64
	stripe  uint64
	// first is the group's first block and blocks their number.
	first, blocks int32
	// spans holds, for the columns last looked up, where the pieces of
	// their filters lie in a stripe of the group; turn counts the entries
	// replaced.
	spans [4]filterSpan
	turn  atomic.Uint32
	// sound has a bit for every page of the group whose checksum was seen
	// to match, and advised one for every page that the kernel was asked
	// for ahead of lookups, since the pages were last let go of.
	sound   []atomic.Uint64
	advised []atomic.Uint64
}

// filterSpan is the range of filter blocks, from and to in the halves of
// span, that the filters of the column named by key take in a stripe.
type filterSpan struct{ key, span atomic.Uint64 }

// span returns where the filters of the column named by key lie in a
// stripe of the group, when the group remembers it.
func (g *filterGroup) span(key uint64) (from, to uint64, ok bool) {
	for i := range g.spans {
		if g.spans[i].key.Load() == key+1 {
			v := g.spans[i].span.Load()

			return v >> 32, v & math.MaxUint32, true
		}
	}

	return 0, 0, false
}

func (g *filterGroup) keepSpan(key, from, to uint64) {
	e := &g.spans[g.turn.Add(1)%uint32(len(g.spans))]
	e.key.Store(0)
	e.span.Store(from<<32 | to)
	e.key.Store(key + 1)
}

// advise asks for the pages that hold the bytes from to of the group
// through readAhead, unless all of them were asked for since the pages were
// last let go of, and reports whether it asked.
func (g *filterGroup) advise(from, to uint64, readAhead func(off, n int)) bool {
	first, end := from/filterPage, (to+filterPage-1)/filterPage
	all := true
	for page := first; page < end && all; page++ {
		all = g.advised[page>>6].Load()>>(page&63)&1 != 0
	}
	if all {
		return false
	}
	readAhead(int(g.off+from), int(to-from))
	for page := first; page < end; page++ {
		g.advised[page>>6].Or(1 << (page & 63))
	}

	return true
}

// aheadGroups is the number of groups after the one a lookup reads whose
// pages it asks for as well: a scan of the part then finds them read, and
// the device reads several at a time.
const aheadGroups = 8

// maxFilterBlocks bounds the filter blocks of one filter so that scaling a
// hash to them stays within 64 bits.
const maxFilterBlocks = 1 << 32

// Open validates the footer of a part held in data.
func Open(data []byte) (*Part, error) {
	if len(data) < headerSize+trailerSize ||
		binary.LittleEndian.Uint32(data) != fileMagic ||
		binary.LittleEndian.Uint32(data[4:]) != formatVersion {
		return nil, ErrCorrupt
	}
	trailer := data[len(data)-trailerSize:]
	if binary.LittleEndian.Uint32(trailer[24:]) != fileMagic ||
		binary.LittleEndian.Uint32(trailer[20:]) != formatVersion {
		return nil, ErrCorrupt
	}
	off, size := binary.LittleEndian.Uint64(trailer[0:]), binary.LittleEndian.Uint64(trailer[8:])
	limit := uint64(len(data) - trailerSize)
	if off < headerSize || off > limit || size != limit-off {
		return nil, ErrCorrupt
	}
	footer := data[off:limit]
	if crc32.Checksum(footer, castagnoli) != binary.LittleEndian.Uint32(trailer[16:]) {
		return nil, ErrCorrupt
	}

	p := &Part{data: data, blocks: data}
	r := reader{buf: footer}
	nPaths := r.uvarint()
	if nPaths > uint64(len(footer)) {
		return nil, ErrCorrupt
	}
	p.paths = make([]string, 0, nPaths)
	p.ids = make(map[string]uint32, nPaths)
	for i := uint64(0); i < nPaths; i++ {
		name := string(r.bytes(r.uvarint()))
		p.ids[name] = uint32(len(p.paths))
		p.paths = append(p.paths, name)
	}
	if n := r.uvarint(); n > 0 {
		p.ordered = true
		for ; n > 1 && !r.bad; n-- {
			id := r.uvarint()
			if id >= uint64(len(p.paths)) {
				return nil, ErrCorrupt
			}
			p.sortKey = append(p.sortKey, p.paths[id])
		}
	}
	nBlocks := r.uvarint()
	if nBlocks > uint64(len(footer))/indexEntrySize {
		return nil, ErrCorrupt
	}
	p.index = r.bytes(nBlocks * indexEntrySize)
	nFilters, nGroups := r.uvarint(), r.uvarint()
	if nFilters > uint64(len(footer))/filterEntrySize || nGroups > nBlocks || (nFilters > 0) != (nGroups > 0) {
		return nil, ErrCorrupt
	}
	p.filters = r.bytes(nFilters * filterEntrySize)
	groups := r.bytes(nGroups * groupEntrySize)
	dirsOff, dirsLen := r.uvarint(), r.uvarint()
	if r.bad || dirsOff < headerSize || dirsOff > off || dirsLen > off-dirsOff {
		return nil, ErrCorrupt
	}
	p.dirs, p.dirsOff = data[dirsOff:dirsOff+dirsLen], int(dirsOff)
	p.sound = make([]atomic.Uint64, len(p.dirs)/dirEntrySize/64+1)
	p.advised = make([]atomic.Uint64, len(p.sound))
	p.groups = make([]filterGroup, nGroups)
	for g, end := 0, uint64(headerSize); g < len(p.groups); g++ {
		e := groups[g*groupEntrySize:]
		start, size := binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint64(e[8:])
		stripes := uint64(binary.LittleEndian.Uint32(e[16:]))
		// Every page spends the room of one filter block on its checksum
		// and holds at least one filter block.
		slots, pages := size/tokenfilter.BlockBytes, (size+filterPage-1)/filterPage
		blocks := slots - pages
		if start < end || start > dirsOff || size > dirsOff-start || size%tokenfilter.BlockBytes != 0 ||
			slots <= pages || (blocks+pageBlocks-1)/pageBlocks != pages ||
			stripes == 0 || blocks%stripes != 0 || blocks >= maxFilterBlocks {
			return nil, ErrCorrupt
		}
		end = start + size
		p.groups[g] = filterGroup{
			off: start, size: size, stripes: stripes, stripe: blocks / stripes,
			sound: make([]atomic.Uint64, (pages+63)/64), advised: make([]atomic.Uint64, (pages+63)/64),
		}
	}
	if p.ordered {
		p.firstKeys = make([][]byte, 0, nBlocks)
		for i := uint64(0); i < nBlocks; i++ {
			p.firstKeys = append(p.firstKeys, r.bytes(r.uvarint()))
		}
		p.lastKey = r.bytes(r.uvarint())
		if !p.readKeyStats(&r, int(nBlocks)) {
			return nil, ErrCorrupt
		}
	}
	p.Rows = r.uvarint()
	p.RawBytes = r.uvarint()
	p.MinTime = unzigzag(r.uvarint())
	p.MaxTime = unzigzag(r.uvarint())
	if r.bad || len(p.paths) == 0 || p.paths[0] != TimePath || !p.readCounts(&r) {
		return nil, ErrCorrupt
	}
	// The filters of a group's blocks fill every stripe, those of one
	// column side by side; group, filter and filled walk them to check
	// that, and first is the first filter of the group.
	group, filter, first, filled := 0, 0, 0, uint64(0)
	var order []int32
	for i := 0; i < p.NumBlocks(); i++ {
		e := p.index[i*indexEntrySize:]
		start, size := binary.LittleEndian.Uint64(e[20:]), uint64(binary.LittleEndian.Uint32(e[28:]))
		dirOff, dirLen := uint64(binary.LittleEndian.Uint32(e[40:])), uint64(binary.LittleEndian.Uint32(e[44:]))
		if start < headerSize || start > dirsOff || size > dirsOff-start ||
			dirOff > dirsLen || dirLen > dirsLen-dirOff {
			return nil, ErrCorrupt
		}
		if len(p.groups) == 0 {
			continue
		}
		if filled == p.groups[group].stripe && group+1 < len(p.groups) {
			if order = p.placed(order, first, filter); order == nil {
				return nil, ErrCorrupt
			}
			group, filled, first = group+1, 0, filter
		}
		g, end := &p.groups[group], len(p.filters)/filterEntrySize
		if len(e) > indexEntrySize {
			end = min(end, int(binary.LittleEndian.Uint32(e[indexEntrySize+36:])))
		}
		if int(binary.LittleEndian.Uint32(e[32:])) != group || int(binary.LittleEndian.Uint32(e[36:])) != filter || end <= filter {
			return nil, ErrCorrupt
		}
		for ; filter < end; filter++ {
			f := p.filters[filter*filterEntrySize:]
			path, pieces := binary.LittleEndian.Uint32(f), uint64(binary.LittleEndian.Uint32(f[8:]))
			kind, cells := vec.Kind(f[4]), uint64(1)<<min(f[5], 63)
			own := uint64(path) < uint64(len(p.paths)) && (kind == vec.KindString || kind == vec.KindVariant)
			// The cells of a filter fill its pieces evenly.
			if (!own && (path != noPath || kind != 0)) || f[5] > 31 || f[6] != 0 || f[7] != 0 ||
				cells%g.stripes != 0 || pieces == 0 || pieces%(cells/g.stripes) != 0 || pieces > g.stripe-filled {
				return nil, ErrCorrupt
			}
			filled += pieces
		}
	}
	if len(p.groups) > 0 && (group != len(p.groups)-1 || filled != p.groups[group].stripe ||
		filter != len(p.filters)/filterEntrySize || p.placed(order, first, filter) == nil) {
		return nil, ErrCorrupt
	}
	for i := int(nBlocks) - 1; i >= 0 && len(p.groups) > 0; i-- {
		g := &p.groups[p.groupOf(i)]
		g.first = int32(i)
		g.blocks++
	}

	return p, nil
}

// groupOf returns the number of the filter group of block i.
func (p *Part) groupOf(i int) int {
	return int(binary.LittleEndian.Uint32(p.index[i*indexEntrySize+32:]))
}

// placed checks that the filters first to end, those of one group, lie in
// every stripe in the order of their columns and, within a column, of their
// blocks, each after the pieces of those before it. It returns order, its
// scratch, or nil when they do not.
func (p *Part) placed(order []int32, first, end int) []int32 {
	order = order[:0]
	for f := first; f < end; f++ {
		order = append(order, int32(f))
	}
	slices.SortStableFunc(order, func(a, b int32) int {
		x, y := p.filters[int(a)*filterEntrySize:], p.filters[int(b)*filterEntrySize:]

		return cmp.Or(cmp.Compare(binary.LittleEndian.Uint32(x), binary.LittleEndian.Uint32(y)), cmp.Compare(x[4], y[4]))
	})
	before := uint32(0)
	for _, f := range order {
		e := p.filters[int(f)*filterEntrySize:]
		if binary.LittleEndian.Uint32(e[12:]) != before {
			return nil
		}
		before += binary.LittleEndian.Uint32(e[8:])
	}
	if order == nil {
		order = []int32{}
	}

	return order
}

// readKeyStats reads the key statistics of every block and reports whether
// they can be true.
func (p *Part) readKeyStats(r *reader, blocks int) bool {
	k := len(p.sortKey)
	if k == 0 {
		return true
	}
	p.keyStats = make([]uint32, 0, blocks*(k+1))
	for i := 0; i < blocks && !r.bad; i++ {
		shared := r.uvarint()
		if shared > uint64(k) || (i == 0 && shared != 0) {
			return false
		}
		p.keyStats = append(p.keyStats, uint32(shared))
		rows, _, _ := p.BlockStats(i)
		prev := uint64(1)
		for d := 0; d < k; d++ {
			runs := r.uvarint()
			if runs < prev || runs > uint64(rows) {
				return false
			}
			p.keyStats = append(p.keyStats, uint32(runs))
			prev = runs
		}
	}

	return !r.bad
}

// KeyShared returns how many leading sort key columns the first row of block
// i shares with the row before it in the part, zero for the first block.
func (p *Part) KeyShared(i int) int { return int(p.keyStats[i*(len(p.sortKey)+1)]) }

// KeyRuns returns the number of distinct values that the first depth sort
// key columns take in block i.
func (p *Part) KeyRuns(i, depth int) int {
	if depth == 0 {
		return 1
	}

	return int(p.keyStats[i*(len(p.sortKey)+1)+depth])
}

// SetBlockData makes the part read the column chunks of its blocks and the
// pages of its token filters from data, which holds the same bytes as the
// data it was opened on; a part mapped from a file gives the two mappings
// advice of their own, since chunks and filter pages are read one here and
// one there and everything else side by side.
func (p *Part) SetBlockData(data []byte) {
	if len(data) == len(p.data) {
		p.blocks = data
	}
}

// Detach copies what locates blocks, filters and keys out of the part's
// data, and the directory of each block when it is first opened, so that
// finding a block reads the data no more; a part mapped from a file then
// keeps its index in memory whatever becomes of the file's pages.
func (p *Part) Detach() {
	p.index, p.filters = bytes.Clone(p.index), bytes.Clone(p.filters)
	size := len(p.lastKey)
	for _, k := range p.firstKeys {
		size += len(k)
	}
	keys := make([]byte, 0, size)
	for i, k := range p.firstKeys {
		keys = append(keys, k...)
		p.firstKeys[i] = keys[len(keys)-len(k) : len(keys) : len(keys)]
	}
	keys = append(keys, p.lastKey...)
	p.lastKey = keys[len(keys)-len(p.lastKey):]
	p.dirHeld = make([]atomic.Uint64, (p.NumBlocks()+63)/64)
	p.detached = true
}

// directory returns the column directory of block i, whose index entry is
// e, once its checksum holds. A detached part reads the directory of a
// block from its data the first time only.
func (p *Part) directory(i int, e []byte) ([]byte, error) {
	off := binary.LittleEndian.Uint32(e[40:])
	end := off + binary.LittleEndian.Uint32(e[44:])
	if !p.detached {
		if crc32.Checksum(p.dirs[off:end], castagnoli) != binary.LittleEndian.Uint32(e[48:]) {
			return nil, ErrCorrupt
		}

		return p.dirs[off:end], nil
	}
	held, bit := &p.dirHeld[i>>6], uint64(1)<<(uint(i)&63)
	if held.Load()&bit == 0 {
		p.dirMu.Lock()
		defer p.dirMu.Unlock()
		if held.Load()&bit == 0 {
			if crc32.Checksum(p.dirs[off:end], castagnoli) != binary.LittleEndian.Uint32(e[48:]) {
				return nil, ErrCorrupt
			}
			if p.dirHeap == nil {
				p.dirHeap = make([]byte, len(p.dirs))
			}
			copy(p.dirHeap[off:end], p.dirs[off:end])
			held.Or(bit)
		}
	}

	return p.dirHeap[off:end], nil
}

// SetReadAhead makes the part announce the bytes it is about to read, as an
// offset and a length in its data, to fn, which a part mapped from a file
// uses to have them read in one request. It announces the block directories
// at once.
func (p *Part) SetReadAhead(fn func(off, n int)) {
	p.readAhead = fn
	if fn != nil && len(p.dirs) > 0 {
		fn(p.dirsOff, len(p.dirs))
	}
}

// minReadAhead is the size from which a chunk is announced; smaller ones
// are a fault or two either way.
const minReadAhead = 8 << 10

// Ordered reports whether the rows are ordered by SortKey and then by
// timestamp.
func (p *Part) Ordered() bool { return p.ordered }

// SortKey returns the paths an ordered part is ordered by.
func (p *Part) SortKey() []string { return p.sortKey }

// Size returns the part's size in bytes.
func (p *Part) Size() int { return len(p.data) }

// FilterBytes returns the bytes of the part's token filters.
func (p *Part) FilterBytes() int64 {
	var n int64
	for i := range p.groups {
		n += int64(p.groups[i].size)
	}

	return n
}

// NumBlocks returns the number of blocks.
func (p *Part) NumBlocks() int { return len(p.index) / indexEntrySize }

// Paths returns the column paths; a path's position is its id in this part.
func (p *Part) Paths() []string { return p.paths }

// HasObject reports whether a column of the part holds a member of the
// object at path.
func (p *Part) HasObject(path string) bool {
	p.objectsOnce.Do(func() {
		p.objects = map[string]struct{}{}
		for _, full := range p.paths {
			for i := 0; i < len(full); i++ {
				if full[i] == '.' {
					p.objects[full[:i]] = struct{}{}
				}
			}
		}
	})
	_, ok := p.objects[path]

	return ok
}

// PathID returns the id of path in this part.
func (p *Part) PathID(path string) (uint32, bool) {
	id, ok := p.ids[path]

	return id, ok
}

// BlockStats returns the row count and time bounds of block i without
// touching the block itself.
func (p *Part) BlockStats(i int) (rows int, minTime, maxTime int64) {
	e := p.index[i*indexEntrySize:]

	return int(binary.LittleEndian.Uint32(e)),
		int64(binary.LittleEndian.Uint64(e[4:])),
		int64(binary.LittleEndian.Uint64(e[12:]))
}

// ColumnRef describes one column of a block.
type ColumnRef struct {
	PathID uint32
	Kind   vec.Kind
	// Count is the number of rows holding a value.
	Count int
	// MinInt and MaxInt bound an int column; MinFloat and MaxFloat bound a
	// float column, ignoring NaN.
	MinInt   int64
	MaxInt   int64
	MinFloat float64
	MaxFloat float64

	enc   uint8
	flags uint8
	// bad marks a directory entry that does not describe a column.
	bad      bool
	off      uint32
	length   uint32
	crc      uint32
	slot     uint32
	constVal []byte
}

// Size returns the number of bytes the column takes in its block.
func (c *ColumnRef) Size() int { return int(c.length) }

// Const reports whether every present row holds the same value.
func (c *ColumnRef) Const() bool { return c.enc == encConst }

// ConstBytes returns the single value of a const string or variant column.
func (c *ColumnRef) ConstBytes() []byte { return c.constVal }

// Block reads the columns of one block and reuses its buffers between blocks,
// so it serves one goroutine at a time.
type Block struct {
	Rows    int
	MinTime int64
	MaxTime int64

	part  *Part
	index int // of the block in the part
	start int // offset of data in the part
	data  []byte
	// dir holds one entry per column, ordered by path id and kind, and stats
	// the statistics and constant values the entries point to.
	dir   []byte
	stats []byte
	// cols[i] is decoded from entry i once bit i of parsed is set.
	cols   []ColumnRef
	parsed vec.Bitmap
	d      decodeScratch
	pos    []uint32
	// slot numbers the block's first column among the columns of the part.
	slot uint32
	// present holds the rows of a sparse column searched as it is stored.
	present vec.Column
	// spans is the scratch of Ahead.
	spans [][2]uint32
}

// OpenBlock points b at block i of p.
func (p *Part) OpenBlock(i int, b *Block) error {
	e := p.index[i*indexEntrySize:]
	b.Rows, b.MinTime, b.MaxTime = p.BlockStats(i)
	start := binary.LittleEndian.Uint64(e[20:])
	b.part, b.index, b.start = p, i, int(start)
	b.data = p.blocks[start : start+uint64(binary.LittleEndian.Uint32(e[28:]))]
	dir, err := p.directory(i, e)
	if err != nil {
		return err
	}
	n, size := binary.Uvarint(dir)
	if size <= 0 || n > uint64(len(dir)-size)/dirEntrySize || b.Rows == 0 || b.Rows > 1<<16 {
		return ErrCorrupt
	}
	entries := size + int(n)*dirEntrySize
	b.dir, b.stats = dir[size:entries], dir[entries:]
	b.slot = (binary.LittleEndian.Uint32(e[40:]) + uint32(size)) / dirEntrySize
	b.cols = grow(b.cols, int(n))
	b.parsed = grow(b.parsed, vec.BitmapWords(int(n)))
	clear(b.parsed)

	return nil
}

// dirKey orders directory entry i by path id, then kind.
func (b *Block) dirKey(i int) uint64 {
	e := b.dir[i*dirEntrySize:]

	return uint64(binary.LittleEndian.Uint32(e))<<8 | uint64(e[4])
}

// seek returns the first directory entry whose key is not below key.
func (b *Block) seek(key uint64) int {
	lo, hi := 0, len(b.cols)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if b.dirKey(mid) < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	return lo
}

// col returns column i, decoding its directory entry on first use.
func (b *Block) col(i int) *ColumnRef {
	c := &b.cols[i]
	if b.parsed.Get(i) {
		return c
	}
	b.parsed.Set(i)
	e := b.dir[i*dirEntrySize:]
	*c = ColumnRef{
		PathID: binary.LittleEndian.Uint32(e),
		Kind:   vec.Kind(e[4]),
		Count:  int(binary.LittleEndian.Uint32(e[8:])),
		enc:    e[5],
		flags:  e[6],
		off:    binary.LittleEndian.Uint32(e[12:]),
		length: binary.LittleEndian.Uint32(e[16:]),
		crc:    binary.LittleEndian.Uint32(e[20:]),
		slot:   b.slot + uint32(i),
	}
	r := reader{}
	if at := binary.LittleEndian.Uint32(e[24:]); uint64(at) <= uint64(len(b.stats)) {
		r.buf = b.stats[at:]
	} else {
		r.bad = true
	}
	switch c.Kind {
	case vec.KindInt:
		c.MinInt, c.MaxInt = unzigzag(r.uvarint()), unzigzag(r.uvarint())
	case vec.KindFloat:
		c.MinFloat, c.MaxFloat = math.Float64frombits(r.uint64()), math.Float64frombits(r.uint64())
	case vec.KindNull, vec.KindBool, vec.KindString, vec.KindVariant:
	default:
		r.bad = true
	}
	if c.enc == encConst && c.Kind != vec.KindInt {
		c.constVal = r.bytes(r.uvarint())
	}
	if r.bad || c.Count <= 0 || c.Count > b.Rows || uint64(c.off)+uint64(c.length) > uint64(len(b.data)) ||
		int(c.PathID) >= len(b.part.paths) || (c.flags&flagSparse == 0) != (c.Count == b.Rows) {
		*c = ColumnRef{PathID: c.PathID, Kind: c.Kind, bad: true}
	}

	return c
}

// Index returns the number of the block in its part.
func (b *Block) Index() int { return b.index }

// Columns returns every column of the block, ordered by path id and kind.
func (b *Block) Columns() []ColumnRef {
	for i := range b.cols {
		b.col(i)
	}

	return b.cols
}

// Path returns the columns stored for a path, one per value kind.
func (b *Block) Path(pathID uint32) []ColumnRef {
	lo := b.seek(uint64(pathID) << 8)
	hi := lo
	for hi < len(b.cols) && b.dirKey(hi)>>8 == uint64(pathID) {
		b.col(hi)
		hi++
	}

	return b.cols[lo:hi]
}

// Column returns the column of a path that holds values of kind.
func (b *Block) Column(pathID uint32, kind vec.Kind) (*ColumnRef, bool) {
	key := uint64(pathID)<<8 | uint64(kind)
	i := b.seek(key)
	if i == len(b.cols) || b.dirKey(i) != key {
		return nil, false
	}

	return b.col(i), true
}

// MayContainToken reports whether any string of the block may contain the
// token with this hash, which is always true for a block written without a
// filter, and fails when the filter is damaged.
func (b *Block) MayContainToken(hash uint64) (bool, error) {
	return b.part.MayContainToken(b.index, hash)
}

// MayContainTokenIn reports whether a string of the block's column with this
// path and kind may contain the token: the answer of the column's own filter,
// or of the one that the columns without a filter of their own share.
func (b *Block) MayContainTokenIn(pathID uint32, kind vec.Kind, hash uint64) (bool, error) {
	return b.part.MayContainTokenIn(b.index, pathID, kind, hash)
}

// MayContainTokensIn reports whether the strings of the block's column with
// this path and kind may contain every token of hashes.
func (b *Block) MayContainTokensIn(pathID uint32, kind vec.Kind, hashes []uint64) (bool, error) {
	return b.part.MayContainTokensIn(b.index, pathID, kind, hashes)
}

// FilterColumns calls fn with the path id and kind of every column of the
// block that has a token filter of its own.
func (b *Block) FilterColumns(fn func(pathID uint32, kind vec.Kind)) {
	b.part.FilterColumns(b.index, fn)
}

// OwnFilter reports whether the block's column with this path and kind has
// a token filter of its own, which the footer of the part tells.
func (b *Block) OwnFilter(pathID uint32, kind vec.Kind) bool {
	return b.part.OwnFilter(b.index, pathID, kind)
}

// ColumnsMayContain returns dst, resized to a bit for every column of the
// block in the order of Columns, with the bit set of each column whose
// strings may contain every token of hashes.
func (b *Block) ColumnsMayContain(dst vec.Bitmap, hashes []uint64) (vec.Bitmap, error) {
	p := b.part
	dst = grow(dst, vec.BitmapWords(len(b.cols)))
	clear(dst)
	if len(p.groups) == 0 {
		dst.SetAll(len(b.cols))

		return dst, nil
	}
	g, first, end := p.blockFilters(b.index)
	p.aheadStripe(p.groupOf(b.index), hashes)
	for f := first; f < end; f++ {
		if binary.LittleEndian.Uint32(p.filters[f*filterEntrySize:]) != noPath {
			continue
		}
		may, err := p.holds(g, f, hashes)
		if err != nil {
			return dst, err
		}
		if may {
			dst.SetAll(len(b.cols))
		}
	}
	for f := first; f < end; f++ {
		e := p.filters[f*filterEntrySize:]
		path := binary.LittleEndian.Uint32(e)
		if path == noPath {
			continue
		}
		key := uint64(path)<<8 | uint64(e[4])
		i := b.seek(key)
		if i == len(b.cols) || b.dirKey(i) != key {
			continue
		}
		may, err := p.holds(g, f, hashes)
		if err != nil {
			return dst, err
		}
		if may {
			dst.Set(i)
		} else {
			dst[i>>6] &^= 1 << (uint(i) & 63)
		}
	}

	return dst, nil
}

// MayContainTokenIn is Block.MayContainTokenIn for block i, without opening
// it.
func (p *Part) MayContainTokenIn(i int, pathID uint32, kind vec.Kind, hash uint64) (bool, error) {
	hashes := [1]uint64{hash}

	return p.MayContainTokensIn(i, pathID, kind, hashes[:])
}

// MayContainTokensIn is Block.MayContainTokensIn for block i, without
// opening it.
func (p *Part) MayContainTokensIn(i int, pathID uint32, kind vec.Kind, hashes []uint64) (bool, error) {
	if len(p.groups) == 0 {
		return true, nil
	}
	g, first, end := p.blockFilters(i)
	f, _ := p.columnFilter(first, end, pathID, kind)
	if f < 0 {
		return false, nil
	}
	if len(hashes) > 0 && p.readAhead != nil {
		p.aheadColumn(p.groupOf(i), pathID, kind, hashes[0])
	}

	return p.holds(g, f, hashes)
}

// ForgetReadAhead makes the lookups in the token filters ask for their
// pages again, and every read check what it reads again, after the pages
// of the part were let go of.
func (p *Part) ForgetReadAhead() {
	for i := range p.groups {
		for j := range p.groups[i].advised {
			p.groups[i].advised[j].Store(0)
		}
	}
	for i := range p.advised {
		p.advised[i].Store(0)
	}
	p.Recheck()
}

// Recheck makes every read of the part check what it reads against its
// checksum again. A column and a page of a token filter are checked when
// they are first read and not again until then: the bytes of a part do not
// change, and what a device returns for them later is checked by calling
// this from time to time.
func (p *Part) Recheck() {
	for i := range p.sound {
		p.sound[i].Store(0)
	}
	for i := range p.groups {
		for j := range p.groups[i].sound {
			p.groups[i].sound[j].Store(0)
		}
	}
}

// checked reports whether the bytes of a column match their checksum,
// which it takes when they were not seen to since the last Recheck.
func (b *Block) checked(ref *ColumnRef, chunk []byte) bool {
	word, bit := &b.part.sound[ref.slot>>6], uint64(1)<<(ref.slot&63)
	if word.Load()&bit != 0 {
		return true
	}
	b.ahead(ref)
	if crc32.Checksum(chunk, castagnoli) != ref.crc {
		return false
	}
	word.Or(bit)

	return true
}

// ahead asks for the bytes of a column ahead of reading them, once until
// the pages of the part are let go of.
func (b *Block) ahead(ref *ColumnRef) {
	p := b.part
	if p.readAhead == nil || ref.length < minReadAhead {
		return
	}
	word, bit := &p.advised[ref.slot>>6], uint64(1)<<(ref.slot&63)
	if word.Load()&bit != 0 {
		return
	}
	word.Or(bit)
	p.readAhead(b.start+int(ref.off), int(ref.length))
}

// aheadGap is the most bytes between two chunks that one request reads
// through rather than asking for the two apart: a device that serves a few
// thousand requests a second reads about this much in the time of one.
const aheadGap = 32 << 10

// Ahead asks for the chunks of every column at the path ids that were not
// asked for yet, in one request for each run of chunks that lie less than
// aheadGap apart, whatever their size.
func (b *Block) Ahead(ids []uint32) {
	p := b.part
	if p.readAhead == nil {
		return
	}
	b.spans = b.spans[:0]
	for _, id := range ids {
		for i := b.seek(uint64(id) << 8); i < len(b.cols) && b.dirKey(i)>>8 == uint64(id); i++ {
			ref := b.col(i)
			if ref.bad || ref.length == 0 {
				continue
			}
			word, bit := &p.advised[ref.slot>>6], uint64(1)<<(ref.slot&63)
			if word.Load()&bit != 0 {
				continue
			}
			word.Or(bit)
			b.spans = append(b.spans, [2]uint32{ref.off, ref.off + ref.length})
		}
	}
	slices.SortFunc(b.spans, func(x, y [2]uint32) int { return cmp.Compare(x[0], y[0]) })
	for i := 0; i < len(b.spans); {
		from, to := b.spans[i][0], b.spans[i][1]
		for i++; i < len(b.spans) && b.spans[i][0] <= to+aheadGap; i++ {
			to = max(to, b.spans[i][1])
		}
		p.readAhead(b.start+int(from), int(to-from))
	}
}

// aheadColumn asks for the pages that lookups of hash in the filters of one
// column read in group gi and in the groups after it. The pieces of the
// column's filters lie side by side in a stripe, block after block.
func (p *Part) aheadColumn(gi int, pathID uint32, kind vec.Kind, hash uint64) {
	key := uint64(pathID)<<8 | uint64(kind)
	ask := func(j int) bool {
		g := &p.groups[j]
		from, to, known := g.span(key)
		if !known {
			from, to = math.MaxUint32, 0
			for b := int(g.first); b < int(g.first+g.blocks); b++ {
				_, first, end := p.blockFilters(b)
				if f, _ := p.columnFilter(first, end, pathID, kind); f >= 0 {
					e := p.filters[f*filterEntrySize:]
					pieces, before := uint64(binary.LittleEndian.Uint32(e[8:])), uint64(binary.LittleEndian.Uint32(e[12:]))
					from, to = min(from, before), max(to, before+pieces)
				}
			}
			from = min(from, to)
			g.keepSpan(key, from, to)
		}
		// A group without the column has nothing to ask for, and the
		// groups after it are looked at.
		if to == 0 {
			return true
		}
		at := uint64(tokenfilter.Block(hash, int(g.stripes))) * g.stripe

		return g.advise((at+from)/pageBlocks*filterPage, min(((at+to-1)/pageBlocks+1)*filterPage, g.size), p.readAhead)
	}
	p.aheadFrom(gi, ask)
}

// aheadFrom asks for the pages of group gi and, when a lookup reads it for
// the first time, of the groups after it; a later lookup in the group asks
// for the one group that the scan then nears.
func (p *Part) aheadFrom(gi int, ask func(group int) bool) {
	last := min(gi+aheadGroups, len(p.groups)-1)
	if !ask(gi) {
		if last > gi {
			ask(last)
		}

		return
	}
	for j := gi + 1; j <= last; j++ {
		ask(j)
	}
}

// aheadStripe asks for the stripe that lookups of hashes in every filter of
// the blocks read in group gi and in the groups after it.
func (p *Part) aheadStripe(gi int, hashes []uint64) {
	if len(hashes) == 0 || p.readAhead == nil {
		return
	}
	p.aheadFrom(gi, func(j int) bool {
		g := &p.groups[j]
		stripe := uint64(tokenfilter.Block(hashes[0], int(g.stripes)))
		from := stripe * g.stripe / pageBlocks * filterPage
		to := min(((stripe+1)*g.stripe+pageBlocks-1)/pageBlocks*filterPage, g.size)

		return g.advise(from, to, p.readAhead)
	})
}

// columnFilter returns which of the filters first to end of a block holds
// the tokens of the column with this path and kind, and whether it is the
// column's own; a block whose every column has its own has none for others.
func (p *Part) columnFilter(first, end int, pathID uint32, kind vec.Kind) (f int, own bool) {
	f = -1
	for ; first < end; first++ {
		switch e := p.filters[first*filterEntrySize:]; binary.LittleEndian.Uint32(e) {
		case noPath:
			f = first
		case pathID:
			if vec.Kind(e[4]) == kind {
				return first, true
			}
		}
	}

	return f, false
}

// holds reports whether filter f of group g may hold every token of hashes.
func (p *Part) holds(g *filterGroup, f int, hashes []uint64) (bool, error) {
	cells := p.filterCells(f)
	for _, h := range hashes {
		n, size := p.cellOf(g, f, h)
		if err := p.checkPages(g, n, size); err != nil {
			return false, err
		}
		may := false
		if at := n % pageBlocks; at+size <= pageBlocks {
			from := g.off + n/pageBlocks*filterPage + (1+at)*tokenfilter.BlockBytes
			may = tokenfilter.CellMayContain(p.blocks[from:from+size*tokenfilter.BlockBytes], h, cells)
		} else {
			may = p.splitCellMayContain(g, n, size, h, cells)
		}
		if !may {
			return false, nil
		}
	}

	return true, nil
}

// checkPages checks the pages that hold the size filter blocks of group g
// from block n on against their checksums, each the first time it is read.
func (p *Part) checkPages(g *filterGroup, n, size uint64) error {
	for pg := n / pageBlocks; pg <= (n+size-1)/pageBlocks; pg++ {
		if g.sound[pg>>6].Load()>>(pg&63)&1 != 0 {
			continue
		}
		// The view of the part that reads the pages asked for and no others.
		page := p.blocks[g.off+pg*filterPage : g.off+min((pg+1)*filterPage, g.size)]
		if crc32.Checksum(page[4:], castagnoli) != binary.LittleEndian.Uint32(page) {
			return ErrCorrupt
		}
		g.sound[pg>>6].Or(1 << (pg & 63))
	}

	return nil
}

// splitCellMayContain is CellMayContain for a cell of size filter blocks,
// from block n of group g on, that two pages or more hold.
func (p *Part) splitCellMayContain(g *filterGroup, n, size, h uint64, cells int) bool {
	var buf [1024]byte
	cell := buf[:0]
	for at, end := n, n+size; at < end; {
		from := g.off + at/pageBlocks*filterPage + (1+at%pageBlocks)*tokenfilter.BlockBytes
		take := min(end-at, pageBlocks-at%pageBlocks)
		cell = append(cell, p.blocks[from:from+take*tokenfilter.BlockBytes]...)
		at += take
	}

	return tokenfilter.CellMayContain(cell, h, cells)
}

// FilterColumns is Block.FilterColumns for block i, without opening it.
func (p *Part) FilterColumns(i int, fn func(pathID uint32, kind vec.Kind)) {
	if len(p.groups) == 0 {
		return
	}
	for _, f, end := p.blockFilters(i); f < end; f++ {
		if e := p.filters[f*filterEntrySize:]; binary.LittleEndian.Uint32(e) != noPath {
			fn(binary.LittleEndian.Uint32(e), vec.Kind(e[4]))
		}
	}
}

// OwnFilter is Block.OwnFilter for block i, without opening it.
func (p *Part) OwnFilter(i int, pathID uint32, kind vec.Kind) bool {
	if len(p.groups) == 0 {
		return false
	}
	_, first, end := p.blockFilters(i)
	_, own := p.columnFilter(first, end, pathID, kind)

	return own
}

// MayContainToken is Block.MayContainToken for block i, without opening it.
func (p *Part) MayContainToken(i int, hash uint64) (bool, error) {
	hashes := [1]uint64{hash}

	return p.MayContainTokens(i, hashes[:])
}

// MayContainTokens reports whether the strings of one column of block i may
// contain every token of hashes, as far as its filters tell columns apart.
func (p *Part) MayContainTokens(i int, hashes []uint64) (bool, error) {
	if len(p.groups) == 0 {
		return true, nil
	}
	g, f, end := p.blockFilters(i)
	p.aheadStripe(p.groupOf(i), hashes)
	for ; f < end; f++ {
		if may, err := p.holds(g, f, hashes); may || err != nil {
			return may, err
		}
	}

	return false, nil
}

// blockFilters returns the filter group of block i and the range of its
// filters among the part's.
func (p *Part) blockFilters(i int) (g *filterGroup, first, end int) {
	e := p.index[i*indexEntrySize:]
	end = len(p.filters) / filterEntrySize
	if len(e) > indexEntrySize {
		end = int(binary.LittleEndian.Uint32(e[indexEntrySize+36:]))
	}

	return &p.groups[binary.LittleEndian.Uint32(e[32:])], int(binary.LittleEndian.Uint32(e[36:])), end
}

// cellOf returns where the cell of hash in filter f of group g begins among
// the filter blocks of the group, and its blocks.
func (p *Part) cellOf(g *filterGroup, f int, hash uint64) (n, size uint64) {
	e := p.filters[f*filterEntrySize:]
	pieces, before := uint64(binary.LittleEndian.Uint32(e[8:])), uint64(binary.LittleEndian.Uint32(e[12:]))
	cells := uint64(p.filterCells(f))
	perStripe := cells / g.stripes
	size = pieces / perStripe
	stripe := uint64(tokenfilter.Block(hash, int(g.stripes)))
	cell := uint64(tokenfilter.Block(hash, int(cells)))

	return stripe*g.stripe + before + (cell-stripe*perStripe)*size, size
}

// filterCells returns the number of cells of filter f.
func (p *Part) filterCells(f int) int { return 1 << p.filters[f*filterEntrySize+5] }

// filterBlocks returns the number of filter blocks of filter f of group g.
func (p *Part) filterBlocks(g *filterGroup, f int) int {
	return int(uint64(binary.LittleEndian.Uint32(p.filters[f*filterEntrySize+8:])) * g.stripes)
}

// pages returns the number of pages of the group.
func (g *filterGroup) pages() int { return int((g.size + filterPage - 1) / filterPage) }

// appendFilter appends filter f of group g, put together from its pieces,
// to dst and reports whether the pages it read match their checksums.
// verified marks the pages of the group that are known to match.
func (p *Part) appendFilter(dst []byte, g *filterGroup, f int, verified vec.Bitmap) ([]byte, bool) {
	e := p.filters[f*filterEntrySize:]
	pieces, before := uint64(binary.LittleEndian.Uint32(e[8:])), uint64(binary.LittleEndian.Uint32(e[12:]))
	group := p.blocks[g.off : g.off+g.size]
	for k := uint64(0); k < g.stripes; k++ {
		for n, end := k*g.stripe+before, k*g.stripe+before+pieces; n < end; {
			page := n / pageBlocks
			read := group[page*filterPage : min((page+1)*filterPage, g.size)]
			if !verified.Get(int(page)) {
				if crc32.Checksum(read[4:], castagnoli) != binary.LittleEndian.Uint32(read) {
					return dst, false
				}
				verified.Set(int(page))
			}
			from := (1 + n%pageBlocks) * tokenfilter.BlockBytes
			take := min(end-n, pageBlocks-n%pageBlocks)
			dst = append(dst, read[from:from+take*tokenfilter.BlockBytes]...)
			n += take
		}
	}

	return dst, true
}

func grow[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}

	return s[:n]
}

// Decode materializes a column into dst, one slot per row, leaving dst with
// no reference to the part's bytes.
func (b *Block) Decode(ref *ColumnRef, dst *vec.Column) error {
	return b.decode(ref, dst, false)
}

// DecodeValues is Decode with one slot per value: slot i of a column with
// gaps holds the value of the i-th row that Valid marks.
func (b *Block) DecodeValues(ref *ColumnRef, dst *vec.Column) error {
	return b.decode(ref, dst, true)
}

// DecodePresence fills dst with which rows hold a value of the column and
// with nothing else, for counting values and testing presence without
// decoding them; Valid is nil when every row holds one.
func (b *Block) DecodePresence(ref *ColumnRef, dst *vec.Column) error {
	if ref.bad {
		return ErrCorrupt
	}
	chunk := b.data[ref.off : ref.off+ref.length]
	if !b.checked(ref, chunk) {
		return ErrCorrupt
	}
	dst.Kind, dst.Rows, dst.Layout, dst.Valid = ref.Kind, b.Rows, vec.Flat, nil
	if ref.flags&flagSparse == 0 {
		return nil
	}
	_, err := b.readPresence(chunk, ref.Count, dst)

	return err
}

// decodeTimes fills dst with the times of the rows in the unit whose power
// of ten is the chunk.
func (b *Block) decodeTimes(chunk []byte, sparse bool, dst *vec.Column) error {
	times, ok := b.Column(0, vec.KindInt)
	if len(chunk) != 1 || int(chunk[0]) >= len(timeUnits) || timeUnits[chunk[0]] == 0 || sparse || !ok || times.enc == encTime {
		return ErrCorrupt
	}
	if err := b.decode(times, dst, false); err != nil {
		return err
	}
	if unit := timeUnits[chunk[0]]; unit != 1 {
		for i := range dst.Ints {
			dst.Ints[i] /= unit
		}
	}

	return nil
}

func (b *Block) decode(ref *ColumnRef, dst *vec.Column, byValue bool) error {
	if ref.bad {
		return ErrCorrupt
	}
	chunk := b.data[ref.off : ref.off+ref.length]
	if !b.checked(ref, chunk) {
		return ErrCorrupt
	}
	rows, count := b.Rows, ref.Count
	dst.Kind, dst.Rows, dst.Layout, dst.Valid = ref.Kind, rows, vec.Flat, nil
	dst.Codes8, dst.Codes16 = dst.Codes8[:0], dst.Codes16[:0]
	sparse := ref.flags&flagSparse != 0
	if sparse {
		var err error
		if chunk, err = b.readPresence(chunk, count, dst); err != nil {
			return err
		}
	}
	// By value, a column is laid out as one without gaps over its values.
	slots := rows
	if byValue && sparse {
		slots, sparse = count, false
	}

	switch ref.Kind {
	case vec.KindNull:
		return nil
	case vec.KindBool:
		return b.decodeBools(ref, chunk, sparse, slots, dst)
	case vec.KindInt:
		if ref.enc == encConst {
			dst.Layout = vec.Const
			dst.Ints = append(dst.Ints[:0], ref.MinInt)

			return nil
		}
		if ref.enc == encTime {
			return b.decodeTimes(chunk, sparse, dst)
		}
		if !sparse {
			vals, _, err := readInts(dst.Ints, chunk, count, &b.d)
			dst.Ints = vals

			return err
		}
		vals, _, err := readInts(b.d.ints, chunk, count, &b.d)
		if err != nil {
			return err
		}
		b.d.ints = vals
		dst.Ints = grow(dst.Ints, rows)
		clear(dst.Ints)
		spread(dst.Ints, vals, dst.Valid)

		return nil
	case vec.KindFloat:
		return b.decodeFloats(ref, chunk, sparse, slots, dst)
	}

	return b.decodeBytes(ref, chunk, sparse, slots, dst)
}

// readPresence fills dst.Valid from the presence section at the start of
// chunk, which must mark count rows, and returns what follows the section.
func (b *Block) readPresence(chunk []byte, count int, dst *vec.Column) ([]byte, error) {
	if len(chunk) == 0 {
		return nil, ErrCorrupt
	}
	mode := chunk[0]
	payload, rest, _, err := readBlob(chunk[1:], &b.d.zbuf)
	if err != nil {
		return nil, err
	}
	rows := b.Rows
	valid := dst.UseValid(rows)
	switch mode {
	case presenceList, presenceHoles:
		listed := count
		if mode == presenceHoles {
			listed = rows - count
			valid.SetAll(rows)
		}
		if len(payload) != 2*listed {
			return nil, ErrCorrupt
		}
		prev := -1
		for i := 0; i < listed; i++ {
			r := int(binary.LittleEndian.Uint16(payload[2*i:]))
			if r <= prev || r >= rows {
				return nil, ErrCorrupt
			}
			prev = r
			valid[r>>6] ^= 1 << (uint(r) & 63)
		}
	case presenceBitmap:
		if len(payload) != (rows+7)/8 {
			return nil, ErrCorrupt
		}
		for i, v := range payload {
			valid[i>>3] |= uint64(v) << (8 * (uint(i) & 7))
		}
		if tail := uint(rows) & 63; tail != 0 && valid[len(valid)-1]>>tail != 0 {
			return nil, ErrCorrupt
		}
		if valid.Count() != count {
			return nil, ErrCorrupt
		}
	default:
		return nil, ErrCorrupt
	}

	return rest, nil
}

// positions returns the rows of valid in order.
func (b *Block) positions(valid vec.Bitmap) []uint32 {
	b.pos = valid.AppendSet(b.pos[:0])

	return b.pos
}

func (b *Block) decodeBools(ref *ColumnRef, chunk []byte, sparse bool, rows int, dst *vec.Column) error {
	count := ref.Count
	if ref.enc == encConst {
		if len(ref.constVal) != 1 {
			return ErrCorrupt
		}
		dst.Layout = vec.Const
		dst.Bools = append(dst.Bools[:0], uint64(ref.constVal[0]&1))

		return nil
	}
	payload, _, _, err := readBlob(chunk, &b.d.zbuf)
	if err != nil || len(payload) != (count+7)/8 {
		return ErrCorrupt
	}
	dst.Bools = grow(dst.Bools, vec.BitmapWords(rows))
	clear(dst.Bools)
	var pos []uint32
	if sparse {
		pos = b.positions(dst.Valid)
	}
	for i := 0; i < count; i++ {
		if payload[i>>3]&(1<<(uint(i)&7)) == 0 {
			continue
		}
		if sparse {
			dst.Bools.Set(int(pos[i]))
		} else {
			dst.Bools.Set(i)
		}
	}

	return nil
}

func (b *Block) decodeFloats(ref *ColumnRef, chunk []byte, sparse bool, rows int, dst *vec.Column) error {
	count := ref.Count
	if ref.enc == encConst {
		if len(ref.constVal) != 8 {
			return ErrCorrupt
		}
		dst.Layout = vec.Const
		dst.Floats = append(dst.Floats[:0], math.Float64frombits(binary.LittleEndian.Uint64(ref.constVal)))

		return nil
	}
	payload, _, _, err := readBlob(chunk, &b.d.zbuf)
	if err != nil || len(payload) != 8*count {
		return ErrCorrupt
	}
	dst.Floats = grow(dst.Floats, rows)
	var pos []uint32
	if sparse {
		clear(dst.Floats)
		pos = b.positions(dst.Valid)
	}
	for i := 0; i < count; i++ {
		v := math.Float64frombits(binary.LittleEndian.Uint64(payload[8*i:]))
		if sparse {
			dst.Floats[pos[i]] = v
		} else {
			dst.Floats[i] = v
		}
	}

	return nil
}

func (b *Block) decodeBytes(ref *ColumnRef, chunk []byte, sparse bool, rows int, dst *vec.Column) error {
	count := ref.Count
	switch ref.enc {
	case encConst:
		dst.Layout = vec.Const
		dst.Data = append(dst.Data[:0], ref.constVal...)
		dst.Offsets = append(dst.Offsets[:0], 0, uint32(len(dst.Data)))

		return nil
	case encFlat:
		lens, rest, err := readInts(b.d.ints, chunk, count, &b.d)
		if err != nil {
			return err
		}
		b.d.ints = lens
		if err := b.readData(rest, dst); err != nil {
			return err
		}
		dst.Offsets = grow(dst.Offsets, rows+1)
		off := uint64(0)
		if !sparse {
			for i, n := range lens {
				dst.Offsets[i] = uint32(off)
				off += uint64(n)
			}
		} else {
			next := 0
			for r := 0; r < rows; r++ {
				dst.Offsets[r] = uint32(off)
				if dst.Valid.Get(r) {
					off += uint64(lens[next])
					next++
				}
			}
		}
		dst.Offsets[rows] = uint32(off)
		if off != uint64(len(dst.Data)) || !nonNegative(lens) {
			return ErrCorrupt
		}

		return nil
	case encDict:
		return b.decodeDict(chunk, count, sparse, rows, ref.flags&flagCounts != 0, dst)
	}

	return ErrCorrupt
}

// dataSize returns the bytes Decode puts in Data for a string or variant
// column, or 0 when the chunk does not say.
func (b *Block) dataSize(ref *ColumnRef) uint32 {
	if ref.bad || ref.Kind != vec.KindString && ref.Kind != vec.KindVariant {
		return 0
	}
	if ref.enc == encConst {
		return uint32(len(ref.constVal))
	}
	chunk := b.data[ref.off : ref.off+ref.length]
	ok := true
	if ref.flags&flagSparse != 0 && len(chunk) > 0 {
		chunk, _, ok = skipBlob(chunk[1:])
	}
	values := ref.Count
	if ok && ref.enc == encDict {
		size, n := binary.Uvarint(chunk)
		if ok = n > 0 && size <= maxDictSize; ok {
			chunk, values = chunk[n:], int(size)
		}
	}
	if ok {
		chunk, ok = skipInts(chunk, values)
	}
	if !ok {
		return 0
	}
	_, size, _ := skipBlob(chunk)

	return size
}

// skipBlob returns the bytes after the section at the start of src and the
// size of its payload once decoded.
func skipBlob(src []byte) (rest []byte, rawLen uint32, ok bool) {
	head, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, 0, false
	}
	src = src[n:]
	size, raw := head>>2, head>>2
	if head&3 != blobStored {
		if raw, n = binary.Uvarint(src); n <= 0 {
			return nil, 0, false
		}
		src = src[n:]
	}
	if size > uint64(len(src)) || raw > maxSectionSize {
		return nil, 0, false
	}

	return src[size:], uint32(raw), true
}

// skipInts returns the bytes after the integer section of count values at
// the start of src.
func skipInts(src []byte, count int) ([]byte, bool) {
	if len(src) == 0 {
		return nil, false
	}
	mode := src[0]
	src = src[1:]
	switch mode {
	case intConst, intBytes8, intBytes16:
		_, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, false
		}
		if mode == intConst {
			return src[n:], true
		}
		rest, _, ok := skipBlob(src[n:])

		return rest, ok
	case intFrames, intDeltaFrames:
		for start := 0; start < count; start += frameLen {
			_, n := binary.Uvarint(src)
			if n <= 0 || len(src) <= n {
				return nil, false
			}
			need := (min(frameLen, count-start)*int(src[n]) + 7) / 8
			if src[n] > 64 || need > len(src)-n-1 {
				return nil, false
			}
			src = src[n+1+need:]
		}

		return src, true
	}

	return nil, false
}

func nonNegative(vals []int64) bool {
	for _, v := range vals {
		if v < 0 {
			return false
		}
	}

	return true
}

// readData decodes the blob at the start of src into dst.Data.
func (b *Block) readData(src []byte, dst *vec.Column) error {
	payload, _, stored, err := readBlob(src, &dst.Data)
	if err != nil {
		return err
	}
	if stored {
		dst.Data = append(dst.Data[:0], payload...)
	}

	return nil
}

// readEntries fills dst with the entries of the dictionary at the start of
// chunk and returns their number and what follows them.
func (b *Block) readEntries(chunk []byte, dst *vec.Column) (size uint64, rest []byte, err error) {
	size, n := binary.Uvarint(chunk)
	if n <= 0 || size < 2 || size > maxDictSize {
		return 0, nil, ErrCorrupt
	}
	lens, rest, err := readInts(b.d.ints, chunk[n:], int(size), &b.d)
	if err != nil {
		return 0, nil, err
	}
	b.d.ints = lens
	payload, rest, stored, err := readBlob(rest, &dst.Data)
	if err != nil {
		return 0, nil, err
	}
	if stored {
		dst.Data = append(dst.Data[:0], payload...)
	}
	dst.Offsets = grow(dst.Offsets, int(size)+1)
	off := uint64(0)
	for i, l := range lens {
		dst.Offsets[i] = uint32(off)
		off += uint64(l)
	}
	dst.Offsets[size] = uint32(off)
	if off != uint64(len(dst.Data)) || !nonNegative(lens) {
		return 0, nil, ErrCorrupt
	}
	dst.Layout = vec.Dict

	return size, rest, nil
}

// skipCounts returns what follows the rows of n dictionary entries.
func skipCounts(src []byte, n int) ([]byte, bool) {
	for ; n > 0; n-- {
		_, used := binary.Uvarint(src)
		if used <= 0 {
			return nil, false
		}
		src = src[used:]
	}

	return src, true
}

// Counts holds the string values at one path of a part, in order, with the rows that hold each.
type Counts struct {
	Values [][]byte
	Rows   []uint64
	// Held is the number of rows that hold a value.
	Held uint64
}

// Counts returns the rows of every value at a path that holds few strings and nothing else; other rows miss the path.
func (p *Part) Counts(pathID uint32) (*Counts, bool) {
	c, ok := p.counts[pathID]

	return c, ok
}

// readCounts reads the rows of the values of counted paths, which end a footer.
func (p *Part) readCounts(r *reader) bool {
	paths := r.uvarint()
	if r.bad || paths > uint64(len(p.paths)) {
		return false
	}
	for ; paths > 0; paths-- {
		id, values := r.uvarint(), r.uvarint()
		if r.bad || id >= uint64(len(p.paths)) || values == 0 || values > maxCounted {
			return false
		}
		c := &Counts{Values: make([][]byte, 0, values), Rows: make([]uint64, 0, values)}
		for ; values > 0; values-- {
			v, rows := r.bytes(r.uvarint()), r.uvarint()
			c.Values, c.Rows, c.Held = append(c.Values, v), append(c.Rows, rows), c.Held+rows
		}
		if r.bad || c.Held > p.Rows {
			return false
		}
		if p.counts == nil {
			p.counts = make(map[uint32]*Counts)
		}
		p.counts[uint32(id)] = c
	}

	return !r.bad
}

// Counted reports whether the column holds, with its dictionary, the
// number of rows of every entry.
func (c *ColumnRef) Counted() bool { return !c.bad && c.enc == encDict && c.flags&flagCounts != 0 }

// DecodeCounts fills dst with the dictionary of a counted column, without
// the codes of its rows, and appends to counts the number of rows that hold
// each entry.
func (b *Block) DecodeCounts(ref *ColumnRef, dst *vec.Column, counts []uint32) ([]uint32, error) {
	if !ref.Counted() {
		return counts, ErrCorrupt
	}
	chunk := b.data[ref.off : ref.off+ref.length]
	if !b.checked(ref, chunk) {
		return counts, ErrCorrupt
	}
	if ref.flags&flagSparse != 0 {
		ok := len(chunk) > 0
		if ok {
			chunk, _, ok = skipBlob(chunk[1:])
		}
		if !ok {
			return counts, ErrCorrupt
		}
	}
	dst.Kind, dst.Rows, dst.Valid = ref.Kind, b.Rows, nil
	dst.Codes8, dst.Codes16 = dst.Codes8[:0], dst.Codes16[:0]
	size, rest, err := b.readEntries(chunk, dst)
	if err != nil {
		return counts, err
	}
	total := uint64(0)
	for i := uint64(0); i < size; i++ {
		n, used := binary.Uvarint(rest)
		if used <= 0 || n > uint64(ref.Count) {
			return counts, ErrCorrupt
		}
		rest, total = rest[used:], total+n
		counts = append(counts, uint32(n))
	}
	if total != uint64(ref.Count) {
		return counts, ErrCorrupt
	}

	return counts, nil
}

func (b *Block) decodeDict(chunk []byte, count int, sparse bool, rows int, counted bool, dst *vec.Column) error {
	size, rest, err := b.readEntries(chunk, dst)
	if err != nil {
		return err
	}
	if counted {
		ok := false
		if rest, ok = skipCounts(rest, int(size)); !ok {
			return ErrCorrupt
		}
	}
	if len(rest) == 0 {
		return ErrCorrupt
	}
	mode, codes := rest[0], rest[1:]
	width := uint(bits.Len64(size - 1))
	if mode == codesPacked && len(codes) != packedSize(count, width) || mode > codesRuns {
		return ErrCorrupt
	}
	if size <= 256 {
		dst.Codes8 = grow(dst.Codes8, rows)
		out := dst.Codes8
		if sparse {
			b.d.codes8 = grow(b.d.codes8, count)
			out = b.d.codes8
		}
		if !readCodes(out, codes, mode, width, int(size), unpack8) {
			return ErrCorrupt
		}
		if sparse {
			clear(dst.Codes8)
			spread(dst.Codes8, out, dst.Valid)
		}

		return nil
	}
	dst.Codes16 = grow(dst.Codes16, rows)
	out := dst.Codes16
	if sparse {
		b.d.codes16 = grow(b.d.codes16, count)
		out = b.d.codes16
	}
	if !readCodes(out, codes, mode, width, int(size), unpack16) {
		return ErrCorrupt
	}
	if sparse {
		clear(dst.Codes16)
		spread(dst.Codes16, out, dst.Valid)
	}

	return nil
}

// readCodes fills dst from a codes section and reports whether the section
// holds exactly len(dst) codes, all below size.
func readCodes[T uint8 | uint16](dst []T, src []byte, mode uint8, width uint, size int,
	unpack func(dst []T, src []byte, width uint, size int) bool) bool {
	if mode == codesPacked {
		return unpack(dst, src, width, size)
	}
	n := 0
	for len(src) > 0 {
		code, a := binary.Uvarint(src)
		if a <= 0 {
			return false
		}
		run, c := binary.Uvarint(src[a:])
		if c <= 0 || code >= uint64(size) || run == 0 || run > uint64(len(dst)-n) {
			return false
		}
		src = src[a+c:]
		fill := dst[n : n+int(run)]
		for i := range fill {
			fill[i] = T(code)
		}
		n += int(run)
	}

	return n == len(dst)
}

// reader consumes a byte slice and remembers whether it ran short.
type reader struct {
	buf []byte
	bad bool
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.bad = true
		r.buf = nil

		return 0
	}
	r.buf = r.buf[n:]

	return v
}

func (r *reader) bytes(n uint64) []byte {
	if n > uint64(len(r.buf)) {
		r.bad = true
		r.buf = nil

		return nil
	}
	out := r.buf[:n]
	r.buf = r.buf[n:]

	return out
}

func (r *reader) uint64() uint64 {
	if b := r.bytes(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}

	return 0
}

// FindEqual appends to dst the rows whose value of a string column equals
// want, read from the form the values are stored in; it reports false for a
// column stored in a form that has to be decoded to be searched.
func (b *Block) FindEqual(ref *ColumnRef, want []byte, dst []uint32) ([]uint32, bool, error) {
	if ref.bad {
		return dst, false, ErrCorrupt
	}
	if ref.Kind != vec.KindString || (ref.enc != encFlat && ref.enc != encDict) {
		return dst, false, nil
	}
	whole := b.data[ref.off : ref.off+ref.length]
	b.ahead(ref)
	chunk, ok := whole, true
	sparse := ref.flags&flagSparse != 0
	if sparse && len(chunk) > 0 {
		chunk, _, ok = skipBlob(chunk[1:])
	}
	// The strings are the values, or the entries of their dictionary.
	texts := ref.Count
	if ok && ref.enc == encDict {
		entries, n := binary.Uvarint(chunk)
		if n <= 0 || entries < 2 || entries > maxDictSize {
			return dst, false, ErrCorrupt
		}
		chunk, texts = chunk[n:], int(entries)
	}
	if ok {
		chunk, ok = skipInts(chunk, texts)
	}
	head, n := binary.Uvarint(chunk)
	if !ok || n <= 0 || head&3 != blobPacked {
		return dst, false, nil
	}
	size := head >> 2
	rawLen, m := binary.Uvarint(chunk[n:])
	if m <= 0 || size > uint64(len(chunk)-n-m) || rawLen > maxSectionSize {
		return dst, false, ErrCorrupt
	}
	if !b.checked(ref, whole) {
		return dst, false, ErrCorrupt
	}
	from := len(dst)
	dst, err := findPacked(dst, chunk[n+m:n+m+int(size)], int(rawLen), want)
	if err != nil || len(dst) == from {
		return dst, err == nil, err
	}
	if ref.enc == encDict {
		entry, codes := dst[from], chunk[n+m+int(size):]
		if ref.flags&flagCounts != 0 {
			codes, ok = skipCounts(codes, texts)
		}
		if !ok {
			return dst[:from], false, ErrCorrupt
		}
		if dst, ok = b.findCode(dst[:from], codes, entry, texts, ref.Count); !ok {
			return dst[:from], false, ErrCorrupt
		}
	}
	if !sparse {
		return dst, true, nil
	}
	// The values found are numbered among the rows that hold one.
	if _, err := b.readPresence(whole, ref.Count, &b.present); err != nil {
		return dst[:from], false, err
	}
	rows := b.positions(b.present.Valid)
	for i, v := range dst[from:] {
		if int(v) >= len(rows) {
			return dst[:from], false, ErrCorrupt
		}
		dst[from+i] = rows[v]
	}

	return dst, true, nil
}

// findCode appends to dst the numbers of the values, count in all, whose
// code in the codes section src of a dictionary of size entries is code,
// and reports whether the section is sound.
func (b *Block) findCode(dst []uint32, src []byte, code uint32, size, count int) ([]uint32, bool) {
	if len(src) == 0 {
		return dst, false
	}
	mode, codes := src[0], src[1:]
	width := uint(bits.Len(uint(size - 1)))
	switch {
	case mode == codesRuns:
		at := 0
		for len(codes) > 0 {
			c, a := binary.Uvarint(codes)
			if a <= 0 {
				return dst, false
			}
			run, r := binary.Uvarint(codes[a:])
			if r <= 0 || c >= uint64(size) || run == 0 || run > uint64(count-at) {
				return dst, false
			}
			codes = codes[a+r:]
			if c == uint64(code) {
				for i := at; i < at+int(run); i++ {
					dst = append(dst, uint32(i))
				}
			}
			at += int(run)
		}

		return dst, at == count
	case mode != codesPacked || len(codes) != packedSize(count, width):
		return dst, false
	case size <= 256:
		b.d.codes8 = grow(b.d.codes8, count)
		if !unpack8(b.d.codes8, codes, width, size) {
			return dst, false
		}
		for i, c := range b.d.codes8 {
			if uint32(c) == code {
				dst = append(dst, uint32(i))
			}
		}
	default:
		b.d.codes16 = grow(b.d.codes16, count)
		if !unpack16(b.d.codes16, codes, width, size) {
			return dst, false
		}
		for i, c := range b.d.codes16 {
			if uint32(c) == code {
				dst = append(dst, uint32(i))
			}
		}
	}

	return dst, true
}

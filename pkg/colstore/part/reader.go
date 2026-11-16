package part

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Part is an opened, immutable part that reads only the bytes it was opened
// on, which must stay valid and unchanged while it is in use.
type Part struct {
	data  []byte
	paths []string
	ids   map[string]uint32
	index []byte

	ordered bool
	sortKey []string
	// firstKeys holds the encoded sort key of each block's first row.
	firstKeys [][]byte

	// Rows is the number of rows in the part.
	Rows uint64
	// RawBytes is the size of the ingested input the part was built from.
	RawBytes uint64
	// MinTime and MaxTime bound the row timestamps.
	MinTime int64
	MaxTime int64
}

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

	p := &Part{data: data}
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
	if p.ordered {
		p.firstKeys = make([][]byte, 0, nBlocks)
		for i := uint64(0); i < nBlocks; i++ {
			p.firstKeys = append(p.firstKeys, r.bytes(r.uvarint()))
		}
	}
	p.Rows = r.uvarint()
	p.RawBytes = r.uvarint()
	p.MinTime = unzigzag(r.uvarint())
	p.MaxTime = unzigzag(r.uvarint())
	if r.bad || len(p.paths) == 0 || p.paths[0] != TimePath {
		return nil, ErrCorrupt
	}
	for i := 0; i < p.NumBlocks(); i++ {
		e := p.index[i*indexEntrySize:]
		start, size := binary.LittleEndian.Uint64(e[20:]), uint64(binary.LittleEndian.Uint32(e[28:]))
		filterOff, filterLen := uint64(binary.LittleEndian.Uint32(e[32:])), uint64(binary.LittleEndian.Uint32(e[36:]))
		dirOff, dirLen := uint64(binary.LittleEndian.Uint32(e[40:])), uint64(binary.LittleEndian.Uint32(e[44:]))
		if start < headerSize || start > off || size > off-start ||
			filterOff+filterLen != dirOff || dirOff+dirLen != size {
			return nil, ErrCorrupt
		}
	}

	return p, nil
}

// Ordered reports whether the rows are ordered by SortKey and then by
// timestamp.
func (p *Part) Ordered() bool { return p.ordered }

// SortKey returns the paths an ordered part is ordered by.
func (p *Part) SortKey() []string { return p.sortKey }

// Size returns the part's size in bytes.
func (p *Part) Size() int { return len(p.data) }

// NumBlocks returns the number of blocks.
func (p *Part) NumBlocks() int { return len(p.index) / indexEntrySize }

// Paths returns the column paths; a path's position is its id in this part.
func (p *Part) Paths() []string { return p.paths }

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
	constVal []byte
}

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

	part   *Part
	data   []byte
	chunks uint32 // size of the column chunks at the start of data
	// filter is the token filter followed by its page checksums.
	filter []byte
	// dir holds one entry per column, ordered by path id and kind, and stats
	// the statistics and constant values the entries point to.
	dir   []byte
	stats []byte
	// cols[i] is decoded from entry i once bit i of parsed is set.
	cols   []ColumnRef
	parsed vec.Bitmap
	d      decodeScratch
	pos    []uint32
}

// OpenBlock points b at block i of p.
func (p *Part) OpenBlock(i int, b *Block) error {
	e := p.index[i*indexEntrySize:]
	b.Rows, b.MinTime, b.MaxTime = p.BlockStats(i)
	start := binary.LittleEndian.Uint64(e[20:])
	b.part = p
	b.data = p.data[start : start+uint64(binary.LittleEndian.Uint32(e[28:]))]
	filterOff, dirOff := binary.LittleEndian.Uint32(e[32:]), binary.LittleEndian.Uint32(e[40:])
	b.chunks = filterOff
	b.filter = b.data[filterOff:dirOff]
	dir := b.data[dirOff:]
	if crc32.Checksum(dir, castagnoli) != binary.LittleEndian.Uint32(e[48:]) {
		return ErrCorrupt
	}
	n, size := binary.Uvarint(dir)
	if size <= 0 || n > uint64(len(dir)-size)/dirEntrySize || b.Rows == 0 || b.Rows > 1<<16 {
		return ErrCorrupt
	}
	entries := size + int(n)*dirEntrySize
	b.dir, b.stats = dir[size:entries], dir[entries:]
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
	if r.bad || c.Count <= 0 || c.Count > b.Rows || uint64(c.off)+uint64(c.length) > uint64(b.chunks) ||
		int(c.PathID) >= len(b.part.paths) || (c.flags&flagSparse == 0) != (c.Count == b.Rows) {
		*c = ColumnRef{PathID: c.PathID, Kind: c.Kind, bad: true}
	}

	return c
}

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
	return filterMayContain(b.filter, hash)
}

// MayContainToken is Block.MayContainToken for block i, without opening it.
func (p *Part) MayContainToken(i int, hash uint64) (bool, error) {
	e := p.index[i*indexEntrySize:]
	start := binary.LittleEndian.Uint64(e[20:]) + uint64(binary.LittleEndian.Uint32(e[32:]))

	return filterMayContain(p.data[start:start+uint64(binary.LittleEndian.Uint32(e[36:]))], hash)
}

// filterMayContain probes a filter stored with its page checksums, verifying
// the one page the probe reads.
func filterMayContain(region []byte, hash uint64) (bool, error) {
	if len(region) == 0 {
		return true, nil
	}
	pages := (len(region) + filterPage + 3) / (filterPage + 4)
	size := len(region) - 4*pages
	if size <= 0 || (size+filterPage-1)/filterPage != pages {
		return false, ErrCorrupt
	}
	filter, sums := region[:size], region[size:]
	page := tokenfilter.ProbeOffset(size, hash) / filterPage
	read := filter[page*filterPage : min((page+1)*filterPage, size)]
	if crc32.Checksum(read, castagnoli) != binary.LittleEndian.Uint32(sums[4*page:]) {
		return false, ErrCorrupt
	}

	return tokenfilter.MayContain(filter, hash), nil
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
	if ref.bad {
		return ErrCorrupt
	}
	chunk := b.data[ref.off : ref.off+ref.length]
	if crc32.Checksum(chunk, castagnoli) != ref.crc {
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

	switch ref.Kind {
	case vec.KindNull:
		return nil
	case vec.KindBool:
		return b.decodeBools(ref, chunk, sparse, dst)
	case vec.KindInt:
		if ref.enc == encConst {
			dst.Layout = vec.Const
			dst.Ints = append(dst.Ints[:0], ref.MinInt)

			return nil
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
		return b.decodeFloats(ref, chunk, sparse, dst)
	}

	return b.decodeBytes(ref, chunk, sparse, dst)
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

func (b *Block) decodeBools(ref *ColumnRef, chunk []byte, sparse bool, dst *vec.Column) error {
	rows, count := b.Rows, ref.Count
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

func (b *Block) decodeFloats(ref *ColumnRef, chunk []byte, sparse bool, dst *vec.Column) error {
	rows, count := b.Rows, ref.Count
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

func (b *Block) decodeBytes(ref *ColumnRef, chunk []byte, sparse bool, dst *vec.Column) error {
	rows, count := b.Rows, ref.Count
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
		return b.decodeDict(chunk, count, sparse, dst)
	}

	return ErrCorrupt
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

func (b *Block) decodeDict(chunk []byte, count int, sparse bool, dst *vec.Column) error {
	rows := b.Rows
	size, n := binary.Uvarint(chunk)
	if n <= 0 || size < 2 || size > maxDictSize {
		return ErrCorrupt
	}
	lens, rest, err := readInts(b.d.ints, chunk[n:], int(size), &b.d)
	if err != nil {
		return err
	}
	b.d.ints = lens
	payload, rest, stored, err := readBlob(rest, &dst.Data)
	if err != nil {
		return err
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
		return ErrCorrupt
	}

	dst.Layout = vec.Dict
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

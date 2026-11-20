package part

import (
	"encoding/binary"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Usage tells where the bytes of parts go; its columns and Regions add up to
// Bytes.
type Usage struct {
	Parts, Blocks    int
	Rows, InputBytes uint64
	Bytes            int64
	// Regions holds by name the bytes that belong to no column: headers,
	// footers, the heads of directories, the filters that columns share,
	// the checksums of filter pages and the padding in front of groups.
	Regions map[string]int64
	Columns map[ColumnKey]*ColumnUsage
}

// ColumnKey names the column of a path that holds values of one kind.
type ColumnKey struct {
	Path string
	Kind vec.Kind
}

// ColumnUsage holds the bytes of one column over the blocks of parts.
type ColumnUsage struct {
	Blocks int
	Values uint64
	// ChunkBytes is what the chunks of the column take in its blocks,
	// DirectoryBytes its directory entries with their statistics and
	// constant values, FilterBytes the token filters of its own.
	ChunkBytes, DirectoryBytes, FilterBytes int64
	// Encodings holds by encoding the blocks, values and chunk bytes.
	Encodings map[string]*Amount
	// Sections holds the sections of the chunks by what they hold and how
	// they hold it; their bytes add up to ChunkBytes.
	Sections map[string]*Amount
}

// Amount is a share of a column, whose Values are a dictionary's entries for
// the sections of entries and whose RawBytes are what a byte section decodes
// to, eight for every integer, or zero where the format does not tell.
type Amount struct {
	Blocks   int
	Values   uint64
	Bytes    int64
	RawBytes int64
}

var (
	encodingNames = [...]string{encNone: "presence", encConst: "const", encFlat: "flat", encDict: "dict", encTime: "time"}
	blobNames     = [...]string{blobStored: "stored", blobZstd: "zstd", blobLZ4: "lz4", blobPacked: "packed"}
	presenceNames = [...]string{presenceBitmap: "bitmap", presenceList: "list", presenceHoles: "holes"}
	codesNames    = [...]string{codesPacked: "dict.codes.packed", codesRuns: "dict.codes.runs"}
	intNames      = [...]string{
		intFrames: "frames", intDeltaFrames: "delta_frames", intBytes8: "bytes8",
		intBytes16: "bytes16", intConst: "const", intScaled: "scaled",
	}
)

// AddUsage adds the bytes of the part to u, from its footer and directories
// and, with sections set, the heads of the sections of every chunk, which
// it reads without decoding any.
func (p *Part) AddUsage(u *Usage, sections bool) error {
	if u.Regions == nil {
		u.Regions = map[string]int64{}
	}
	if u.Columns == nil {
		u.Columns = map[ColumnKey]*ColumnUsage{}
	}
	u.Parts++
	u.Blocks += p.NumBlocks()
	u.Rows += p.Rows
	u.InputBytes += p.RawBytes
	u.Bytes += int64(len(p.data))
	u.Regions["header"] += headerSize
	u.Regions["trailer"] += trailerSize
	footer := p.addFooter(u)
	placed := int64(headerSize+trailerSize+footer) + int64(len(p.dirs))

	cols := make([]*ColumnUsage, len(p.paths)*kindSlots)
	column := func(pathID uint32, kind vec.Kind) *ColumnUsage {
		slot := int(pathID)*kindSlots + int(kind)%kindSlots
		if cols[slot] == nil {
			key := ColumnKey{Path: p.paths[pathID], Kind: kind}
			if cols[slot] = u.Columns[key]; cols[slot] == nil {
				cols[slot] = &ColumnUsage{Encodings: map[string]*Amount{}, Sections: map[string]*Amount{}}
				u.Columns[key] = cols[slot]
			}
		}

		return cols[slot]
	}
	for i := 0; i < p.NumBlocks(); i++ {
		e := p.index[i*indexEntrySize:]
		start, size := binary.LittleEndian.Uint64(e[20:]), binary.LittleEndian.Uint32(e[28:])
		data := p.blocks[start : start+uint64(size)]
		placed += int64(size)
		dir, err := p.directory(i, e)
		if err != nil {
			return err
		}
		n, head := binary.Uvarint(dir)
		if head <= 0 || n > uint64(len(dir)-head)/dirEntrySize {
			return ErrCorrupt
		}
		u.Regions["directories.heads"] += int64(head)
		ents, stats := dir[head:head+int(n)*dirEntrySize], dir[head+int(n)*dirEntrySize:]
		chunks, statBytes := int64(0), 0
		for j := 0; j < int(n); j++ {
			ent := ents[j*dirEntrySize:]
			pathID, kind, enc, flags := binary.LittleEndian.Uint32(ent), vec.Kind(ent[4]), ent[5], ent[6]
			count := int(binary.LittleEndian.Uint32(ent[8:]))
			off, length := binary.LittleEndian.Uint32(ent[12:]), binary.LittleEndian.Uint32(ent[16:])
			if int(pathID) >= len(p.paths) || uint64(off)+uint64(length) > uint64(len(data)) {
				return ErrCorrupt
			}
			stat := statSize(stats, binary.LittleEndian.Uint32(ent[24:]), kind, enc)
			statBytes += stat
			c := column(pathID, kind)
			c.Blocks++
			c.Values += uint64(count)
			c.ChunkBytes += int64(length)
			c.DirectoryBytes += int64(dirEntrySize + stat)
			name := "invalid"
			if int(enc) < len(encodingNames) {
				name = encodingNames[enc]
			}
			c.Encodings[name] = c.Encodings[name].add(1, count, int64(length), 0)
			chunks += int64(length)
			if sections {
				walkChunk(c, kind, enc, flags, count, data[off:off+length])
			}
		}
		if rest := len(stats) - statBytes; rest != 0 {
			u.Regions["directories.unaccounted"] += int64(rest)
		}
		if rest := int64(size) - chunks; rest != 0 {
			u.Regions["blocks.unaccounted"] += rest
		}
	}
	for g := range p.groups {
		grp := &p.groups[g]
		placed += int64(grp.size)
		u.Regions["filters.page_checksums"] += int64(grp.pages()) * tokenfilter.BlockBytes
	}
	for b := 0; b < p.NumBlocks() && len(p.groups) > 0; b++ {
		g, first, end := p.blockFilters(b)
		for f := first; f < end; f++ {
			bytes := int64(p.filterBlocks(g, f)) * tokenfilter.BlockBytes
			fe := p.filters[f*filterEntrySize:]
			if path := binary.LittleEndian.Uint32(fe); path != noPath {
				column(path, vec.Kind(fe[4])).FilterBytes += bytes
			} else {
				u.Regions["filters.shared"] += bytes
			}
		}
	}
	// Only the alignment of large filter groups leaves gaps between regions.
	u.Regions["filters.padding"] += int64(len(p.data)) - placed

	return nil
}

func (a *Amount) add(blocks, values int, bytes, raw int64) *Amount {
	if a == nil {
		a = &Amount{}
	}
	a.Blocks += blocks
	a.Values += uint64(values)
	a.Bytes += bytes
	a.RawBytes += raw

	return a
}

// statSize returns the bytes of the statistics and constant value of a
// directory entry at offset at of stats.
func statSize(stats []byte, at uint32, kind vec.Kind, enc uint8) int {
	if uint64(at) > uint64(len(stats)) {
		return 0
	}
	r := reader{buf: stats[at:]}
	switch kind {
	case vec.KindInt:
		r.uvarint()
		r.uvarint()
	case vec.KindFloat:
		r.uint64()
		r.uint64()
	}
	if enc == encConst && kind != vec.KindInt {
		r.bytes(r.uvarint())
	}
	if r.bad {
		return 0
	}

	return len(stats) - int(at) - len(r.buf)
}

// addFooter adds the sections of the footer to the regions of u and returns
// the footer's size.
func (p *Part) addFooter(u *Usage) int {
	trailer := p.data[len(p.data)-trailerSize:]
	off := binary.LittleEndian.Uint64(trailer)
	r := reader{buf: p.data[off : len(p.data)-trailerSize]}
	size, mark := len(r.buf), len(r.buf)
	take := func(name string) {
		u.Regions[name] += int64(mark - len(r.buf))
		mark = len(r.buf)
	}
	for n := r.uvarint(); n > 0 && !r.bad; n-- {
		r.bytes(r.uvarint())
	}
	take("footer.paths")
	for n := r.uvarint(); n > 1 && !r.bad; n-- {
		r.uvarint()
	}
	take("footer.sort_key")
	r.bytes(r.uvarint() * indexEntrySize)
	take("footer.block_index")
	filters, groups := r.uvarint(), r.uvarint()
	r.bytes(filters * filterEntrySize)
	take("footer.filter_entries")
	r.bytes(groups * groupEntrySize)
	take("footer.filter_groups")
	r.uvarint()
	r.uvarint()
	take("footer.totals")
	if p.ordered {
		for i := 0; i <= p.NumBlocks() && !r.bad; i++ {
			r.bytes(r.uvarint())
		}
		take("footer.block_keys")
	}
	if p.ordered && len(p.sortKey) > 0 {
		for i := 0; i < p.NumBlocks()*(len(p.sortKey)+1) && !r.bad; i++ {
			r.uvarint()
		}
		take("footer.key_stats")
	}
	for i := 0; i < 4; i++ {
		r.uvarint()
	}
	take("footer.totals")
	if r.bad {
		u.Regions["footer.unparsed"] += int64(mark)
	} else {
		u.Regions["footer.counted_values"] += int64(mark)
	}

	return size
}

// walkChunk adds the sections of a chunk to c from their heads alone.
func walkChunk(c *ColumnUsage, kind vec.Kind, enc, flags uint8, count int, chunk []byte) {
	s := sectionWalk{c: c, src: chunk, ok: true}
	if flags&flagSparse != 0 {
		mode := uint8(len(presenceNames))
		if len(chunk) > 0 {
			mode = chunk[0]
		}
		if mode >= uint8(len(presenceNames)) {
			s.ok = false
		} else {
			s.src = s.src[1:]
			s.blob("presence."+presenceNames[mode], count, 1)
		}
	}
	switch {
	case !s.ok, enc == encNone, enc == encConst:
	case enc == encTime:
		s.take("time", min(1, len(s.src)), count, 0)
	case kind == vec.KindBool && enc == encFlat:
		s.blob("bools", count, 0)
	case kind == vec.KindFloat && enc == encFlat:
		s.blob("floats", count, 0)
	case kind == vec.KindInt && enc == encFlat:
		s.ints("ints", count, 0)
	case enc == encFlat:
		s.ints("lengths", count, 0)
		s.blob("values", count, 0)
	case enc == encDict:
		entries, n := binary.Uvarint(s.src)
		if n <= 0 || entries > maxDictSize {
			s.ok = false

			break
		}
		s.src = s.src[n:]
		s.ints("dict.lengths", int(entries), n)
		s.blob("dict.entries", int(entries), 0)
		if s.ok && flags&flagCounts != 0 {
			rest, ok := skipCounts(s.src, int(entries))
			s.ok = ok
			if ok {
				s.take("dict.counts", len(s.src)-len(rest), int(entries), 0)
			}
		}
		if s.ok && len(s.src) > 0 && s.src[0] <= codesRuns {
			s.take(codesNames[s.src[0]], len(s.src), count, 0)
		}
	default:
		s.ok = false
	}
	if len(s.src) > 0 {
		s.add("unparsed", len(s.src), count, 0)
	}
}

// sectionWalk steps over the sections of a chunk.
type sectionWalk struct {
	c   *ColumnUsage
	src []byte
	ok  bool
}

func (s *sectionWalk) add(name string, n, values int, raw int64) {
	s.c.Sections[name] = s.c.Sections[name].add(1, values, int64(n), raw)
}

// take adds the next n bytes as a section and steps over them.
func (s *sectionWalk) take(name string, n, values int, raw int64) {
	s.add(name, n, values, raw)
	s.src = s.src[n:]
}

// blob steps over a byte section, adding to it the extra bytes before it
// that belong to it.
func (s *sectionWalk) blob(role string, values, extra int) {
	if !s.ok {
		return
	}
	form, n, raw, ok := blobSpan(s.src)
	if s.ok = ok; ok {
		s.add(role+"."+form, extra+n, values, int64(raw))
		s.src = s.src[n:]
	}
}

// ints steps over an integer section of count values.
func (s *sectionWalk) ints(role string, count, extra int) {
	if !s.ok {
		return
	}
	form, n, ok := intsSpan(s.src, count)
	if s.ok = ok; ok {
		s.add(role+"."+form, extra+n, count, 8*int64(count))
		s.src = s.src[n:]
	}
}

// blobSpan returns how the byte section at the start of src is stored, its
// size and the size of its payload once decoded.
func blobSpan(src []byte) (form string, n int, raw uint64, ok bool) {
	head, used := binary.Uvarint(src)
	if used <= 0 {
		return "", 0, 0, false
	}
	size, kind := head>>2, head&3
	raw = size
	if kind != blobStored {
		r, m := binary.Uvarint(src[used:])
		if m <= 0 {
			return "", 0, 0, false
		}
		raw, used = r, used+m
	}
	if size > uint64(len(src)-used) {
		return "", 0, 0, false
	}

	return blobNames[kind], used + int(size), raw, true
}

// intsSpan returns the framing of the integer section of count values at
// the start of src and its size.
func intsSpan(src []byte, count int) (form string, n int, ok bool) {
	if len(src) == 0 || int(src[0]) >= len(intNames) {
		return "", 0, false
	}
	switch mode := src[0]; mode {
	case intScaled:
		_, a := binary.Uvarint(src[1:])
		if a <= 0 {
			return "", 0, false
		}
		_, b := binary.Uvarint(src[1+a:])
		if b <= 0 {
			return "", 0, false
		}
		inner, m, ok := intsSpan(src[1+a+b:], count)

		return "scaled." + inner, 1 + a + b + m, ok
	case intBytes8, intBytes16:
		_, a := binary.Uvarint(src[1:])
		if a <= 0 {
			return "", 0, false
		}
		codec, m, _, ok := blobSpan(src[1+a:])

		return intNames[mode] + "." + codec, 1 + a + m, ok
	default:
		rest, ok := skipInts(src, count)

		return intNames[mode], len(src) - len(rest), ok
	}
}

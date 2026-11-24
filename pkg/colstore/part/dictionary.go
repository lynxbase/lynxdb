package part

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A part dictionary holds the distinct strings that the blocks of a part
// store as codes at one path, in the order of their bytes. A code is the
// place of a string in that order among the strings the dictionary was laid
// out for; the part may hold fewer of them, since a merge lays a dictionary
// out before it knows which rows it will write, and then says which codes
// have a string. Strings of the most common length are packed like those of
// a packed section, each in a slot of as many bits, so that any of them is
// read where it lies; the others are kept as they are.
//
//	slots       one for every stored string, in order: when some strings
//	            are kept as they are, a bit that marks those, then the
//	            packed codes; eight zero bytes follow the last slot
//	exceptions  for the strings kept as they are: the number of each among
//	            the stored strings, then where each begins among their
//	            bytes and where the last ends, 32 bits each, then the bytes
//	live        when codes lack a string: a bit for every code, set where
//	            it has one, in words of 64 bits, then the number of strings
//	            before every liveBlock codes, 32 bits each
//	checksums   the checksum of every dictPage bytes of the sections above
//
// After the dictionaries the footer lists the columns that a merge found
// not worth one, each with the number of values it then had: merges leave
// such a column alone until they read twice as many values of it.
//
// The footer holds, for every dictionary, its path, the numbers of its codes
// and of its stored strings, the values of its column in the part, what
// they would take in the forms of single blocks, where its sections lie,
// the sizes of the slots and of the exceptions with their number, whether a
// slot begins with the bit that marks an exception, and the head of a packed
// section body that lays out the codes of a slot.
const (
	// dictPage is the bytes that one checksum covers, which a read of one
	// string checks and nothing else.
	dictPage = 4096
	// dictChunk is the number of strings packed at a time; that many slots
	// end on a byte whatever their bits.
	dictChunk = 4096
	// dictFlush is the size from which the bytes of a dictionary being
	// written are handed on to the file.
	dictFlush = 1 << 20
	liveBlock = 512
	// maxDictStride bounds the length of packed strings, so that one is
	// compared in a buffer on the stack.
	maxDictStride = 255
	// maxDictOthers bounds the strings kept as they are: one in this many
	// of the packed ones, and a few whatever their number.
	maxDictOthers = 64
)

// Dictionary is the part dictionary of the string column at one path. It
// reads the bytes of the part where they lie and serves any number of
// goroutines at once.
type Dictionary struct {
	pathID uint32
	codes  int
	stored int
	// values is the number of values of the column in the part, and flat an
	// estimate of the bytes they would take in the forms of single blocks.
	values, flat uint64
	// off and size locate the sections in the part; slotsLen, exceptLen and
	// liveLen are the bytes of the first three, dataLen their sum.
	off, size                             uint64
	slotsLen, exceptLen, liveLen, dataLen uint64
	exceptions                            int
	head                                  []byte
	data                                  []byte
	reader                                packedReader
	scratch                               packScratch
	// sound has a bit for every page whose checksum was seen to match
	// since the last Recheck, and asked is set once the kernel was asked
	// for the sections ahead of reads of many strings.
	sound     []atomic.Uint64
	asked     atomic.Bool
	readAhead func(off, n int)
}

// Len returns the number of codes: every code of the column is below it.
func (d *Dictionary) Len() int { return d.codes }

// Entries returns the number of codes that have a string.
func (d *Dictionary) Entries() int { return d.stored }

// Values returns the number of values that the column holds in the part.
func (d *Dictionary) Values() uint64 { return d.values }

// Size returns the bytes of the dictionary in the part.
func (d *Dictionary) Size() int64 { return int64(d.size) }

// Dictionary returns the part dictionary of the string column at a path,
// which a part has when blocks store that column as codes.
func (p *Part) Dictionary(pathID uint32) (*Dictionary, bool) {
	if uint64(pathID) >= uint64(len(p.dicts)) || p.dicts[pathID] == nil {
		return nil, false
	}

	return p.dicts[pathID], true
}

// readDictionaries reads the dictionaries that end a footer, whose sections
// lie before limit, and reports whether they can be true.
func (p *Part) readDictionaries(r *reader, limit uint64) bool {
	before := len(r.buf)
	defer func() { p.dictFooter = before - len(r.buf) }()
	n := r.uvarint()
	if r.bad || n > uint64(len(p.paths)) {
		return false
	}
	if n > 0 {
		p.dicts = make([]*Dictionary, len(p.paths))
	}
	for ; n > 0; n-- {
		d := &Dictionary{}
		id, codes, stored := r.uvarint(), r.uvarint(), r.uvarint()
		d.values, d.flat, d.off, d.slotsLen = r.uvarint(), r.uvarint(), r.uvarint(), r.uvarint()
		exceptions := r.uvarint()
		d.exceptLen = r.uvarint()
		lead := r.bytes(1)
		head := r.bytes(r.uvarint())
		if r.bad || id >= uint64(len(p.paths)) || p.dicts[id] != nil || stored == 0 || stored > codes || codes > 1<<32-1 ||
			exceptions > stored || d.slotsLen > limit || d.exceptLen > limit || lead[0] > 1 || (exceptions > 0 && lead[0] == 0) {
			return false
		}
		d.pathID, d.codes, d.stored, d.exceptions = uint32(id), int(codes), int(stored), int(exceptions)
		d.head = bytes.Clone(head)
		rest, err := d.reader.readHead(d.head, &d.scratch)
		if err != nil || len(rest) != 0 || d.reader.stride > maxDictStride {
			return false
		}
		d.reader.lead = int(lead[0])
		d.reader.slot = d.reader.bitsPerString + d.reader.lead
		d.reader.count = d.stored
		if stored < codes {
			d.liveLen = 8*((codes+63)/64) + 4*(codes/liveBlock+1)
		}
		d.dataLen = d.slotsLen + d.exceptLen + d.liveLen
		d.size = d.dataLen + 4*((d.dataLen+dictPage-1)/dictPage)
		if d.reader.slot == 0 || d.slotsLen != (stored*uint64(d.reader.slot)+7)/8+8 ||
			(exceptions == 0) != (d.exceptLen == 0) || (exceptions > 0 && d.exceptLen < 8*exceptions+4) ||
			d.off < headerSize || d.off > limit || d.size > limit-d.off {
			return false
		}
		d.sound = make([]atomic.Uint64, (d.dataLen+dictPage-1)/dictPage/64+1)
		d.bind(p.data)
		p.dicts[id] = d
	}
	n = r.uvarint()
	if r.bad || n > uint64(len(p.paths)) {
		return false
	}
	for ; n > 0; n-- {
		id, values := r.uvarint(), r.uvarint()
		if r.bad || id >= uint64(len(p.paths)) {
			return false
		}
		if p.tried == nil {
			p.tried = map[uint32]uint64{}
		}
		p.tried[uint32(id)] = values
	}

	return !r.bad
}

// bind makes the dictionary read its sections from data, the bytes of its
// part.
func (d *Dictionary) bind(data []byte) {
	d.data = data[d.off : d.off+d.size]
	d.reader.codes = d.data[:d.slotsLen]
}

// ahead asks for the sections of the dictionary ahead of reads of many of
// its strings, once until the pages of the part are let go of.
func (d *Dictionary) ahead() {
	if d.readAhead == nil || d.asked.Load() || d.asked.Swap(true) {
		return
	}
	d.readAhead(int(d.off), int(d.size))
}

// checked reports whether the n bytes at off of the sections match the
// checksums of their pages, which it takes for the pages that were not seen
// to match since the last Recheck.
func (d *Dictionary) checked(off, n uint64) bool {
	if n == 0 {
		return true
	}
	if off+n > d.dataLen {
		return false
	}
	for pg := off / dictPage; pg <= (off+n-1)/dictPage; pg++ {
		if d.sound[pg>>6].Load()>>(pg&63)&1 != 0 {
			continue
		}
		page := d.data[pg*dictPage : min((pg+1)*dictPage, d.dataLen)]
		if crc32.Checksum(page, castagnoli) != binary.LittleEndian.Uint32(d.data[d.dataLen+4*pg:]) {
			return false
		}
		d.sound[pg>>6].Or(1 << (pg & 63))
	}

	return true
}

// recheck makes every read of the dictionary check its pages again.
func (d *Dictionary) recheck() {
	for i := range d.sound {
		d.sound[i].Store(0)
	}
}

// entry appends stored string i to dst.
func (d *Dictionary) entry(dst []byte, i int) ([]byte, error) {
	r := &d.reader
	bit := uint64(i) * uint64(r.slot)
	at := bit >> 3
	// The codes of a slot are read in words of eight bytes, the last of
	// which may reach into the zero bytes after the slots.
	if !d.checked(at, min(uint64(r.slot+7)/8+8, d.slotsLen-at)) {
		return dst, ErrCorrupt
	}
	if r.lead != 0 && d.data[at]>>(bit&7)&1 != 0 {
		return d.exception(dst, i)
	}
	n := len(dst)
	if cap(dst)-n < r.stride {
		dst = append(dst, r.template...)
	} else {
		dst = dst[:n+r.stride]
	}
	r.put(dst[n:], i, i+1)

	return dst, nil
}

// exception appends stored string i, one kept as it is, to dst.
func (d *Dictionary) exception(dst []byte, i int) ([]byte, error) {
	base, count := d.slotsLen, uint64(d.exceptions)
	lo, hi := uint64(0), count
	for lo < hi {
		mid := (lo + hi) / 2
		if !d.checked(base+4*mid, 4) {
			return dst, ErrCorrupt
		}
		if binary.LittleEndian.Uint32(d.data[base+4*mid:]) < uint32(i) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	ends := base + 4*count
	if lo == count || !d.checked(base+4*lo, 4) || binary.LittleEndian.Uint32(d.data[base+4*lo:]) != uint32(i) ||
		!d.checked(ends+4*lo, 8) {
		return dst, ErrCorrupt
	}
	from, to := uint64(binary.LittleEndian.Uint32(d.data[ends+4*lo:])), uint64(binary.LittleEndian.Uint32(d.data[ends+4*lo+4:]))
	text := ends + 4*(count+1)
	if from > to || to > base+d.exceptLen-text || !d.checked(text+from, to-from) {
		return dst, ErrCorrupt
	}

	return append(dst, d.data[text+from:text+to]...), nil
}

// liveWord returns word i of the bits that tell which codes have a string.
func (d *Dictionary) liveWord(i uint64) (uint64, bool) {
	at := d.slotsLen + d.exceptLen + 8*i
	if !d.checked(at, 8) {
		return 0, false
	}

	return binary.LittleEndian.Uint64(d.data[at:]), true
}

// before returns the number of stored strings whose codes lie below block
// number b of liveBlock codes.
func (d *Dictionary) before(b uint64) (uint64, bool) {
	at := d.slotsLen + d.exceptLen + 8*uint64((d.codes+63)/64) + 4*b
	if !d.checked(at, 4) {
		return 0, false
	}

	return uint64(binary.LittleEndian.Uint32(d.data[at:])), true
}

// index returns the number of the string of code among the stored strings;
// it fails for a code that has none.
func (d *Dictionary) index(code uint32) (int, error) {
	if d.liveLen == 0 {
		if int(code) >= d.stored {
			return 0, ErrCorrupt
		}

		return int(code), nil
	}
	if int(code) >= d.codes {
		return 0, ErrCorrupt
	}
	word := uint64(code) >> 6
	w, ok := d.liveWord(word)
	if !ok || w>>(code&63)&1 == 0 {
		return 0, ErrCorrupt
	}
	n, ok := d.before(uint64(code) / liveBlock)
	if !ok {
		return 0, ErrCorrupt
	}
	for i := uint64(code) / liveBlock * (liveBlock / 64); i < word; i++ {
		v, ok := d.liveWord(i)
		if !ok {
			return 0, ErrCorrupt
		}
		n += uint64(bits.OnesCount64(v))
	}

	return int(n) + bits.OnesCount64(w&(1<<(code&63)-1)), nil
}

// rank is index for a dictionary some of whose codes lack a string, once
// the caller has checked the bits that tell which, as a reader of many
// strings does for all of them at once: it reads those bits as they lie and
// reports whether code, which is below the number of codes, has a string.
func (d *Dictionary) rank(code uint32) (int, bool) {
	live := d.data[d.slotsLen+d.exceptLen:]
	word := uint64(code) >> 6
	w := binary.LittleEndian.Uint64(live[8*word:])
	if w>>(code&63)&1 == 0 {
		return 0, false
	}
	block := uint64(code) / liveBlock
	n := uint64(binary.LittleEndian.Uint32(live[8*uint64((d.codes+63)/64)+4*block:]))
	for i := block * (liveBlock / 64); i < word; i++ {
		n += uint64(bits.OnesCount64(binary.LittleEndian.Uint64(live[8*i:])))
	}

	return int(n) + bits.OnesCount64(w&(1<<(code&63)-1)), true
}

// checkedLive reports whether the bits that tell which codes have a string
// match their checksums.
func (d *Dictionary) checkedLive() bool { return d.checked(d.slotsLen+d.exceptLen, d.liveLen) }

// code returns the code of stored string i.
func (d *Dictionary) code(i int) (uint32, error) {
	if d.liveLen == 0 {
		return uint32(i), nil
	}
	// The last block of codes that begins at or before string i.
	lo, hi := uint64(0), uint64(d.codes)/liveBlock+1
	for lo+1 < hi {
		mid := (lo + hi) / 2
		n, ok := d.before(mid)
		if !ok {
			return 0, ErrCorrupt
		}
		if n <= uint64(i) {
			lo = mid
		} else {
			hi = mid
		}
	}
	left, ok := d.before(lo)
	if !ok {
		return 0, ErrCorrupt
	}
	left = uint64(i) - left
	for word := lo * (liveBlock / 64); word < uint64(d.codes+63)/64; word++ {
		w, ok := d.liveWord(word)
		if !ok {
			return 0, ErrCorrupt
		}
		if c := uint64(bits.OnesCount64(w)); left >= c {
			left -= c

			continue
		}
		for ; left > 0; left-- {
			w &= w - 1
		}

		return uint32(word<<6) + uint32(bits.TrailingZeros64(w)), nil
	}

	return 0, ErrCorrupt
}

// Value appends the string of code to dst. It reads the pages of the part
// that hold that string and no others.
func (d *Dictionary) Value(dst []byte, code uint32) ([]byte, error) {
	i, err := d.index(code)
	if err != nil {
		return dst, err
	}

	return d.entry(dst, i)
}

// findBuffers holds the strings that searches of dictionaries compare.
var findBuffers = sync.Pool{New: func() any { return new([]byte) }}

// Find returns the code of value and whether the dictionary holds it. It
// halves the dictionary until it has found the place of value, so it reads
// about as many pages as the number of strings has bits, not the dictionary.
func (d *Dictionary) Find(value []byte) (code uint32, ok bool, err error) {
	buf := findBuffers.Get().(*[]byte)
	defer findBuffers.Put(buf)
	lo, hi := 0, d.stored
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if *buf, err = d.entry((*buf)[:0], mid); err != nil {
			return 0, false, err
		}
		switch c := bytes.Compare(*buf, value); {
		case c < 0:
			lo = mid + 1
		case c > 0:
			hi = mid
		default:
			code, err = d.code(mid)

			return code, err == nil, err
		}
	}

	return 0, false, nil
}

// entryStats gathers, over the strings of a dictionary, what packing those
// of one length takes: the least and the greatest byte at each position,
// eight positions to a word, and how often every byte occurs, in four tables
// that take turns so that equal neighbours do not wait on each other.
type entryStats struct {
	stride  int
	first   []byte
	lo, hi  []byte
	loWords []uint64
	hiWords []uint64
	hists   [4][256]uint64
	// packed counts the strings of the length, others and otherBytes the
	// strings of other lengths and their bytes.
	packed, others, otherBytes uint64
}

func (s *entryStats) reset(stride int) {
	*s = entryStats{
		stride: stride, first: s.first[:0], lo: growBytes(s.lo, stride), hi: growBytes(s.hi, stride),
		loWords: growWords(s.loWords, stride/8), hiWords: growWords(s.hiWords, stride/8),
	}
}

func (s *entryStats) add(val []byte) {
	if len(val) != s.stride {
		s.others++
		s.otherBytes += uint64(len(val))

		return
	}
	loW, hiW := s.loWords, s.hiWords
	tail := 8 * len(loW)
	if s.packed == 0 {
		s.first = append(s.first[:0], val...)
		for w := range loW {
			loW[w] = binary.LittleEndian.Uint64(val[8*w:])
			hiW[w] = loW[w]
		}
		copy(s.lo[tail:], val[tail:])
		copy(s.hi[tail:], val[tail:])
	}
	for w := range loW {
		x := binary.LittleEndian.Uint64(val[8*w:])
		below := (lessBytes(x, loW[w]) >> 7) * 0xFF
		above := (lessBytes(hiW[w], x) >> 7) * 0xFF
		loW[w] = x&below | loW[w]&^below
		hiW[w] = x&above | hiW[w]&^above
	}
	lo, hi := s.lo[:len(val)], s.hi[:len(val)]
	for p := tail; p < len(val); p++ {
		lo[p], hi[p] = min(lo[p], val[p]), max(hi[p], val[p])
	}
	i := 0
	for ; i+4 <= len(val); i += 4 {
		b := val[i : i+4 : i+4]
		s.hists[0][b[0]]++
		s.hists[1][b[1]]++
		s.hists[2][b[2]]++
		s.hists[3][b[3]]++
	}
	for ; i < len(val); i++ {
		s.hists[0][val[i]]++
	}
	s.packed++
}

// sums spells the least and the greatest bytes out into lo and hi and
// returns how often every byte occurs.
func (s *entryStats) sums() (hist [256]uint64) {
	for w := range s.loWords {
		binary.LittleEndian.PutUint64(s.lo[8*w:], s.loWords[w])
		binary.LittleEndian.PutUint64(s.hi[8*w:], s.hiWords[w])
	}
	for c := range hist {
		hist[c] = s.hists[0][c] + s.hists[1][c] + s.hists[2][c] + s.hists[3][c]
	}

	return hist
}

// packLayout is how the strings of one length of a dictionary are packed:
// the head of a packed section body and what packing a string takes.
type packLayout struct {
	stride, bits, lead int
	code               [256]uint8
	head               []byte
	varPos             []int
	bases, widths      []byte
	steps              []codeStep
}

// set lays out the packing of the strings that s has seen and reports
// whether they pack: when the bytes that differ between them come from at
// most 64 symbols and few of the strings are of another length.
func (l *packLayout) set(s *entryStats) bool {
	if s.packed == 0 || s.stride > maxDictStride || s.others > s.packed/maxDictOthers+16 {
		return false
	}
	hist := s.sums()
	for p, c := range s.first {
		if s.lo[p] == s.hi[p] {
			hist[c] -= s.packed
		}
	}
	var symbols [1 << maxPackedBits]byte
	n := 0
	l.code = [256]uint8{}
	for c, seen := range hist {
		if seen == 0 {
			continue
		}
		if n == len(symbols) {
			return false
		}
		l.code[c], symbols[n] = uint8(n), byte(c)
		n++
	}
	// One string alone has no byte that differs, and a head needs a symbol.
	n = max(n, 1)
	l.stride, l.bits, l.lead = s.stride, 0, 0
	if s.others > 0 {
		l.lead = 1
	}
	l.head = binary.AppendUvarint(l.head[:0], uint64(s.stride))
	l.head = append(l.head, byte(n))
	l.head = append(l.head, symbols[:n]...)
	l.head = append(l.head, s.first...)
	l.varPos, l.bases, l.widths = l.varPos[:0], l.bases[:0], l.widths[:0]
	for p := 0; p < s.stride; p++ {
		base, width := l.code[s.lo[p]], byte(bits.Len8(l.code[s.hi[p]]-l.code[s.lo[p]]))
		l.head = append(l.head, base, width)
		if width > 0 {
			l.varPos, l.bases, l.widths = append(l.varPos, p), append(l.bases, base), append(l.widths, width)
			l.bits += int(width)
		}
	}
	l.steps = packSchedule(l.steps[:0], l.varPos, l.bases, l.widths)

	return l.bits+l.lead > 0
}

// size returns the bytes of a dictionary of this layout that stores
// stored strings, others of them in otherBytes bytes as they are, and that
// has codes codes.
func (l *packLayout) size(stored, others, otherBytes, codes uint64) uint64 {
	n := (stored*uint64(l.bits+l.lead)+7)/8 + 8
	if others > 0 {
		n += 8*others + 4 + otherBytes
	}
	if stored < codes {
		n += 8*((codes+63)/64) + 4*(codes/liveBlock+1)
	}

	return n + 4*((n+dictPage-1)/dictPage)
}

// dictWriter writes the sections of one dictionary as its strings come, in
// order, and holds little more than the page being filled.
type dictWriter struct {
	w *Writer
	l *packLayout
	// chunk holds the slots of the last strings, n of them, until dictChunk
	// are packed; pending the bytes of sections that are not written yet and
	// sums the checksums of the pages that are.
	chunk   []byte
	n       int
	stored  uint64
	pending []byte
	sums    []byte
	dataLen uint64
	// The strings kept as they are: their numbers, where each ends among
	// their bytes, and the bytes.
	others    []uint32
	otherEnds []uint32
	otherData []byte
}

func (dw *dictWriter) begin(w *Writer, l *packLayout) {
	dw.w, dw.l, dw.n, dw.stored, dw.dataLen = w, l, 0, 0, 0
	dw.chunk = growBytes(dw.chunk, dictChunk*(l.bits+l.lead)/8+8)
	clear(dw.chunk)
	dw.pending, dw.sums = dw.pending[:0], dw.sums[:0]
	dw.others, dw.otherEnds, dw.otherData = dw.others[:0], dw.otherEnds[:0], dw.otherData[:0]
}

// add stores the next string.
func (dw *dictWriter) add(val []byte) {
	l := dw.l
	bitPos := dw.n * (l.bits + l.lead)
	out := dw.chunk
	if len(val) != l.stride {
		if l.lead == 0 && dw.w.err == nil {
			// The layout was made for other strings than it is given.
			dw.w.err = errShared
		}
		out[bitPos>>3] |= 1 << (bitPos & 7)
		dw.others = append(dw.others, uint32(dw.stored))
		dw.otherData = append(dw.otherData, val...)
		dw.otherEnds = append(dw.otherEnds, uint32(len(dw.otherData)))
	} else {
		bitPos += l.lead
		code := &l.code
		for _, step := range l.steps {
			var v uint64
			if step.width != 0 {
				b := val[step.at : step.at+8 : step.at+8]
				v = uint64(code[b[0]]) | uint64(code[b[1]])<<8 | uint64(code[b[2]])<<16 | uint64(code[b[3]])<<24 |
					uint64(code[b[4]])<<32 | uint64(code[b[5]])<<40 | uint64(code[b[6]])<<48 | uint64(code[b[7]])<<56
				v = squeeze(v-step.base, uint(step.width))
			} else {
				shift := uint(0)
				for k := step.first; k < step.first+step.count; k++ {
					v |= uint64(code[val[l.varPos[k]]]-l.bases[k]) << shift
					shift += uint(l.widths[k])
				}
			}
			word := out[bitPos>>3:]
			binary.LittleEndian.PutUint64(word, binary.LittleEndian.Uint64(word)|v<<(bitPos&7))
			bitPos += step.bits
		}
	}
	dw.stored++
	if dw.n++; dw.n == dictChunk {
		dw.flushChunk()
	}
}

// flushChunk hands the slots packed so far on to the sections.
func (dw *dictWriter) flushChunk() {
	size := (dw.n*(dw.l.bits+dw.l.lead) + 7) / 8
	dw.emit(dw.chunk[:size])
	clear(dw.chunk[:size+8])
	dw.n = 0
}

// emit appends b to the sections.
func (dw *dictWriter) emit(b []byte) {
	dw.pending = append(dw.pending, b...)
	if len(dw.pending) >= dictFlush {
		dw.flush(false)
	}
}

// flush writes the whole pages of the sections that wait, and with last
// set whatever waits.
func (dw *dictWriter) flush(last bool) {
	n := len(dw.pending) &^ (dictPage - 1)
	if last {
		n = len(dw.pending)
	}
	for at := 0; at < n; at += dictPage {
		dw.sums = binary.LittleEndian.AppendUint32(dw.sums, crc32.Checksum(dw.pending[at:min(at+dictPage, n)], castagnoli))
	}
	dw.w.write(dw.pending[:n])
	dw.dataLen += uint64(n)
	dw.pending = dw.pending[:copy(dw.pending, dw.pending[n:])]
}

// finish writes what follows the slots of a dictionary of codes codes, of
// which those of live have a string, and returns the bytes of the slots and
// of the strings kept as they are.
func (dw *dictWriter) finish(codes int, live vec.Bitmap) (slotsLen, exceptLen uint64) {
	dw.flushChunk()
	var pad [8]byte
	dw.emit(pad[:])
	slotsLen = dw.dataLen + uint64(len(dw.pending))
	if len(dw.others) > 0 {
		var word [4]byte
		for _, i := range dw.others {
			binary.LittleEndian.PutUint32(word[:], i)
			dw.emit(word[:])
		}
		dw.emit(pad[:4])
		for _, end := range dw.otherEnds {
			binary.LittleEndian.PutUint32(word[:], end)
			dw.emit(word[:])
		}
		dw.emit(dw.otherData)
	}
	exceptLen = dw.dataLen + uint64(len(dw.pending)) - slotsLen
	if dw.stored < uint64(codes) {
		words := (codes + 63) / 64
		for _, w := range live[:words] {
			binary.LittleEndian.PutUint64(pad[:], w)
			dw.emit(pad[:])
		}
		before := uint32(0)
		for b := 0; b <= codes/liveBlock; b++ {
			binary.LittleEndian.PutUint32(pad[:], before)
			dw.emit(pad[:4])
			for _, w := range live[min(b*liveBlock/64, words):min((b+1)*liveBlock/64, words)] {
				before += uint32(bits.OnesCount64(w))
			}
		}
	}
	dw.flush(true)
	dw.w.write(dw.sums)

	return slotsLen, exceptLen
}

package part

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// A packed section holds byte strings of one length whose bytes, where they
// differ from string to string, come from at most 64 symbols: identifiers,
// hashes, base32 and hexadecimal keys. A differing byte takes the bits that
// the symbols seen at its position need instead of eight, with no entropy
// coding to undo on a read.
//
//	stride     uvarint, the length of every string
//	symbols    count byte, then the alphabet in code order
//	template   stride bytes, the first string
//	positions  per byte position its lowest code and the width in bits of
//	           its codes above that, zero where every string has one byte
//	codes      the codes of every string, low bits first
const (
	maxPackedBits = 6
	// minPackedStrings keeps the fixed cost of a section worth paying.
	minPackedStrings = 16
	// maxPackedSlack is how far the mean width of a symbol may exceed the
	// entropy of the symbols before the general codec is the better choice.
	maxPackedSlack = 0.4
)

// packScratch holds what packing reuses between sections.
type packScratch struct {
	differ           []byte
	lo, hi           []byte
	loWords, hiWords []uint64
	varPos           []int
	width            []uint8
	schedule         []packStep
	steps            []codeStep
	// table holds, for every varying position, the byte of each of its 64
	// possible codes.
	table []byte
}

// packStep reads, from one word of codes, count symbols of one width that
// fill adjacent bytes of a string starting at byte at; first numbers the
// first of them among the varying positions.
type packStep struct {
	first, count int
	width        uint8
	at           int
}

// codeStep packs, into one word, the codes of count varying positions from
// the first on, which take bits bits: when width is set, eight adjacent
// positions from byte at on whose base codes are the bytes of base, and
// otherwise any positions, one at a time.
type codeStep struct {
	first, count, at, bits int
	width                  uint8
	base                   uint64
}

func growBytes(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}

	return b[:n]
}

func growWords(b []uint64, n int) []uint64 {
	if cap(b) < n {
		return make([]uint64, n)
	}

	return b[:n]
}

// lessBytes returns, in each of the eight bytes of the result, 0x80 where
// the byte of x is below the byte of y as unsigned numbers and 0 elsewhere.
func lessBytes(x, y uint64) uint64 {
	const high = 0x8080808080808080
	// Bit 7 of each byte of low is set where the low seven bits of x are at
	// least those of y; setting them in x keeps borrows within each byte.
	low := (x | high) - (y &^ high)

	return (^x&y | ^(x^y)&^low) & high
}

// appendSymbols appends payload, strings of length stride laid end to end,
// as a packed section body and reports whether packing suits it.
func appendSymbols(dst, payload []byte, stride int, s *packScratch) ([]byte, bool) {
	if stride <= 0 || len(payload)%stride != 0 || len(payload)/stride < minPackedStrings {
		return dst, false
	}
	count := len(payload) / stride
	first := payload[:stride]
	s.differ = growBytes(s.differ, stride)
	differ := s.differ
	clear(differ)
	for at := stride; at < len(payload); at += stride {
		val := payload[at : at+stride]
		i := 0
		for ; i+8 <= stride; i += 8 {
			d := binary.LittleEndian.Uint64(differ[i:]) | (binary.LittleEndian.Uint64(val[i:]) ^ binary.LittleEndian.Uint64(first[i:]))
			binary.LittleEndian.PutUint64(differ[i:], d)
		}
		for ; i < stride; i++ {
			differ[i] |= val[i] ^ first[i]
		}
	}
	s.varPos = s.varPos[:0]
	for i, d := range differ {
		if d != 0 {
			s.varPos = append(s.varPos, i)
		}
	}
	varPos := s.varPos
	if len(varPos) == 0 {
		return dst, false
	}

	// The alphabet: the bytes of every position are counted in one pass over
	// the payload, four tables taking turns so that equal neighbours do not
	// wait on each other, and those of the positions that hold one byte are
	// taken back out.
	var hists [4][256]uint32
	i := 0
	for ; i+4 <= len(payload); i += 4 {
		b := payload[i : i+4 : i+4]
		hists[0][b[0]]++
		hists[1][b[1]]++
		hists[2][b[2]]++
		hists[3][b[3]]++
	}
	for ; i < len(payload); i++ {
		hists[0][payload[i]]++
	}
	var hist [256]uint32
	for c := range hist {
		hist[c] = hists[0][c] + hists[1][c] + hists[2][c] + hists[3][c]
	}
	for p, d := range differ {
		if d == 0 {
			hist[first[p]] -= uint32(count)
		}
	}
	// Each position's range of bytes, eight positions to a word.
	s.lo, s.hi = growBytes(s.lo, stride), growBytes(s.hi, stride)
	lo, hi := s.lo, s.hi
	copy(lo, first)
	copy(hi, first)
	words := stride / 8
	s.loWords, s.hiWords = growWords(s.loWords, words), growWords(s.hiWords, words)
	loW, hiW := s.loWords[:words], s.hiWords[:words]
	for w := range loW {
		loW[w] = binary.LittleEndian.Uint64(first[8*w:])
		hiW[w] = loW[w]
	}
	for at := 0; at < len(payload); at += stride {
		val := payload[at : at+stride : at+stride]
		for w := range loW {
			x := binary.LittleEndian.Uint64(val[8*w:])
			below := (lessBytes(x, loW[w]) >> 7) * 0xFF
			above := (lessBytes(hiW[w], x) >> 7) * 0xFF
			loW[w] = x&below | loW[w]&^below
			hiW[w] = x&above | hiW[w]&^above
		}
		for p := 8 * words; p < stride; p++ {
			c := val[p]
			lo[p], hi[p] = min(lo[p], c), max(hi[p], c)
		}
	}
	for w := range loW {
		binary.LittleEndian.PutUint64(lo[8*w:], loW[w])
		binary.LittleEndian.PutUint64(hi[8*w:], hiW[w])
	}
	var code [256]uint8
	var symbols [1 << maxPackedBits]byte
	n := 0
	entropy, total := 0.0, float64(count*len(varPos))
	for c, seen := range hist {
		if seen == 0 {
			continue
		}
		if n == len(symbols) {
			return dst, false
		}
		code[c], symbols[n] = uint8(n), byte(c)
		n++
		p := float64(seen) / total
		entropy -= p * math.Log2(p)
	}
	bitsPerString := 0
	for _, p := range varPos {
		bitsPerString += bits.Len8(code[hi[p]] - code[lo[p]])
	}
	if float64(bitsPerString)/float64(len(varPos))-entropy > maxPackedSlack {
		return dst, false
	}

	dst = binary.AppendUvarint(dst, uint64(stride))
	dst = append(dst, byte(n))
	dst = append(dst, symbols[:n]...)
	dst = append(dst, first...)
	for p := 0; p < stride; p++ {
		dst = append(dst, code[lo[p]], byte(bits.Len8(code[hi[p]]-code[lo[p]])))
	}
	// lo and differ are reused for each varying position's base code and
	// width, in the order of varPos.
	bases, widths := lo[:len(varPos)], differ[:len(varPos)]
	for k, p := range varPos {
		bases[k], widths[k] = code[lo[p]], byte(bits.Len8(code[hi[p]]-code[lo[p]]))
	}
	s.steps = packSchedule(s.steps[:0], varPos, bases, widths)
	need := (count*bitsPerString+7)/8 + 8
	start := len(dst)
	if cap(dst)-start < need {
		grown := make([]byte, start, start+need)
		copy(grown, dst)
		dst = grown
	}
	out := dst[start : start+need]
	clear(out)
	bitPos := 0
	for at := 0; at < len(payload); at += stride {
		val := payload[at : at+stride : at+stride]
		for _, step := range s.steps {
			var v uint64
			if step.width != 0 {
				b := val[step.at : step.at+8 : step.at+8]
				v = uint64(code[b[0]]) | uint64(code[b[1]])<<8 | uint64(code[b[2]])<<16 | uint64(code[b[3]])<<24 |
					uint64(code[b[4]])<<32 | uint64(code[b[5]])<<40 | uint64(code[b[6]])<<48 | uint64(code[b[7]])<<56
				v = squeeze(v-step.base, uint(step.width))
			} else {
				shift := uint(0)
				for k := step.first; k < step.first+step.count; k++ {
					v |= uint64(code[val[varPos[k]]]-bases[k]) << shift
					shift += uint(widths[k])
				}
			}
			word := out[bitPos>>3:]
			binary.LittleEndian.PutUint64(word, binary.LittleEndian.Uint64(word)|v<<(bitPos&7))
			bitPos += step.bits
		}
	}

	return dst[:start+(bitPos+7)/8], true
}

// packSchedule appends the steps that pack the codes of a string: eight
// adjacent varying positions of one width at a time where there are, and
// the others a few at a time.
func packSchedule(dst []codeStep, varPos []int, bases, widths []byte) []codeStep {
	for k := 0; k < len(varPos); {
		run := 1
		for run < 8 && k+run < len(varPos) && varPos[k+run] == varPos[k]+run && widths[k+run] == widths[k] {
			run++
		}
		if run == 8 {
			step := codeStep{first: k, count: 8, width: widths[k], at: varPos[k], bits: 8 * int(widths[k])}
			for j := range 8 {
				step.base |= uint64(bases[k+j]) << (8 * j)
			}
			dst = append(dst, step)
			k += 8

			continue
		}
		step := codeStep{first: k}
		for k < len(varPos) && step.bits+int(widths[k]) <= 57 && (k == step.first || !startsEight(varPos, widths, k)) {
			step.bits += int(widths[k])
			k++
		}
		step.count = k - step.first
		dst = append(dst, step)
	}

	return dst
}

// startsEight reports whether eight adjacent varying positions of one width
// begin at the k-th.
func startsEight(varPos []int, widths []byte, k int) bool {
	if k+8 > len(varPos) {
		return false
	}
	for j := 1; j < 8; j++ {
		if varPos[k+j] != varPos[k]+j || widths[k+j] != widths[k] {
			return false
		}
	}

	return true
}

// squeeze packs the eight bytes of v, each below 1<<width, into the low
// 8*width bits of the result, the lowest byte lowest.
func squeeze(v uint64, width uint) uint64 {
	v = v&0x00FF00FF00FF00FF | (v>>8&0x00FF00FF00FF00FF)<<width
	v = v&0x0000FFFF0000FFFF | (v>>16&0x0000FFFF0000FFFF)<<(2*width)

	return v&0x00000000FFFFFFFF | (v>>32)<<(4*width)
}

// readSymbols appends the strings of a packed section body, rawLen bytes in
// all.
func readSymbols(dst, src []byte, rawLen int, s *packScratch) ([]byte, error) {
	var r packedReader
	if err := r.open(src, rawLen, s); err != nil {
		return dst, err
	}
	start := len(dst)
	if cap(dst)-start < rawLen {
		grown := make([]byte, start, start+rawLen)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:start+rawLen]
	r.put(dst[start:], 0, r.count)

	return dst, nil
}

// packedReader writes the strings of a packed section body from where the
// body lies, any run of them at a time. The codes of string i begin lead bits
// into slot i of slot bits, which in a section body is the bits of a string.
type packedReader struct {
	stride, count, bitsPerString int
	slot, lead                   int
	template, codes              []byte
	schedule                     []packStep
	table                        []byte
}

// open readies r for a packed section body of rawLen bytes, whose tables it
// keeps in s.
func (r *packedReader) open(src []byte, rawLen int, s *packScratch) error {
	codes, err := r.readHead(src, s)
	if err != nil {
		return err
	}
	if r.stride > rawLen || rawLen%r.stride != 0 {
		return ErrCorrupt
	}
	count := rawLen / r.stride
	if need := (uint64(count)*uint64(r.bitsPerString) + 7) / 8; r.bitsPerString == 0 || uint64(len(codes)) < need {
		return ErrCorrupt
	}
	r.count, r.codes = count, codes

	return nil
}

// readHead readies r for strings laid out as the head of a packed section
// body at the start of src tells, keeping its tables in s, and returns the
// bytes after the head. It leaves the codes and their count to the caller.
func (r *packedReader) readHead(src []byte, s *packScratch) ([]byte, error) {
	stride64, used := binary.Uvarint(src)
	if used <= 0 || stride64 == 0 || stride64 > uint64(len(src)) || len(src)-used < 1 {
		return nil, ErrCorrupt
	}
	stride := int(stride64)
	n := int(src[used])
	src = src[used+1:]
	if n < 1 || n > 1<<maxPackedBits || len(src) < n+3*stride {
		return nil, ErrCorrupt
	}
	var symbols [2 << maxPackedBits]byte
	copy(symbols[:], src[:n])
	template, positions, codes := src[n:n+stride], src[n+stride:n+3*stride], src[n+3*stride:]
	s.varPos, s.width, s.table = s.varPos[:0], s.width[:0], s.table[:0]
	bitsPerString := 0
	for p := 0; p < stride; p++ {
		base, width := positions[2*p], positions[2*p+1]
		if width == 0 {
			continue
		}
		// A code is a base below 64 plus up to 6 bits, inside symbols.
		if width > maxPackedBits || base >= 1<<maxPackedBits {
			return nil, ErrCorrupt
		}
		s.varPos = append(s.varPos, p)
		s.width = append(s.width, width)
		s.table = append(s.table, symbols[base:int(base)+1<<maxPackedBits]...)
		bitsPerString += int(width)
	}
	varPos, widths := s.varPos, s.width
	// One word of codes yields at least 57 bits wherever it starts. A step
	// ends where the width changes or the bytes stop being adjacent, so
	// that the steps of a key's body all take the fast path below.
	s.schedule = s.schedule[:0]
	for k := 0; k < len(varPos); {
		step, used := packStep{first: k, width: widths[k], at: varPos[k]}, 0
		for k < len(varPos) && used+int(widths[k]) <= 57 &&
			widths[k] == step.width && varPos[k] == step.at+k-step.first {
			used += int(widths[k])
			k++
		}
		step.count = k - step.first
		s.schedule = append(s.schedule, step)
	}
	*r = packedReader{
		stride: stride, bitsPerString: bitsPerString, slot: bitsPerString,
		template: template, schedule: s.schedule, table: s.table,
	}

	return codes, nil
}

// put writes the strings from from to to, below count, one after another
// into out.
func (r *packedReader) put(out []byte, from, to int) {
	stride, codes, table := r.stride, r.codes, r.table
	var tail [16]byte
	// A slot is its lead bits and then the bits of a string, so the steps of
	// one string end where the next slot begins.
	lead := uint64(r.lead)
	bitPos := uint64(from) * uint64(r.slot)
	for i := 0; i < to-from; i++ {
		bitPos += lead
		val := out[i*stride : (i+1)*stride]
		copy(val, r.template)
		for _, step := range r.schedule {
			var w uint64
			if at := int(bitPos >> 3); at+8 <= len(codes) {
				w = binary.LittleEndian.Uint64(codes[at:]) >> (bitPos & 7)
			} else {
				// The last symbols come from a copy padded with zeros.
				clear(tail[:])
				copy(tail[:], codes[min(at, len(codes)):])
				w = binary.LittleEndian.Uint64(tail[:]) >> (bitPos & 7)
			}
			tab := table[step.first<<maxPackedBits : (step.first+step.count)<<maxPackedBits]
			run := val[step.at : step.at+step.count]
			width, mask := uint(step.width), uint64(1)<<step.width-1
			for j := range run {
				run[j] = tab[j<<maxPackedBits|int(w&mask)]
				w >>= width
			}
			bitPos += uint64(step.count) * uint64(width)
		}
	}
}

// packedChunk is the number of code bits compared at once: a word read at
// any byte holds that many whichever bit it starts at.
const packedChunk = 56

// findPacked appends to dst the numbers of the strings of a packed section
// body of rawLen bytes that equal want, comparing the codes as they are
// stored instead of unpacking the strings.
func findPacked(dst []uint32, src []byte, rawLen int, want []byte) ([]uint32, error) {
	stride64, used := binary.Uvarint(src)
	if used <= 0 || stride64 == 0 || stride64 > uint64(rawLen) || uint64(rawLen)%stride64 != 0 || len(src)-used < 1 {
		return dst, ErrCorrupt
	}
	stride, count := int(stride64), rawLen/int(stride64)
	n := int(src[used])
	src = src[used+1:]
	if n < 1 || n > 1<<maxPackedBits || len(src) < n+3*stride {
		return dst, ErrCorrupt
	}
	symbols, template, positions, codes := src[:n], src[n:n+stride], src[n+stride:n+3*stride], src[n+3*stride:]
	if len(want) != stride {
		return dst, nil
	}
	var codeOf [256]uint8
	for i := range codeOf {
		codeOf[i] = 0xFF
	}
	for c, b := range symbols {
		codeOf[b] = uint8(c)
	}
	// The codes of want, in chunks as they are compared.
	var few [8]uint64
	needle := few[:]
	if chunks := (maxPackedBits*stride + packedChunk - 1) / packedChunk; chunks > len(few) {
		needle = make([]uint64, chunks)
	}
	const chunkMask = 1<<packedChunk - 1
	bitsPerString := 0
	for p := 0; p < stride; p++ {
		base, width := positions[2*p], uint(positions[2*p+1])
		if width == 0 {
			// A position where every string has the byte of the first.
			if want[p] != template[p] {
				return dst, nil
			}

			continue
		}
		if width > maxPackedBits || base >= 1<<maxPackedBits {
			return dst, ErrCorrupt
		}
		c := codeOf[want[p]]
		if c == 0xFF || c < base || uint(c-base) >= 1<<width {
			return dst, nil
		}
		v := uint64(c - base)
		chunk, at := bitsPerString/packedChunk, uint(bitsPerString%packedChunk)
		needle[chunk] |= v << at & chunkMask
		if at+width > packedChunk {
			needle[chunk+1] |= v >> (packedChunk - at)
		}
		bitsPerString += int(width)
	}
	if need := (uint64(count)*uint64(bitsPerString) + 7) / 8; bitsPerString == 0 || uint64(len(codes)) < need {
		return dst, ErrCorrupt
	}
	chunks := (bitsPerString + packedChunk - 1) / packedChunk
	firstMask, lastMask := uint64(chunkMask), uint64(1)<<uint(bitsPerString-packedChunk*(chunks-1))-1
	if chunks == 1 {
		firstMask = lastMask
	}
	// A word read whole lies inside codes up to this bit.
	whole := 8*len(codes) - 64
	for i := 0; i < count; i++ {
		off := i * bitsPerString
		var w uint64
		if off <= whole {
			w = binary.LittleEndian.Uint64(codes[off>>3:]) >> uint(off&7)
		} else {
			w = packedBits(codes, off)
		}
		if w&firstMask != needle[0] {
			continue
		}
		equal := true
		for k := 1; k < chunks && equal; k++ {
			mask := uint64(chunkMask)
			if k == chunks-1 {
				mask = lastMask
			}
			equal = packedBits(codes, off+packedChunk*k)&mask == needle[k]
		}
		if equal {
			dst = append(dst, uint32(i))
		}
	}

	return dst, nil
}

// packedBits returns the bits of codes from bit off on, at least
// packedChunk of them where codes has that many left.
func packedBits(codes []byte, off int) uint64 {
	at := off >> 3
	if at+8 <= len(codes) {
		return binary.LittleEndian.Uint64(codes[at:]) >> uint(off&7)
	}
	var tail [8]byte
	if at < len(codes) {
		copy(tail[:], codes[at:])
	}

	return binary.LittleEndian.Uint64(tail[:]) >> uint(off&7)
}

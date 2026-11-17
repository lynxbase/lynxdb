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
	differ   []byte
	lo, hi   []byte
	varPos   []int
	width    []uint8
	schedule []packStep
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

func growBytes(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}

	return b[:n]
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

	// The alphabet and each position's range of bytes.
	var hist [256]uint32
	s.lo, s.hi = growBytes(s.lo, stride), growBytes(s.hi, stride)
	lo, hi := s.lo, s.hi
	copy(lo, first)
	copy(hi, first)
	for at := 0; at < len(payload); at += stride {
		val := payload[at : at+stride]
		for _, p := range varPos {
			c := val[p]
			hist[c]++
			lo[p], hi[p] = min(lo[p], c), max(hi[p], c)
		}
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
	need := (count*bitsPerString+7)/8 + 8
	start := len(dst)
	if cap(dst)-start < need {
		grown := make([]byte, start, start+need)
		copy(grown, dst)
		dst = grown
	}
	out := dst[start : start+need]
	var acc uint64
	var held uint
	n = 0
	for at := 0; at < len(payload); at += stride {
		val := payload[at : at+stride]
		for k, p := range varPos {
			acc |= uint64(code[val[p]]-bases[k]) << held
			if held += uint(widths[k]); held >= 32 {
				binary.LittleEndian.PutUint32(out[n:], uint32(acc))
				n += 4
				acc >>= 32
				held -= 32
			}
		}
	}
	binary.LittleEndian.PutUint64(out[n:], acc)

	return dst[:start+n+int(held+7)/8], true
}

// readSymbols appends the strings of a packed section body, rawLen bytes in
// all.
func readSymbols(dst, src []byte, rawLen int, s *packScratch) ([]byte, error) {
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
			return dst, ErrCorrupt
		}
		s.varPos = append(s.varPos, p)
		s.width = append(s.width, width)
		s.table = append(s.table, symbols[base:int(base)+1<<maxPackedBits]...)
		bitsPerString += int(width)
	}
	varPos, widths, table := s.varPos, s.width, s.table
	if need := (uint64(count)*uint64(bitsPerString) + 7) / 8; len(varPos) == 0 || uint64(len(codes)) < need {
		return dst, ErrCorrupt
	}
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

	start := len(dst)
	if cap(dst)-start < rawLen {
		grown := make([]byte, start, start+rawLen)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:start+rawLen]
	out := dst[start:]
	var tail [16]byte
	bitPos := uint64(0)
	for i := 0; i < count; i++ {
		val := out[i*stride : (i+1)*stride]
		copy(val, template)
		for _, step := range s.schedule {
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

	return dst, nil
}

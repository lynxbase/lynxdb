package part

import (
	"encoding/binary"
	"math/bits"
	"slices"

	"github.com/klauspost/compress/huff0"
)

// Integer section modes.
const (
	intFrames      = 0 // frame-of-reference bit packing
	intDeltaFrames = 1 // the same over consecutive differences
	intBytes8      = 2 // one byte per value above the minimum, then a blob
	intBytes16     = 3 // two bytes per value above the minimum, then a blob
	intConst       = 4 // one value shared by all
	intScaled      = 5 // a factor, a base, and a section of how many factors each value is above the base
	intPlanes      = 6 // distances from a center in byte planes, see planes.go
)

// minScaled is the number of values from which a common factor is worth
// its two numbers.
const minScaled = 16

const frameLen = 128

// encodeScratch holds the buffers reused while encoding one block.
type encodeScratch struct {
	tmp    []byte
	zbuf   []byte
	scaled []int64
	plane  []byte
	dists  []uint64
	huff   *huff0.Scratch
}

// decodeScratch holds the buffers reused while decoding one column.
type decodeScratch struct {
	zbuf    []byte
	ints    []int64
	codes8  []uint8
	codes16 []uint16
	timed   timedValues
	huff    *huff0.Scratch
	// valueOffsets holds the offsets of the values of a sparse timed chunk
	// before they are spread over its rows.
	valueOffsets []uint32
}

func zigzag(v int64) uint64   { return uint64(v<<1) ^ uint64(v>>63) }
func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

// framesSizes returns the encoded sizes of vals as frames and as frames of
// their consecutive differences.
func framesSizes(vals []int64) (plain, delta int) {
	prev := int64(0)
	for start := 0; start < len(vals); start += frameLen {
		frame := vals[start:min(start+frameLen, len(vals))]
		lo, hi := int64(1<<63-1), int64(-1<<63)
		dlo, dhi := lo, hi
		for _, v := range frame {
			lo, hi = min(lo, v), max(hi, v)
			d := v - prev
			prev = v
			dlo, dhi = min(dlo, d), max(dhi, d)
		}
		plain += 10 + 1 + (len(frame)*bits.Len64(uint64(hi)-uint64(lo))+7)/8
		delta += 10 + 1 + (len(frame)*bits.Len64(uint64(dhi)-uint64(dlo))+7)/8
	}

	return plain, delta
}

// appendInts appends the integer section for vals.
func appendInts(dst []byte, vals []int64, bc *blobCodec, e *encodeScratch) []byte {
	if len(vals) == 0 {
		return append(dst, intFrames)
	}
	lo, hi := vals[0], vals[0]
	for _, v := range vals[1:] {
		lo, hi = min(lo, v), max(hi, v)
	}
	span := uint64(hi) - uint64(lo)
	if span == 0 {
		return binary.AppendUvarint(append(dst, intConst), zigzag(lo))
	}
	if len(vals) >= minScaled {
		// Times in a coarser unit than they are held in, and sizes in whole
		// units, are multiples: the factor is taken out of every value.
		if factor := commonFactor(vals, lo); factor > 1 {
			e.scaled = e.scaled[:0]
			for _, v := range vals {
				e.scaled = append(e.scaled, int64((uint64(v)-uint64(lo))/factor))
			}
			dst = binary.AppendUvarint(append(dst, intScaled), factor)
			dst = binary.AppendUvarint(dst, zigzag(lo))

			return appendInts(dst, e.scaled, bc, e)
		}
	}
	plain, delta := framesSizes(vals)
	best := min(plain, delta)

	// Narrow ranges keep byte alignment so the general codec can exploit
	// skew and runs that bit packing would hide.
	if span < 1<<16 && len(vals) >= minCompressSize {
		width := 1
		if span >= 1<<8 {
			width = 2
		}
		e.tmp = e.tmp[:0]
		for _, v := range vals {
			off := uint64(v) - uint64(lo)
			e.tmp = append(e.tmp, byte(off))
			if width == 2 {
				e.tmp = append(e.tmp, byte(off>>8))
			}
		}
		mark := len(dst)
		dst = append(dst, byte(intBytes8+width-1))
		dst = binary.AppendUvarint(dst, zigzag(lo))
		dst = bc.appendBlob(dst, e.tmp, &e.zbuf)
		if len(dst)-mark <= best {
			return dst
		}
		dst = dst[:mark]
	}
	if delta < plain {
		dst = append(dst, intDeltaFrames)

		return appendFrames(dst, vals, true)
	}
	dst = append(dst, intFrames)

	return appendFrames(dst, vals, false)
}

// commonFactor returns the largest number that divides the distance of
// every value from lo, which is 1 for most columns after a few values.
func commonFactor(vals []int64, lo int64) uint64 {
	factor := uint64(0)
	for _, v := range vals {
		d := uint64(v) - uint64(lo)
		if factor != 0 && d%factor == 0 {
			continue
		}
		for a, b := factor, d; ; {
			if b == 0 {
				factor = a

				break
			}
			a, b = b, a%b
		}
		if factor == 1 {
			return 1
		}
	}

	return factor
}

func appendFrames(dst []byte, vals []int64, delta bool) []byte {
	prev := int64(0)
	for start := 0; start < len(vals); start += frameLen {
		frame := vals[start:min(start+frameLen, len(vals))]
		lo, hi := int64(1<<63-1), int64(-1<<63)
		p := prev
		for _, v := range frame {
			x := v
			if delta {
				x, p = v-p, v
			}
			lo, hi = min(lo, x), max(hi, x)
		}
		width := uint(bits.Len64(uint64(hi) - uint64(lo)))
		dst = binary.AppendUvarint(dst, zigzag(lo))
		dst = append(dst, byte(width))
		if width == 0 {
			prev = p

			continue
		}
		// Every value stores the word it ends in, which leaves the bits
		// past it zero, and moves to the next word when it fills one.
		at, size := len(dst), (len(frame)*int(width)+7)/8
		dst = slices.Grow(dst, size+8)[:at+size+8]
		w, acc, filled := at, uint64(0), uint(0)
		for _, v := range frame {
			x := v
			if delta {
				x, prev = v-prev, v
			}
			off := uint64(x) - uint64(lo)
			acc |= off << filled
			binary.LittleEndian.PutUint64(dst[w:], acc)
			// The bits of off past the word, none when it starts one.
			carry := off >> (64 - filled)
			filled += width
			full := -uint64(filled >> 6)
			w += int(full & 8)
			acc = acc&^full | carry&full
			filled &= 63
		}
		binary.LittleEndian.PutUint64(dst[w:], acc)
		dst = dst[:at+size]
	}

	return dst
}

// readInts decodes count integers of the section at the start of src into
// dst and returns the bytes after the section.
func readInts(dst []int64, src []byte, count int, d *decodeScratch) ([]int64, []byte, error) {
	if len(src) == 0 {
		return nil, nil, ErrCorrupt
	}
	if cap(dst) < count {
		dst = make([]int64, count)
	}
	dst = dst[:count]
	if src[0] != intScaled {
		return readSection(dst, src, d, 0, 1)
	}
	factor, n := binary.Uvarint(src[1:])
	if n <= 0 || factor < 2 {
		return nil, nil, ErrCorrupt
	}
	base, m := binary.Uvarint(src[1+n:])
	// A section of factors is not one of factors again.
	if src = src[1+n+max(m, 0):]; m <= 0 || len(src) == 0 || src[0] == intScaled {
		return nil, nil, ErrCorrupt
	}

	return readSection(dst, src, d, uint64(unzigzag(base)), factor)
}

// readSection fills dst with the integers of the section at the start of
// src, each times factor and above base, and returns the bytes after the
// section.
func readSection(dst []int64, src []byte, d *decodeScratch, base, factor uint64) ([]int64, []byte, error) {
	mode, count := src[0], len(dst)
	src = src[1:]
	switch mode {
	case intConst:
		at, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, nil, ErrCorrupt
		}
		v := int64(base + factor*uint64(unzigzag(at)))
		for i := range dst {
			dst[i] = v
		}

		return dst, src[n:], nil
	case intBytes8, intBytes16:
		at, n := binary.Uvarint(src)
		if n <= 0 {
			return nil, nil, ErrCorrupt
		}
		lo := base + factor*uint64(unzigzag(at))
		raw, rest, _, err := readBlob(src[n:], &d.zbuf)
		if err != nil {
			return nil, nil, err
		}
		if mode == intBytes8 {
			if len(raw) != count {
				return nil, nil, ErrCorrupt
			}
			for i, b := range raw {
				dst[i] = int64(lo + factor*uint64(b))
			}
		} else {
			if len(raw) != 2*count {
				return nil, nil, ErrCorrupt
			}
			for i := range dst {
				dst[i] = int64(lo + factor*uint64(binary.LittleEndian.Uint16(raw[2*i:])))
			}
		}

		return dst, rest, nil
	case intPlanes:
		return readPlanes(dst, src, d, base, factor)
	case intFrames, intDeltaFrames:
		prev := int64(0)
		for start := 0; start < count; start += frameLen {
			frame := dst[start:min(start+frameLen, count)]
			at, n := binary.Uvarint(src)
			if n <= 0 || len(src) <= n {
				return nil, nil, ErrCorrupt
			}
			lo := uint64(unzigzag(at))
			width := uint(src[n])
			src = src[n+1:]
			if width > 64 {
				return nil, nil, ErrCorrupt
			}
			need := (len(frame)*int(width) + 7) / 8
			if need > len(src) {
				return nil, nil, ErrCorrupt
			}
			if mode == intFrames {
				unpack(frame, src[:need], width, base+factor*lo, factor)
				src = src[need:]

				continue
			}
			unpack(frame, src[:need], width, lo, 1)
			src = src[need:]
			for i, x := range frame {
				prev += x
				frame[i] = int64(base + factor*uint64(prev))
			}
		}

		return dst, src, nil
	}

	return nil, nil, ErrCorrupt
}

// unpack reads len(dst) values of width bits from src and stores each times
// factor and above base.
func unpack(dst []int64, src []byte, width uint, base, factor uint64) {
	if width == 0 {
		for i := range dst {
			dst[i] = int64(base)
		}

		return
	}
	mask := ^uint64(0) >> (64 - width)
	i := 0
	// Two unaligned 8-byte loads cover any value while 16 bytes remain.
	for ; i < len(dst); i++ {
		bit := uint(i) * width
		pos := bit >> 3
		if pos+16 > uint(len(src)) {
			break
		}
		shift := bit & 7
		v := binary.LittleEndian.Uint64(src[pos:]) >> shift
		if shift+width > 64 {
			v |= binary.LittleEndian.Uint64(src[pos+8:]) << (64 - shift)
		}
		dst[i] = int64(base + factor*(v&mask))
	}
	if i == len(dst) {
		return
	}
	var tail [32]byte
	origin := (uint(i) * width) >> 3
	copy(tail[:], src[origin:])
	for ; i < len(dst); i++ {
		bit := uint(i)*width - origin*8
		pos, shift := bit>>3, bit&7
		v := binary.LittleEndian.Uint64(tail[pos:]) >> shift
		if shift+width > 64 {
			v |= binary.LittleEndian.Uint64(tail[pos+8:]) << (64 - shift)
		}
		dst[i] = int64(base + factor*(v&mask))
	}
}

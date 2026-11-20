package part

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"slices"

	"github.com/klauspost/compress/huff0"
)

// A section of byte planes holds integers as their zigzagged distances from
// a center, each split into bytes: the low bytes, noise, as they are, value
// by value, and every byte above them in a plane of its own, where most
// distances are short and a few long, in a Huffman block. Distances whose
// widths spread over many bits, as a delay that is seconds for most and
// hours for some, take the bits of their own width and few more, where
// frames take the widest.
//
//	center  zigzag uvarint
//	low     byte, the low bytes of every distance kept as they are
//	planes  byte, the bytes of the longest distance
//	low     the low bytes of every distance, the lowest first
//	plane   for each byte above the low ones, a byte of its form: planeRaw
//	        and a byte a value, planeSame and the byte of all, or
//	        planeHuff, the uvarint length and the Huffman block
const (
	planeRaw = iota
	planeSame
	planeHuff
)

const (
	// planeSample is the number of values the center is the median of.
	planeSample = 255
	// noiseBits is the entropy of a byte a value above which a plane is
	// kept as it is.
	noiseBits = 7.5
)

// appendPlanes appends vals as a section of byte planes.
func appendPlanes(dst []byte, vals []int64, e *encodeScratch) []byte {
	n := len(vals)
	sample := e.scaled[:0]
	for i := 0; i < n; i += max(n/planeSample, 1) {
		sample = append(sample, vals[i])
	}
	slices.Sort(sample)
	center := sample[len(sample)/2]
	e.scaled = sample
	e.dists = grow(e.dists, n)
	var wide uint64
	for i, v := range vals {
		e.dists[i] = zigzag(v - center)
		wide |= e.dists[i]
	}
	dists := e.dists[:n]
	planes := (bits.Len64(wide) + 7) / 8
	// The planes from the low end that are noise are kept as they are.
	low := 0
	for ; low < planes; low++ {
		var hist [256]uint32
		shift := uint(8 * low)
		for _, d := range dists {
			hist[byte(d>>shift)]++
		}
		if entropy(&hist, n) < noiseBits {
			break
		}
	}
	dst = append(dst, intPlanes)
	dst = binary.AppendUvarint(dst, zigzag(center))
	dst = append(dst, byte(low), byte(planes))
	if low > 0 {
		at := len(dst)
		dst = slices.Grow(dst, n*low+8)[:at+n*low+8]
		for i, d := range dists {
			binary.LittleEndian.PutUint64(dst[at+i*low:], d)
		}
		dst = dst[:at+n*low]
	}
	if e.huff == nil {
		e.huff = &huff0.Scratch{Reuse: huff0.ReusePolicyNone}
	}
	e.plane = grow(e.plane, n)
	for k := low; k < planes; k++ {
		shift := uint(8 * k)
		for i, d := range dists {
			e.plane[i] = byte(d >> shift)
		}
		out, _, err := huff0.Compress4X(e.plane, e.huff)
		switch {
		case errors.Is(err, huff0.ErrUseRLE):
			dst = append(dst, planeSame, e.plane[0])
		case err == nil && len(out) < n:
			dst = append(dst, planeHuff)
			dst = binary.AppendUvarint(dst, uint64(len(out)))
			dst = append(dst, out...)
		default:
			dst = append(dst, planeRaw)
			dst = append(dst, e.plane...)
		}
	}

	return dst
}

// entropy returns the bits a value that the n values counted in hist take.
func entropy(hist *[256]uint32, n int) float64 {
	bits := 0.0
	for _, c := range hist {
		if c != 0 {
			p := float64(c) / float64(n)
			bits -= p * math.Log2(p)
		}
	}

	return bits
}

// readPlanes fills dst with the integers of the section of byte planes at
// the start of src, past its mode, each times factor and above base, and
// returns the bytes after the section.
func readPlanes(dst []int64, src []byte, d *decodeScratch, base, factor uint64) ([]int64, []byte, error) {
	at, n := binary.Uvarint(src)
	if n <= 0 || len(src) < n+2 || src[n] > src[n+1] || src[n+1] > 8 {
		return nil, nil, ErrCorrupt
	}
	center, low, planes := unzigzag(at), int(src[n]), int(src[n+1])
	src = src[n+2:]
	count := len(dst)
	if len(src) < count*low {
		return nil, nil, ErrCorrupt
	}
	if low == 0 {
		clear(dst)
	} else {
		// A word read at every value holds its low bytes; the last few
		// values read from a copy padded with zeros.
		mask := ^uint64(0) >> (64 - 8*low)
		i := 0
		for ; i < count && i*low+8 <= len(src); i++ {
			dst[i] = int64(binary.LittleEndian.Uint64(src[i*low:]) & mask)
		}
		var tail [16]byte
		for ; i < count; i++ {
			copy(tail[:], src[i*low:count*low])
			dst[i] = int64(binary.LittleEndian.Uint64(tail[:]) & mask)
		}
		src = src[count*low:]
	}
	for k := low; k < planes; k++ {
		if len(src) == 0 {
			return nil, nil, ErrCorrupt
		}
		form, shift := src[0], uint(8*k)
		src = src[1:]
		var plane []byte
		switch form {
		case planeRaw:
			if len(src) < count {
				return nil, nil, ErrCorrupt
			}
			plane, src = src[:count], src[count:]
		case planeSame:
			if len(src) == 0 {
				return nil, nil, ErrCorrupt
			}
			same := int64(uint64(src[0]) << shift)
			for i := range dst {
				dst[i] |= same
			}
			src = src[1:]

			continue
		case planeHuff:
			size, m := binary.Uvarint(src)
			if m <= 0 || size > uint64(len(src)-m) {
				return nil, nil, ErrCorrupt
			}
			block := src[m : m+int(size)]
			src = src[m+int(size):]
			s, remain, err := huff0.ReadTable(block, d.huff)
			if err != nil {
				return nil, nil, ErrCorrupt
			}
			d.huff = s
			if plane, err = s.Decompress4X(remain, count); err != nil || len(plane) != count {
				return nil, nil, ErrCorrupt
			}
		default:
			return nil, nil, ErrCorrupt
		}
		for i, b := range plane[:count] {
			dst[i] |= int64(uint64(b) << shift)
		}
	}
	for i, x := range dst {
		dst[i] = int64(base + factor*uint64(center+unzigzag(uint64(x))))
	}

	return dst, src, nil
}

// skipPlanes returns the bytes after the section of byte planes of count
// values at the start of src, past its mode.
func skipPlanes(src []byte, count int) ([]byte, bool) {
	_, n := binary.Uvarint(src)
	if n <= 0 || len(src) < n+2 || src[n] > src[n+1] || src[n+1] > 8 {
		return nil, false
	}
	low, planes := int(src[n]), int(src[n+1])
	src = src[n+2:]
	if len(src) < count*low {
		return nil, false
	}
	src = src[count*low:]
	for k := low; k < planes; k++ {
		if len(src) == 0 {
			return nil, false
		}
		form := src[0]
		src = src[1:]
		switch form {
		case planeRaw:
			if len(src) < count {
				return nil, false
			}
			src = src[count:]
		case planeSame:
			if len(src) == 0 {
				return nil, false
			}
			src = src[1:]
		case planeHuff:
			size, m := binary.Uvarint(src)
			if m <= 0 || size > uint64(len(src)-m) {
				return nil, false
			}
			src = src[m+int(size):]
		default:
			return nil, false
		}
	}

	return src, true
}

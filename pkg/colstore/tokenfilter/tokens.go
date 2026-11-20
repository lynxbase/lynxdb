// Package tokenfilter splits text into case-folded search tokens and tests
// whether a block may contain one, using a filter of the hashes of its
// tokens.
package tokenfilter

import (
	"encoding/binary"
	"math/bits"
	"sync"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

// A token is a maximal run of letters and digits, hashed over its case-folded
// UTF-8 bytes in little-endian 8-byte words: h = seed, h = mum(h ^ word) for
// every full word, then once more for the zero-padded remainder, which cannot
// collide with content because token bytes are never zero.
const (
	hashSeed = 14695981039346656037
	hashMul  = 0x9e3779b97f4a7c15

	lowBits  = 0x0101010101010101
	highBits = 0x8080808080808080
)

// mum folds the 128-bit product of a and b into 64 bits.
func mum(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)

	return hi ^ lo
}

// ge sets the high bit of every byte of an ASCII word that is at least n;
// forcing the high bits on first keeps each borrow inside its own byte.
func ge(x, n uint64) uint64 {
	return ((x | highBits) - lowBits*n) & highBits
}

// asciiFold maps an ASCII byte to its lowercase form, or 0 for a separator.
var asciiFold = func() (t [utf8.RuneSelf]byte) {
	for c := byte('0'); c <= '9'; c++ {
		t[c] = c
	}
	for c := byte('a'); c <= 'z'; c++ {
		t[c] = c
		t[c-'a'+'A'] = c
	}

	return t
}()

// A fold entry packs a rune's case-folded UTF-8 bytes, first byte lowest,
// under their count in the top byte; zero marks a separator.
const (
	foldBytes = 0x00FFFFFF
	foldShift = 24
)

func packFold(r rune) uint32 {
	if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
		return 0
	}
	var buf [utf8.UTFMax]byte
	n := utf8.EncodeRune(buf[:], unicode.ToLower(r))
	if n > 3 {
		return 0
	}

	return uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(n)<<foldShift
}

// fold2 covers the runes encoded in two bytes: Latin supplements, Greek,
// Cyrillic, Armenian, Hebrew and Arabic.
var fold2 = func() (t [1 << 11]uint32) {
	for r := rune(utf8.RuneSelf); r < 1<<11; r++ {
		t[r] = packFold(r)
	}

	return t
}()

// fold3 covers the rest of the basic multilingual plane and is built on the
// first three-byte rune, since most log text never has one.
var (
	fold3     atomic.Pointer[[1 << 16]uint32]
	fold3Once sync.Once
)

func loadFold3() *[1 << 16]uint32 {
	if t := fold3.Load(); t != nil {
		return t
	}
	fold3Once.Do(func() {
		t := new([1 << 16]uint32)
		for r := rune(1 << 11); r < 1<<16; r++ {
			if r < 0xD800 || r > 0xDFFF {
				t[r] = packFold(r)
			}
		}
		fold3.Store(t)
	})

	return fold3.Load()
}

// loadTail returns the last rem bytes of text, rem between 1 and 7, as a
// zero-padded word.
func loadTail(text []byte, rem int) uint64 {
	if len(text) >= 8 {
		return binary.LittleEndian.Uint64(text[len(text)-8:]) >> (8 * uint(8-rem))
	}
	p := text[len(text)-rem:]
	switch {
	case rem >= 4:
		return uint64(binary.LittleEndian.Uint32(p)) | uint64(binary.LittleEndian.Uint32(p[rem-4:]))<<(8*uint(rem-4))
	case rem >= 2:
		return uint64(binary.LittleEndian.Uint16(p)) | uint64(binary.LittleEndian.Uint16(p[rem-2:]))<<(8*uint(rem-2))
	}

	return uint64(p[0])
}

// AppendHashes appends the hash of every token of text to dst.
func AppendHashes(dst []uint64, text []byte) []uint64 {
	// The token in progress: h has absorbed its full words, acc holds the
	// n%8 bytes after them.
	var (
		h   uint64 = hashSeed
		acc uint64
		n   uint
	)
	// A short value that is a single token, such as a level or a status
	// code, needs no scanning.
	if len(text) > 0 && len(text) < 8 {
		if w := loadTail(text, len(text)); w&highBits == 0 {
			folded := w | lowBits*0x20
			letter := ge(folded, 'a') &^ ge(folded, 'z'+1)
			if letter|ge(w, '0')&^ge(w, '9'+1) == highBits>>(8*uint(8-len(text))) {
				return append(dst, mum(hashSeed^(w|letter>>2), hashMul))
			}
		}
	}
	for i := 0; i < len(text); {
		var w uint64
		step := 8
		if i+8 <= len(text) {
			w = binary.LittleEndian.Uint64(text[i:])
		} else {
			step = len(text) - i
			w = loadTail(text, step)
		}
		if w&highBits != 0 {
			// Walk up to the last byte of this window that is outside ASCII.
			limit := i + (bits.Len64(w&highBits)+7)>>3
			dst, i, h, acc, n = appendRunes(dst, text, i, limit, h, acc, n)

			continue
		}
		i += step

		folded := w | lowBits*0x20
		letter := ge(folded, 'a') &^ ge(folded, 'z'+1)
		alnum := letter | ge(w, '0')&^ge(w, '9'+1)
		w |= letter >> 2

		if alnum == highBits {
			k := (n & 7) * 8
			h = mum(h^(acc|w<<k), hashMul)
			// A shift by 64 when k is 0 must give 0, hence the two steps.
			acc = (w >> 1) >> (63 - k)
			n += 8

			continue
		}
		if alnum&0x80 == 0 && n != 0 {
			dst = append(dst, mum(h^acc, hashMul))
			h, acc, n = hashSeed, 0, 0
		}
		// Walking the first and last bytes of each run through two masks
		// keeps one token's bookkeeping off the next one's path.
		starts := alnum &^ (alnum << 8)
		ends := alnum &^ (alnum >> 8)
		for starts != 0 {
			first := uint(bits.TrailingZeros64(starts)) &^ 7
			last := uint(bits.TrailingZeros64(ends)) &^ 7
			starts &= starts - 1
			ends &= ends - 1
			run := last - first + 8
			seg := (w >> first) & (^uint64(0) >> (64 - run))
			k := (n & 7) * 8
			acc |= seg << k
			if k+run >= 64 {
				h = mum(h^acc, hashMul)
				acc = (seg >> 1) >> (63 - k)
			}
			n += run >> 3
			if last == 56 {
				// The run reaches the end of the word and may go on.
				break
			}
			dst = append(dst, mum(h^acc, hashMul))
			h, acc, n = hashSeed, 0, 0
		}
	}
	if n != 0 {
		dst = append(dst, mum(h^acc, hashMul))
	}

	return dst
}

// appendRunes continues the token (h, acc, n) over text[i:], one byte or
// rune at a time, until the next eight bytes are ASCII again.
func appendRunes(dst []uint64, text []byte, i, limit int, h, acc uint64, n uint) ([]uint64, int, uint64, uint64, uint) {
	for {
		if i >= limit {
			if i+8 > len(text) {
				break
			}
			w := binary.LittleEndian.Uint64(text[i:]) & highBits
			if w == 0 {
				break
			}
			limit = i + (bits.Len64(w)+7)>>3
		}
		c := text[i]
		var entry uint32
		switch {
		case c < utf8.RuneSelf:
			i++
			if f := asciiFold[c]; f != 0 {
				entry = uint32(f) | 1<<foldShift
			}
		case c >= 0xC2 && c <= 0xDF && i+1 < len(text) && text[i+1]&0xC0 == 0x80:
			entry = fold2[uint32(c&0x1F)<<6|uint32(text[i+1]&0x3F)]
			i += 2
		case c >= 0xE0 && c <= 0xEF && i+2 < len(text) && text[i+1]&0xC0 == 0x80 && text[i+2]&0xC0 == 0x80:
			r := uint32(c&0x0F)<<12 | uint32(text[i+1]&0x3F)<<6 | uint32(text[i+2]&0x3F)
			// Overlong forms and surrogates are invalid: one separator byte.
			if r < 1<<11 || r >= 0xD800 && r <= 0xDFFF {
				i++

				break
			}
			entry = loadFold3()[r]
			i += 3
		default:
			r, size := utf8.DecodeRune(text[i:])
			i += size
			if r == utf8.RuneError || !unicode.IsLetter(r) && !unicode.IsDigit(r) {
				break
			}
			var buf [utf8.UTFMax]byte
			m := uint(utf8.EncodeRune(buf[:], unicode.ToLower(r)))
			seg := uint64(binary.LittleEndian.Uint32(buf[:]))
			k := (n & 7) * 8
			acc |= seg << k
			if k+8*m >= 64 {
				h = mum(h^acc, hashMul)
				acc = (seg >> 1) >> (63 - k)
			}
			n += m

			continue
		}
		if entry == 0 {
			if n != 0 {
				dst = append(dst, mum(h^acc, hashMul))
				h, acc, n = hashSeed, 0, 0
			}

			continue
		}
		seg, m := uint64(entry&foldBytes), uint(entry>>foldShift)
		k := (n & 7) * 8
		acc |= seg << k
		if k+8*m >= 64 {
			h = mum(h^acc, hashMul)
			acc = (seg >> 1) >> (63 - k)
		}
		n += m
	}

	return dst, i, h, acc, n
}

// Set collects distinct token hashes.
type Set struct {
	slots []uint64
	keys  []uint64
}

// Reset empties the set and keeps its memory.
func (s *Set) Reset() {
	clear(s.slots)
	s.keys = s.keys[:0]
}

// Keys returns the distinct hashes added since the last Reset.
func (s *Set) Keys() []uint64 { return s.keys }

// Add inserts every hash in hashes.
func (s *Set) Add(hashes []uint64) {
	for _, h := range hashes {
		// Zero marks an empty slot.
		if h == 0 {
			h = 1
		}
		if 2*(len(s.keys)+1) > len(s.slots) {
			s.grow()
		}
		mask := uint64(len(s.slots) - 1)
		for i := h & mask; ; i = (i + 1) & mask {
			if s.slots[i] == h {
				break
			}
			if s.slots[i] == 0 {
				s.slots[i] = h
				s.keys = append(s.keys, h)

				break
			}
		}
	}
}

func (s *Set) grow() {
	size := max(1024, 2*len(s.slots))
	s.slots = make([]uint64, size)
	mask := uint64(size - 1)
	for _, h := range s.keys {
		i := h & mask
		for s.slots[i] != 0 {
			i = (i + 1) & mask
		}
		s.slots[i] = h
	}
}

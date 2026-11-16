// Package tokenfilter splits text into case-folded search tokens and tests
// whether a block may contain one, using a split-block Bloom filter.
package tokenfilter

import (
	"unicode"
	"unicode/utf8"
)

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

// class maps an ASCII byte to its lowercase form, or 0 when it separates
// tokens. Bytes with the high bit set are handled as runes.
var class = func() (t [128]byte) {
	for c := byte('0'); c <= '9'; c++ {
		t[c] = c
	}
	for c := byte('a'); c <= 'z'; c++ {
		t[c] = c
		t[c-'a'+'A'] = c
	}

	return t
}()

func mix(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33

	return h
}

// AppendHashes appends the hash of every token of text to dst. A token is a
// maximal run of letters and digits; hashing folds case.
func AppendHashes(dst []uint64, text []byte) []uint64 {
	h := uint64(fnvOffset)
	inToken := false
	for i := 0; i < len(text); {
		c := text[i]
		if c < utf8.RuneSelf {
			i++
			if l := class[c]; l != 0 {
				h = (h ^ uint64(l)) * fnvPrime
				inToken = true

				continue
			}
		} else {
			r, size := utf8.DecodeRune(text[i:])
			i += size
			if r != utf8.RuneError && (unicode.IsLetter(r) || unicode.IsDigit(r)) {
				var buf [utf8.UTFMax]byte
				for _, b := range buf[:utf8.EncodeRune(buf[:], unicode.ToLower(r))] {
					h = (h ^ uint64(b)) * fnvPrime
				}
				inToken = true

				continue
			}
		}
		if inToken {
			dst = append(dst, mix(h))
			h, inToken = fnvOffset, false
		}
	}
	if inToken {
		dst = append(dst, mix(h))
	}

	return dst
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

package tokenfilter

import "unicode/utf8"

// ByteSet is a set of bytes.
type ByteSet [4]uint64

// Add puts b in the set.
func (s *ByteSet) Add(b byte) { s[b>>6] |= 1 << (b & 63) }

// Has reports whether b is in the set.
func (s *ByteSet) Has(b byte) bool { return s[b>>6]>>(b&63)&1 != 0 }

// separators holds the bytes that may end or begin something other than a
// token rune: the ASCII separators, and every byte of a longer rune, which
// may be of either sort.
var separators = func() (s ByteSet) {
	for b := 0; b < 256; b++ {
		if b >= utf8.RuneSelf || asciiFold[b] == 0 {
			s.Add(byte(b))
		}
	}

	return s
}()

func (s *ByteSet) meets(o *ByteSet) bool {
	return s[0]&o[0]|s[1]&o[1]|s[2]&o[2]|s[3]&o[3] != 0
}

// Within reports whether some text of len(sets) bytes whose byte at every
// position is one of sets at that position may hold the pattern. False
// tells that no such text does; true tells nothing, and is the answer for
// what it does not look into: a regular expression without a string that
// every match holds, and a needle whose letter cases are not those of
// ASCII.
func (p Pattern) Within(sets []ByteSet) bool {
	n := len(p.needle)
	switch {
	case n == 0:
		return p.re != nil
	case p.fold && p.foldMask == nil:
		return true
	}
	// A regular expression needs its string anywhere; what else it needs
	// is not looked into.
	bounded := p.re == nil
	for at := 0; at+n <= len(sets); at++ {
		fits := true
		for i := 0; i < n && fits; i++ {
			c := p.needle[i]
			fits = sets[at+i].Has(c) || (p.foldMask != nil && p.foldMask[i] != 0 && sets[at+i].Has(c^0x20))
		}
		if !fits {
			continue
		}
		if bounded && p.wordStart && p.startInToken && at > 0 && !sets[at-1].meets(&separators) {
			continue
		}
		if bounded && p.wordEnd && p.endInToken && at+n < len(sets) && !sets[at+n].meets(&separators) {
			continue
		}

		return true
	}

	return false
}

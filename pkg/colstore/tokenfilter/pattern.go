package tokenfilter

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"sort"
	"sync"
	"unicode"
	"unicode/utf8"
)

// Pattern is a byte string searched in text, with its ends optionally
// required to fall on token boundaries, and with or without regard to
// letter case.
type Pattern struct {
	needle []byte
	// startInToken and endInToken say whether the needle's first and last
	// runes are token runes; only then does a boundary need a look outside.
	startInToken, endInToken bool
	wordStart, wordEnd       bool
	// fold marks a pattern that ignores case; its needle is lowercase, and
	// foldMask has 0x20 under each ASCII letter of an all-ASCII needle and
	// is nil for any other.
	fold     bool
	foldMask []byte
}

func newPattern(needle string, wordStart, wordEnd bool) Pattern {
	p := Pattern{needle: []byte(needle), wordStart: wordStart, wordEnd: wordEnd}
	p.startInToken = tokenRuneAt(p.needle, 0)
	p.endInToken = tokenRuneBefore(p.needle, len(p.needle))

	return p
}

// Word matches needle where no letter or digit adjoins it: a token, or a
// phrase of tokens with the separators between them.
func Word(needle string) Pattern { return newPattern(needle, true, true) }

// Prefix matches needle where a token starts with it.
func Prefix(needle string) Pattern { return newPattern(needle, true, false) }

// Substring matches needle anywhere.
func Substring(needle string) Pattern { return newPattern(needle, false, false) }

// IgnoreCase returns the pattern that matches without regard to letter
// case: an ASCII letter of the needle matches both of its cases, any other
// letter the runes with the same lowercase form.
func (p Pattern) IgnoreCase() Pattern {
	p.fold = true
	p.needle = appendLower(nil, p.needle)
	ascii := true
	for _, c := range p.needle {
		ascii = ascii && c < utf8.RuneSelf
	}
	if ascii {
		p.foldMask = make([]byte, len(p.needle))
		for i, c := range p.needle {
			if c >= 'a' && c <= 'z' {
				p.foldMask[i] = 0x20
			}
		}
	}

	return p
}

// appendLower appends text with every rune in lowercase; bytes that are not
// valid UTF-8 are kept.
func appendLower(dst, text []byte) []byte {
	for i := 0; i < len(text); {
		c := text[i]
		if c < utf8.RuneSelf {
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			dst = append(dst, c)
			i++

			continue
		}
		r, size := utf8.DecodeRune(text[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, c)
		} else {
			dst = utf8.AppendRune(dst, unicode.ToLower(r))
		}
		i += size
	}

	return dst
}

// index returns the first position at or after from where the needle occurs
// in text, or -1.
func (p Pattern) index(text []byte, from int) int {
	var i int
	if p.foldMask != nil {
		i = indexFold(text[from:], p.needle, p.foldMask)
	} else {
		i = bytes.Index(text[from:], p.needle)
	}
	if i < 0 {
		return -1
	}

	return from + i
}

// indexFold returns the first position where needle, lowercase ASCII with
// mask marking its letters, occurs in text in any letter case, or -1. It
// tests the needle's first and last bytes at eight positions per step.
func indexFold(text, needle, mask []byte) int {
	n := len(needle)
	if n == 0 || n > len(text) {
		return -1
	}
	first, last := lowBits*uint64(needle[0]), lowBits*uint64(needle[n-1])
	firstFold, lastFold := lowBits*uint64(mask[0]), lowBits*uint64(mask[n-1])
	i := 0
	for ; i+n+7 <= len(text); i += 8 {
		a := binary.LittleEndian.Uint64(text[i:]) | firstFold
		b := binary.LittleEndian.Uint64(text[i+n-1:]) | lastFold
		differ := (a ^ first) | (b ^ last)
		// A zero byte marks a candidate; a borrow can mark the byte above
		// one as well, which the comparison then rejects.
		for found := (differ - lowBits) &^ differ & highBits; found != 0; found &= found - 1 {
			at := i + bits.TrailingZeros64(found)>>3
			if equalFold(text[at:at+n], needle, mask) {
				return at
			}
		}
	}
	for ; i+n <= len(text); i++ {
		if text[i]|mask[0] == needle[0] && equalFold(text[i:i+n], needle, mask) {
			return i
		}
	}

	return -1
}

// equalFold reports whether text equals needle once the bits under mask are
// set, which lowers ASCII capitals at the needle's letters and nothing else.
func equalFold(text, needle, mask []byte) bool {
	i := 0
	for ; i+8 <= len(needle); i += 8 {
		if binary.LittleEndian.Uint64(text[i:])|binary.LittleEndian.Uint64(mask[i:]) != binary.LittleEndian.Uint64(needle[i:]) {
			return false
		}
	}
	for ; i < len(needle); i++ {
		if text[i]|mask[i] != needle[i] {
			return false
		}
	}

	return true
}

// lowered holds the buffers that matching a needle with letters outside
// ASCII lowers text into.
var lowered = sync.Pool{New: func() any { return new([]byte) }}

func tokenRune(r rune) bool {
	return r != utf8.RuneError && (unicode.IsLetter(r) || unicode.IsDigit(r))
}

func tokenRuneAt(text []byte, i int) bool {
	if i >= len(text) {
		return false
	}
	if c := text[i]; c < utf8.RuneSelf {
		return asciiFold[c] != 0
	}
	r, _ := utf8.DecodeRune(text[i:])

	return tokenRune(r)
}

func tokenRuneBefore(text []byte, i int) bool {
	if i <= 0 {
		return false
	}
	if c := text[i-1]; c < utf8.RuneSelf {
		return asciiFold[c] != 0
	}
	r, _ := utf8.DecodeLastRune(text[:i])

	return tokenRune(r)
}

// bounded reports whether the needle found at text[at:] meets the pattern's
// boundary rules.
func (p Pattern) bounded(text []byte, at int) bool {
	if p.wordStart && p.startInToken && tokenRuneBefore(text, at) {
		return false
	}

	return !p.wordEnd || !p.endInToken || !tokenRuneAt(text, at+len(p.needle))
}

// Match reports whether text holds the pattern.
func (p Pattern) Match(text []byte) bool {
	if len(p.needle) == 0 {
		return false
	}
	if p.fold && p.foldMask == nil {
		// Lowering keeps token runes token runes, so boundaries hold in
		// the lowered text as they do in the original.
		buf := lowered.Get().(*[]byte)
		defer lowered.Put(buf)
		*buf = appendLower((*buf)[:0], text)
		text = *buf
	}
	for pos := 0; ; pos++ {
		if pos = p.index(text, pos); pos < 0 {
			return false
		}
		if p.bounded(text, pos) {
			return true
		}
	}
}

// AppendMatches appends to dst, in ascending order, every i for which the
// value data[offsets[i]:offsets[i+1]] holds the pattern. It searches the
// values as one run of bytes, so its cost follows their total size and not
// their number.
func (p Pattern) AppendMatches(dst, offsets []uint32, data []byte) []uint32 {
	n := len(offsets) - 1
	if n <= 0 || len(p.needle) == 0 {
		return dst
	}
	if p.fold && p.foldMask == nil {
		for i := 0; i < n; i++ {
			if p.Match(data[offsets[i]:offsets[i+1]]) {
				dst = append(dst, uint32(i))
			}
		}

		return dst
	}
	end := int(offsets[n])
	for pos, entry := int(offsets[0]), 0; entry < n; {
		at := p.index(data[:end], pos)
		if at < 0 {
			break
		}
		entry += sort.Search(n-entry-1, func(k int) bool { return int(offsets[entry+k+1]) > at })
		lo, hi := int(offsets[entry]), int(offsets[entry+1])
		// A needle that runs past the end of its value is no match.
		if at+len(p.needle) <= hi && p.bounded(data[lo:hi], at-lo) {
			dst = append(dst, uint32(entry))
			pos = hi
			entry++

			continue
		}
		pos = at + 1
	}

	return dst
}

// Tokens appends the hashes of the tokens that every text holding the
// pattern has, which a block's filter can rule out.
func (p Pattern) Tokens(dst []uint64) []uint64 {
	start := len(dst)
	dst = AppendHashes(dst, p.needle)
	// A token at an end of the needle may be a piece of a longer one.
	if p.endInToken && !p.wordEnd && len(dst) > start {
		dst = dst[:len(dst)-1]
	}
	if p.startInToken && !p.wordStart && len(dst) > start {
		dst = append(dst[:start], dst[start+1:]...)
	}

	return dst
}

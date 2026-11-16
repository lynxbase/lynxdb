package tokenfilter

import (
	"bytes"
	"sort"
	"unicode"
	"unicode/utf8"
)

// Pattern is a byte string searched in text, case-sensitively, with its ends
// optionally required to fall on token boundaries.
type Pattern struct {
	needle []byte
	// startInToken and endInToken say whether the needle's first and last
	// runes are token runes; only then does a boundary need a look outside.
	startInToken, endInToken bool
	wordStart, wordEnd       bool
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
	for pos := 0; ; {
		i := bytes.Index(text[pos:], p.needle)
		if i < 0 {
			return false
		}
		if p.bounded(text, pos+i) {
			return true
		}
		pos += i + 1
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
	end := int(offsets[n])
	for pos, entry := int(offsets[0]), 0; entry < n; {
		i := bytes.Index(data[pos:end], p.needle)
		if i < 0 {
			break
		}
		at := pos + i
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

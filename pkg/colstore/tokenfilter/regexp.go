package tokenfilter

import (
	"regexp"
	"regexp/syntax"
	"sort"
	"unicode/utf8"
)

// Regexp matches the regular expression expr, in RE2 syntax, anywhere in
// text. A string that every match must hold, when the expression has one,
// rules out texts and blocks before the expression is run.
func Regexp(expr string) (Pattern, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return Pattern{}, err
	}
	tree, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		return Pattern{}, err
	}
	needle, fold := requiredLiteral(tree)
	p := newPattern(needle, false, false)
	if fold {
		p = p.IgnoreCase()
	}
	p.re = re

	return p, nil
}

type literalRune struct {
	r    rune
	fold bool
}

// requiredLiteral returns the longest string that every match of re holds,
// and whether it is to be found without regard to letter case.
func requiredLiteral(re *syntax.Regexp) (needle string, fold bool) {
	var runs [][]literalRune
	requiredRuns(re, &runs)
	for _, run := range runs {
		anyFold := false
		for _, l := range run {
			anyFold = anyFold || l.fold
		}
		for start := 0; start < len(run); {
			end := start
			for end < len(run) && (!anyFold || !breaksFold(run[end])) {
				end++
			}
			var piece []byte
			for _, l := range run[start:end] {
				piece = utf8.AppendRune(piece, l.r)
			}
			if len(piece) > len(needle) {
				needle, fold = string(piece), anyFold
			}
			start = end + 1
		}
	}

	return needle, fold
}

// breaksFold reports whether a rune cannot be part of a needle matched
// without regard to case by setting a bit of ASCII letters: a rune outside
// ASCII, or one of the two letters whose other forms include one outside
// it, the Kelvin sign for k and the long s for s.
func breaksFold(l literalRune) bool {
	if l.r >= utf8.RuneSelf {
		return true
	}

	return l.fold && (l.r|0x20 == 'k' || l.r|0x20 == 's')
}

// requiredRuns appends the runs of literal runes that every match of re
// holds.
func requiredRuns(re *syntax.Regexp, runs *[][]literalRune) {
	literal := func(re *syntax.Regexp, run []literalRune) []literalRune {
		for _, r := range re.Rune {
			run = append(run, literalRune{r, re.Flags&syntax.FoldCase != 0})
		}

		return run
	}
	switch re.Op {
	case syntax.OpLiteral:
		*runs = append(*runs, literal(re, nil))
	case syntax.OpCapture, syntax.OpPlus:
		requiredRuns(re.Sub[0], runs)
	case syntax.OpRepeat:
		if re.Min >= 1 {
			requiredRuns(re.Sub[0], runs)
		}
	case syntax.OpConcat:
		var run []literalRune
		for _, sub := range re.Sub {
			if sub.Op == syntax.OpLiteral {
				run = literal(sub, run)

				continue
			}
			if run != nil {
				*runs = append(*runs, run)
				run = nil
			}
			requiredRuns(sub, runs)
		}
		if run != nil {
			*runs = append(*runs, run)
		}
	}
}

func (p Pattern) appendRegexpMatches(dst, offsets []uint32, data []byte) []uint32 {
	n := len(offsets) - 1
	if len(p.needle) == 0 {
		for i := 0; i < n; i++ {
			if p.re.Match(data[offsets[i]:offsets[i+1]]) {
				dst = append(dst, uint32(i))
			}
		}

		return dst
	}
	if n <= 0 {
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
		// A needle that runs past the end of its value is not in it.
		if at+len(p.needle) > hi {
			pos = at + 1

			continue
		}
		if p.re.Match(data[lo:hi]) {
			dst = append(dst, uint32(entry))
		}
		pos = hi
		entry++
	}

	return dst
}

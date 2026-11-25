package part

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// mayHold reports whether a string of d may hold the pattern, as a search
// learns it: from the layout of the packed strings, and from each of the
// strings kept as they are, which it tells from the others by their codes.
func mayHold(t *testing.T, d *Dictionary, pat *tokenfilter.Pattern) bool {
	t.Helper()
	if d.PackedMayHold(pat) {
		return true
	}
	if !d.Odd() {
		return false
	}
	codes := make([]uint32, d.Len())
	for i := range codes {
		codes[i] = uint32(i)
	}
	odd, err := d.AppendOdd(nil, codes)
	if err != nil {
		t.Fatal(err)
	}
	if len(odd) != d.exceptions {
		t.Fatalf("%d codes have a string kept as it is, the dictionary keeps %d", len(odd), d.exceptions)
	}
	var val []byte
	for _, code := range odd {
		if val, err = d.Value(val[:0], code); err != nil {
			t.Fatal(err)
		}
		if len(val) == d.reader.stride {
			t.Fatalf("code %d is told to be kept as it is and has the length of the packed strings", code)
		}
		if pat.Match(val) {
			return true
		}
	}

	return false
}

// TestLayoutOfADictionaryRulesPatternsOut asks dictionaries of identifiers,
// with and without strings of other lengths, whether a string of theirs may
// hold a pattern: never no for a pattern that one holds, and no for the
// words that the layout of an identifier cannot spell.
func TestLayoutOfADictionaryRulesPatternsOut(t *testing.T) {
	regexp := func(expr string) tokenfilter.Pattern {
		p, err := tokenfilter.Regexp(expr)
		if err != nil {
			t.Fatal(err)
		}

		return p
	}
	for _, odd := range []int{0, 300} {
		p := plannedPart(t, accountDocs(30000, 5000, odd, 7), 4096, Options{})
		id, _ := p.PathID("did")
		d, ok := p.Dictionary(id)
		if !ok || (odd != 0) != (d.exceptions != 0) {
			t.Fatalf("one string in %d of another length: a dictionary %v that keeps %d as they are", odd, ok, d.exceptions)
		}
		var values [][]byte
		var val []byte
		for code := 0; code < d.Len(); code++ {
			var err error
			if val, err = d.Value(val[:0], uint32(code)); err != nil {
				t.Fatal(err)
			}
			values = append(values, append([]byte(nil), val...))
		}
		some := string(values[len(values)/2])
		token := some[len("did:plc:"):]
		patterns := map[string]tokenfilter.Pattern{
			"a word no identifier spells":    tokenfilter.Word("earthquake"),
			"a word with a byte none has":    tokenfilter.Word("did_plc"),
			"a constant word":                tokenfilter.Word("plc"),
			"the word of one identifier":     tokenfilter.Word(token),
			"the start of one":               tokenfilter.Prefix(token[:9]),
			"a piece of one":                 tokenfilter.Substring(token[5:14]),
			"capitals":                       tokenfilter.Word("PLC"),
			"capitals, whatever the case":    tokenfilter.Word("PLC").IgnoreCase(),
			"a word of the odd strings":      tokenfilter.Word("example"),
			"a word of no odd string":        tokenfilter.Word("examples"),
			"the start of the odd strings":   tokenfilter.Prefix("host1"),
			"an expression with a string":    regexp("plc:[a-z2-7]{24}"),
			"an expression none matches":     regexp("PLC:[a-z2-7]{24}"),
			"an expression without a string": regexp("[0-9]{40}"),
		}
		ruledOut := 0
		for name, pat := range patterns {
			held := false
			for _, v := range values {
				held = held || pat.Match(v)
			}
			may := mayHold(t, d, &pat)
			if held && !may {
				t.Fatalf("one in %d odd, %s: a string holds the pattern that the dictionary rules out", odd, name)
			}
			if !may {
				ruledOut++
			}
		}
		for _, name := range []string{"a word no identifier spells", "a word with a byte none has", "capitals", "a word of no odd string", "an expression none matches"} {
			pat := patterns[name]
			if mayHold(t, d, &pat) {
				t.Fatalf("one in %d odd, %s: the dictionary does not rule the pattern out", odd, name)
			}
		}
		if odd == 0 {
			pat := patterns["a word of the odd strings"]
			if mayHold(t, d, &pat) {
				t.Fatal("a dictionary of identifiers alone does not rule out the word of other strings")
			}
		}
		if ruledOut == len(patterns) {
			t.Fatal("every pattern was ruled out")
		}
	}
}

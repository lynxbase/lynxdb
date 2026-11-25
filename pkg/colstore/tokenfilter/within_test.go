package tokenfilter

import (
	"math/rand/v2"
	"testing"
)

// setsOf returns, for a text of the length of the shapes, the bytes that
// each shape allows at its position: every byte of the string at that
// position.
func setsOf(shapes ...string) []ByteSet {
	sets := make([]ByteSet, len(shapes))
	for i, allowed := range shapes {
		for _, b := range []byte(allowed) {
			sets[i].Add(b)
		}
	}

	return sets
}

// identifier is the layout of an identifier: a constant head, then letters
// and digits.
func identifier(head string, varying int) []ByteSet {
	var shapes []string
	for _, b := range []byte(head) {
		shapes = append(shapes, string(b))
	}
	for i := 0; i < varying; i++ {
		shapes = append(shapes, "abcdefghijklmnopqrstuvwxyz234567")
	}

	return setsOf(shapes...)
}

func TestWithinRulesOutWhatNoTextHolds(t *testing.T) {
	regexp := func(expr string) Pattern {
		p, err := Regexp(expr)
		if err != nil {
			t.Fatal(err)
		}

		return p
	}
	account := identifier("did:plc:", 24)
	for _, c := range []struct {
		name string
		p    Pattern
		sets []ByteSet
		want bool
	}{
		{"a word shorter than the token it would be", Word("earthquake"), account, false},
		{"a word that is a constant token", Word("plc"), account, true},
		{"a word as long as the varying token", Word("abcdefghijklmnopqrstuvwx"), account, true},
		{"a word with a letter that no position has", Word("abcdefghijklmnopqrstuvw9"), account, false},
		{"the start of the varying token", Prefix("earth"), account, true},
		{"the start of a token, after a separator", Prefix("lc"), account, true},
		{"the start of a token, after a letter", Prefix("zz"), setsOf("a", ":", "c", "z", "z"), false},
		{"a piece of the varying token", Substring("quake"), account, true},
		{"a piece that needs a digit the layout lacks", Substring("qu4ke"), account, true},
		{"a piece that needs a byte the layout lacks", Substring("qu9ke"), account, false},
		{"a piece across the constant and the varying bytes", Substring("plc:ea"), account, true},
		{"a piece longer than the text", Substring("did:plc:abcdefghijklmnopqrstuvwxy"), account, false},
		{"capitals of a pattern that ignores case", Word("PLC").IgnoreCase(), account, true},
		{"capitals of a pattern that does not", Word("PLC"), account, false},
		{"lowercase against a layout of capitals", Word("abc").IgnoreCase(), setsOf("A", "B", "C"), true},
		{"a word that ends the text", Word("xyz"), setsOf("-", "x", "y", "z"), true},
		{"a word that a letter follows", Word("xy"), setsOf("-", "x", "y", "z"), false},
		{"a word that a letter or a dash follows", Word("xy"), setsOf("-", "x", "y", "z-"), true},
		{"a word after a byte of a longer rune", Word("xy"), setsOf("\xc3\xa9", "x", "y"), true},
		{"an empty needle", Substring(""), account, false},
		{"an expression with a string that fits", regexp("plc:[a-z]+"), account, true},
		{"an expression with a string that does not", regexp("PLC:[a-z]+"), account, false},
		{"an expression without a string", regexp("[0-9]+"), account, true},
		{"an expression that ignores case", regexp("(?i)PLC:"), account, true},
	} {
		if got := c.p.Within(c.sets); got != c.want {
			t.Errorf("%s: within is %v, want %v", c.name, got, c.want)
		}
	}
}

// TestWithinAdmitsEveryTextThatMatches draws texts from random layouts and
// requires that a pattern which one of them holds is not ruled out.
func TestWithinAdmitsEveryTextThatMatches(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 14))
	alphabet := []byte("abAB1:- .\xc3\xa9")
	text := func(n int) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = alphabet[rng.IntN(len(alphabet))]
		}

		return out
	}
	admitted, ruledOut := 0, 0
	for round := 0; round < 4000; round++ {
		sets := make([]ByteSet, 1+rng.IntN(9))
		for i := range sets {
			for k := 0; k <= rng.IntN(3); k++ {
				sets[i].Add(alphabet[rng.IntN(len(alphabet))])
			}
		}
		needle := string(text(1 + rng.IntN(3)))
		p := []Pattern{Word(needle), Prefix(needle), Substring(needle)}[rng.IntN(3)]
		if rng.IntN(3) == 0 {
			p = p.IgnoreCase()
		}
		held := false
		for try := 0; try < 200 && !held; try++ {
			drawn := make([]byte, len(sets))
			for i := range drawn {
				for {
					if b := alphabet[rng.IntN(len(alphabet))]; sets[i].Has(b) {
						drawn[i] = b

						break
					}
				}
			}
			held = p.Match(drawn)
		}
		within := p.Within(sets)
		if held && !within {
			t.Fatalf("round %d: a text of the layout holds the pattern %q that the layout rules out", round, needle)
		}
		if within {
			admitted++
		} else {
			ruledOut++
		}
	}
	if admitted == 0 || ruledOut == 0 {
		t.Fatalf("%d patterns admitted and %d ruled out: the rounds test one answer only", admitted, ruledOut)
	}
}

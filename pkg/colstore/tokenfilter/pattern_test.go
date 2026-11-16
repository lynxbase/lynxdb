package tokenfilter

import (
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestPatternBoundaries(t *testing.T) {
	cases := []struct {
		p    Pattern
		text string
		want bool
	}{
		{Word("love"), "I love it", true},
		{Word("love"), "love", true},
		{Word("love"), "glove", false},
		{Word("love"), "lovely", false},
		{Word("love"), "lovely love.", true},
		{Word("love"), "Love", false},
		{Word("love"), "love_letters", true},
		{Word("love"), "люблюlove", false},
		{Word("love"), "love😀", true},
		{Word("love"), "\xfflove\xff", true},
		{Word("Good morning"), "Good morning!", true},
		{Word("Good morning"), "Good  morning", false},
		{Word("Good morning"), "Good mornings", false},
		{Word("Good morning"), "aGood morning", false},
		{Word("доброе утро"), "всем доброе утро.", true},
		{Word("доброе утро"), "недоброе утро", false},
		{Word("(error)"), "x(error)y", true},
		{Prefix("crypto"), "cryptocurrency", true},
		{Prefix("crypto"), "the crypto", true},
		{Prefix("crypto"), "anticrypto", false},
		{Prefix("crypto"), "anticrypto #cryptography", true},
		{Substring("ware"), "software", true},
		{Substring("ware"), "war e", false},
		{Substring(""), "anything", false},
		{Word(""), "", false},
	}
	for _, c := range cases {
		if got := c.p.Match([]byte(c.text)); got != c.want {
			t.Errorf("%q in %q: %v, want %v", c.p.needle, c.text, got, c.want)
		}
	}
}

func TestPatternTokensAreHeldByEveryMatch(t *testing.T) {
	hashes := func(words ...string) []uint64 {
		var out []uint64
		for _, w := range words {
			out = AppendHashes(out, []byte(w))
		}

		return out
	}
	cases := []struct {
		p    Pattern
		want []uint64
	}{
		{Word("Good morning"), hashes("good", "morning")},
		{Word("did:plc:abc"), hashes("did", "plc", "abc")},
		{Prefix("crypto"), nil},
		{Prefix("big crypto"), hashes("big")},
		{Prefix("crypto "), hashes("crypto")},
		{Substring("ware"), nil},
		{Substring("soft ware house"), hashes("ware")},
		{Substring(" ware "), hashes("ware")},
		{Substring("a b"), nil},
	}
	for _, c := range cases {
		if got := c.p.Tokens(nil); len(got)+len(c.want) != 0 && !reflect.DeepEqual(got, c.want) {
			t.Errorf("tokens of %q: %v, want %v", c.p.needle, got, c.want)
		}
	}
	if got := Word("x y").Tokens([]uint64{7}); len(got) != 3 || got[0] != 7 {
		t.Errorf("Tokens must keep what dst holds: %v", got)
	}
}

func TestAppendMatchesAgreesWithMatchPerValue(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	words := []string{"love", "glove", "lovely", "ware", "software", "crypto", "cryptography", "Good", "morning", "x", "é", "утро", "lo", "ve"}
	seps := []string{" ", "", ", ", "-", "\n", "_"}
	patterns := []Pattern{Word("love"), Word("Good morning"), Prefix("crypto"), Substring("ware"), Substring("ve lo"), Word("x"), Word("утро"), Substring("e")}
	for round := 0; round < 200; round++ {
		var data []byte
		offsets := []uint32{0}
		// Values start past the beginning of data, as dictionary pages do.
		if round%2 == 1 {
			data = append(data, "love ware crypto"...)
			offsets[0] = uint32(len(data))
		}
		var values []string
		for i, n := 0, r.Intn(60); i < n; i++ {
			var b strings.Builder
			for j, m := 0, r.Intn(6); j < m; j++ {
				b.WriteString(words[r.Intn(len(words))])
				b.WriteString(seps[r.Intn(len(seps))])
			}
			values = append(values, b.String())
			data = append(data, b.String()...)
			offsets = append(offsets, uint32(len(data)))
		}
		data = append(data, " love Good morning"...)
		for _, p := range patterns {
			var want []uint32
			for i, v := range values {
				if p.Match([]byte(v)) {
					want = append(want, uint32(i))
				}
			}
			if got := p.AppendMatches(nil, offsets, data); !reflect.DeepEqual(got, want) {
				t.Fatalf("round %d, %q over %q: %v, want %v", round, p.needle, values, got, want)
			}
		}
	}
}

func TestPatternAgreesWithRegexp(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	alphabet := []string{"a", "b", "A", " ", ".", "é", "1", "_"}
	tokenFirst := regexp.MustCompile(`^[\p{L}\p{N}]`)
	tokenLast := regexp.MustCompile(`[\p{L}\p{N}]$`)
	for round := 0; round < 3000; round++ {
		var text, needle strings.Builder
		for i, n := 0, r.Intn(14); i < n; i++ {
			text.WriteString(alphabet[r.Intn(len(alphabet))])
		}
		for i, n := 0, 1+r.Intn(3); i < n; i++ {
			needle.WriteString(alphabet[r.Intn(len(alphabet))])
		}
		s, n := text.String(), needle.String()
		var word, prefix bool
		for i := 0; i+len(n) <= len(s); i++ {
			if s[i:i+len(n)] != n {
				continue
			}
			// Two token runes side by side across an end of the needle
			// mean that end is inside a token.
			startInside := tokenFirst.MatchString(n) && tokenLast.MatchString(s[:i])
			endInside := tokenLast.MatchString(n) && tokenFirst.MatchString(s[i+len(n):])
			word = word || !startInside && !endInside
			prefix = prefix || !startInside
		}
		if got := Word(n).Match([]byte(s)); got != word {
			t.Fatalf("Word(%q) in %q: %v, want %v", n, s, got, word)
		}
		if got := Prefix(n).Match([]byte(s)); got != prefix {
			t.Fatalf("Prefix(%q) in %q: %v, want %v", n, s, got, prefix)
		}
		if got := Substring(n).Match([]byte(s)); got != strings.Contains(s, n) {
			t.Fatalf("Substring(%q) in %q: %v", n, s, got)
		}
	}
}

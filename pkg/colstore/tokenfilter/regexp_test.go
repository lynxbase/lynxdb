package tokenfilter

import (
	"math/rand"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestRegexpFindsTheLiteralEveryMatchHolds(t *testing.T) {
	for _, c := range []struct {
		expr, needle string
		fold         bool
	}{
		{`error`, "error", false},
		{`error \d+ in (\w+)`, "error ", false},
		// A class of the two cases of a letter is that letter in any case.
		{`[Gg]ood (morning|night)`, "good ", true},
		{`[Gx]ood (morning|night)`, "ood ", false},
		{`(connection reset)+ by peer`, "connection reset", false},
		{`a{2,3}timeout`, "timeout", false},
		{`(timeout){0,3}`, "", false},
		{`foo|bar`, "", false},
		{`\d{5}`, "", false},
		{`x*`, "", false},
		{`^GET /api/v[12]/users\b`, "GET /api/v", false},
		{`(?i)error`, "error", true},
		{`(?i)crypto\w*`, "crypto", true},
		// k and s have forms outside ASCII, which a needle cannot stand for.
		{`(?i)token`, "to", true},
		{`(?i)mask-data`, "-data", true},
		{`(?i)caf\x{e9} au lait`, " au lait", true},
		{`caf\x{e9} au lait`, "caf\u00e9 au lait", false},
		{`(?i)id=42 `, "id=42 ", true},
		// The replacement rune matches bytes that are not UTF-8.
		{"ab\ufffdcde", "cde", false},
	} {
		p, err := Regexp(c.expr)
		if err != nil {
			t.Fatalf("%s: %v", c.expr, err)
		}
		if string(p.needle) != c.needle || (p.fold != c.fold && c.needle != "") {
			t.Errorf("%s: needle %q, any case %v; want %q, %v", c.expr, p.needle, p.fold, c.needle, c.fold)
		}
	}
	if _, err := Regexp(`(`); err == nil {
		t.Error("an expression that does not compile was taken")
	}
}

func TestRegexpAgreesWithTheStandardLibrary(t *testing.T) {
	exprs := []string{
		`error`, `err(or)? 5\d\d`, `(?i)error`, `(?i)token=\w+`, `(?i)kelvin`, `[Gg]ood (morning|night)`,
		`\d{3}`, `^GET `, `done$`, `a b c`, ` b `, `(?i) B `, `(ab)+c`, `\bid=\d+\b`, `(?i)stra\x{df}e`, `caf\x{e9}`,
		`(?s)error.*done`, `one two|three`,
	}
	pieces := []string{
		"error", "Error", "ERROR 503", "err 500", "token=abc", "TOKEN=x", "\u212aelvin", "kelvin", "Good morning", "good night",
		"GET ", "done", "a b c", " B ", "ab", "c", "id=42", "id=x", "Stra\u00dfe", "caf\u00e9", "one two", "three", " ", "\n", "123", "-", "\xff",
	}
	r := rand.New(rand.NewSource(7))
	var data []byte
	offsets := []uint32{0}
	var values [][]byte
	for range 3000 {
		var text strings.Builder
		for i, n := 0, r.Intn(6); i < n; i++ {
			text.WriteString(pieces[r.Intn(len(pieces))])
		}
		values = append(values, []byte(text.String()))
		data = append(data, text.String()...)
		offsets = append(offsets, uint32(len(data)))
	}
	for _, expr := range exprs {
		p, err := Regexp(expr)
		if err != nil {
			t.Fatalf("%s: %v", expr, err)
		}
		re := regexp.MustCompile(expr)
		tokens := p.Tokens(nil)
		var want []uint32
		for i, v := range values {
			matches := re.Match(v)
			if got := p.Match(v); got != matches {
				t.Fatalf("%s on %q: %v, the library says %v", expr, v, got, matches)
			}
			if !matches {
				continue
			}
			want = append(want, uint32(i))
			has := AppendHashes(nil, v)
			for _, token := range tokens {
				if !slices.Contains(has, token) {
					t.Fatalf("%s matches %q, which lacks a token the pattern requires", expr, v)
				}
			}
		}
		if len(want) == 0 || len(want) == len(values) {
			t.Fatalf("%s matches %d of %d values, which tests nothing", expr, len(want), len(values))
		}
		if got := p.AppendMatches(nil, offsets, data); !slices.Equal(got, want) {
			t.Fatalf("%s over the run: %d values, want %d", expr, len(got), len(want))
		}
		if got := p.AppendMatches(nil, offsets[:1], data); len(got) != 0 {
			t.Fatalf("%s over no values gives %v", expr, got)
		}
	}
}

func TestRegexpRequiresTheWholeTokensOfItsLiteral(t *testing.T) {
	for _, c := range []struct {
		expr   string
		tokens []string
	}{
		{`connection reset by peer \d+`, []string{"reset", "peer", "by"}},
		{`error`, nil},
		{` error `, []string{"error"}},
		{`(?i)Deadline Exceeded: `, []string{"exceeded"}},
		{`(?i)Deadline Exceeded`, nil},
		{`(?i)code: Deadline Exceeded: `, []string{"deadline", "exceeded"}},
		{`\d+`, nil},
	} {
		p, err := Regexp(c.expr)
		if err != nil {
			t.Fatal(err)
		}
		var want []uint64
		for _, token := range c.tokens {
			want = AppendHashes(want, []byte(token))
		}
		if got := p.Tokens(nil); !slices.Equal(got, want) {
			t.Errorf("%s requires %d tokens, want %v", c.expr, len(got), c.tokens)
		}
	}
}

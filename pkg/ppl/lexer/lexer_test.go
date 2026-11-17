package lexer

import (
	"bufio"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestEveryWhiteSpaceSeparatesTokens(t *testing.T) {
	for _, src := range []string{"a b", "a\vb", "a b", "a b", "a\u0085b"} {
		toks, errs := Tokenize(src)
		if len(errs) > 0 || len(toks) != 3 {
			t.Fatalf("%q: tokens %v, errors %v", src, toks, errs)
		}
		if !toks[1].SpaceBefore {
			t.Errorf("%q: the second token is not preceded by space", src)
		}
	}
}

// posAt computes the position of a byte offset from the text before it.
func posAt(src string, off int) Pos {
	before := src[:off]
	nl := strings.LastIndexByte(before, '\n')
	return Pos{Offset: off, Line: 1 + strings.Count(before, "\n"), Col: 1 + utf8.RuneCountInString(before[nl+1:])}
}

// checkTokens verifies that tokens are ordered slices of the text with
// positions consistent with their offsets.
func checkTokens(t *testing.T, src string) {
	toks, _ := Tokenize(src)
	if len(toks) == 0 || toks[len(toks)-1].Kind != EOF {
		t.Fatalf("%q: no EOF token", src)
	}
	prev := 0
	for _, tok := range toks {
		if tok.Pos.Offset < prev || tok.End.Offset < tok.Pos.Offset || tok.End.Offset > len(src) {
			t.Fatalf("%q: token %q at %v-%v out of order", src, tok.Text, tok.Pos, tok.End)
		}
		if tok.Text != src[tok.Pos.Offset:tok.End.Offset] {
			t.Fatalf("%q: token text %q is not the source at %v-%v", src, tok.Text, tok.Pos, tok.End)
		}
		if tok.Pos != posAt(src, tok.Pos.Offset) || tok.End != posAt(src, tok.End.Offset) {
			t.Fatalf("%q: token %q has positions %v-%v", src, tok.Text, tok.Pos, tok.End)
		}
		prev = tok.End.Offset
	}
}

func corpus(t testing.TB) []string {
	f, err := os.Open("../parser/testdata/opensearch_examples.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if _, q, ok := strings.Cut(sc.Text(), "\t"); ok {
			out = append(out, q)
		}
	}
	return out
}

var lexerSeeds = []string{
	"", " ", "a.b", "x.5", "(a).5", ".5", "1e5", "1e5x", "1.5d", "1.5F", "5m", "5M", "1.5h", "10Seconds",
	"@d", "@W3", "@w9", "*a", "a- b", "'it''s'", `"a\"b"`, `'a\é'`, "`f``g`", "'open", "/* open",
	"a /* c */ b", "a // c\nb", "a\vb", "\xff", "'\xff'", "aé", "<> != == <= >= -> !", "'a\nb'\n c",
}

func TestTokenPositions(t *testing.T) {
	for _, src := range append(corpus(t), lexerSeeds...) {
		checkTokens(t, src)
	}
}

func FuzzTokenize(f *testing.F) {
	for _, src := range append(corpus(f), lexerSeeds...) {
		f.Add(src)
	}
	f.Fuzz(checkTokens)
}

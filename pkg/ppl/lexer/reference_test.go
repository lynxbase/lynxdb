package lexer

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"

	reflexer "github.com/lynxbase/lynxdb/pkg/ppl/internal/reference/lexer"
)

func describeTokens(src string) string {
	toks, errs := Tokenize(src)
	var b strings.Builder
	for _, t := range toks {
		fmt.Fprintf(&b, "%d %q %q %v %v %v\n", t.Kind, t.Text, t.Value, t.Pos, t.End, t.SpaceBefore)
	}
	for _, e := range errs {
		fmt.Fprintf(&b, "error %v %s\n", e.Pos, e.Msg)
	}
	return b.String()
}

func describeReferenceTokens(src string) string {
	toks, errs := reflexer.Tokenize(src)
	var b strings.Builder
	for _, t := range toks {
		fmt.Fprintf(&b, "%d %q %q %v %v %v\n", t.Kind, t.Text, t.Value, Pos(t.Pos), Pos(t.End), t.SpaceBefore)
	}
	for _, e := range errs {
		fmt.Fprintf(&b, "error %v %s\n", Pos(e.Pos), e.Msg)
	}
	return b.String()
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
	"", " ", "a.b", "x.5", "(a).5", ".5", "1e5", "1e5x", "1.5d", "1.5F", "5m", "5M", "1.5h", "5mx", "10Seconds",
	"@d", "@W3", "@w9", "@Quarters", "@x", "*", "*a", "a*", "a-", "a- b", "a-b", "'it''s'", `"a\"b"`, `'a\é'`,
	"`f``g`", "'unterminated", "/* open", "a /* c */ b", "a // c\nb", "a\vb", "a b", "a b", "\xff",
	"'\xff'", "é", "aé", "á", "<> != == <= >= -> !", "x|y", "\r\n\tz", "'a\nb'", "'\\\n'",
}

func TestTokenizeMatchesReference(t *testing.T) {
	for _, src := range append(corpus(t), lexerSeeds...) {
		if got, want := describeTokens(src), describeReferenceTokens(src); got != want {
			t.Fatalf("tokens differ for %q\n--- got\n%s--- want\n%s", src, got, want)
		}
	}
}

func FuzzTokenizeMatchesReference(f *testing.F) {
	for _, src := range append(corpus(f), lexerSeeds...) {
		f.Add(src)
	}
	f.Fuzz(func(t *testing.T, src string) {
		if got, want := describeTokens(src), describeReferenceTokens(src); got != want {
			t.Fatalf("tokens differ for %q\n--- got\n%s--- want\n%s", src, got, want)
		}
	})
}

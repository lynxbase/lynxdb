package ppl

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

var update = flag.Bool("update", false, "rewrite golden files")

// TestCompileGolden compiles each query in testdata/compile/*.ppl and
// compares the rendered IR with the .ir golden file next to it.
func TestCompileGolden(t *testing.T) {
	files, err := filepath.Glob("testdata/compile/*.ppl")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no golden queries")
	}
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".ppl")
		t.Run(name, func(t *testing.T) {
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var got string
			res, err := Compile(string(src))
			if err != nil {
				got = "ERROR " + err.Error() + "\n"
			} else {
				got = ir.Render(res.Plan)
			}
			golden := strings.TrimSuffix(f, ".ppl") + ".ir"
			if *update {
				if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("missing golden %s (run with -update): %v", golden, err)
			}
			if got != string(want) {
				t.Errorf("plan mismatch for %s\n--- got\n%s--- want\n%s", src, got, want)
			}
		})
	}
}

func TestCompileErrors(t *testing.T) {
	cases := []struct {
		q    string
		kind ErrorKind
		msg  string
	}{
		{"source=t | where", ErrSyntax, "expected an expression"},
		{"where x > 1", ErrSyntax, "missing source"},
		{"source=t | stats count() by host | where status > 1", ErrSemantic, "field status not found"},
		{"source=t | eval x = 1 + true", ErrType, "not defined"},
		{"source=t | where 'a' = true", ErrType, "cannot compare"},
		{"source=t | where multi_match(['body'], 'x')", ErrUnsupportedFunction, "relevance"},
		{"source=t | where match_phrase(body, 'a b', slop=2)", ErrUnsupportedFeature, "slop option"},
		{"source=t | where match(body, 'a', operator='xor')", ErrSemantic, "operator must be AND or OR"},
		{"source=t | where match(1, 'a')", ErrSemantic, "must be a field"},
		{"source=t | stats count() by host | where match(body, 'a')", ErrSemantic, "field body not found"},
		{"source=t | kmeans centroids=3", ErrUnsupportedCommand, "ML Commons"},
		{"source=t | eval x = nosuchfunc(1)", ErrUnsupportedFunction, "unknown function nosuchfunc"},
		{"source=t | eval x = count()", ErrSemantic, "only valid in stats"},
		{"source=t | stats foo(x)", ErrUnsupportedFunction, "unknown aggregation foo"},
		{"source=t | head -1", ErrSyntax, "unexpected"},
		{"source=t | dedup 0 a", ErrSemantic, "greater than 0"},
		{"source=t | rename a* as b", ErrSemantic, "same number of wildcards"},
		{"source=t | where abs('x') > 1", ErrType, "abs"},
		{"source=t earliest=-7x", ErrSemantic, "invalid time modifier"},
		{"source=t | stats count() as c, avg(x) as c", ErrSemantic, "duplicate aggregation name"},
		{"source=t | stats percentile(x, y)", ErrType, "percent must be a number"},
		{"sourCe=0|stAts perCentile(0,A)", ErrType, "percent must be a number"},
		{"source=t | patterns body method=brain", ErrUnsupportedFeature, "patterns method=brain is not supported"},
		{"source=t | patterns body method=BRAIN mode=aggregation", ErrUnsupportedFeature, "patterns method=brain is not supported"},
	}
	for _, c := range cases {
		_, err := Compile(c.q)
		if err == nil {
			t.Errorf("%q: expected an error", c.q)
			continue
		}
		pe, ok := AsError(err)
		if !ok {
			t.Errorf("%q: %v is not a PPL error", c.q, err)
			continue
		}
		if pe.Kind != c.kind || !strings.Contains(pe.Error(), c.msg) {
			t.Errorf("%q: got %s %q, want %s containing %q", c.q, pe.Kind, pe.Error(), c.kind, c.msg)
		}
		if pe.Pos.Line < 1 || pe.Pos.Col < 1 {
			t.Errorf("%q: missing position in %v", c.q, pe)
		}
	}
}

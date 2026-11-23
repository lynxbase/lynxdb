package ppl

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl/parser"
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

// TestUncorrelatedSubqueriesCompile pins subqueries that read only their own
// fields, including through their own source name or alias.
func TestUncorrelatedSubqueriesCompile(t *testing.T) {
	for _, q := range []string{
		"source=outer | where a in [ source=inner | fields b ]",
		"source=logs | where exists [ source=logs | where logs.x = 1 ]",
		"source=outer | where exists [ source=inner as i | where i.c > 10 ]",
		"source=outer | where a = [ source=inner | join left=l right=r on l.k = r.k other | stats max(c) ]",
		"source=outer | eval m = [ source=inner | stats max(c) ] | where m > b",
		"source=outer | stats count() as n by k | where k in [ source=inner | fields k ]",
	} {
		if _, err := Compile(q); err != nil {
			t.Errorf("%s: %v", q, err)
		}
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
		{"source=t | eventstats bucket_nullable=false count() by host", ErrUnsupportedFeature, "eventstats bucket_nullable=false is not supported"},
		{"source=t | streamstats count() by host bucket_nullable=false", ErrUnsupportedFeature, "streamstats bucket_nullable=false is not supported"},
		{"source=t | streamstats reset_before=status > 300 count()", ErrUnsupportedFeature, "streamstats reset_before is not supported"},
		{"source=t | streamstats reset_after=(status > 300) count() as n by host", ErrUnsupportedFeature, "streamstats reset_after is not supported"},
		{"source=t | stats earliest(status, event_time) by host", ErrUnsupportedFeature, "earliest ordered by event_time is not supported"},
		{"source=t | eventstats latest(msg, date_add(_time, INTERVAL 1 DAY)) as m", ErrUnsupportedFeature, "latest ordered by an expression is not supported"},
		{"source=outer | eval m = [ source=inner | where outer.b = inner.d | stats max(c) ]", ErrUnsupportedFeature, "correlated subqueries are not supported: the subquery refers to outer.b"},
		{"source=t1 as a | where exists [ source=t2 | where a.id = id ]", ErrUnsupportedFeature, "the subquery refers to a.id of the outer query"},
		{"source=outer | where a in [ source=inner | where b > 1 ]", ErrSemantic, "an IN subquery must name the columns it returns"},
		{"source=outer | where a in [ source=inner | fields b, c ]", ErrSemantic, "the IN subquery returns 2 columns for 1 values"},
		{"source=a | where exists [ source=b | where exists [ source=c | where a.x = y ] ]", ErrUnsupportedFeature, "the subquery refers to a.x"},
		{"source=a | where exists [ source=b | where exists [ source=c | where b.x = y ] ]", ErrUnsupportedFeature, "the subquery refers to b.x"},
		{"source=logs | eval lim = 10 | where exists [ source=users | fields owner, n | where n > lim ]", ErrUnsupportedFeature, "the subquery refers to field lim of the outer query"},
		{"source=logs | eval lim = 10 | where host in [ source=users | stats count() by host | sort lim | fields host ]", ErrUnsupportedFeature, "the subquery refers to field lim"},
		{"source=a | eval k = 1 | where exists [ source=b | where exists [ source=c | fields n | where n = k ] ]", ErrUnsupportedFeature, "the subquery refers to field k of the outer query"},
		{"source=outer | where exists [ source=inner | fields c | where a = c ]", ErrSemantic, "field a not found"},
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

// TestDefaultSource pins that a query naming no source compiles with a
// default source as the query that names it, and that errors keep their
// positions in the text as written.
func TestDefaultSource(t *testing.T) {
	opts := parser.Options{DefaultSource: "main"}
	for q, same := range map[string]string{
		"where status >= 500 | stats count()":       "source=main | where status >= 500 | stats count()",
		"| where status >= 500":                     "source=main | where status >= 500",
		"grace ERROR | stats count()":               "search source=main grace ERROR | stats count()",
		"search grace":                              "search source=main grace",
		"status=500 | head 1":                       "search source=main status=500 | head 1",
		"top=5":                                     "search source=main top=5",
		"source=web | head 1":                       "source=web | head 1",
		"explain where x > 1":                       "explain source=main | where x > 1",
		"head 3":                                    "source=main | head 3",
		"union [search source=a] [search source=b]": "union [search source=a] [search source=b]",
	} {
		got, err := CompileWith(q, opts)
		if err != nil {
			t.Errorf("%s: %v", q, err)
			continue
		}
		want, err := Compile(same)
		if err != nil {
			t.Fatalf("%s: %v", same, err)
		}
		if ir.Render(got.Plan) != ir.Render(want.Plan) || got.Explain != want.Explain {
			t.Errorf("%s compiles to\n%s\nwant the plan of %s:\n%s", q, ir.Render(got.Plan), same, ir.Render(want.Plan))
		}
	}
	for q, msg := range map[string]string{
		"where status >":                   "line 1, column 15: expected an expression",
		"| tstats count":                   `line 1, column 3: unknown command "tstats"`,
		"| stats count() by":               "line 1, column 19",
		"":                                 "expected a query",
		"where x > 1 | search y":           "search can only be the first command",
		"x | where exists [ where y > 1 ]": "missing source",
	} {
		_, err := CompileWith(q, opts)
		if err == nil || !strings.Contains(err.Error(), msg) {
			t.Errorf("%q: got %v, want an error containing %q", q, err, msg)
		}
	}
	if _, err := Compile("where x > 1"); err == nil || !strings.Contains(err.Error(), "missing source") {
		t.Errorf("without a default source: got %v, want a missing source error", err)
	}
}

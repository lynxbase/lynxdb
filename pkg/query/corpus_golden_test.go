package query

import (
	"bufio"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

var updateGolden = flag.Bool("update", false, "rewrite golden files")

func corpusQueries(t testing.TB) []string {
	t.Helper()
	f, err := os.Open("../ppl/parser/testdata/opensearch_examples.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		_, q, _ := strings.Cut(line, "\t")
		out = append(out, q)
	}
	return out
}

// outcome renders everything a compilation produces: the error with its
// kind, span, hint and expected tokens, or the logical plan, the optimized
// plan and the rules that fired.
func outcome(src string) string {
	var b strings.Builder
	res, err := ppl.Compile(src)
	if err != nil {
		if pe, ok := ppl.AsError(err); ok {
			fmt.Fprintf(&b, "error %s %d:%d:%d-%d:%d:%d %q hint=%q expected=%q\n", pe.Kind,
				pe.Pos.Offset, pe.Pos.Line, pe.Pos.Col, pe.End.Offset, pe.End.Line, pe.End.Col, pe.Msg, pe.Hint, pe.Expected)
		} else {
			fmt.Fprintf(&b, "error %T %v\n", err, err)
		}
		return b.String()
	}
	fmt.Fprintf(&b, "explain=%v mode=%q sources=%q\n", res.Explain, res.ExplainMode, res.Sources)
	b.WriteString(ir.Render(res.Plan))
	c, err := Compile(src)
	if err != nil {
		fmt.Fprintf(&b, "optimize error %v\n", err)
		return b.String()
	}
	b.WriteString("optimized:\n")
	b.WriteString(ir.Render(c.Plan))
	for _, a := range c.Applied {
		fmt.Fprintf(&b, "rule %s x%d\n", a.Rule, a.Count)
	}
	return b.String()
}

// prefixes returns the query cut before every token-looking boundary, so
// the error paths of the parser and the compiler are pinned too.
func prefixes(q string) []string {
	var out []string
	for i := 1; i < len(q); i++ {
		if q[i-1] == ' ' || strings.IndexByte("|()[],=<>!'\"`.", q[i]) >= 0 {
			out = append(out, q[:i])
		}
	}
	return out
}

// TestCorpusPlansGolden pins the outcome of compiling every documented
// example and its prefixes, so that changes to the frontend cannot alter a
// plan, an error message or an error position unnoticed. Set
// PPL_CORPUS_DUMP to a file name to write the full outcomes for diffing.
func TestCorpusPlansGolden(t *testing.T) {
	queries := corpusQueries(t)
	for _, q := range typicalQueries {
		queries = append(queries, q.ppl)
	}
	var sum, dump strings.Builder
	for _, q := range queries {
		h := fnv.New64a()
		full := outcome(q)
		h.Write([]byte(full))
		dump.WriteString("== " + q + "\n" + full)
		for _, p := range prefixes(q) {
			o := outcome(p)
			h.Write([]byte(o))
			dump.WriteString("-- " + p + "\n" + o)
		}
		fmt.Fprintf(&sum, "%016x\t%s\n", h.Sum64(), q)
	}
	if path := os.Getenv("PPL_CORPUS_DUMP"); path != "" {
		if err := os.WriteFile(path, []byte(dump.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got := sum.String()
	const golden = "testdata/corpus_plans.sum"
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("missing golden %s (run with -update): %v", golden, err)
	}
	gotLines := strings.Split(got, "\n")
	wantLines := strings.Split(string(want), "\n")
	if len(gotLines) != len(wantLines) {
		t.Fatalf("got %d entries, want %d", len(gotLines), len(wantLines))
	}
	shown := 0
	for i := range gotLines {
		if gotLines[i] != wantLines[i] && shown < 5 {
			_, q, _ := strings.Cut(gotLines[i], "\t")
			t.Errorf("outcome changed for %s; it is now:\n%s", q, outcome(q))
			shown++
		}
	}
}

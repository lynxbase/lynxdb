package ppl_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ir/opt"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/ppl/internal/reference"
	refopt "github.com/lynxbase/lynxdb/pkg/ppl/internal/reference/opt"
	"github.com/lynxbase/lynxdb/pkg/query"
)

func liveOutcome(src string) string {
	var b strings.Builder
	res, err := ppl.Compile(src)
	if err != nil {
		if pe, ok := ppl.AsError(err); ok {
			fmt.Fprintf(&b, "error %s %v-%v %v-%v %q hint=%q expected=%q\n", pe.Kind, pe.Pos.Offset, pe.Pos,
				pe.End.Offset, pe.End, pe.Msg, pe.Hint, pe.Expected)
			return b.String()
		}
		return fmt.Sprintf("error %v\n", err)
	}
	fmt.Fprintf(&b, "explain=%v mode=%q sources=%q\n", res.Explain, res.ExplainMode, res.Sources)
	b.WriteString(ir.Render(res.Plan))
	plan, applied := opt.Optimize(res.Plan, opt.Options{Fold: query.Fold})
	b.WriteString(ir.Render(plan))
	for _, a := range applied {
		fmt.Fprintf(&b, "rule %s x%d\n", a.Rule, a.Count)
	}
	return b.String()
}

func referenceOutcome(src string) string {
	var b strings.Builder
	res, err := reference.Compile(src)
	if err != nil {
		if pe, ok := reference.AsError(err); ok {
			fmt.Fprintf(&b, "error %s %v-%v %v-%v %q hint=%q expected=%q\n", pe.Kind, pe.Pos.Offset, pe.Pos,
				pe.End.Offset, pe.End, pe.Msg, pe.Hint, pe.Expected)
			return b.String()
		}
		return fmt.Sprintf("error %v\n", err)
	}
	fmt.Fprintf(&b, "explain=%v mode=%q sources=%q\n", res.Explain, res.ExplainMode, res.Sources)
	b.WriteString(ir.Render(res.Plan))
	plan, applied := refopt.Optimize(res.Plan, refopt.Options{Fold: query.Fold})
	b.WriteString(ir.Render(plan))
	for _, a := range applied {
		fmt.Fprintf(&b, "rule %s x%d\n", a.Rule, a.Count)
	}
	return b.String()
}

func referenceSeeds(t testing.TB) []string {
	data, err := os.ReadFile("parser/testdata/opensearch_examples.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if _, q, ok := strings.Cut(line, "\t"); ok && !strings.HasPrefix(line, "#") {
			out = append(out, q)
		}
	}
	return append(out,
		"source=t | where a = 'x\\é'", "source=t | eval `a``b` = 1", "search source=t \"a b\" c* -d",
		"source=t | stats count() by span(@timestamp, 1h)", "source=t | where x > 1 and (y < 2 or not z)",
		"source=t | eval s = case(a > 1, 'x' else 'y')", "explain extended source=t | head 3",
		"source=t | where a.b.c = 1.5e3", "source=t | timechart span=1m count() by host",
	)
}

func TestCompileMatchesReference(t *testing.T) {
	for _, src := range referenceSeeds(t) {
		for i := 1; i <= len(src); i++ {
			if i < len(src) && src[i-1] != ' ' && strings.IndexByte("|()[],=<>!'\"`.", src[i]) < 0 {
				continue
			}
			q := src[:i]
			if got, want := liveOutcome(q), referenceOutcome(q); got != want {
				t.Fatalf("outcome differs for %q\n--- got\n%s--- want\n%s", q, got, want)
			}
		}
	}
}

func FuzzCompileMatchesReference(f *testing.F) {
	for _, src := range referenceSeeds(f) {
		f.Add(src)
	}
	f.Fuzz(func(t *testing.T, src string) {
		got := liveOutcome(src)
		if strings.Contains(got, "nests deeper than") || strings.Contains(got, "more than 1000 commands") {
			return
		}
		if want := referenceOutcome(src); got != want {
			t.Fatalf("outcome differs for %q\n--- got\n%s--- want\n%s", src, got, want)
		}
	})
}

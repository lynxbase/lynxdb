package query

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ir/opt"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/lexer"
	"github.com/lynxbase/lynxdb/pkg/ppl/parser"
)

// typicalQueries are the shapes log analytics sends most often.
var typicalQueries = []struct {
	name string
	ppl  string
}{
	{"Search", "search source=main error timeout host=web-1"},
	{"BoolFilter", "source=main | where (status >= 500 or status = 404) and host != 'web-3' and not like(uri, '/health%')"},
	{"StatsBy2", "source=main | stats count() as c, avg(bytes) as avg_b, max(latency) as max_l, dc(user) as users by host, status"},
	{"Timechart", "source=main | timechart span=1m count() by host"},
	{"EvalChain", "source=main | eval kb = bytes / 1024, mb = kb / 1024 | eval slow = if(latency > 1000, 'yes', 'no') | eval tag = concat(host, '-', status)"},
	{"TenCommands", "source=main | where status >= 400 | eval kb = bytes / 1024 | fields host, status, kb, uri | rename uri as path | where kb > 1 | stats count() as c, sum(kb) as total by host, status | sort - c | head 20 | eval share = total / c | fields host, status, c, share"},
}

func loadBenchCorpus(b *testing.B) []string {
	b.Helper()
	f, err := os.Open("../ppl/parser/testdata/opensearch_examples.tsv")
	if err != nil {
		b.Fatal(err)
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
		if _, err := Compile(q); err == nil {
			out = append(out, q)
		}
	}
	return out
}

type stageInput struct {
	src  string
	q    *ast.Query
	plan *ir.Plan
}

func prepareStages(b *testing.B, srcs []string) []stageInput {
	b.Helper()
	in := make([]stageInput, len(srcs))
	for i, s := range srcs {
		q, err := parser.Parse(s)
		if err != nil {
			b.Fatalf("%s: %v", s, err)
		}
		res, err := ppl.CompileQuery(q)
		if err != nil {
			b.Fatalf("%s: %v", s, err)
		}
		in[i] = stageInput{src: s, q: q, plan: res.Plan}
	}
	return in
}

// benchStages reports each compile stage separately; ns/op and allocs/op
// are per query, averaged over the set.
func benchStages(b *testing.B, srcs []string) {
	in := prepareStages(b, srcs)
	n := len(in)
	per := func(b *testing.B, f func(stageInput)) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			f(in[i%n])
		}
	}
	b.Run("Lex", func(b *testing.B) {
		per(b, func(s stageInput) { lexer.Tokenize(s.src) })
	})
	b.Run("Parse", func(b *testing.B) {
		per(b, func(s stageInput) {
			if _, err := parser.Parse(s.src); err != nil {
				b.Fatal(err)
			}
		})
	})
	b.Run("Lower", func(b *testing.B) {
		per(b, func(s stageInput) {
			if _, err := ppl.CompileQuery(s.q); err != nil {
				b.Fatal(err)
			}
		})
	})
	b.Run("Optimize", func(b *testing.B) {
		per(b, func(s stageInput) {
			p, _ := opt.Optimize(s.plan, opt.Options{Fold: Fold})
			if err := ir.Validate(p); err != nil {
				b.Fatal(err)
			}
		})
	})
	b.Run("Compile", func(b *testing.B) {
		per(b, func(s stageInput) {
			if _, err := Compile(s.src); err != nil {
				b.Fatal(err)
			}
		})
	})
}

func BenchmarkStagesCorpus(b *testing.B) {
	benchStages(b, loadBenchCorpus(b))
}

func BenchmarkStagesTypical(b *testing.B) {
	for _, q := range typicalQueries {
		b.Run(q.name, func(b *testing.B) { benchStages(b, []string{q.ppl}) })
	}
}

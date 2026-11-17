package query

import "testing"

var benchQueries = []struct {
	name string
	ppl  string
}{
	{"FilterStatsBy", "source=main | where status >= 500 | stats count() by host"},
	{"SearchTerm", "search source=main error | stats count()"},
	{"EvalSortHead", "source=main | eval kb = bytes / 1024 | sort - kb | head 10"},
	{"TimeBuckets", "source=main | stats count() by span(_time, 1m)"},
	{"MultiAgg", "source=main | stats avg(bytes), max(bytes), dc(host), percentile(bytes, 95) by level"},
}

func BenchmarkCompile(b *testing.B) {
	for _, q := range benchQueries {
		b.Run(q.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := Compile(q.ppl); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

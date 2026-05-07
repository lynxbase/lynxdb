# Benchmarks

Benchmark numbers are checked in as regression baselines. Refresh them when a
change intentionally moves performance.

## rsigma compatibility surface

Measured on Apple M4 Pro with:

```bash
go test -run '^$' -bench='Benchmark(ParseGoldenCorpus|PlanGoldenCorpus|ExecuteRegexShape)$' -benchmem ./pkg/sigmaqueries
```

| bench name | p50 ns/op | allocs/op | budget |
|---|---:|---:|---|
| BenchmarkParseGoldenCorpus/and_or_not.spl2 | 1811 | 39 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/and_or_not_minimal.spl2 | 1565 | 36 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/brute_force.spl2 | 1375 | 31 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/brute_force_minimal.spl2 | 1245 | 29 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/cidr.spl2 | 1019 | 26 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/cidr_minimal.spl2 | 893 | 24 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/exists_null_bool.spl2 | 1812 | 34 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/exists_null_bool_minimal.spl2 | 1580 | 31 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/keywords.spl2 | 1182 | 29 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/keywords_minimal.spl2 | 844 | 26 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/numeric_compare.spl2 | 1268 | 23 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/numeric_compare_minimal.spl2 | 1158 | 21 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/regex.spl2 | 984 | 24 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/regex_minimal.spl2 | 693 | 21 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/simple_eq.spl2 | 823 | 20 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/simple_eq_index.spl2 | 850 | 20 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/simple_eq_minimal.spl2 | 664 | 18 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/wildcards.spl2 | 2581 | 44 | < 5000 ns/op |
| BenchmarkParseGoldenCorpus/wildcards_minimal.spl2 | 2413 | 42 | < 5000 ns/op |
| BenchmarkPlanGoldenCorpus/and_or_not.spl2 | 237 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/and_or_not_minimal.spl2 | 229 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/brute_force.spl2 | 247 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/brute_force_minimal.spl2 | 234 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/cidr.spl2 | 1541 | 34 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/cidr_minimal.spl2 | 1532 | 34 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/exists_null_bool.spl2 | 237 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/exists_null_bool_minimal.spl2 | 225 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/keywords.spl2 | 212 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/keywords_minimal.spl2 | 208 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/numeric_compare.spl2 | 221 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/numeric_compare_minimal.spl2 | 227 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/regex.spl2 | 1312 | 28 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/regex_minimal.spl2 | 1308 | 28 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/simple_eq.spl2 | 214 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/simple_eq_index.spl2 | 215 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/simple_eq_minimal.spl2 | 212 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/wildcards.spl2 | 219 | 12 | < 50000 ns/op |
| BenchmarkPlanGoldenCorpus/wildcards_minimal.spl2 | 215 | 12 | < 50000 ns/op |
| BenchmarkExecuteRegexShape | 152633792 | 1154090 | <= 183160550 ns/op |

Summary budgets:

| group | p50 ns/op | budget |
|---|---:|---|
| parse golden corpus | 1182 | < 5000 |
| plan golden corpus | 227 | < 50000 |
| execute regex shape | 152633792 | <= 183160550 |

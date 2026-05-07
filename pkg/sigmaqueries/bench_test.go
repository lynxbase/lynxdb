package sigmaqueries

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/test/integration/sigmacompat"
)

type benchmarkQuery struct {
	Fixture string
	Line    string
}

type benchmarkPlan struct {
	Fixture string
	Line    string
	Program *spl2.Program
}

func BenchmarkParseGoldenCorpus(b *testing.B) {
	queries := loadBenchmarkQueries(b)
	for _, query := range queries {
		query := query
		b.Run(query.Fixture, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(query.Line)))
			for i := 0; i < b.N; i++ {
				if _, err := spl2.ParseProgram(query.Line); err != nil {
					b.Fatalf("ParseProgram(%s): %v", query.Fixture, err)
				}
			}
		})
	}
}

func BenchmarkPlanGoldenCorpus(b *testing.B) {
	plans := loadBenchmarkPlans(b)
	for _, plan := range plans {
		plan := plan
		b.Run(plan.Fixture, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(plan.Line)))
			for i := 0; i < b.N; i++ {
				iter, err := pipeline.BuildProgram(context.Background(), plan.Program, &pipeline.ServerIndexStore{}, 0)
				if err != nil {
					b.Fatalf("BuildProgram(%s): %v", plan.Fixture, err)
				}
				if iter != nil {
					if err := iter.Close(); err != nil {
						b.Fatalf("close plan %s: %v", plan.Fixture, err)
					}
				}
			}
		})
	}
}

func BenchmarkExecuteRegexShape(b *testing.B) {
	query := readBenchmarkFixture(b, "regex.spl2")
	eng := storage.NewEphemeralEngine()
	defer eng.Close()

	data := buildRegexBenchmarkDataset(100000)
	if _, err := eng.IngestReader(context.Background(), bytes.NewReader(data), storage.IngestOpts{
		Source:     "rsigma-regex-bench",
		SourceType: "json",
	}); err != nil {
		b.Fatalf("ingest regex benchmark data: %v", err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _, err := eng.Query(context.Background(), query, storage.QueryOpts{})
		if err != nil {
			b.Fatalf("query regex shape: %v", err)
		}
		if len(result.Rows) != 35000 {
			b.Fatalf("regex shape returned %d rows, want 35000", len(result.Rows))
		}
	}
}

func loadBenchmarkQueries(b *testing.B) []benchmarkQuery {
	b.Helper()

	fixtures, err := filepath.Glob(filepath.Join("testdata", "golden", "*.spl2"))
	if err != nil {
		b.Fatalf("glob golden fixtures: %v", err)
	}
	if len(fixtures) == 0 {
		b.Fatal("no golden SPL2 fixtures discovered")
	}

	var queries []benchmarkQuery
	for _, fixture := range fixtures {
		data, err := os.ReadFile(fixture)
		if err != nil {
			b.Fatalf("read %s: %v", fixture, err)
		}
		for _, raw := range strings.Split(string(data), "\n") {
			line := strings.TrimSpace(raw)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			queries = append(queries, benchmarkQuery{
				Fixture: filepath.Base(fixture),
				Line:    line,
			})
		}
	}

	return queries
}

func loadBenchmarkPlans(b *testing.B) []benchmarkPlan {
	b.Helper()

	queries := loadBenchmarkQueries(b)
	plans := make([]benchmarkPlan, 0, len(queries))
	for _, query := range queries {
		prog, err := spl2.ParseProgram(query.Line)
		if err != nil {
			b.Fatalf("ParseProgram(%s): %v", query.Fixture, err)
		}
		plans = append(plans, benchmarkPlan{
			Fixture: query.Fixture,
			Line:    query.Line,
			Program: prog,
		})
	}

	return plans
}

func readBenchmarkFixture(b *testing.B, name string) string {
	b.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "golden", name))
	if err != nil {
		b.Fatalf("read %s: %v", name, err)
	}

	return strings.TrimSpace(string(data))
}

func buildRegexBenchmarkDataset(events int) []byte {
	base := sigmacompat.DatasetFor("regex")
	if len(base) == 0 {
		panic("regex benchmark dataset is empty")
	}

	var buf bytes.Buffer
	for i := 0; i < events; i++ {
		buf.WriteString(base[i%len(base)].Raw)
		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

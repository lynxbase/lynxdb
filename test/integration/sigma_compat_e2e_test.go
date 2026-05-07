//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage"
	"github.com/lynxbase/lynxdb/test/integration/sigmacompat"
)

const placeholderMessage = "reference match set not yet generated -- run scripts/sync_rsigma_golden.sh --with-matches"

func TestSigmaCompatE2E(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		query   string
		index   string
	}{
		{"01_simple_eq", "simple_eq", "simple_eq.spl2", "main"},
		{"02_and_or_not", "and_or_not", "and_or_not.spl2", "main"},
		{"03_wildcards", "wildcards", "wildcards.spl2", "main"},
		{"04_regex", "regex", "regex.spl2", "main"},
		{"05_cidr", "cidr", "cidr.spl2", "main"},
		{"06_keywords", "keywords", "keywords.spl2", "main"},
		{"07_exists_null_bool", "exists_null_bool", "exists_null_bool.spl2", "main"},
		{"08_numeric_compare", "numeric_compare", "numeric_compare.spl2", "main"},
		{"09_brute_force", "brute_force", "brute_force.spl2", "main"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runFixtureQuery(t, tt.fixture, tt.query, tt.index)
		})
	}

	t.Run("10_multi_query_batch", func(t *testing.T) {
		runMultiQueryBatch(t)
	})

	t.Run("11_minimal_format", func(t *testing.T) {
		for _, fixture := range sigmacompat.FixtureNames {
			fixture := fixture
			t.Run(fixture, func(t *testing.T) {
				t.Parallel()
				runFixtureQuery(t, fixture, fixture+"_minimal.spl2", "main")
			})
		}
	})

	t.Run("12_custom_index", func(t *testing.T) {
		t.Parallel()
		runFixtureQuery(t, "simple_eq", "simple_eq_index.spl2", "security_logs")
	})
}

func runFixtureQuery(t *testing.T, fixture, queryFile, index string) {
	t.Helper()
	events := sigmacompat.DatasetFor(fixture)
	if events == nil {
		t.Fatalf("unknown fixture %q", fixture)
	}
	ref := readReference(t, fixture)
	query := readGolden(t, queryFile)

	eng := storage.NewEphemeralEngine()
	got := ingestAndQuery(t, eng, events, index, query)
	assertMatchIndices(t, fixture, query, got, ref.MatchIndices)
}

func runMultiQueryBatch(t *testing.T) {
	t.Helper()
	eng := storage.NewEphemeralEngine()
	for _, fixture := range sigmacompat.FixtureNames {
		events := sigmacompat.DatasetFor(fixture)
		if events == nil {
			t.Fatalf("unknown fixture %q", fixture)
		}
		ingestEvents(t, eng, events, fixture)
	}

	for _, fixture := range sigmacompat.FixtureNames {
		ref := readReference(t, fixture)
		query := strings.Replace(readGolden(t, fixture+".spl2"), "FROM main", "FROM "+fixture, 1)
		got := queryIndices(t, eng, query)
		assertMatchIndices(t, fixture, query, got, ref.MatchIndices)
	}
}

func ingestAndQuery(t *testing.T, eng *storage.Engine, events []sigmacompat.Event, index, query string) []int {
	t.Helper()
	ingestEvents(t, eng, events, index)
	return queryIndices(t, eng, query)
}

func ingestEvents(t *testing.T, eng *storage.Engine, events []sigmacompat.Event, index string) {
	t.Helper()
	n, err := eng.IngestLines(context.Background(), sigmacompat.Lines(events), storage.IngestOpts{
		Source:     "sigma-compat",
		SourceType: "json",
		Index:      index,
	})
	if err != nil {
		t.Fatalf("ingest index %s: %v", index, err)
	}
	if n != len(events) {
		t.Fatalf("ingest index %s: accepted %d events, want %d", index, n, len(events))
	}
}

func queryIndices(t *testing.T, eng *storage.Engine, query string) []int {
	t.Helper()
	result, _, err := eng.Query(context.Background(), query, storage.QueryOpts{})
	if err != nil {
		t.Fatalf("query failed: %v\nSPL2: %s", err, query)
	}
	indices := make([]int, 0, len(result.Rows))
	for rowNum, row := range result.Rows {
		raw, ok := row["__sigma_index"]
		if !ok {
			t.Fatalf("result row %d has no __sigma_index: %#v", rowNum, row)
		}
		index, ok := numericIndex(raw)
		if !ok {
			t.Fatalf("result row %d has non-numeric __sigma_index %T(%v)", rowNum, raw, raw)
		}
		indices = append(indices, index)
	}
	return indices
}

func numericIndex(raw any) (int, bool) {
	switch v := raw.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		if v == float64(int(v)) {
			return int(v), true
		}
	case json.Number:
		i, err := v.Int64()
		return int(i), err == nil
	}
	return 0, false
}

func assertMatchIndices(t *testing.T, fixture, query string, got, want []int) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s match indices mismatch\nSPL2: %s\ngot:  %v\nwant: %v", fixture, query, got, want)
	}
}

func readReference(t *testing.T, fixture string) sigmacompat.MatchReference {
	t.Helper()
	path := filepath.Join(goldenDir(t), fixture+".matches.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var ref sigmacompat.MatchReference
	if err := json.Unmarshal(data, &ref); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if len(ref.MatchIndices) == 0 {
		t.Fatalf("%s: %s", fixture, placeholderMessage)
	}
	if ref.MatchCount != len(ref.MatchIndices) {
		t.Fatalf("%s: match_count=%d but len(match_indices)=%d", fixture, ref.MatchCount, len(ref.MatchIndices))
	}
	if ref.Fixture != fixture {
		t.Fatalf("%s: reference fixture=%q", fixture, ref.Fixture)
	}
	return ref
}

func readGolden(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(goldenDir(t), name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.TrimSpace(string(data))
}

func goldenDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "pkg", "sigmaqueries", "testdata", "golden")
}

func TestSigmaCompatReferenceData(t *testing.T) {
	for _, fixture := range sigmacompat.FixtureNames {
		fixture := fixture
		t.Run(fixture, func(t *testing.T) {
			ref := readReference(t, fixture)
			generated, err := sigmacompat.ReferenceFor(fixture)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ref.MatchIndices, generated.MatchIndices) {
				t.Fatalf("%s reference drift\nfile:      %v\ngenerated: %v", fixture, ref.MatchIndices, generated.MatchIndices)
			}
			if ref.ReferenceSource == "" {
				t.Fatalf("%s reference_source is required", fixture)
			}
		})
	}
}

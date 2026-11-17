package ppl

import (
	"bufio"
	"os"
	"sort"
	"strings"
	"testing"
)

func loadExamples(t *testing.T) [][2]string {
	t.Helper()
	f, err := os.Open("parser/testdata/opensearch_examples.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out [][2]string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		page, q, _ := strings.Cut(line, "\t")
		out = append(out, [2]string{page, q})
	}
	return out
}

// TestOpenSearchExamplesCompile compiles every documented example and
// requires that each either compiles to a valid plan or fails with an
// explicit unsupported-feature error; any other error is a compiler gap.
func TestOpenSearchExamplesCompile(t *testing.T) {
	examples := loadExamples(t)
	exampleGaps := map[string]string{}
	data, err := os.ReadFile("parser/testdata/opensearch_example_gaps.tsv")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if parts := strings.Split(line, "\t"); len(parts) == 3 {
			exampleGaps[parts[1]] = parts[2]
		}
	}
	counts := map[string]int{}
	for _, ex := range examples {
		if _, gap := exampleGaps[ex[1]]; gap {
			counts["documented gap"]++
			continue
		}
		_, err := Compile(ex[1])
		if err == nil {
			counts["compiled"]++
			continue
		}
		pe, ok := AsError(err)
		if !ok {
			t.Errorf("%s: non-PPL error %v\n  %s", ex[0], err, ex[1])
			continue
		}
		switch pe.Kind {
		case ErrUnsupportedCommand, ErrUnsupportedFunction, ErrUnsupportedFeature:
			counts[string(pe.Kind)]++
		default:
			counts["failed"]++
			t.Errorf("%s: %v\n  %s", ex[0], err, ex[1])
		}
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%-22s %d", k, counts[k])
	}
}

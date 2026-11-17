package parser

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

type corpusEntry struct {
	page  string
	query string
}

func loadCorpus(t *testing.T) []corpusEntry {
	t.Helper()
	f, err := os.Open("testdata/opensearch_examples.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []corpusEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		page, q, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("bad corpus line %q", line)
		}
		out = append(out, corpusEntry{page: page, query: q})
	}
	return out
}

// loadGaps reads documented examples that are invalid as written, keyed by
// query, for the given stage.
func loadGaps(t *testing.T, stage string) map[string]string {
	t.Helper()
	data, err := os.ReadFile("testdata/opensearch_example_gaps.tsv")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) != 3 {
			t.Fatalf("bad gap line %q", line)
		}
		if parts[0] == stage {
			out[parts[1]] = parts[2]
		}
	}
	return out
}

func TestOpenSearchExamplesParse(t *testing.T) {
	entries := loadCorpus(t)
	corpusSyntaxGaps := loadGaps(t, "parse")
	if len(entries) < 500 {
		t.Fatalf("corpus has %d entries", len(entries))
	}
	failed := 0
	for _, e := range entries {
		_, err := Parse(e.query)
		if _, gap := corpusSyntaxGaps[e.query]; gap {
			if err == nil {
				t.Errorf("%s: listed as a syntax gap but parses: %s", e.page, e.query)
			}
			continue
		}
		if err != nil {
			failed++
			t.Errorf("%s: %v\n  %s", e.page, err, e.query)
		}
	}
	t.Logf("%d/%d examples parse", len(entries)-failed, len(entries))
}

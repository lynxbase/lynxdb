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

// corpusSyntaxGaps lists documented examples whose syntax the parser does
// not accept, each with the reason.
var corpusSyntaxGaps = map[string]string{
	"source = outer a in [ source = inner | fields b ]":                                                                             searchSubquery,
	"source = outer a not in [ source = inner | fields b ]":                                                                         searchSubquery,
	"source = outer exists [ source = inner | where a = c ]":                                                                        searchSubquery,
	"source = outer not exists [ source = inner | where a = c ]":                                                                    searchSubquery,
	"source = table as t1 exists [ source = table as t2 | where t1.a = t2.a ]":                                                      searchSubquery,
	"source = outer a > [ source = inner | stats min(c) ] | fields a":                                                               searchSubquery,
	"source = outer a = [ source = inner | where b = d | stats max(c) ]":                                                            searchSubquery,
	"source = outer [ source = inner | where outer.b = inner.d OR inner.d = 1 | stats count() ] > 0 | fields a":                     searchSubquery,
	"source = table1 | inner join left = l right = r on l.a = r.a AND r.a in [ source = inner | fields d ] | fields l.a, r.a, b, c": "the join has no right-side dataset after its condition",
}

const searchSubquery = "the official grammar has no subqueries inside search expressions; use where"

func TestOpenSearchExamplesParse(t *testing.T) {
	entries := loadCorpus(t)
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

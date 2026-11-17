package exec_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// wordyDocs returns n documents whose text and ref hold words of their own,
// enough of them for both columns of a block to get a token filter of their
// own, and whose few notes share one with the other small columns; extra
// adds words to a path of the numbered documents.
func wordyDocs(n int, seed int64, extra map[int][2]string) [][]byte {
	r := rand.New(rand.NewSource(seed))
	docs := make([][]byte, n)
	for i := range docs {
		doc := fmt.Sprintf(`{"id":%d,"ts":%d,"kind":"k%d"`, i, 1_700_000_000_000_000+int64(i)*1000, r.Intn(3))
		values := map[string]string{
			"text": fmt.Sprintf("%016x %016x", r.Uint64(), r.Uint64()),
			"ref":  fmt.Sprintf("%016x %016x", r.Uint64(), r.Uint64()),
		}
		if i%100 == 7 {
			values["note"] = fmt.Sprintf("n%d", i%7)
		}
		if e, ok := extra[i]; ok {
			values[e[0]] += " " + e[1]
		}
		for _, path := range []string{"text", "ref", "note"} {
			if v, ok := values[path]; ok {
				doc += fmt.Sprintf(`,"%s":"%s"`, path, v)
			}
		}
		docs[i] = []byte(doc + "}")
	}

	return docs
}

func TestMatchesReadTheColumnsThatMayHoldTheirTokens(t *testing.T) {
	const blockRows = 1500
	// Blocks 0 and 3 hold needle in text, blocks 2 and 5 in ref; block 3
	// holds thread in another column than needle; blocks 0 and 3 hold love
	// in a note.
	docs := wordyDocs(6*blockRows, 3, map[int][2]string{
		10: {"text", "needle thread"}, 4600: {"text", "needle"}, 4601: {"ref", "thread"},
		3100: {"ref", "needle"}, 7700: {"ref", "a needle"},
		107: {"note", "love is here"}, 5007: {"note", "love"},
	})
	rows := referenceRows(t, docs, "ts")
	ref := string(rows[3100]["ref"].Bytes)
	cases := []struct {
		name   string
		filter *exec.Expr
		// blocks is how many blocks of the layout in arrival order are
		// opened, searched and skipped how many of their columns a match of
		// every column decodes and leaves alone; -1 leaves them unchecked.
		blocks, searched, skipped int64
	}{
		{"any column", exec.MatchAny(tokenfilter.Word("needle")), 4, 4, 12},
		{"any column, any case", exec.MatchAny(tokenfilter.Word("NEEDLE").IgnoreCase()), 4, 4, 12},
		{"any column, phrase", exec.MatchAny(tokenfilter.Word("needle thread")), 1, 1, 3},
		{"any column, small columns", exec.MatchAny(tokenfilter.Word("love")), 2, 4, 4},
		{"any column, prefix", exec.MatchAny(tokenfilter.Prefix("need")), 6, 24, 0},
		{"any column, absent", exec.MatchAny(tokenfilter.Word("nowhere")), 0, 0, 0},
		{"named column", exec.Match(str("text"), tokenfilter.Word("needle")), 2, 0, 0},
		{"other named column", exec.Match(str("ref"), tokenfilter.Word("needle")), 2, 0, 0},
		{"small named column", exec.Match(str("note"), tokenfilter.Word("love")), 2, 0, 0},
		{"named column without the token", exec.Match(str("note"), tokenfilter.Word("needle")), 0, 0, 0},
		{"equality", exec.Eq(str("ref"), exec.Str(ref)), 1, 0, 0},
		{"two named columns", exec.And(exec.Match(str("text"), tokenfilter.Word("needle")), exec.Match(str("ref"), tokenfilter.Word("thread"))), 1, 0, 0},
		{"not any column", exec.Not(exec.MatchAny(tokenfilter.Word("needle"))), 6, 4, 20},
		{"any column or named", exec.Or(exec.MatchAny(tokenfilter.Word("love")), exec.Match(str("ref"), tokenfilter.Word("needle"))), 6, -1, -1},
		{"any column and named", exec.And(exec.MatchAny(tokenfilter.Word("needle")), exec.Match(str("note"), tokenfilter.Word("love"))), 2, -1, -1},
	}
	for _, l := range []layout{
		{name: "arrival", blockRows: blockRows, parts: 1},
		{name: "parts", blockRows: blockRows, parts: 3},
		{name: "ordered", blockRows: blockRows, parts: 2, sortKey: []string{"kind"}},
	} {
		tbl := buildTable(t, docs, "ts", l)
		snap := tbl.Snapshot()
		for _, c := range cases {
			t.Run(l.name+"/"+c.name, func(t *testing.T) {
				rowsPlan := &exec.Sort{
					Input: &exec.Project{Input: &exec.Scan{Filter: c.filter}, Exprs: []exec.Named{named("id", num("id"))}},
					By:    []exec.Order{{Column: "id"}},
				}
				check(t, snap, rows, rowsPlan, true, 4)
				count := &exec.Aggregate{Input: &exec.Scan{Filter: c.filter}, Aggs: []exec.Agg{{Name: "count", Func: exec.Count}}}
				s := check(t, snap, rows, count, true, 1).Stats
				if l.sortKey != nil {
					return
				}
				if s.Blocks != c.blocks || s.Blocks+s.SkippedByToken != 6 {
					t.Errorf("%d blocks opened and %d skipped by token, want %d opened", s.Blocks, s.SkippedByToken, c.blocks)
				}
				if c.searched >= 0 && (s.ColumnsSearched != c.searched || s.ColumnsSkippedByToken != c.skipped) {
					t.Errorf("%d columns searched and %d skipped by token, want %d and %d", s.ColumnsSearched, s.ColumnsSkippedByToken, c.searched, c.skipped)
				}
			})
		}
		snap.Release()
	}
}

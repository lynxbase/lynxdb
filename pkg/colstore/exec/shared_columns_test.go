package exec_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// accountID returns the identifier of account n: 32 bytes, of which 24
// differ from account to account.
func accountID(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	rng := rand.New(rand.NewPCG(uint64(n), 99))
	id := make([]byte, 24)
	for i := range id {
		id[i] = alphabet[rng.IntN(len(alphabet))]
	}

	return "did:plc:" + string(id)
}

// accountDocs returns n events of accounts whose identifiers repeat across
// the blocks of a part, in a column that every row holds and in one that a
// third of the rows hold.
func accountDocs(n int) [][]byte {
	rng := rand.New(rand.NewPCG(5, 8))
	kinds := []string{"commit", "identity", "account"}
	words := []string{"sun", "moon", "rare", "morning", "good"}
	docs := make([][]byte, n)
	for i := range docs {
		var b strings.Builder
		fmt.Fprintf(&b, `{"id":%d,"ts":%d,"kind":%q,"acct":%q`, i, 1_700_000_000_000_000+int64(i)*1000,
			kinds[rng.IntN(len(kinds))], accountID(int(rng.ExpFloat64()*200)%1500))
		if rng.IntN(3) == 0 {
			fmt.Fprintf(&b, `,"peer":%q`, accountID(rng.IntN(900)))
		}
		fmt.Fprintf(&b, `,"n":%d,"msg":"%s %s"}`, rng.IntN(50), words[rng.IntN(len(words))], words[rng.IntN(len(words))])
		docs[i] = []byte(b.String())
	}

	return docs
}

// sharedBlocks counts the blocks of the parts of snap that store the string
// column at path as codes of a part dictionary.
func sharedBlocks(t *testing.T, snap *colstore.Snapshot, path string) int {
	t.Helper()
	n := 0
	var b part.Block
	for _, h := range snap.Parts() {
		id, ok := h.PathID(path)
		if !ok {
			continue
		}
		for i := 0; i < h.NumBlocks(); i++ {
			if err := h.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			if ref, ok := b.Column(id, vec.KindString); ok && ref.Shared() {
				n++
			}
		}
	}

	return n
}

// TestSharedColumnsAnswerAsOthersDo runs queries that read a column in every
// way the executor has over a table whose parts store it as codes of part
// dictionaries, and over the same rows stored block by block: the answers
// are the same.
func TestSharedColumnsAnswerAsOthersDo(t *testing.T) {
	docs := accountDocs(14000)
	some, other := accountID(3), accountID(700)
	word := some[len("did:plc:"):]
	count := []exec.Agg{{Name: "rows", Func: exec.Count}}
	plans := map[string]exec.Node{
		"equal":           &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(str("acct"), exec.Str(some))}, Aggs: count},
		"equal, the rows": &exec.Project{Input: &exec.Scan{Filter: exec.Eq(anyOf("acct"), exec.Str(some))}, Exprs: []exec.Named{named("id", num("id")), named("acct", str("acct")), named("peer", str("peer"))}},
		"equal to no account": &exec.Aggregate{Input: &exec.Scan{Filter: exec.Eq(str("acct"), exec.Str("did:plc:nobodynobodynobodynobody"))},
			Aggs: count},
		"equal in a column with gaps": &exec.Project{Input: &exec.Scan{Filter: exec.Eq(str("peer"), exec.Str(other))}, Exprs: []exec.Named{named("id", num("id"))}},
		"not equal":                   &exec.Aggregate{Input: &exec.Scan{Filter: exec.Ne(str("acct"), exec.Str(some))}, Aggs: count},
		"below":                       &exec.Aggregate{Input: &exec.Scan{Filter: exec.Lt(str("acct"), exec.Str("did:plc:m"))}, Aggs: count},
		"at or above":                 &exec.Aggregate{Input: &exec.Scan{Filter: exec.Ge(str("peer"), exec.Str(other))}, Aggs: count},
		"a word of any field":         &exec.Aggregate{Input: &exec.Scan{Filter: exec.MatchAny(tokenfilter.Word(word))}, Aggs: count},
		"a word of the column":        &exec.Project{Input: &exec.Scan{Filter: exec.Match(str("acct"), tokenfilter.Word(word))}, Exprs: []exec.Named{named("id", num("id"))}},
		"a prefix":                    &exec.Aggregate{Input: &exec.Scan{Filter: exec.Match(str("peer"), tokenfilter.Prefix(other[8:14]))}, Aggs: count},
		"a part of a value":           &exec.Aggregate{Input: &exec.Scan{Filter: exec.Match(anyOf("acct"), tokenfilter.Substring(some[12:20]))}, Aggs: count},
		"another column's word":       &exec.Aggregate{Input: &exec.Scan{Filter: exec.MatchAny(tokenfilter.Word("rare morning"))}, Keys: keys("acct"), Aggs: count},
		"rows by account":             &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("acct"), Aggs: count},
		"rows by peer":                &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("peer"), Aggs: count},
		"rows by kind and account":    &exec.Aggregate{Input: &exec.Scan{Filter: exec.Lt(num("n"), exec.Int(25))}, Keys: keys("kind", "acct"), Aggs: count},
		"sums by account": &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("acct"),
			Aggs: []exec.Agg{{Name: "sum", Func: exec.Sum, Arg: num("n")}, {Name: "peers", Func: exec.CountDistinct, Arg: str("peer")}}},
		"accounts": &exec.Aggregate{Input: &exec.Scan{}, Aggs: []exec.Agg{
			{Name: "distinct", Func: exec.CountDistinct, Arg: str("acct")}, {Name: "least", Func: exec.Min, Arg: str("acct")},
			{Name: "most", Func: exec.Max, Arg: str("peer")}, {Name: "held", Func: exec.Count, Arg: str("peer")}}},
		"accounts by kind": &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind"), Aggs: []exec.Agg{
			{Name: "distinct", Func: exec.CountDistinct, Arg: str("acct")}, {Name: "most", Func: exec.Max, Arg: str("acct")}}},
		"the first rows": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("id", num("id")), named("acct", str("acct")), named("peer", anyOf("peer"))}},
			By: []exec.Order{{Column: "id"}}, Limit: 40},
		"the last rows, whole": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: exec.Gt(num("n"), exec.Int(40))}, Rest: true}, Limit: 25},
		"rows in the order of the column": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("acct", str("acct")), named("id", num("id"))}},
			By: []exec.Order{{Column: "acct"}, {Column: "id"}}, Limit: 60},
		"rows in the reverse order of a column with gaps": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("peer", str("peer")), named("id", num("id"))}},
			By: []exec.Order{{Column: "peer", Desc: true}, {Column: "id"}}, Limit: 60},
	}
	for _, l := range []layout{
		{name: "arrival", blockRows: 4096, parts: 1},
		{name: "ordered", blockRows: 4096, parts: 1, sortKey: []string{"kind"}},
	} {
		plain := buildTable(t, docs, "ts", l)
		l.shared = true
		shared := buildTable(t, docs, "ts", l)
		t.Run(l.name, func(t *testing.T) {
			was, is := plain.Snapshot(), shared.Snapshot()
			defer was.Release()
			defer is.Release()
			for _, path := range []string{"acct", "peer"} {
				if sharedBlocks(t, was, path) != 0 || sharedBlocks(t, is, path) == 0 {
					t.Fatalf("%s: %d blocks of the plain table and %d of the other store codes", path, sharedBlocks(t, was, path), sharedBlocks(t, is, path))
				}
			}
			for name, plan := range plans {
				for _, workers := range []int{1, 4} {
					answer := func(snap *colstore.Snapshot) []string {
						res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: workers})
						if err != nil {
							t.Fatalf("%s: %v", name, err)
						}
						rows := namedRows(res)
						if _, sorted := plan.(*exec.Sort); !sorted {
							slices.Sort(rows)
						}

						return rows
					}
					want, got := answer(was), answer(is)
					if len(want) == 0 && !strings.Contains(name, "no account") {
						t.Fatalf("%s: no rows, so the query tests nothing", name)
					}
					if !slices.Equal(got, want) {
						t.Fatalf("%s with %d workers: %d rows, want %d; first %v, want %v", name, workers, len(got), len(want), first(got), first(want))
					}
				}
			}
		})
	}
}

// first returns the first rows of a result, for a message.
func first(rows []string) []string { return rows[:min(len(rows), 3)] }

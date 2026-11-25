package exec_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// codedPart says how the rows of one part of a table of codedTable are
// written.
type codedPart struct {
	// shared gives the part dictionaries for the columns that repeat.
	shared bool
	// first is the number of rows of a first block too small for the writer
	// to store them as codes, so that the part holds blocks of both forms.
	first int
	// unfiltered leaves the part without token filters, so that nothing
	// but its rows and its dictionaries tells what a search finds.
	unfiltered bool
}

// codedDocs returns n events of accounts drawn from so many: acct is in
// every row, peer in a third of the rows and few is an account of forty
// times fewer; kind, n and ts vary beside them.
func codedDocs(n, accounts int) [][]byte {
	rng := rand.New(rand.NewPCG(11, uint64(accounts)))
	kinds := []string{"commit", "identity", "account"}
	docs := make([][]byte, n)
	for i := range docs {
		var b strings.Builder
		fmt.Fprintf(&b, `{"id":%d,"ts":%d,"kind":%q,"acct":%q,"few":%q`, i, 1_700_000_000_000_000+int64(i)*1000,
			kinds[rng.IntN(len(kinds))], accountID(int(rng.ExpFloat64()*float64(accounts)/6)%accounts), accountID(rng.IntN(accounts/40+1)))
		if rng.IntN(3) == 0 {
			fmt.Fprintf(&b, `,"peer":%q`, accountID(rng.IntN(accounts)))
		}
		fmt.Fprintf(&b, `,"n":%d}`, rng.IntN(50))
		docs[i] = []byte(b.String())
	}

	return docs
}

// codedTable writes docs as one part per entry of parts, the same number of
// rows in each, in blocks of blockRows rows.
func codedTable(t testing.TB, docs [][]byte, blockRows int, parts []codedPart) *colstore.Table {
	t.Helper()
	var handles []*colstore.Handle
	per := (len(docs) + len(parts) - 1) / len(parts)
	for i, cp := range parts {
		var buf bytes.Buffer
		w := part.NewWriter(&buf, part.Options{NoTokenFilter: cp.unfiltered})
		all := shred.NewBatch(shred.Options{TimeField: "ts"})
		for _, doc := range docs[min(i*per, len(docs)):min((i+1)*per, len(docs))] {
			if err := all.AddJSON(doc, 0); err != nil {
				t.Fatalf("%v in %s", err, doc)
			}
		}
		if cp.shared {
			w.Share([]shred.Run{{Batch: all, From: 0, To: all.Rows}})
		}
		block := shred.NewBatch(shred.Options{})
		for row, rows := 0, cp.first; row < all.Rows; row, rows = row+rows, blockRows {
			if rows == 0 {
				rows = blockRows
			}
			block.Reset()
			all.CopyRows(block, row, min(row+rows, all.Rows))
			if err := w.WriteBatch(block); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := part.Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, colstore.NewHandle(p, nil))
	}
	tbl := colstore.NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tbl.Close)

	return tbl
}

// blockForms counts the blocks of snap that store the string column at path
// as codes of a part dictionary and those that store its strings.
func blockForms(t *testing.T, snap *colstore.Snapshot, path string) (codes, strs int) {
	t.Helper()
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
			switch ref, ok := b.Column(id, vec.KindString); {
			case !ok:
			case ref.Shared():
				codes++
			default:
				strs++
			}
		}
	}

	return codes, strs
}

// codedPlan is a plan and the column by whose codes the executor is to
// aggregate the blocks that store it as codes of a part dictionary: nothing
// when it is to read strings, and a question mark when it may do either.
type codedPlan struct {
	name string
	plan exec.Node
	by   string
}

func codedPlans() []codedPlan {
	count := []exec.Agg{{Name: "rows", Func: exec.Count}}
	scan, some := &exec.Scan{}, &exec.Scan{Filter: exec.Lt(num("n"), exec.Int(20))}
	sums := []exec.Agg{
		{Name: "rows", Func: exec.Count}, {Name: "sum", Func: exec.Sum, Arg: num("n")}, {Name: "least", Func: exec.Min, Arg: num("n")},
		{Name: "most", Func: exec.Max, Arg: num("n")}, {Name: "mean", Func: exec.Avg, Arg: num("n")}, {Name: "peers", Func: exec.Count, Arg: str("peer")},
	}
	distinct := func(path string) []exec.Agg {
		return []exec.Agg{{Name: "rows", Func: exec.Count}, {Name: "distinct", Func: exec.CountDistinct, Arg: str(path)}}
	}
	top := func(in exec.Node, by string) exec.Node {
		return &exec.Sort{Input: in, By: []exec.Order{{Column: by, Desc: true}, {Column: "acct"}}, Limit: 12}
	}

	return []codedPlan{
		{"rows by account", &exec.Aggregate{Input: scan, Keys: keys("acct"), Aggs: count}, "acct"},
		{"rows by an account of few", &exec.Aggregate{Input: scan, Keys: keys("few"), Aggs: count}, ""},
		{"rows by account, a string", &exec.Aggregate{Input: scan, Keys: []exec.Named{named("acct", str("acct"))}, Aggs: count}, "acct"},
		{"rows by a column with gaps", &exec.Aggregate{Input: scan, Keys: keys("peer"), Aggs: count}, "peer"},
		{"some rows by account", &exec.Aggregate{Input: some, Keys: keys("acct"), Aggs: count}, "acct"},
		{"some rows by a column with gaps", &exec.Aggregate{Input: some, Keys: keys("peer"), Aggs: sums}, "peer"},
		{"sums by account", &exec.Aggregate{Input: scan, Keys: keys("acct"), Aggs: sums}, "acct"},
		{"times by account", &exec.Aggregate{Input: some, Keys: keys("acct"), Aggs: []exec.Agg{
			{Name: "tmin", Func: exec.Min, Arg: exec.Time()}, {Name: "tmax", Func: exec.Max, Arg: exec.Time()}}}, "acct"},
		{"picks by account", &exec.Aggregate{Input: scan, Keys: keys("acct"), Aggs: []exec.Agg{
			{Name: "first", Func: exec.First, Arg: num("id")}, {Name: "latest", Func: exec.Latest, Arg: str("kind")},
			{Name: "peer", Func: exec.Earliest, Arg: str("peer")}, {Name: "top", Func: exec.ArgMax, Arg: num("id"), By: num("n")}}}, "acct"},
		{"rows by kind and account", &exec.Aggregate{Input: scan, Keys: keys("kind", "acct"), Aggs: count}, "acct"},
		{"sums by account and kind", &exec.Aggregate{Input: some, Keys: keys("acct", "kind"), Aggs: sums}, "acct"},
		{"rows by kind, account and a number", &exec.Aggregate{Input: scan,
			Keys: []exec.Named{named("kind", anyOf("kind")), named("acct", anyOf("acct")), named("fifth", exec.Mod(num("n"), exec.Int(5)))}, Aggs: count}, "acct"},
		{"rows by two accounts", &exec.Aggregate{Input: scan, Keys: keys("acct", "peer"), Aggs: count}, ""},
		{"rows by account and one of few", &exec.Aggregate{Input: scan, Keys: keys("few", "acct"), Aggs: count}, "acct"},
		{"rows by account and id", &exec.Aggregate{Input: some, Keys: keys("acct", "id"), Aggs: count}, ""},
		{"the accounts with most rows", top(&exec.Aggregate{Input: scan, Keys: keys("acct"), Aggs: count}, "rows"), "acct"},
		{"the accounts with the greatest sums", top(&exec.Aggregate{Input: some, Keys: keys("acct"), Aggs: sums}, "sum"), "acct"},
		{"the accounts of many rows", &exec.Filter{Input: &exec.Aggregate{Input: scan, Keys: keys("acct"), Aggs: count},
			Cond: exec.Gt(exec.Col("rows", vec.KindInt), exec.Int(8))}, "acct"},
		{"distinct accounts", &exec.Aggregate{Input: scan, Aggs: distinct("acct")}, "acct"},
		{"distinct accounts of some rows", &exec.Aggregate{Input: some, Aggs: distinct("acct")}, "acct"},
		{"distinct values of a column with gaps", &exec.Aggregate{Input: some, Aggs: []exec.Agg{
			{Name: "distinct", Func: exec.CountDistinct, Arg: str("peer")}, {Name: "held", Func: exec.Count, Arg: str("peer")}}}, "peer"},
		{"distinct values of two columns", &exec.Aggregate{Input: some, Aggs: []exec.Agg{
			{Name: "distinct", Func: exec.CountDistinct, Arg: str("peer")}, {Name: "held", Func: exec.Count, Arg: str("peer")},
			{Name: "accounts", Func: exec.CountDistinct, Arg: anyOf("acct")}}}, "acct"},
		{"distinct accounts by kind", &exec.Aggregate{Input: scan, Keys: keys("kind"), Aggs: distinct("acct")}, "acct"},
		{"distinct accounts by kind and a number", &exec.Aggregate{Input: some,
			Keys: []exec.Named{named("kind", anyOf("kind")), named("n", num("n"))}, Aggs: distinct("peer")}, "peer"},
		{"distinct accounts by one of few", &exec.Aggregate{Input: scan, Keys: keys("few"), Aggs: distinct("acct")}, "acct"},
		{"distinct accounts by account", &exec.Aggregate{Input: scan, Keys: keys("peer"), Aggs: distinct("acct")}, "?"},
		{"the least account by kind", &exec.Aggregate{Input: scan, Keys: keys("kind"), Aggs: []exec.Agg{
			{Name: "distinct", Func: exec.CountDistinct, Arg: str("acct")}, {Name: "least", Func: exec.Min, Arg: str("acct")}}}, ""},
		{"the least kind by account", &exec.Aggregate{Input: scan, Keys: keys("acct"), Aggs: []exec.Agg{
			{Name: "least", Func: exec.Min, Arg: str("kind")}, {Name: "about", Func: exec.ApproxDistinct, Arg: num("n")}}}, ""},
		{"values held by account", &exec.Aggregate{Input: scan, Keys: keys("acct"), Aggs: []exec.Agg{
			{Name: "held", Func: exec.Count, Arg: str("peer")}, {Name: "accounts", Func: exec.Count, Arg: str("acct")}}}, "acct"},
	}
}

// countsDistinct reports whether plan counts the distinct values of the
// column at path.
func countsDistinct(plan exec.Node, path string) bool {
	agg, ok := plan.(*exec.Aggregate)
	if !ok {
		return false
	}

	return slices.ContainsFunc(agg.Aggs, func(a exec.Agg) bool { return a.Func == exec.CountDistinct && a.Arg.Path == path })
}

// TestCodesAggregateAsStringsDo runs aggregations over tables whose parts
// store the columns of accounts as codes of part dictionaries, in every
// block, in all but the first of a part, and in some parts only, and over
// the same rows stored block by block: the answers are the same, and the
// queries that can be are aggregated by the codes.
func TestCodesAggregateAsStringsDo(t *testing.T) {
	for _, shape := range []struct {
		name     string
		rows     int
		accounts int
		block    int
		parts    []codedPart
	}{
		{"every block of two parts", 40000, 2500, 4096, []codedPart{{shared: true}, {shared: true}}},
		{"a part of strings between two of codes", 60000, 2500, 4096, []codedPart{{shared: true}, {}, {shared: true}}},
		{"parts that begin with a block of strings", 40000, 2500, 4096, []codedPart{{shared: true, first: 700}, {shared: true, first: 300}}},
		{"few accounts", 40000, 900, 2048, []codedPart{{shared: true}, {}}},
		{"small blocks", 16000, 2800, 1024, []codedPart{{shared: true}}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			docs := codedDocs(shape.rows, shape.accounts)
			plain := codedTable(t, docs, shape.block, make([]codedPart, len(shape.parts)))
			coded := codedTable(t, docs, shape.block, shape.parts)
			was, is := plain.Snapshot(), coded.Snapshot()
			defer was.Release()
			defer is.Release()
			if codes, _ := blockForms(t, was, "acct"); codes != 0 {
				t.Fatalf("%d blocks of the plain table store codes", codes)
			}
			codes, strs := blockForms(t, is, "acct")
			if mixed := slices.ContainsFunc(shape.parts, func(p codedPart) bool { return !p.shared || p.first != 0 }); codes == 0 || mixed != (strs != 0) {
				t.Fatalf("%d blocks store codes of accounts and %d their strings", codes, strs)
			}
			for _, limits := range []struct {
				name                         string
				table, serial, budget, codes int
			}{
				{"as set", 0, 0, 0, 0},
				{"groups handed on at once", 64, 16, 1 << 20, 8},
				{"no memory to wait in", 64, 16, 1 << 10, 8},
			} {
				if limits.budget != 0 {
					defer exec.SetLimits(limits.table, limits.serial)()
					defer exec.SetStageBudget(limits.budget)()
					defer exec.SetRangeGroups(limits.codes)()
				}
				for _, p := range codedPlans() {
					for _, workers := range []int{1, 5} {
						answer := func(snap *colstore.Snapshot) ([]string, *exec.Result) {
							res, err := exec.Run(context.Background(), snap, p.plan, exec.Options{Workers: workers})
							if err != nil {
								t.Fatalf("%s: %v", p.name, err)
							}
							rows := namedRows(res)
							if _, sorted := p.plan.(*exec.Sort); !sorted {
								slices.Sort(rows)
							}

							return rows, res
						}
						want, _ := answer(was)
						got, res := answer(is)
						if len(want) == 0 {
							t.Fatalf("%s: no rows, so the query tests nothing", p.name)
						}
						if !slices.Equal(got, want) {
							t.Fatalf("%s, %s, with %d workers: %d rows, want %d; first %v, want %v", p.name, limits.name, workers, len(got), len(want), first(got), first(want))
						}
						// With no memory to wait in, the bits that tell the
						// codes of a group are more than a part may hold.
						if p.by == "?" || limits.budget == 1<<10 {
							continue
						}
						// The codes of a key of few accounts go through a
						// table, as the entries of a block's own dictionary do.
						coded := p.by != "" && (shape.accounts > 1024 || countsDistinct(p.plan, p.by))
						if coded {
							stored, _ := blockForms(t, is, p.by)
							coded = stored > 0
						}
						if byCode := res.Stats.AggregatedByCode; coded != (byCode > 0) {
							t.Fatalf("%s, %s, with %d workers: %d blocks were aggregated by code", p.name, limits.name, workers, byCode)
						}
					}
				}
			}
		})
	}
}

// TestCodesOfAQueryThatStops ends aggregations by code at every stage of
// their scans: each returns its rows or the reason it stopped, and the
// queries that follow answer as if none had.
func TestCodesOfAQueryThatStops(t *testing.T) {
	docs := codedDocs(60000, 2500)
	tbl := codedTable(t, docs, 2048, []codedPart{{shared: true}, {shared: true}, {shared: true}})
	snap := tbl.Snapshot()
	defer snap.Release()
	defer exec.SetStageBudget(1 << 16)()
	defer exec.SetRangeGroups(64)()
	plans := []exec.Node{
		&exec.Aggregate{Input: &exec.Scan{}, Keys: keys("acct"), Aggs: []exec.Agg{{Name: "rows", Func: exec.Count}}},
		&exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind", "acct"), Aggs: []exec.Agg{{Name: "sum", Func: exec.Sum, Arg: num("n")}}},
		&exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind"), Aggs: []exec.Agg{{Name: "distinct", Func: exec.CountDistinct, Arg: str("acct")}}},
	}
	var want [][]string
	for _, plan := range plans {
		res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 4})
		if err != nil {
			t.Fatal(err)
		}
		rows := namedRows(res)
		slices.Sort(rows)
		want = append(want, rows)
	}
	stopped := 0
	for round := 0; round < 60; round++ {
		i := round % len(plans)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Duration(round*40) * time.Microsecond)
			cancel()
		}()
		res, err := exec.Run(ctx, snap, plans[i], exec.Options{Workers: 4})
		cancel()
		switch {
		case errors.Is(err, context.Canceled):
			stopped++
		case err != nil:
			t.Fatalf("round %d: %v", round, err)
		default:
			rows := namedRows(res)
			slices.Sort(rows)
			if !slices.Equal(rows, want[i]) {
				t.Fatalf("round %d: %d rows, want %d", round, len(rows), len(want[i]))
			}
		}
	}
	if stopped == 0 {
		t.Fatal("no query was stopped")
	}
}

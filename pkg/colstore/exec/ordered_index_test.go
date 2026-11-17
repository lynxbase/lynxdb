package exec_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// keyedDocs returns n documents whose kind, op, coll and user repeat over
// runs that are long next to a small block. With odd set, some rows lack a
// coll or a user, or hold a number or a null there.
func keyedDocs(n int, seed int64, odd bool) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	colls := []string{"like", "like", "like", "like", "follow", "follow", "post", "block"}
	docs := make([][]byte, n)
	ts := int64(1_700_000_000_000_000)
	for i := range docs {
		var b strings.Builder
		ts += int64(rng.Intn(1_000_000))
		kind, op := "commit", "create"
		if rng.Intn(10) == 0 {
			kind = "identity"
		}
		if rng.Intn(5) == 0 {
			op = "delete"
		}
		fmt.Fprintf(&b, `{"ts":%d,"kind":%q,"op":%q,"n":%d`, ts, kind, op, rng.Intn(50))
		coll := colls[rng.Intn(len(colls))]
		users := 12
		if coll == "follow" {
			users = 400
		}
		switch r := rng.Intn(100); {
		case odd && r < 2:
			fmt.Fprintf(&b, `,"coll":%d`, rng.Intn(3))
		case odd && r < 3:
		case odd && r < 4:
			b.WriteString(`,"coll":null`)
		default:
			fmt.Fprintf(&b, `,"coll":%q`, coll)
		}
		switch r := rng.Intn(100); {
		case odd && r < 2:
			fmt.Fprintf(&b, `,"user":%d`, rng.Intn(5))
		case odd && r < 4:
		case odd && r < 5:
			b.WriteString(`,"user":null`)
		case odd && r < 6:
			fmt.Fprintf(&b, `,"user":%v`, rng.Intn(2) == 0)
		default:
			fmt.Fprintf(&b, `,"user":"u%03d"`, rng.Intn(users))
		}
		b.WriteString("}")
		docs[i] = []byte(b.String())
	}

	return docs
}

type indexTable struct {
	name string
	snap *colstore.Snapshot
	// oneStream is set when the parts follow one another in key order.
	oneStream bool
}

// indexTables builds docs into tables ordered by key: one part, parts merged
// into one stream, overlapping parts, and one block.
func indexTables(t *testing.T, docs [][]byte, key []string) []indexTable {
	t.Helper()
	merged := mergedTable(t, bytes.Join(docs, []byte("\n")), 4, key, "ts", 32)
	t.Cleanup(merged.Close)
	tables := []indexTable{
		{"one part", buildTable(t, docs, "ts", layout{blockRows: 32, parts: 1, sortKey: key}).Snapshot(), true},
		{"merged parts", merged.Snapshot(), true},
		{"overlapping parts", buildTable(t, docs, "ts", layout{blockRows: 32, parts: 3, sortKey: key}).Snapshot(), false},
		{"one block", buildTable(t, docs, "ts", layout{blockRows: 1 << 16, parts: 1, sortKey: key}).Snapshot(), true},
	}
	for _, tbl := range tables {
		t.Cleanup(tbl.snap.Release)
	}

	return tables
}

func TestCountsFromThePartIndex(t *testing.T) {
	eq := func(path, v string) *exec.Expr { return exec.Eq(str(path), exec.Str(v)) }
	created := exec.And(eq("kind", "commit"), eq("op", "create"))
	likes := exec.And(eq("kind", "commit"), eq("op", "create"), eq("coll", "like"))
	count := exec.Agg{Name: "rows", Func: exec.Count}
	users := exec.Agg{Name: "users", Func: exec.CountDistinct, Arg: str("user")}
	first := exec.Agg{Name: "first", Func: exec.Min, Arg: exec.Time()}
	type query struct {
		name string
		plan exec.Node
		// index is set when blocks inside one group can come from the index.
		index bool
	}
	queries := []query{
		{"rows and users", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: []exec.Agg{count, users}}, true},
		{"users", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: []exec.Agg{users}}, true},
		{"users of one group", &exec.Aggregate{Input: &exec.Scan{Filter: likes}, Aggs: []exec.Agg{users, count}}, true},
		{"rows of the last key", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("user", "coll"), Aggs: []exec.Agg{count}}, true},
		{"sorted", &exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: []exec.Agg{count, users}},
			By: []exec.Order{{Column: "users", Desc: true}, {Column: "coll"}}, Limit: 3}, true},
		{"another aggregate", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"), Aggs: []exec.Agg{count, users, first}}, false},
		{"a count of values", &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: keys("coll"),
			Aggs: []exec.Agg{users, {Name: "values", Func: exec.Count, Arg: str("user")}}}, false},
		{"a filter on rows", &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(created, exec.Gt(num("n"), exec.Int(20)))},
			Keys: keys("coll"), Aggs: []exec.Agg{count, users}}, false},
	}
	key := []string{"kind", "op", "coll", "user"}
	for _, odd := range []bool{false, true} {
		docs := keyedDocs(9000, 3, odd)
		rows := referenceRows(t, docs, "ts")
		for _, tbl := range indexTables(t, docs, key) {
			for _, q := range queries {
				for _, workers := range []int{1, 5} {
					t.Run(fmt.Sprintf("odd=%v/%s/%s/%d", odd, tbl.name, q.name, workers), func(t *testing.T) {
						_, sorted := q.plan.(*exec.Sort)
						res := check(t, tbl.snap, rows, q.plan, sorted, workers)
						if odd {
							return
						}
						if !res.Stats.Ordered {
							t.Fatal("not aggregated in key order")
						}
						want := q.index && tbl.oneStream && tbl.name != "one block"
						if got := res.Stats.CountedByIndex > 0; got != want {
							t.Fatalf("%d blocks counted from the index, want some: %v", res.Stats.CountedByIndex, want)
						}
						if res.Stats.CountedByIndex+res.Stats.Blocks > blocksOf(tbl.snap) {
							t.Fatalf("%d blocks counted and %d opened of %d", res.Stats.CountedByIndex, res.Stats.Blocks, blocksOf(tbl.snap))
						}
					})
				}
			}
		}
	}
}

func blocksOf(snap *colstore.Snapshot) int64 {
	n := 0
	for _, h := range snap.Parts() {
		n += h.NumBlocks()
	}

	return int64(n)
}

// TestIndexCountsOfAShortKey covers a sort key that ends with the group key,
// where a block inside one group needs no row at all.
func TestIndexCountsOfAShortKey(t *testing.T) {
	docs := keyedDocs(6000, 11, false)
	rows := referenceRows(t, docs, "ts")
	plan := &exec.Aggregate{
		Input: &exec.Scan{Filter: exec.And(exec.Eq(str("kind"), exec.Str("commit")), exec.Eq(str("op"), exec.Str("create")))},
		Keys:  keys("coll"),
		Aggs:  []exec.Agg{{Name: "rows", Func: exec.Count}},
	}
	for _, tbl := range indexTables(t, docs, []string{"kind", "op", "coll"}) {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/%d", tbl.name, workers), func(t *testing.T) {
				res := check(t, tbl.snap, rows, plan, false, workers)
				if !res.Stats.Ordered {
					t.Fatal("not aggregated in key order")
				}
				if want := tbl.oneStream && tbl.name != "one block"; (res.Stats.CountedByIndex > 0) != want {
					t.Fatalf("%d blocks counted from the index, want some: %v", res.Stats.CountedByIndex, want)
				}
			})
		}
	}
}

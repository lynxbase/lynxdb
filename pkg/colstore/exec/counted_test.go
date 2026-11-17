package exec_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestCountByAFieldOfFewValues requires the count of rows by a field of few
// values to be exact where blocks answer it from the rows their
// dictionaries hold and where they do not: with every row holding the
// field and with rows that lack it, over all rows, over a range of time
// that cuts through blocks, and with a second aggregate.
func TestCountByAFieldOfFewValues(t *testing.T) {
	const rows = 7000
	rng := rand.New(rand.NewPCG(31, 32))
	type doc struct {
		level, zone string
		n           int
	}
	docs := make([]doc, rows)
	raw := make([][]byte, rows)
	for i := range docs {
		d := &docs[i]
		d.n = i
		d.level = []string{"debug", "info", "info", "info", "warn", "error"}[rng.IntN(6)]
		line := fmt.Sprintf(`{"n":%d,"ts":%d,"level":%q`, i, 1700000000000000+int64(i)*1000, d.level)
		if i%5 != 2 {
			d.zone = fmt.Sprintf("zone-%02d", rng.IntN(1+rng.IntN(40)))
			line += fmt.Sprintf(`,"zone":%q`, d.zone)
		}
		raw[i] = []byte(line + "}")
	}
	type want map[string]int64
	list := func(w want) []string {
		var out []string
		for k, v := range w {
			out = append(out, fmt.Sprintf("%s=%d", k, v))
		}
		slices.Sort(out)

		return out
	}
	for _, l := range layouts {
		t.Run(l.name, func(t *testing.T) {
			tbl := buildTable(t, raw, "ts", l)
			snap := tbl.Snapshot()
			defer snap.Release()
			for _, c := range []struct {
				name   string
				field  string
				get    func(*doc) string
				from   int
				to     int
				second bool
			}{
				{"level over all rows", "level", func(d *doc) string { return d.level }, 0, rows, false},
				{"zone over all rows", "zone", func(d *doc) string { return d.zone }, 0, rows, false},
				{"zone in a range of time", "zone", func(d *doc) string { return d.zone }, 333, 5111, false},
				{"level in a range of time", "level", func(d *doc) string { return d.level }, 1000, 1001, false},
				{"zone with a sum", "zone", func(d *doc) string { return d.zone }, 0, rows, true},
			} {
				expect, sums := want{}, want{}
				for i := c.from; i < c.to; i++ {
					k := c.get(&docs[i])
					expect[k]++
					sums[k] += int64(docs[i].n)
				}
				var filter *exec.Expr
				if c.from != 0 || c.to != rows {
					lo, hi := 1700000000000000+int64(c.from)*1000, 1700000000000000+int64(c.to)*1000
					filter = exec.And(exec.Ge(num("ts"), exec.Int(lo)), exec.Lt(num("ts"), exec.Int(hi)))
				}
				aggs := []exec.Agg{{Name: "rows", Func: exec.Count}}
				if c.second {
					aggs = append(aggs, exec.Agg{Name: "sum", Func: exec.Sum, Arg: num("n")})
				}
				plan := &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Keys: []exec.Named{named("k", anyOf(c.field))}, Aggs: aggs}
				res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 3})
				if err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				got, gotSums := want{}, want{}
				for i := 0; i < res.Rows; i++ {
					row := res.Row(i, nil)
					key := ""
					if row[0].Kind == vec.KindString {
						key = string(row[0].Bytes)
					}
					got[key] = row[1].Int
					if c.second {
						gotSums[key] = row[2].Int
					}
				}
				if !slices.Equal(list(got), list(expect)) {
					t.Fatalf("%s: counts %v, want %v", c.name, list(got), list(expect))
				}
				if c.second && !slices.Equal(list(gotSums), list(sums)) {
					t.Fatalf("%s: sums %v, want %v", c.name, list(gotSums), list(sums))
				}
			}
		})
	}
}

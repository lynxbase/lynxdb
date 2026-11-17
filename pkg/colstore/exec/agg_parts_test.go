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

// TestCountByAFieldComesFromThePartsThatKnowIt requires the count of rows
// by a field to be exact and to open no block of a part that knows the
// rows of the field's values and lies within the times asked for, and to
// read the blocks of the parts that do not: for a field with a null, with
// values of two kinds or with many values, and for a part that the times
// cut through.
func TestCountByAFieldComesFromThePartsThatKnowIt(t *testing.T) {
	const (
		rows  = 6000
		start = int64(1700000000000000)
	)
	rng := rand.New(rand.NewPCG(51, 52))
	fields := []string{"level", "zone", "nullable", "mixed", "id"}
	values := make([]map[string]string, rows)
	raw := make([][]byte, rows)
	for i := range raw {
		v := map[string]string{"level": []string{"debug", "info", "info", "warn"}[rng.IntN(4)], "id": fmt.Sprintf("id-%05d", i)}
		line := fmt.Sprintf(`{"ts":%d,"level":%q,"id":%q`, start+int64(i)*1000, v["level"], v["id"])
		if i%4 != 1 {
			v["zone"] = fmt.Sprintf("zone-%d", rng.IntN(7))
			line += fmt.Sprintf(`,"zone":%q`, v["zone"])
		}
		switch {
		case i%7 == 0:
			line += `,"nullable":null`
		case i%3 == 0:
			v["nullable"] = "set"
			line += `,"nullable":"set"`
		}
		switch i % 3 {
		case 0:
			v["mixed"] = "text"
			line += `,"mixed":"text"`
		case 1:
			v["mixed"] = "7"
			line += `,"mixed":7`
		}
		values[i], raw[i] = v, []byte(line+"}")
	}
	list := func(m map[string]int64) []string {
		var out []string
		for k, v := range m {
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
			per := (rows + l.parts - 1) / l.parts
			for _, c := range []struct {
				name     string
				from, to int
			}{
				{"all rows", 0, rows},
				{"the rows of the first part", 0, per},
				{"rows that end within a part", 0, per + per/2},
				{"rows within a part", 10, per - 10},
				{"no rows", rows, rows + 5},
			} {
				lo, hi := (start+int64(c.from)*1000)*1000, (start+int64(c.to)*1000)*1000
				// within counts the blocks of the parts that the times
				// hold whole.
				blocks, within := int64(0), int64(0)
				for _, h := range snap.Parts() {
					blocks += int64(h.NumBlocks())
					if h.MinTime >= lo && h.MaxTime < hi {
						within += int64(h.NumBlocks())
					}
				}
				for _, field := range fields {
					expect := map[string]int64{}
					for i := c.from; i < min(c.to, rows); i++ {
						expect[values[i][field]]++
					}
					var filter *exec.Expr
					if c.from != 0 || c.to != rows {
						filter = exec.And(exec.Ge(exec.Time(), exec.Int(lo)), exec.Lt(exec.Time(), exec.Int(hi)))
					}
					plan := &exec.Aggregate{
						Input: &exec.Scan{Filter: filter},
						Keys:  []exec.Named{named("k", anyOf(field))},
						Aggs:  []exec.Agg{{Name: "rows", Func: exec.Count}},
					}
					res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 3, Forgetful: true})
					if err != nil {
						t.Fatalf("%s by %s: %v", c.name, field, err)
					}
					got := map[string]int64{}
					for i := 0; i < res.Rows; i++ {
						row := res.Row(i, nil)
						// A row shows a null and a missing key alike.
						key := ""
						switch row[0].Kind {
						case vec.KindString:
							key = string(row[0].Bytes)
						case vec.KindInt:
							key = fmt.Sprint(row[0].Int)
						}
						got[key] += row[1].Int
					}
					if !slices.Equal(list(got), list(expect)) {
						t.Fatalf("%s by %s: counts %v, want %v", c.name, field, list(got), list(expect))
					}
					want := int64(0)
					if field == "level" || field == "zone" {
						want = within
					}
					if counted := res.Stats.CountedByIndex; counted != want || res.Stats.Blocks > blocks-want {
						t.Fatalf("%s by %s: %d blocks counted by their parts and %d read, want %d counted of %d",
							c.name, field, counted, res.Stats.Blocks, want, blocks)
					}
				}
			}
		})
	}
}

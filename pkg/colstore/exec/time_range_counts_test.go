package exec_test

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// rowRange returns plan with every row time that the filter of its scan
// compares replaced by the same time plus zero, which the executor computes
// for every row: nothing then tells it that the filter is a range of time.
func rowRange(n exec.Node) exec.Node {
	var bare func(e *exec.Expr) *exec.Expr
	bare = func(e *exec.Expr) *exec.Expr {
		if e == nil {
			return nil
		}
		if e.Op == exec.OpColumn && e.Path == "_time" {
			return exec.Add(e, exec.Int(0))
		}
		c := *e
		c.Args = make([]*exec.Expr, len(e.Args))
		for i, a := range e.Args {
			c.Args[i] = bare(a)
		}

		return &c
	}
	switch n := n.(type) {
	case *exec.Sort:
		c := *n
		c.Input = rowRange(n.Input)

		return &c
	case *exec.Project:
		c := *n
		c.Input = rowRange(n.Input)

		return &c
	case *exec.Filter:
		c := *n
		c.Input = rowRange(n.Input)

		return &c
	case *exec.Aggregate:
		c := *n
		c.Input = rowRange(n.Input)

		return &c
	case *exec.Scan:
		c := *n
		c.Filter = bare(n.Filter)

		return &c
	}

	return n
}

// A count of rows whose keys are columns, or that has no key, reads the
// records of the blocks that a time range cuts, and answers as the times of
// the rows do.
func TestCountsUnderATimeRangeAnswerAsTheTimesOfTheRowsDo(t *testing.T) {
	s := timeCountStore(t)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	const (
		some = iota // blocks that the range cuts are counted from their records
		none        // no block is
	)
	day := func(d, h, m, sec int) time.Time { return time.Date(2024, 11, d, h, m, sec, 0, time.UTC) }
	cases := []struct {
		name, text string
		byTime     int
		from, to   time.Time
	}{
		{"every row of five minutes", `source=x | stats count() as count`, some, day(23, 5, 10, 0), day(23, 5, 15, 0)},
		{"every row of a range off the minutes", `source=x | stats count() as count`, some, day(22, 3, 17, 31), day(23, 9, 0, 1)},
		{"every row of a second", `source=x | stats count() as count`, some, day(23, 5, 10, 7), day(23, 5, 10, 8)},
		{"five minutes by collection", `source=x | stats count() as count by commit.collection | sort commit.collection`, some, day(23, 5, 10, 0), day(23, 5, 15, 0)},
		{"a day and a half by kind and operation", `source=x | stats count() as count by kind, commit.operation | sort kind, commit.operation`, some, day(22, 12, 0, 0), day(24, 0, 0, 0)},
		{"an hour of the rows a filter passes", `source=x | where kind = 'commit' and commit.operation = 'create' | stats count() as count by commit.collection | sort commit.collection`, some, day(23, 9, 12, 7), day(23, 10, 12, 7)},
		{"two counts of an hour", `source=x | stats count() as a, count() as b by commit.collection | sort commit.collection`, some, day(23, 9, 0, 0), day(23, 10, 0, 0)},
		{"an hour by user", `source=x | stats count() as count by did | sort did | head 50`, none, day(23, 9, 0, 0), day(23, 10, 0, 0)},
		{"an hour of a count and a sum", `source=x | stats count() as count, sum(time_us) as s by commit.collection | sort commit.collection`, none, day(23, 9, 0, 0), day(23, 10, 0, 0)},
		{"every row by collection", `source=x | stats count() as count by commit.collection | sort commit.collection`, none, time.Time{}, time.Time{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plan, err := pplPlan(c.text, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if !c.from.IsZero() {
				plan = within(plan, c.from, c.to)
			}
			want, err := exec.Run(context.Background(), snap, rowRange(plan), exec.Options{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := exec.Run(context.Background(), snap, plan, exec.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if g, w := rendered(got), rendered(want); g != w {
				t.Fatalf("answer\n %s\nwant\n %s", g, w)
			}
			if want.Rows == 0 {
				t.Fatal("the query has no rows")
			}
			if counted := got.Stats.CountedByTime; want.Stats.CountedByTime != 0 || (c.byTime == some) != (counted != 0) {
				t.Fatalf("%d blocks counted by time, of the reference %d: %+v", counted, want.Stats.CountedByTime, got.Stats)
			}
			if got.Stats.RowsMatched != want.Stats.RowsMatched {
				t.Fatalf("%d rows matched, the reference %d", got.Stats.RowsMatched, want.Stats.RowsMatched)
			}
		})
	}
}

// A scan leaves out the blocks whose records hold no row in the time range
// that cuts them, whatever the plan reads.
func TestAScanLeavesOutBlocksWithoutRowsInTheRange(t *testing.T) {
	s := timeCountStore(t)
	defer s.Close()
	snap := s.Table().Snapshot()
	defer snap.Release()
	day := func(d, h, m, sec int) time.Time { return time.Date(2024, 11, d, h, m, sec, 0, time.UTC) }
	for _, text := range []string{
		`source=x | fields did, commit.collection | sort did, commit.collection | head 1000`,
		`source=x | stats count() as count, sum(time_us) as s by commit.collection | sort commit.collection`,
	} {
		plan, err := pplPlan(text, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		plan = within(plan, day(23, 5, 10, 0), day(23, 5, 11, 0))
		want, err := exec.Run(context.Background(), snap, rowRange(plan), exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := exec.Run(context.Background(), snap, plan, exec.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if g, w := rendered(got), rendered(want); g != w {
			t.Fatalf("%s: answer\n %s\nwant\n %s", text, g, w)
		}
		if want.Rows == 0 {
			t.Fatalf("%s: the query has no rows", text)
		}
		// Every block that the scan opens holds a row of the minute.
		if st := got.Stats; st.Blocks != st.BlocksMatched || st.Blocks == 0 {
			t.Fatalf("%s: %d blocks opened, %d matched: %+v", text, st.Blocks, st.BlocksMatched, st)
		}
	}
}

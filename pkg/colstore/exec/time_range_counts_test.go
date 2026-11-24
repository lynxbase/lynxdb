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

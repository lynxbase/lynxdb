package exec_test

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestAFilterRunAgainLeavesOutTheBlocksItFoundEmpty requires that a query
// opens, the second time, only the blocks that hold rows passing its
// filter, with the same answer, and that it learns nothing for a filter
// that differs or that calls a function.
func TestAFilterRunAgainLeavesOutTheBlocksItFoundEmpty(t *testing.T) {
	tbl := buildTable(t, synthDocs(4000, 7), "ts", layouts[0])
	snap := tbl.Snapshot()
	defer snap.Release()
	blocks := int64(0)
	for _, h := range snap.Parts() {
		blocks += int64(h.NumBlocks())
	}
	run := func(filter *exec.Expr, workers int) (int64, exec.Stats) {
		t.Helper()
		plan := &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Aggs: []exec.Agg{{Name: "n", Func: exec.Count}}}
		res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: workers})
		if err != nil {
			t.Fatal(err)
		}

		return res.Value(0, 0).Int, res.Stats
	}
	rows := func(filter *exec.Expr) (int64, int64) {
		t.Helper()
		plan := &exec.Project{Input: &exec.Scan{Filter: filter}, Exprs: []exec.Named{named("id", num("id"))}}
		res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 2})
		if err != nil {
			t.Fatal(err)
		}

		return int64(res.Rows), res.Stats.Blocks
	}

	// A number in few rows: no token rules a block out.
	few := exec.Eq(num("id"), exec.Int(1234))
	n, first := run(few, 4)
	if n != 1 || first.Blocks != blocks || first.SkippedAsEmpty != 0 {
		t.Fatalf("first: %d rows from %d of %d blocks, %d left out", n, first.Blocks, blocks, first.SkippedAsEmpty)
	}
	n, again := run(few, 4)
	if n != 1 || again.Blocks != 1 || again.SkippedAsEmpty != blocks-1 {
		t.Fatalf("again: %d rows from %d blocks, %d of %d left out", n, again.Blocks, again.SkippedAsEmpty, blocks)
	}
	if n, opened := rows(few); n != 1 || opened != 1 {
		t.Fatalf("as rows: %d rows from %d blocks", n, opened)
	}

	// The same comparison with another number has learned nothing.
	other := exec.Eq(num("id"), exec.Int(1235))
	if n, st := run(other, 1); n != 1 || st.SkippedAsEmpty != 0 {
		t.Fatalf("another number: %d rows, %d blocks left out", n, st.SkippedAsEmpty)
	}

	// A word that tokens rule out of blocks is remembered as well.
	word := exec.Eq(str("user"), exec.Str("u0007"))
	want, st := run(word, 2)
	passed := st.Blocks
	if st.SkippedByToken == 0 {
		t.Fatalf("no block ruled out by tokens: %+v", st)
	}
	got, st := run(word, 2)
	if got != want || st.SkippedByToken != 0 || st.Blocks > passed || st.Blocks+st.SkippedAsEmpty != blocks {
		t.Fatalf("a word again: %d rows, want %d; %+v", got, want, st)
	}

	// A function has no name to remember blocks under.
	length := func() exec.Func {
		return func(args []exec.Value, buf []byte) (exec.Value, []byte) {
			return exec.Value{Kind: vec.KindInt, Int: int64(len(args[0].Bytes))}, buf
		}
	}
	fn := exec.Eq(exec.Call(length, str("user")), exec.Int(99))
	for range 2 {
		if n, st := run(fn, 2); n != 0 || st.SkippedAsEmpty != 0 || st.Blocks != blocks {
			t.Fatalf("a function: %d rows, %+v", n, st)
		}
	}
}

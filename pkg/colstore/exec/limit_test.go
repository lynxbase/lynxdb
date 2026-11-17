package exec_test

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

func TestLimitStopsTheScan(t *testing.T) {
	docs := synthDocs(4000, 9)
	tbl := buildTable(t, docs, "ts", layout{blockRows: 100, parts: 2})
	snap := tbl.Snapshot()
	defer snap.Release()
	plan := &exec.Sort{Input: &exec.Project{
		Input: &exec.Scan{Filter: exec.Eq(str("kind"), exec.Str("commit"))},
		Exprs: []exec.Named{named("id", num("id")), named("kind", str("kind"))},
	}, Limit: 150}
	for _, workers := range []int{1, 4} {
		res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: workers})
		if err != nil {
			t.Fatal(err)
		}
		if res.Rows != 150 {
			t.Fatalf("%d workers: %d rows, want 150", workers, res.Rows)
		}
		for i := 0; i < res.Rows; i++ {
			if string(res.Value(i, 1).Bytes) != "commit" {
				t.Fatalf("row %d does not pass the filter: %v", i, res.Row(i, nil))
			}
		}
		// Each worker may finish the block it is in.
		if res.Stats.Blocks > int64(3+workers) {
			t.Errorf("%d workers: opened %d of 40 blocks for 150 rows", workers, res.Stats.Blocks)
		}
	}
}

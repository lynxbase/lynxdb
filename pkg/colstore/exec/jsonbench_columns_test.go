package exec_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestJSONBenchSearchedColumns reports how many columns the searches of
// every string column decode in the blocks they open, and how many the
// token filters of the columns spare them.
func TestJSONBenchSearchedColumns(t *testing.T) {
	data := benchData(t)
	cores := runtime.GOMAXPROCS(0)
	for _, l := range []struct {
		name string
		load func() *colstore.Table
	}{
		{"arrival", func() *colstore.Table { return loadArrival(t, data, cores) }},
		{"ordered", func() *colstore.Table { return loadOrdered(t, data, cores, benchSortKey) }},
	} {
		tbl := l.load()
		snap := tbl.Snapshot()
		for _, q := range benchPlans(windowsOf(t, snap)) {
			if !strings.Contains(q.name, "any field") {
				continue
			}
			res, err := exec.Run(context.Background(), snap, q.plan, exec.Options{Workers: cores})
			if err != nil {
				t.Fatal(err)
			}
			s := res.Stats
			t.Logf("%-8s %-28s %3d blocks opened, %d skipped by tokens; %4d columns searched, %.1f a block, %4d skipped by tokens, %.1f a block; %d blocks pass",
				l.name, q.name, s.Blocks, s.SkippedByToken, s.ColumnsSearched, float64(s.ColumnsSearched)/float64(max(s.Blocks, 1)),
				s.ColumnsSkippedByToken, float64(s.ColumnsSkippedByToken)/float64(max(s.Blocks, 1)), s.BlocksMatched)
			if s.Blocks > 0 && s.ColumnsSkippedByToken < 4*s.Blocks {
				t.Errorf("%s %s: token filters spare the search %d columns in %d blocks", l.name, q.name, s.ColumnsSkippedByToken, s.Blocks)
			}
		}
		snap.Release()
		tbl.Close()
	}
}

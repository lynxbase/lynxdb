package exec_test

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// TestJSONBenchPlansAgree runs the plans of Q1 to Q5 over the table in
// LYNXDB_BENCH_DIR and requires the answers that the hand-written queries
// gave over the same table, which the disk harness of pkg/colstore writes to
// the file named by LYNXDB_BENCH_ANSWERS.
func TestJSONBenchPlansAgree(t *testing.T) {
	dir, path := os.Getenv("LYNXDB_BENCH_DIR"), os.Getenv("LYNXDB_BENCH_ANSWERS")
	if dir == "" || path == "" {
		t.Skip("set LYNXDB_BENCH_DIR and LYNXDB_BENCH_ANSWERS")
	}
	file, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for _, line := range strings.Split(string(file), "\n") {
		if name, answer, ok := strings.Cut(line, "\t"); ok {
			want[name] = answer
		}
	}
	d, err := colstore.OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	snap := d.Table().Snapshot()
	defer snap.Release()
	compared := 0
	for _, q := range benchPlans(windowsOf(t, snap)) {
		hand, ok := want[q.name]
		if !ok {
			continue
		}
		res, err := exec.Run(context.Background(), snap, q.plan, exec.Options{Workers: runtime.GOMAXPROCS(0)})
		if err != nil {
			t.Fatalf("%s: %v", q.name, err)
		}
		if plan := answer(q.name, res); plan != hand {
			t.Errorf("%s: the plan and the hand-written query differ\n plan %s\n hand %s", q.name, plan, hand)
		}
		compared++
	}
	if compared != len(want) || compared < 5 {
		t.Fatalf("%d plans compared with %d hand-written answers", compared, len(want))
	}
	t.Logf("the plans of %d queries agree with the hand-written queries", compared)
}

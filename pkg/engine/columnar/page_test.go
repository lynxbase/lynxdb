package columnar

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/query"
)

func TestEventPagesMatchFullSortDuringFlush(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Store.MergeFanIn = -1
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Write overlapping ranges in two indexes, with equal times and a
	// late batch still in memory when the query pins its inputs.
	for batch := range 8 {
		for _, index := range []string{"a", "b"} {
			var buf strings.Builder
			for i := range 24 {
				fmt.Fprintf(&buf, `{"ts":%d,"id":%d,"msg":"%s","index":%q}`+"\n", testNow.Add(time.Duration(i%5-batch)*time.Minute).UnixMicro(), batch*24+i, []string{"common rare", "common", "common"}[i%3], index)
			}
			if _, err := appendTo(db, index, []byte(buf.String())); err != nil {
				t.Fatal(err)
			}
		}
		if batch < 7 {
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, text := range []string{"source=a,b common", "source=a,b rare", "source=a,b absentword", "source=a,b | where id % 3 = 1 | eval x = id + 1 | rename x as next | fields id, next", "source=a,b | sort - id", "source=a,b | head 13", "source=a,b | stats count() as n by index | sort index"} {
		c, err := query.Compile(text)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := db.Query(t.Context(), c.Plan, Options{Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, bound := range []int{1, 2, 13, 14, 110, 384, 385, 1000} {
			res, err := db.QueryPage(t.Context(), c.Plan, Options{Workers: 4}, bound, nil)
			if err != nil {
				t.Fatal(err)
			}
			n := min(bound, ref.Len())
			if res.Len() < n || render(res.Maps()[:n]) != render(ref.Maps()[:n]) {
				t.Fatalf("%s, bound %d: different prefix", text, bound)
			}
			total, exact := res.Total()
			if total < n || total > ref.Len() || exact && total != ref.Len() {
				t.Fatalf("%s, bound %d: total %d exact %v, full %d", text, bound, total, exact, ref.Len())
			}
		}
	}
	c, err := query.Compile("source=a,b common")
	if err != nil {
		t.Fatal(err)
	}
	// A timed-out job must finish on the same snapshot, even if flush
	// publishes its parts and the next write lands before the full scan.
	res, err := db.QueryPage(t.Context(), c.Plan, Options{Workers: 4}, 3, func() bool {
		if _, e := appendTo(db, "a", []byte(`{"msg":"common","id":1000}`+"\n")); e != nil {
			t.Fatal(e)
		}
		if e := db.Sync(); e != nil {
			t.Fatal(e)
		}
		return false
	})
	if err != nil || res.Len() != 384 {
		t.Fatalf("detached job: rows %v, error %v", res, err)
	}
}

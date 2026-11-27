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

func TestPageAndFullSortShareBufferedEventsAcrossFlush(t *testing.T) {
	db, err := Open(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, err := query.Compile("source=main error | fields id, _time")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := query.Compile("source=main error | sort - _time | fields id, _time")
	if err != nil {
		t.Fatal(err)
	}
	write := func(start, end int) error {
		var lines strings.Builder
		for id := start; id < end; id++ {
			fmt.Fprintf(&lines, `{"ts":%d,"id":%d,"msg":"error"}`+"\n", testNow.Add(time.Duration(id%9)*time.Minute).UnixMicro(), id)
		}
		_, err := appendTo(db, "main", []byte(lines.String()))
		return err
	}
	if err := write(0, 512); err != nil {
		t.Fatal(err)
	}
	snap, err := db.querySources(t.Context(), c.Plan, "main")
	defer snap.release()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for batch := 1; batch <= 8; batch++ {
			if err := db.Sync(); err != nil {
				done <- err
				return
			}
			if err := write(512*batch, 512*(batch+1)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	defer func() {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	full, err := Run(t.Context(), ref.Plan, snap, Options{Workers: 1})
	if err != nil || full.Len() != 512 {
		t.Fatalf("full result: %v, %v", full, err)
	}
	for _, start := range []int{0, 20, 40, 480, 500, 512} {
		page, _ := pagePlan(c.Plan, start+20+1)
		got, err := Run(t.Context(), page, snap, Options{Workers: 4})
		if err != nil {
			t.Fatal(err)
		}
		end := min(512, start+20)
		if got.Len() < end || render(got.Maps()[start:end]) != render(full.Maps()[start:end]) {
			t.Fatalf("different rows at offset %d", start)
		}
	}
}

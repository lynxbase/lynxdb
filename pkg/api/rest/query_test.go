package rest

import (
	"fmt"
	"math"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/query"
)

func TestDetectResultType(t *testing.T) {
	tests := []struct {
		query string
		want  ResultType
	}{
		{"source=main", ResultTypeEvents},
		{"source=main | where status >= 500 | head 5", ResultTypeEvents},
		{"source=main | stats count() by host", ResultTypeAggregate},
		{"source=main | stats count() by host | sort - `count()` | head 3", ResultTypeAggregate},
		{"source=main | stats count() as c by host | where c > 1 | fields host", ResultTypeAggregate},
		{"source=main | top 3 host", ResultTypeAggregate},
		{"source=main | eventstats count() by host", ResultTypeEvents},
		{"describe main", ResultTypeAggregate},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			c, err := query.Compile(tt.query)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if got := DetectResultType(c.Plan); got != tt.want {
				t.Errorf("DetectResultType() = %q, want %q", got, tt.want)
			}
		})
	}
	if got := DetectResultType(nil); got != ResultTypeEvents {
		t.Errorf("DetectResultType(nil) = %q, want events", got)
	}
}

func TestEventPageLookahead(t *testing.T) {
	srv, closeServer := startTestServer(t)
	defer closeServer()
	appendStatusRangeEvents(t, srv, 5)
	for _, tc := range []struct {
		q                          string
		limit, offset, total, rows int
		more                       bool
	}{
		{"source=main | where status > 800", 2, 0, 0, 0, false},
		{"source=main | where status >= 203", 2, 0, 2, 2, false},
		{"source=main | where status >= 202", 2, 0, 3, 2, true},
		{"source=main", 2, 3, 5, 2, false},
		{"source=main", 2, 100, 5, 0, false},
		{"source=main | head 0", 2, 0, 0, 0, false},
		{"source=main | head 2 from 2", 2, 0, 2, 2, false},
		{"source=main | head 2", 2, 0, 2, 2, false},
	} {
		code, reply := call(t, "POST", "http://"+srv.Addr()+"/api/v1/query", fmt.Sprintf(`{"q":%q,"limit":%d,"offset":%d}`, tc.q, tc.limit, tc.offset))
		if code != 200 {
			t.Fatal(reply)
		}
		d := reply["data"].(map[string]interface{})
		if d["total"] != float64(tc.total) || d["has_more"] != tc.more || len(d["events"].([]interface{})) != tc.rows {
			t.Errorf("%+v: %v", tc, d)
		}
	}
	code, _ := call(t, "POST", "http://"+srv.Addr()+"/api/v1/query", fmt.Sprintf(`{"q":"source=main","limit":2,"offset":%d}`, math.MaxInt))
	if code != 400 {
		t.Errorf("overflow status %d", code)
	}
}

func TestEventPageReportsLowerBoundAndExactCount(t *testing.T) {
	srv, stop := startTestServer(t)
	defer stop()
	for i := 1; i <= 4; i++ {
		ingestNDJSON(t, srv.Addr(), "main", fmt.Sprintf(`{"timestamp":"2026-05-0%dT12:00:00Z","id":%d}`, i, i))
		if err := srv.DB().Sync(); err != nil {
			t.Fatal(err)
		}
	}
	for _, exact := range []bool{false, true} {
		code, reply := call(t, "POST", "http://"+srv.Addr()+"/api/v1/query", fmt.Sprintf(`{"q":"source=main","limit":1,"exact_total":%t}`, exact))
		if code != 200 {
			t.Fatal(reply)
		}
		d := reply["data"].(map[string]interface{})
		meta := reply["meta"].(map[string]interface{})
		total := 2.
		if exact {
			total = 4
		}
		if d["total"] != total || d["has_more"] != true || (d["total_exact"] == false) == exact || (meta["rows_exact"] == false) == exact {
			t.Fatalf("exact %t: %v", exact, reply)
		}
		rows := d["events"].([]interface{})
		if len(rows) != 1 || rows[0].(map[string]interface{})["id"] != 4. {
			t.Fatalf("events: %v", rows)
		}
	}
	for _, param := range []string{"offset=99999999999999999999", "limit=99999999999999999999"} {
		code, _ := call(t, "GET", "http://"+srv.Addr()+"/api/v1/query?q=source%3Dmain&"+param, "")
		if code != 400 {
			t.Fatalf("%s: status %d", param, code)
		}
	}
}

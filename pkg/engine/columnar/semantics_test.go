package columnar

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/query"
)

// TestQuerySemantics requires the documented meaning of values of every
// kind in one field, by the columnar executor and by the row operators.
func TestQuerySemantics(t *testing.T) {
	var many []string
	for i := 0; i < 30000; i++ {
		many = append(many, fmt.Sprintf(`{"ts":1,"k":"v%d"}`, i))
	}
	tables := testTables{
		"nums":  tableOf(t, `{"ts":1,"s":"10"}`, `{"ts":2,"s":"9"}`, `{"ts":3,"s":"100"}`),
		"keys":  tableOf(t, `{"ts":1,"a":null,"k":1}`, `{"ts":2,"k":2}`, `{"ts":3,"a":"x","k":3}`),
		"mixed": tableOf(t, `{"ts":1,"x":1}`, `{"ts":2,"x":1.0}`, `{"ts":3,"x":"1"}`, `{"ts":4,"x":"2"}`, `{"ts":5,"x":"10"}`, `{"ts":6,"x":"1z"}`),
		"many":  tableOf(t, many...),
		"lines": tableOf(t, `{"ts":1,"_raw":"req took 10ms"}`, `{"ts":2,"_raw":"req took 9ms"}`, `{"ts":3,"_raw":"req took 100ms"}`,
			`{"ts":4,"_raw":"connection fused"}`, `{"ts":5,"msg":"a fused relay"}`, `{"ts":6,"msg":"defused","tags":["fusedx"]}`),
		"lists": tableOf(t, `{"ts":1,"g":"a","h":"a|||b"}`, `{"ts":2,"g":"a","h":"c"}`, `{"ts":3,"g":"b","h":"a|||b"}`),
	}
	for _, c := range []struct{ text, want string }{
		{`source=nums | eval x = s + 1 | sort ts | fields x`, "x=11; x=10; x=101"},
		{`source=nums | stats sum(s) as x, avg(s) as y, min(s) as lo, max(s) as hi`, "hi=100 lo=9 x=119 y=39.666666666666664"},
		{`source=nums | sort s | fields s`, "s=9; s=10; s=100"},
		{`source=nums | where s > 9 | sort ts | fields s`, "s=10; s=100"},
		{`source=keys | stats count() as c by a | sort a`, "a=<null> c=2; a=x c=1"},
		{`source=mixed | stats dc(x) as d, count() as c by x | sort x`, "c=2 d=1 x=1; c=1 d=1 x=1; c=1 d=1 x=2; c=1 d=1 x=10; c=1 d=1 x=1z"},
		{`source=mixed | sort x, ts | fields ts`, "ts=1; ts=2; ts=3; ts=4; ts=5; ts=6"},
		{`source=mixed | where x = 1 | stats count() as c`, "c=3"},
		{`source=many | stats dc(k) as d`, "d=30000"},
		{`source=lines | rex field=_raw "took (?<ms>\d+)ms" | stats avg(ms) as a`, "a=39.666666666666664"},
		{`source=lines | rex field=_raw "took (?<ms>\d+)ms" | where ms > 9 | sort ts | fields ms`, "ms=10; ms=100"},
		{`source=lines *fused | sort ts | fields ts`, "ts=4; ts=5; ts=6"},
		{`source=lines fus* | sort ts | fields ts`, "ts=4; ts=5; ts=6"},
		{`source=lists | stats list(h) as l, values(h) as v, take(h, 1) as t by g | sort g`,
			`g=a l=["a|||b","c"] t=["a|||b"] v=["a|||b","c"]; g=b l=["a|||b"] t=["a|||b"] v=["a|||b"]`},
		{`source=lists | eventstats values(h) as v by g | sort ts | fields v`, `v=["a|||b","c"]; v=["a|||b","c"]; v=["a|||b"]`},
		{`source=lists | sort ts | streamstats list(h) as l | fields l`, `l=["a|||b"]; l=["a|||b","c"]; l=["a|||b","c","a|||b"]`},
	} {
		compiled, err := query.Compile(c.text)
		if err != nil {
			t.Fatalf("%s: %v", c.text, err)
		}
		for _, rows := range []bool{false, true} {
			res, err := Run(context.Background(), compiled.Plan, tables, Options{Now: testNow, rows: rows})
			if err != nil {
				t.Fatalf("%s: %v", c.text, err)
			}
			if got := render(res.Maps(), "_time"); got != c.want {
				t.Errorf("%s (row operators %v)\n got  %s\n want %s", c.text, rows, got, c.want)
			}
		}
	}
}

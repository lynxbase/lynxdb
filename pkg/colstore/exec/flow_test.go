package exec_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/flow"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// TestFlowQueries runs queries stated in LynxFlow over the synthetic
// documents and requires the rows of the plan that states the same by hand,
// and of the reference evaluator.
func TestFlowQueries(t *testing.T) {
	eq := func(path, v string) *exec.Expr { return exec.Eq(anyOf(path), exec.Str(v)) }
	created := exec.And(eq("kind", "commit"), eq("op", "create"))
	count := exec.Agg{Name: "count", Func: exec.Count}
	coll := []exec.Named{named("coll", anyOf("coll"))}
	byCount := []exec.Order{{Column: "count", Desc: true}, {Column: "coll"}}
	lo, hi := int64(1_700_030_000_000_000_000), int64(1_700_090_000_000_000_000)
	stamp := func(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }
	n := anyOf("n")
	cases := []struct {
		name, text string
		plan       exec.Node
	}{
		{"groups", `from logs | stats count() as count by coll | sort -count, coll`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: coll, Aggs: []exec.Agg{count}}, By: byCount}},
		{"groups and users", `from logs | where kind == "commit" and op == "create" | stats count() as count, dc(user) as users by coll | sort -count, coll`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: coll,
				Aggs: []exec.Agg{count, {Name: "users", Func: exec.CountDistinct, Arg: anyOf("user")}}}, By: byCount}},
		{"hours", `from logs | where kind == "commit" and coll in ["feed.like", "feed.post"] | extend hour = date_part(_time, "hour") | stats count() as count by coll, hour | sort hour, coll`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), exec.Or(eq("coll", "feed.like"), eq("coll", "feed.post")))},
				Keys: []exec.Named{coll[0], named("hour", exec.HourOfDay(exec.Time()))}, Aggs: []exec.Agg{count}}, By: []exec.Order{{Column: "hour"}, {Column: "coll"}}}},
		{"first of users", `from logs | where kind == "commit" and op == "create" and coll == "feed.post" | stats min(_time) as first by user | sort first, user | head 3`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), eq("op", "create"), eq("coll", "feed.post"))},
				Keys: []exec.Named{named("user", anyOf("user"))}, Aggs: []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}}},
				By: []exec.Order{{Column: "first"}, {Column: "user"}}, Limit: 3}},
		{"spans", `from logs | where kind == "commit" | stats min(_time) as first, max(_time) as last by user | extend span = last / 1000000 - first / 1000000 | keep user, span | sort -span, user | head 5`,
			&exec.Sort{Input: &exec.Project{Input: &exec.Aggregate{Input: &exec.Scan{Filter: eq("kind", "commit")}, Keys: []exec.Named{named("user", anyOf("user"))},
				Aggs: []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}, {Name: "last", Func: exec.Max, Arg: exec.Time()}}},
				Exprs: []exec.Named{named("user", anyOf("user")), named("span", exec.Sub(exec.Div(anyOf("last"), exec.Int(1_000_000)), exec.Div(anyOf("first"), exec.Int(1_000_000))))}},
				By: []exec.Order{{Column: "span", Desc: true}, {Column: "user"}}, Limit: 5}},
		{"word in a field", `from logs | where has(msg, "LOVE") | stats count()`,
			&exec.Aggregate{Input: &exec.Scan{Filter: exec.Match(anyOf("msg"), tokenfilter.Word("LOVE").IgnoreCase())}, Aggs: []exec.Agg{{Name: "count()", Func: exec.Count}}}},
		{"word anywhere", `from logs love | stats count() as count`,
			&exec.Aggregate{Input: &exec.Scan{Filter: exec.MatchAny(tokenfilter.Word("love").IgnoreCase())}, Aggs: []exec.Agg{count}}},
		{"words and a field", `from logs "good morning" crypto* NOT rare kind=commit | count`,
			&exec.Aggregate{Input: &exec.Scan{Filter: exec.And(exec.MatchAny(tokenfilter.Substring("good morning").IgnoreCase()),
				exec.MatchAny(tokenfilter.Prefix("crypto").IgnoreCase()), exec.Not(exec.MatchAny(tokenfilter.Word("rare").IgnoreCase())), eq("kind", "commit"))},
				Aggs: []exec.Agg{count}}},
		{"window", fmt.Sprintf(`from logs | where _time >= timestamp(%q) and _time < timestamp(%q) | stats count() as count by coll | sort -count, coll`, stamp(lo), stamp(hi)),
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(exec.Ge(exec.Time(), exec.Int(lo)), exec.Lt(exec.Time(), exec.Int(hi)))},
				Keys: coll, Aggs: []exec.Agg{count}}, By: byCount}},
		{"latest of a user", `from logs | where user == "u0001" | extend at = _time | keep at, coll, id | sort -at, id | head 20`,
			&exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: eq("user", "u0001")},
				Exprs: []exec.Named{named("at", exec.Time()), named("coll", anyOf("coll")), named("id", anyOf("id"))}},
				By: []exec.Order{{Column: "at", Desc: true}, {Column: "id"}}, Limit: 20}},
		{"negations", `from logs | where not (n > 50 or kind == "identity") and not has(msg, "love") and not exists(tags) and flag != true | count`,
			&exec.Aggregate{Input: &exec.Scan{Filter: exec.And(exec.Le(n, exec.Int(50)), exec.Ne(anyOf("kind"), exec.Str("identity")),
				exec.Eq(exec.Match(anyOf("msg"), tokenfilter.Word("love").IgnoreCase()), exec.Bool(false)), exec.Not(exec.Exists(anyOf("tags"))),
				exec.Ne(anyOf("flag"), exec.Bool(true)))}, Aggs: []exec.Agg{count}}},
		{"functions", `from logs | extend size = case(n < 10, "low", n between 10 and 60, "mid", "high"), label = upper(kind) + ":" + (coll ?? "-"), hour = bin(_time, 1h) ` +
			`| stats count() as count, avg(n * 1.5) as mean, stdev(n) as spread by size, label, hour | where count > 2 | sort -count, label, size, hour | head 40`,
			&exec.Sort{Input: &exec.Filter{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{
				named("size", exec.Case(exec.Lt(n, exec.Int(10)), exec.Str("low"), exec.And(exec.Ge(n, exec.Int(10)), exec.Le(n, exec.Int(60))), exec.Str("mid"), exec.Str("high"))),
				named("label", exec.Add(exec.Add(exec.Upper(anyOf("kind")), exec.Str(":")), exec.Coalesce(anyOf("coll"), exec.Str("-")))),
				named("hour", exec.Bin(exec.Time(), exec.Int(int64(time.Hour))))},
				Aggs: []exec.Agg{count, {Name: "mean", Func: exec.Avg, Arg: exec.Mul(n, exec.Float(1.5))}, {Name: "spread", Func: exec.Stdev, Arg: n}}},
				Cond: exec.Gt(anyOf("count"), exec.Int(2))}, By: []exec.Order{{Column: "count", Desc: true}, {Column: "label"}, {Column: "size"}, {Column: "hour"}}, Limit: 40}},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				plan, err := flow.Compile(c.text, flow.Options{})
				if err != nil {
					t.Fatalf("%s: %v", c.text, err)
				}
				_, sorted := plan.(*exec.Sort)
				got := resultRows(check(t, snap, rows, plan, sorted, workers))
				want := resultRows(check(t, snap, rows, c.plan, sorted, workers))
				if len(got) != len(want) {
					t.Fatalf("%d rows, the plan by hand gives %d", len(got), len(want))
				}
				for i := range got {
					if sorted && got[i] != want[i] {
						t.Fatalf("row %d: %s, the plan by hand gives %s", i, got[i], want[i])
					}
				}
			})
		}
	})
}

func TestFlowTimeRangesCountFromNow(t *testing.T) {
	docs := synthDocs(3000, 9)
	rows := referenceRows(t, docs, "ts")
	tbl := buildTable(t, docs, "ts", layout{blockRows: 256, parts: 2})
	snap := tbl.Snapshot()
	defer snap.Release()
	now := time.Unix(0, 1_700_060_000_000_000_000)
	plan, err := flow.Compile(`from logs[-8h..-2h] | stats count() as count`, flow.Options{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	want := &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(exec.Ge(exec.Time(), exec.Int(now.Add(-8*time.Hour).UnixNano())),
		exec.Lt(exec.Time(), exec.Int(now.Add(-2*time.Hour).UnixNano())))}, Aggs: []exec.Agg{{Name: "count", Func: exec.Count}}}
	got, by := resultRows(check(t, snap, rows, plan, true, 2)), resultRows(check(t, snap, rows, want, true, 2))
	if fmt.Sprint(got) != fmt.Sprint(by) || got[0] == "0" {
		t.Fatalf("got %v, the plan by hand gives %v", got, by)
	}
}

func TestFlowRefusesWhatItCannotRun(t *testing.T) {
	for _, text := range []string{
		`from logs | top 5 coll`,
		`from logs | stats perc(n) by coll`,
		`from logs | where kind == "commit"`,
		`from logs | stats count() by coll | stats count()`,
		`from logs | dedup user | count`,
		`from logs | where extract(msg, "a(.*)b") == "x" | count`,
		`from logs | stats mode(n) by coll`,
		`from logs | stats count() as count by coll | sort -count | keep coll`,
		`from logs[@d] | count`,
		`let $a = from logs; from $a | count`,
	} {
		if _, err := flow.Compile(text, flow.Options{}); !errors.Is(err, flow.ErrUnsupported) {
			t.Errorf("%s: got %v, want an unsupported error", text, err)
		}
	}
	if _, err := flow.Compile(`from logs | where (`, flow.Options{}); err == nil || errors.Is(err, flow.ErrUnsupported) {
		t.Errorf("a query that does not parse: got %v", err)
	}
}

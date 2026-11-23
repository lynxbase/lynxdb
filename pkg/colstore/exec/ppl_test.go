package exec_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irplan"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

// pplPlan returns the plan of a query stated in PPL; now is the time that
// its relative times count from.
func pplPlan(text string, now time.Time) (exec.Node, error) {
	res, err := ppl.Compile(text)
	if err != nil {
		return nil, err
	}

	return irplan.Plan(res.Plan, irplan.Options{Now: now})
}

// TestPPLQueries runs queries stated in PPL over the synthetic documents and
// requires the rows of the plan that states the same by hand, and of the
// reference evaluator.
func TestPPLQueries(t *testing.T) {
	eq := func(path, v string) *exec.Expr { return exec.Eq(anyOf(path), exec.Str(v)) }
	created := exec.And(eq("kind", "commit"), eq("op", "create"))
	count := exec.Agg{Name: "count", Func: exec.Count}
	coll := []exec.Named{named("coll", anyOf("coll"))}
	asc := func(column string) exec.Order { return exec.Order{Column: column, NullsFirst: true} }
	desc := func(column string) exec.Order { return exec.Order{Column: column, Desc: true} }
	byCount := []exec.Order{desc("count"), asc("coll")}
	lo, hi := int64(1_700_030_000_000_000_000), int64(1_700_090_000_000_000_000)
	stamp := func(ns int64) string { return time.Unix(0, ns).UTC().Format(time.RFC3339Nano) }
	n := anyOf("n")
	any := func(p tokenfilter.Pattern) *exec.Expr { return exec.MatchAny(p.IgnoreCase()) }
	cases := []struct {
		name, text string
		plan       exec.Node
	}{
		{"groups", `source=logs | stats count() as count by coll | fields coll, count | sort - count, coll`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: coll, Aggs: []exec.Agg{count}}, By: byCount}},
		{"groups in the order of stats", `source=logs | stats count() as count by coll | sort - count, coll`,
			&exec.Sort{Input: &exec.Project{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: coll, Aggs: []exec.Agg{count}},
				Exprs: []exec.Named{named("count", anyOf("count")), named("coll", anyOf("coll"))}}, By: byCount}},
		{"groups and users", `source=logs | where kind = 'commit' and op = 'create' | stats count() as count, distinct_count(user) as users by coll | fields coll, count, users | sort - count, coll`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: created}, Keys: coll,
				Aggs: []exec.Agg{count, {Name: "users", Func: exec.CountDistinct, Arg: anyOf("user")}}}, By: byCount}},
		{"hours", `source=logs | where kind = 'commit' and coll in ('feed.like', 'feed.post') | eval hour = hour(_time) | stats count() as count by coll, hour | fields coll, hour, count | sort hour, coll`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), exec.Or(eq("coll", "feed.like"), eq("coll", "feed.post")))},
				Keys: []exec.Named{coll[0], named("hour", exec.HourOfDay(exec.Time()))}, Aggs: []exec.Agg{count}}, By: []exec.Order{asc("hour"), asc("coll")}}},
		{"first of users", `source=logs | where kind = 'commit' and op = 'create' and coll = 'feed.post' | stats min(_time) as first by user | fields user, first | sort first, user | head 3`,
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(eq("kind", "commit"), eq("op", "create"), eq("coll", "feed.post"))},
				Keys: []exec.Named{named("user", anyOf("user"))}, Aggs: []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}}},
				By: []exec.Order{asc("first"), asc("user")}, Limit: 3}},
		{"spans", `source=logs | where kind = 'commit' | stats min(_time) as first, max(_time) as last by user | eval span = timestampdiff(MILLISECOND, first, last) | fields user, span | sort - span, user | head 5`,
			&exec.Sort{Input: &exec.Project{Input: &exec.Aggregate{Input: &exec.Scan{Filter: eq("kind", "commit")}, Keys: []exec.Named{named("user", anyOf("user"))},
				Aggs: []exec.Agg{{Name: "first", Func: exec.Min, Arg: exec.Time()}, {Name: "last", Func: exec.Max, Arg: exec.Time()}}},
				Exprs: []exec.Named{named("user", anyOf("user")), named("span", exec.Div(exec.Sub(anyOf("last"), anyOf("first")), exec.Int(1_000_000)))}},
				By: []exec.Order{desc("span"), asc("user")}, Limit: 5}},
		{"text in a field", `source=logs | where like(msg, '%LOVE%') | stats count()`,
			&exec.Aggregate{Input: &exec.Scan{Filter: exec.Match(anyOf("msg"), tokenfilter.Substring("LOVE").IgnoreCase())}, Aggs: []exec.Agg{{Name: "count()", Func: exec.Count}}}},
		{"word anywhere", `source=logs love | stats count() as count`,
			&exec.Aggregate{Input: &exec.Scan{Filter: any(tokenfilter.Word("love"))}, Aggs: []exec.Agg{count}}},
		{"words and a field", `source=logs "good morning" crypto* NOT rare kind=commit | stats count() as count`,
			&exec.Aggregate{Input: &exec.Scan{Filter: exec.And(any(tokenfilter.Word("good morning")), any(tokenfilter.Prefix("crypto")),
				exec.Not(any(tokenfilter.Word("rare"))), eq("kind", "commit"))}, Aggs: []exec.Agg{count}}},
		{"window", fmt.Sprintf(`source=logs | where _time >= '%s' and _time < '%s' | stats count() as count by coll | fields coll, count | sort - count, coll`, stamp(lo), stamp(hi)),
			&exec.Sort{Input: &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(exec.Ge(exec.Time(), exec.Int(lo)), exec.Lt(exec.Time(), exec.Int(hi)))},
				Keys: coll, Aggs: []exec.Agg{count}}, By: byCount}},
		{"latest of a user", `source=logs | where user = 'u0001' | eval at = _time | fields at, coll, id | sort - at, id | head 20`,
			&exec.Sort{Input: &exec.Project{Input: &exec.Scan{Filter: eq("user", "u0001")},
				Exprs: []exec.Named{named("at", exec.Time()), named("coll", anyOf("coll")), named("id", anyOf("id"))}},
				By: []exec.Order{desc("at"), asc("id")}, Limit: 20}},
		{"negations", `source=logs | where not (n > 50 or kind = 'identity') and not like(msg, '%love%') and isnull(tags) and flag != true | stats count() as count`,
			&exec.Aggregate{Input: &exec.Scan{Filter: exec.And(exec.Le(n, exec.Int(50)), exec.Ne(anyOf("kind"), exec.Str("identity")),
				exec.Eq(exec.Match(anyOf("msg"), tokenfilter.Substring("love").IgnoreCase()), exec.Bool(false)),
				exec.Or(exec.IsNull(anyOf("tags")), exec.IsMissing(anyOf("tags"))), exec.Ne(anyOf("flag"), exec.Bool(true)))}, Aggs: []exec.Agg{count}}},
		{"functions", `source=logs | eval size = case(n < 10, 'low', n between 10 and 60, 'mid' else 'high'), label = concat(upper(kind), ':', coalesce(coll, '-')) ` +
			`| stats count() as count, avg(n * 1.5) as mean, stddev_samp(n) as spread by size, label, span(_time, 1h) as hour | where count > 2 ` +
			`| fields size, label, hour, count, mean, spread | sort - count, label, size, hour | head 40`,
			&exec.Sort{Input: &exec.Filter{Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{
				named("size", exec.Case(exec.Lt(n, exec.Int(10)), exec.Str("low"), exec.And(exec.Ge(n, exec.Int(10)), exec.Le(n, exec.Int(60))), exec.Str("mid"), exec.Str("high"))),
				named("label", fn("concat", fn("upper", anyOf("kind")), exec.Str(":"), exec.Coalesce(anyOf("coll"), exec.Str("-")))),
				named("hour", exec.Bin(exec.Time(), exec.Int(int64(time.Hour))))},
				Aggs: []exec.Agg{count, {Name: "mean", Func: exec.Avg, Arg: exec.Mul(n, exec.Float(1.5))}, {Name: "spread", Func: exec.Stdev, Arg: n}}},
				Cond: exec.Gt(anyOf("count"), exec.Int(2))}, By: []exec.Order{desc("count"), asc("label"), asc("size"), asc("hour")}, Limit: 40}},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				plan, err := pplPlan(c.text, time.Time{})
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

func TestPPLTimeRangesCountFromNow(t *testing.T) {
	docs := synthDocs(3000, 9)
	rows := referenceRows(t, docs, "ts")
	tbl := buildTable(t, docs, "ts", layout{blockRows: 256, parts: 2})
	snap := tbl.Snapshot()
	defer snap.Release()
	now := time.Unix(0, 1_700_060_000_000_000_000)
	plan, err := pplPlan(`source=logs earliest=-8h latest=-2h | stats count() as count`, now)
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

package exec_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// shout is a function of the caller's: the upper case of a string between
// marks, the number of an integer as text, "none" for a null and "absent"
// for a field that the row does not hold.
func shout(made *atomic.Int64) func() exec.Func {
	return func() exec.Func {
		if made != nil {
			made.Add(1)
		}

		return func(args []exec.Value, buf []byte) (exec.Value, []byte) {
			switch v := args[0]; v.Kind {
			case vec.KindString:
				buf = append(append(append(buf, '<'), bytes.ToUpper(v.Bytes)...), '>')
			case vec.KindInt:
				buf = strconv.AppendInt(buf, v.Int, 10)
			case vec.KindNull:
				buf = append(buf, "none"...)
			case exec.KindAny:
				buf = append(buf, "absent"...)
			default:
				return exec.Value{}, buf
			}
			for _, more := range args[1:] {
				buf = append(append(buf, '+'), more.Bytes...)
			}

			return exec.Value{Kind: vec.KindString, Bytes: buf}, buf
		}
	}
}

// longer is true for strings of more bytes than the second argument says.
func longer() exec.Func {
	return func(args []exec.Value, buf []byte) (exec.Value, []byte) {
		if args[0].Kind != vec.KindString || args[1].Kind != vec.KindInt {
			return exec.Value{}, buf
		}

		return exec.Value{Kind: vec.KindBool, Bool: int64(len(args[0].Bytes)) > args[1].Int}, buf
	}
}

func TestFunctionsOfTheCaller(t *testing.T) {
	var made atomic.Int64
	count := []exec.Agg{{Name: "n", Func: exec.Count}}
	cases := map[string]exec.Node{
		"a key of a dictionary column": &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("k", exec.Call(shout(&made), anyOf("kind")))}, Aggs: count},
		"a key of a column of many values": &exec.Aggregate{Input: &exec.Scan{Filter: exec.Lt(num("id"), exec.Int(400))},
			Keys: []exec.Named{named("k", exec.Call(shout(nil), anyOf("user"), exec.Str("x")))}, Aggs: count},
		"a key of a column of several kinds": &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("k", exec.Call(shout(nil), anyOf("n")))}, Aggs: count},
		"an argument that is a function":     &exec.Aggregate{Input: &exec.Scan{}, Keys: []exec.Named{named("k", exec.Call(shout(nil), exec.Upper(anyOf("coll")), anyOf("op")))}, Aggs: count},
		"rows": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("id", num("id")), named("m", exec.Call(shout(nil), anyOf("msg")))}},
			By: []exec.Order{{Column: "id"}}, Limit: 50},
		"a filter":                  &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(exec.Call(longer, anyOf("msg"), exec.Int(20)), exec.Eq(anyOf("kind"), exec.Str("commit")))}, Aggs: count},
		"a filter that is not true": &exec.Aggregate{Input: &exec.Scan{Filter: exec.Not(exec.Call(longer, anyOf("msg"), exec.Int(20)))}, Aggs: count},
		"an aggregate of a function": &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("kind"),
			Aggs: []exec.Agg{{Name: "least", Func: exec.Min, Arg: exec.Call(shout(nil), anyOf("coll"))}, {Name: "kinds", Func: exec.CountDistinct, Arg: exec.Call(shout(nil), anyOf("op"))}}},
		"above an aggregate": &exec.Sort{Input: &exec.Project{Input: &exec.Filter{
			Input: &exec.Aggregate{Input: &exec.Scan{}, Keys: keys("coll"), Aggs: []exec.Agg{{Name: "n", Func: exec.Count}, {Name: "last", Func: exec.Max, Arg: anyOf("msg")}}},
			Cond:  exec.And(exec.Call(longer, anyOf("last"), exec.Int(3)), exec.Gt(anyOf("n"), exec.Int(1)))},
			Exprs: []exec.Named{named("coll", anyOf("coll")), named("loud", exec.Call(shout(nil), anyOf("last"), anyOf("coll")))}}, By: []exec.Order{{Column: "coll"}}},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for name, plan := range cases {
			t.Run(name, func(t *testing.T) {
				_, sorted := plan.(*exec.Sort)
				check(t, snap, rows, plan, sorted, workers)
			})
		}
	})
	if made.Load() == 0 {
		t.Fatal("the function of a key was never made")
	}
	tbl := buildTable(t, synthDocs(10, 1), "ts", layout{blockRows: 64, parts: 1})
	snap := tbl.Snapshot()
	defer snap.Release()
	for name, e := range map[string]*exec.Expr{"without an implementation": exec.Call(nil, anyOf("kind")), "without arguments": exec.Call(shout(nil))} {
		plan := &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{named("x", e)}}
		if _, err := exec.Run(context.Background(), snap, plan, exec.Options{}); !errors.Is(err, exec.ErrUnsupported) {
			t.Errorf("a function %s: %v", name, err)
		}
	}
}

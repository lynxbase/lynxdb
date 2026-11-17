package exec_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// The reference evaluator runs a plan one row at a time over the parsed
// documents, following the rules in the package documentation and nothing
// of the executor's code.

// refRow maps a flattened path to the value a document holds there.
type refRow map[string]exec.Value

// flatten parses one JSON object into a row; timeField names the member
// that becomes the row time.
func flatten(t testing.TB, doc []byte, timeField string) refRow {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var v map[string]any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("reference: %v in %s", err, doc)
	}
	row := refRow{}
	flattenInto(row, "", v)
	ts := int64(0)
	if tv, ok := row[timeField]; ok && tv.Kind == vec.KindInt {
		ts = shred.EpochNanos(tv.Int)
	}
	row[part.TimePath] = exec.Value{Kind: vec.KindInt, Int: ts}

	return row
}

func flattenInto(row refRow, prefix string, obj map[string]any) {
	for name, v := range obj {
		path := prefix + name
		switch v := v.(type) {
		case map[string]any:
			if len(v) == 0 {
				row[path] = exec.Value{Kind: vec.KindVariant}

				continue
			}
			flattenInto(row, path+".", v)
		case string:
			row[path] = exec.Value{Kind: vec.KindString, Bytes: []byte(v)}
		case bool:
			row[path] = exec.Value{Kind: vec.KindBool, Bool: v}
		case json.Number:
			if i, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
				row[path] = exec.Value{Kind: vec.KindInt, Int: i}
			} else if f, err := strconv.ParseFloat(v.String(), 64); err == nil {
				row[path] = exec.Value{Kind: vec.KindFloat, Float: f}
			}
		case nil:
			row[path] = exec.Value{Kind: vec.KindNull}
		default:
			row[path] = exec.Value{Kind: vec.KindVariant}
		}
	}
}

// refValue evaluates a value expression for a row of the table.
func refValue(e *exec.Expr, row refRow) exec.Value {
	switch e.Op {
	case exec.OpColumn:
		v, ok := row[e.Path]
		if !ok || v.Kind == vec.KindNull || (e.Kind != exec.KindAny && v.Kind != e.Kind) {
			return exec.Value{}
		}

		return v
	case exec.OpLiteral:
		return e.Value
	case exec.OpHourOfDay:
		v := refInt(e.Args[0], row)
		if v.Kind != vec.KindInt {
			return exec.Value{}
		}
		const hour = 3_600_000_000_000
		hours := v.Int / hour
		if v.Int%hour < 0 {
			hours--
		}

		return exec.Value{Kind: vec.KindInt, Int: (hours%24 + 24) % 24}
	}
	a, b := refInt(e.Args[0], row), refInt(e.Args[1], row)
	if a.Kind != vec.KindInt || b.Kind != vec.KindInt {
		return exec.Value{}
	}

	return refArith(e.Op, a, b)
}

// refInt evaluates an argument of a scalar function, which reads integers
// only.
func refInt(e *exec.Expr, row refRow) exec.Value {
	v := refValue(e, row)
	if v.Kind != vec.KindInt {
		return exec.Value{}
	}

	return v
}

func refArith(op exec.Op, a, b exec.Value) exec.Value {
	if a.Kind == vec.KindInt && b.Kind == vec.KindInt {
		switch op {
		case exec.OpAdd:
			return exec.Value{Kind: vec.KindInt, Int: a.Int + b.Int}
		case exec.OpSub:
			return exec.Value{Kind: vec.KindInt, Int: a.Int - b.Int}
		case exec.OpMul:
			return exec.Value{Kind: vec.KindInt, Int: a.Int * b.Int}
		}
		if b.Int == 0 {
			return exec.Value{}
		}

		return exec.Value{Kind: vec.KindInt, Int: a.Int / b.Int}
	}
	x, okx := refNumber(a)
	y, oky := refNumber(b)
	if !okx || !oky {
		return exec.Value{}
	}
	switch op {
	case exec.OpAdd:
		x += y
	case exec.OpSub:
		x -= y
	case exec.OpMul:
		x *= y
	default:
		x /= y
	}

	return exec.Value{Kind: vec.KindFloat, Float: x}
}

func refNumber(v exec.Value) (float64, bool) {
	switch v.Kind {
	case vec.KindInt:
		return float64(v.Int), true
	case vec.KindFloat:
		return v.Float, true
	}

	return 0, false
}

func refCompare(op exec.Op, c int) bool {
	switch op {
	case exec.OpEq:
		return c == 0
	case exec.OpNe:
		return c != 0
	case exec.OpLt:
		return c < 0
	case exec.OpLe:
		return c <= 0
	case exec.OpGt:
		return c > 0
	}

	return c >= 0
}

// refTrue evaluates a filter for a row.
func refTrue(e *exec.Expr, row refRow) bool {
	switch e.Op {
	case exec.OpAnd:
		for _, a := range e.Args {
			if !refTrue(a, row) {
				return false
			}
		}

		return true
	case exec.OpOr:
		for _, a := range e.Args {
			if refTrue(a, row) {
				return true
			}
		}

		return false
	case exec.OpNot:
		return !refTrue(e.Args[0], row)
	case exec.OpMatch:
		v := refValue(e.Args[0], row)

		return v.Kind == vec.KindString && e.Pattern.Match(v.Bytes)
	case exec.OpColumn:
		v := refValue(e, row)

		return v.Kind == vec.KindBool && v.Bool
	case exec.OpLiteral:
		return e.Value.Bool
	}
	a, b, op := e.Args[0], e.Args[1], e.Op
	if a.Op == exec.OpLiteral && b.Op == exec.OpColumn {
		a, b = b, a
		switch op {
		case exec.OpLt:
			op = exec.OpGt
		case exec.OpLe:
			op = exec.OpGe
		case exec.OpGt:
			op = exec.OpLt
		case exec.OpGe:
			op = exec.OpLe
		}
	}
	if a.Op != exec.OpColumn || b.Op != exec.OpLiteral {
		x, y := refInt(a, row), refInt(b, row)
		if x.Kind != vec.KindInt || y.Kind != vec.KindInt {
			return false
		}

		return refCompare(op, cmpInt(x.Int, y.Int))
	}
	v, lit := refValue(a, row), b.Value
	switch {
	case lit.Kind == vec.KindString && v.Kind == vec.KindString:
		return refCompare(op, bytes.Compare(v.Bytes, lit.Bytes))
	case lit.Kind == vec.KindBool && v.Kind == vec.KindBool:
		return (v.Bool == lit.Bool) == (op == exec.OpEq)
	case lit.Kind == vec.KindInt && v.Kind == vec.KindInt:
		return refCompare(op, cmpInt(v.Int, lit.Int))
	}
	x, okx := refNumber(v)
	y, oky := refNumber(lit)
	if !okx || !oky {
		return false
	}
	switch op {
	case exec.OpEq:
		return x == y
	case exec.OpNe:
		return x != y
	case exec.OpLt:
		return x < y
	case exec.OpLe:
		return x <= y
	case exec.OpGt:
		return x > y
	}

	return x >= y
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

// refKey renders a value so that equal values of equal kinds, and only
// those, render alike.
func refKey(v exec.Value) string {
	switch v.Kind {
	case vec.KindBool:
		return fmt.Sprintf("b:%v", v.Bool)
	case vec.KindInt:
		return fmt.Sprintf("i:%d", v.Int)
	case vec.KindFloat:
		return fmt.Sprintf("f:%016x", math.Float64bits(v.Float))
	case vec.KindString:
		return "s:" + string(v.Bytes)
	case vec.KindVariant:
		return "v:" + string(v.Bytes)
	}

	return "null"
}

type refGroup struct {
	keys []exec.Value
	aggs []refAgg
}

type refAgg struct {
	rows, values, ints, floats int64
	sumInt, minInt, maxInt     int64
	sumFloat, minFloat, maxFlt float64
	distinct                   map[string]struct{}
}

// refRun evaluates a plan over rows and returns its output rows and column
// names.
func refRun(rows []refRow, plan exec.Node) ([][]exec.Value, []string) {
	srt, _ := plan.(*exec.Sort)
	if srt != nil {
		plan = srt.Input
	}
	prj, _ := plan.(*exec.Project)
	if prj != nil {
		plan = prj.Input
	}
	flt, _ := plan.(*exec.Filter)
	if flt != nil {
		plan = flt.Input
	}
	agg, _ := plan.(*exec.Aggregate)
	if agg != nil {
		plan = agg.Input
	}
	scan := plan.(*exec.Scan)
	var kept []refRow
	for _, row := range rows {
		if scan.Filter == nil || refTrue(scan.Filter, row) {
			kept = append(kept, row)
		}
	}

	var out [][]exec.Value
	var names []string
	switch {
	case agg != nil:
		out, names = refAggregate(kept, agg)
		if flt != nil {
			var passed [][]exec.Value
			for _, row := range out {
				named := refRow{}
				for i, name := range names {
					named[name] = row[i]
				}
				if refHaving(flt.Cond, named) {
					passed = append(passed, row)
				}
			}
			out = passed
		}
		if prj != nil {
			in, inNames := out, names
			out, names = nil, nil
			for _, e := range prj.Exprs {
				names = append(names, e.Name)
			}
			for _, row := range in {
				named := refRow{}
				for i, name := range inNames {
					named[name] = row[i]
				}
				var res []exec.Value
				for _, e := range prj.Exprs {
					res = append(res, refPost(e.Expr, named))
				}
				out = append(out, res)
			}
		}
	default:
		for _, e := range prj.Exprs {
			names = append(names, e.Name)
		}
		for _, row := range kept {
			var res []exec.Value
			for _, e := range prj.Exprs {
				res = append(res, refValue(e.Expr, row))
			}
			out = append(out, res)
		}
	}
	if srt == nil {
		return out, names
	}
	if len(srt.By) > 0 {
		cols := make([]int, len(srt.By))
		for i, o := range srt.By {
			for c, name := range names {
				if name == o.Column {
					cols[i] = c
				}
			}
		}
		sort.SliceStable(out, func(i, j int) bool {
			for k, o := range srt.By {
				if c := refOrder(out[i][cols[k]], out[j][cols[k]], o.Desc); c != 0 {
					return c < 0
				}
			}

			return false
		})
	}
	if srt.Limit > 0 && len(out) > srt.Limit {
		out = out[:srt.Limit]
	}

	return out, names
}

// refHaving evaluates a filter over an aggregate's row.
func refHaving(e *exec.Expr, row refRow) bool {
	switch e.Op {
	case exec.OpAnd:
		for _, a := range e.Args {
			if !refHaving(a, row) {
				return false
			}
		}

		return true
	case exec.OpOr:
		for _, a := range e.Args {
			if refHaving(a, row) {
				return true
			}
		}

		return false
	case exec.OpNot:
		return !refHaving(e.Args[0], row)
	}
	a, b := refPost(e.Args[0], row), refPost(e.Args[1], row)
	if a.IsNull() || b.IsNull() || refRank(a.Kind) != refRank(b.Kind) || a.Kind == vec.KindVariant {
		return false
	}

	return refCompare(e.Op, refOrder(a, b, false))
}

// refPost evaluates a projection over an aggregate's row.
func refPost(e *exec.Expr, row refRow) exec.Value {
	switch e.Op {
	case exec.OpColumn:
		return row[e.Path]
	case exec.OpLiteral:
		return e.Value
	case exec.OpHourOfDay:
		v := refPost(e.Args[0], row)
		if v.Kind != vec.KindInt {
			return exec.Value{}
		}

		return refValue(exec.HourOfDay(exec.Int(v.Int)), nil)
	}

	return refArith(e.Op, refPost(e.Args[0], row), refPost(e.Args[1], row))
}

func refRank(k vec.Kind) int {
	switch k {
	case vec.KindBool:
		return 1
	case vec.KindInt, vec.KindFloat:
		return 2
	case vec.KindString:
		return 3
	}

	return 4
}

// refOrder orders two values for a sort column: nulls last, then booleans,
// numbers, strings.
func refOrder(a, b exec.Value, desc bool) int {
	switch {
	case a.IsNull() && b.IsNull():
		return 0
	case a.IsNull():
		return 1
	case b.IsNull():
		return -1
	}
	c := cmpInt(int64(refRank(a.Kind)), int64(refRank(b.Kind)))
	if c == 0 {
		switch refRank(a.Kind) {
		case 1:
			c = cmpInt(boolInt(a.Bool), boolInt(b.Bool))
		case 2:
			x, _ := refNumber(a)
			y, _ := refNumber(b)
			switch {
			case a.Kind == vec.KindInt && b.Kind == vec.KindInt:
				c = cmpInt(a.Int, b.Int)
			case x < y:
				c = -1
			case x > y:
				c = 1
			}
		default:
			c = bytes.Compare(a.Bytes, b.Bytes)
		}
	}
	if desc {
		return -c
	}

	return c
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}

	return 0
}

func refAggregate(rows []refRow, agg *exec.Aggregate) ([][]exec.Value, []string) {
	var names []string
	for _, k := range agg.Keys {
		names = append(names, k.Name)
	}
	for _, a := range agg.Aggs {
		names = append(names, a.Name)
	}
	groups := map[string]*refGroup{}
	var order []*refGroup
	floats := make([]bool, len(agg.Aggs))
	for _, row := range rows {
		var keys []exec.Value
		var id strings.Builder
		for _, k := range agg.Keys {
			v := refValue(k.Expr, row)
			keys = append(keys, v)
			id.WriteString(strconv.Quote(refKey(v)))
		}
		g := groups[id.String()]
		if g == nil {
			g = &refGroup{keys: keys, aggs: make([]refAgg, len(agg.Aggs))}
			for i := range g.aggs {
				g.aggs[i] = refAgg{minInt: math.MaxInt64, maxInt: math.MinInt64, minFloat: math.Inf(1), maxFlt: math.Inf(-1), distinct: map[string]struct{}{}}
			}
			groups[id.String()] = g
			order = append(order, g)
		}
		for i, a := range agg.Aggs {
			st := &g.aggs[i]
			st.rows++
			if a.Arg == nil {
				continue
			}
			v := refValue(a.Arg, row)
			if v.IsNull() {
				continue
			}
			st.values++
			st.distinct[refKey(v)] = struct{}{}
			switch v.Kind {
			case vec.KindInt:
				st.ints++
				st.sumInt += v.Int
				st.minInt, st.maxInt = min(st.minInt, v.Int), max(st.maxInt, v.Int)
			case vec.KindFloat:
				floats[i] = true
				st.floats++
				st.sumFloat += v.Float
				st.minFloat, st.maxFlt = min(st.minFloat, v.Float), max(st.maxFlt, v.Float)
			}
		}
	}
	if len(agg.Keys) == 0 && len(order) == 0 {
		g := &refGroup{aggs: make([]refAgg, len(agg.Aggs))}
		order = append(order, g)
	}
	var out [][]exec.Value
	for _, g := range order {
		row := append([]exec.Value(nil), g.keys...)
		for i, a := range agg.Aggs {
			row = append(row, refFinish(a, &g.aggs[i], floats[i]))
		}
		out = append(out, row)
	}

	return out, names
}

func refFinish(a exec.Agg, st *refAgg, float bool) exec.Value {
	intValue := func(v int64) exec.Value { return exec.Value{Kind: vec.KindInt, Int: v} }
	floatValue := func(v float64) exec.Value { return exec.Value{Kind: vec.KindFloat, Float: v} }
	n := st.ints + st.floats
	switch a.Func {
	case exec.Count:
		if a.Arg == nil {
			return intValue(st.rows)
		}

		return intValue(st.values)
	case exec.CountDistinct:
		return intValue(int64(len(st.distinct)))
	}
	if n == 0 {
		return exec.Value{}
	}
	switch a.Func {
	case exec.Avg:
		return floatValue((float64(st.sumInt) + st.sumFloat) / float64(n))
	case exec.Sum:
		if !float {
			return intValue(st.sumInt)
		}

		return floatValue(float64(st.sumInt) + st.sumFloat)
	case exec.Min:
		switch {
		case !float:
			return intValue(st.minInt)
		case st.ints == 0:
			return floatValue(st.minFloat)
		case st.floats == 0:
			return floatValue(float64(st.minInt))
		}

		return floatValue(min(float64(st.minInt), st.minFloat))
	}
	switch {
	case !float:
		return intValue(st.maxInt)
	case st.ints == 0:
		return floatValue(st.maxFlt)
	case st.floats == 0:
		return floatValue(float64(st.maxInt))
	}

	return floatValue(max(float64(st.maxInt), st.maxFlt))
}

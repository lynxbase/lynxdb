package exec_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
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
	}
	args := make([]exec.Value, len(e.Args))
	for i, a := range e.Args {
		args[i] = refValue(a, row)
		// A function or a literal without a value is null, not missing.
		if a.Op != exec.OpColumn && args[i].Kind == exec.KindAny {
			args[i].Kind = vec.KindNull
		}
		switch e.Op {
		case exec.OpExists, exec.OpIsNull, exec.OpIsMissing, exec.OpTypeOf:
			// A reference to every kind tells a null from a missing value.
			if a.Op == exec.OpColumn && a.Kind == exec.KindAny {
				args[i] = row[a.Path]
			}
		}
	}

	return refApply(e, args)
}

func refBool(b bool) exec.Value { return exec.Value{Kind: vec.KindBool, Bool: b} }

func refText(s string) exec.Value { return exec.Value{Kind: vec.KindString, Bytes: []byte(s)} }

// refApply applies a scalar function to the values of its arguments.
func refApply(e *exec.Expr, args []exec.Value) exec.Value {
	null := exec.Value{}
	isString := func(v exec.Value) bool { return v.Kind == vec.KindString }
	a := args[0]
	var b exec.Value
	if len(args) > 1 {
		b = args[1]
	}
	switch e.Op {
	case exec.OpHourOfDay:
		if a.Kind != vec.KindInt {
			return null
		}
		const hour = 3_600_000_000_000
		hours := a.Int / hour
		if a.Int%hour < 0 {
			hours--
		}

		return exec.Value{Kind: vec.KindInt, Int: (hours%24 + 24) % 24}
	case exec.OpAdd:
		if isString(a) && isString(b) {
			return refText(string(a.Bytes) + string(b.Bytes))
		}

		return refArith(e.Op, a, b)
	case exec.OpSub, exec.OpMul, exec.OpDiv:
		return refArith(e.Op, a, b)
	case exec.OpMod:
		if a.Kind != vec.KindInt || b.Kind != vec.KindInt || b.Int == 0 {
			return null
		}

		return exec.Value{Kind: vec.KindInt, Int: int64(new(big.Int).Rem(big.NewInt(a.Int), big.NewInt(b.Int)).Int64())}
	case exec.OpBin:
		x, okx := refNumber(a)
		w, okw := refNumber(b)
		if !okx || !okw || !(w > 0) {
			return null
		}
		if a.Kind == vec.KindInt && b.Kind == vec.KindInt {
			q := new(big.Int)
			q.Div(big.NewInt(a.Int), big.NewInt(b.Int))

			return exec.Value{Kind: vec.KindInt, Int: q.Mul(q, big.NewInt(b.Int)).Int64()}
		}

		return exec.Value{Kind: vec.KindFloat, Float: math.Floor(x/w) * w}
	case exec.OpToInt, exec.OpToFloat:
		var f float64
		switch a.Kind {
		case vec.KindInt:
			if e.Op == exec.OpToInt {
				return a
			}
			f = float64(a.Int)
		case vec.KindFloat:
			f = a.Float
		case vec.KindBool:
			f = float64(boolInt(a.Bool))
		case vec.KindString:
			text := string(a.Bytes)
			if n, err := strconv.ParseInt(text, 10, 64); err == nil && e.Op == exec.OpToInt {
				return exec.Value{Kind: vec.KindInt, Int: n}
			}
			if strings.Trim(text, "0123456789+-.eE") != "" {
				return null
			}
			var err error
			if f, err = strconv.ParseFloat(text, 64); err != nil {
				return null
			}
		default:
			return null
		}
		if e.Op == exec.OpToFloat {
			return exec.Value{Kind: vec.KindFloat, Float: f}
		}
		if !(f >= -9223372036854775808.0 && f < 9223372036854775808.0) {
			return null
		}

		return exec.Value{Kind: vec.KindInt, Int: int64(f)}
	case exec.OpToString:
		switch a.Kind {
		case vec.KindString:
			return a
		case vec.KindInt:
			return refText(strconv.FormatInt(a.Int, 10))
		case vec.KindFloat:
			return refText(strconv.FormatFloat(a.Float, 'g', -1, 64))
		case vec.KindBool:
			return refText(strconv.FormatBool(a.Bool))
		}

		return null
	case exec.OpToBool:
		switch a.Kind {
		case vec.KindBool:
			return a
		case vec.KindInt:
			return refBool(a.Int != 0)
		case vec.KindFloat:
			return refBool(a.Float != 0)
		case vec.KindString:
			switch strings.ToLower(string(a.Bytes)) {
			case "true":
				return refBool(true)
			case "false":
				return refBool(false)
			}
		}

		return null
	case exec.OpIf:
		switch {
		case a.Kind != vec.KindBool:
			return null
		case a.Bool:
			return b
		}

		return args[2]
	case exec.OpCase:
		for i := 0; i+1 < len(args); i += 2 {
			if args[i].Kind == vec.KindBool && args[i].Bool {
				return args[i+1]
			}
		}
		if len(args)%2 == 1 {
			return args[len(args)-1]
		}

		return null
	case exec.OpCoalesce:
		for _, v := range args {
			if !v.IsNull() {
				return v
			}
		}

		return null
	case exec.OpNullIf:
		if c, ok := refSameSort(a, b); ok && c == 0 {
			return null
		}

		return a
	case exec.OpExists:
		return refBool(a.Kind != exec.KindAny)
	case exec.OpIsNull:
		return refBool(a.Kind == vec.KindNull)
	case exec.OpIsMissing:
		return refBool(a.Kind == exec.KindAny)
	case exec.OpTypeOf:
		return refText(map[vec.Kind]string{exec.KindAny: "missing", vec.KindNull: "null", vec.KindBool: "bool", vec.KindInt: "int",
			vec.KindFloat: "float", vec.KindString: "string", vec.KindVariant: "array"}[a.Kind])
	case exec.OpEq, exec.OpNe, exec.OpLt, exec.OpLe, exec.OpGt, exec.OpGe:
		c, ok := refSameSort(a, b)
		if !ok {
			return null
		}

		return refBool(refCompare(e.Op, c))
	case exec.OpAnd, exec.OpOr:
		unknown := false
		for _, v := range args {
			switch {
			case v.Kind != vec.KindBool:
				unknown = true
			case v.Bool == (e.Op == exec.OpOr):
				return v
			}
		}
		if unknown {
			return null
		}

		return refBool(e.Op == exec.OpAnd)
	case exec.OpNot:
		if a.Kind != vec.KindBool {
			return null
		}

		return refBool(!a.Bool)
	}
	if !isString(a) {
		return null
	}
	text := string(a.Bytes)
	switch e.Op {
	case exec.OpMatch:
		return refBool(e.Pattern.Match(a.Bytes))
	case exec.OpLength:
		return exec.Value{Kind: vec.KindInt, Int: int64(len([]rune(text)))}
	case exec.OpLower:
		return refText(strings.ToLower(text))
	case exec.OpUpper:
		return refText(strings.ToUpper(text))
	case exec.OpSubstr:
		if b.Kind != vec.KindInt || (len(args) > 2 && args[2].Kind != vec.KindInt) {
			return null
		}
		runes := []rune(text)
		start := int(b.Int)
		if start < 0 {
			start = max(start+len(runes), 0)
		}
		start = min(start, len(runes))
		end := len(runes)
		if len(args) > 2 {
			end = min(end, start+int(max(args[2].Int, 0)))
		}

		return refText(string(runes[start:end]))
	case exec.OpConcat:
		var sb strings.Builder
		for _, v := range args {
			if !isString(v) {
				return null
			}
			sb.Write(v.Bytes)
		}

		return refText(sb.String())
	}
	if !isString(b) {
		return null
	}
	switch e.Op {
	case exec.OpStartsWith:
		return refBool(strings.HasPrefix(text, string(b.Bytes)))
	case exec.OpEndsWith:
		return refBool(strings.HasSuffix(text, string(b.Bytes)))
	case exec.OpReplace:
		if !isString(args[2]) {
			return null
		}
		if len(b.Bytes) == 0 {
			return a
		}

		return refText(strings.ReplaceAll(text, string(b.Bytes), string(args[2].Bytes)))
	}

	return null
}

// refSameSort orders two numbers, two strings or two booleans.
func refSameSort(a, b exec.Value) (int, bool) {
	switch {
	case a.Kind == vec.KindString && b.Kind == vec.KindString:
		return bytes.Compare(a.Bytes, b.Bytes), true
	case a.Kind == vec.KindBool && b.Kind == vec.KindBool:
		return int(boolInt(a.Bool) - boolInt(b.Bool)), true
	case a.Kind == vec.KindInt && b.Kind == vec.KindInt:
		return cmpInt(a.Int, b.Int), true
	}
	x, okx := refNumber(a)
	y, oky := refNumber(b)
	switch {
	case !okx || !oky || math.IsNaN(x) || math.IsNaN(y):
		return 0, false
	case x < y:
		return -1, true
	case x > y:
		return 1, true
	}

	return 0, true
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
		if y == 0 {
			return exec.Value{}
		}
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
	case exec.OpMatchAny:
		for _, v := range row {
			if v.Kind == vec.KindString && e.Pattern.Match(v.Bytes) {
				return true
			}
		}

		return false
	case exec.OpColumn:
		v := refValue(e, row)

		return v.Kind == vec.KindBool && v.Bool
	case exec.OpLiteral:
		return e.Value.Bool
	case exec.OpEq, exec.OpNe, exec.OpLt, exec.OpLe, exec.OpGt, exec.OpGe:
	default:
		v := refValue(e, row)

		return v.Kind == vec.KindBool && v.Bool
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
		v := refValue(e, row)

		return v.Kind == vec.KindBool && v.Bool
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
// refKeyValue returns the value of a group key and what tells its group
// apart: a null and a missing key form two groups with a null key each.
func refKeyValue(e *exec.Expr, row refRow) (exec.Value, string) {
	if e.Op == exec.OpColumn {
		v, ok := row[e.Path]
		switch {
		case ok && e.Kind == exec.KindAny && v.Kind == vec.KindNull:
			return exec.Value{}, "null"
		case !ok || (e.Kind != exec.KindAny && v.Kind != e.Kind):
			return exec.Value{}, "missing"
		}

		return v, refKey(v)
	}
	v := refValue(e, row)
	if v.IsNull() {
		return exec.Value{}, "null"
	}

	return v, refKey(v)
}

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
	numbers                    []float64
	// The values of the first and the last row with one, and of the rows
	// with the smallest and the largest time.
	first, last, earliest, latest exec.Value
	minTime, maxTime              int64
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
	}
	args := make([]exec.Value, len(e.Args))
	for i, a := range e.Args {
		if args[i] = refPost(a, row); args[i].Kind == exec.KindAny {
			args[i].Kind = vec.KindNull
		}
	}

	return refApply(e, args)
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
			v, name := refKeyValue(k.Expr, row)
			keys = append(keys, v)
			id.WriteString(strconv.Quote(name))
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
			if at := row[part.TimePath].Int; st.values == 1 {
				st.first, st.earliest, st.latest, st.minTime, st.maxTime = v, v, v, at, at
			} else if at < st.minTime {
				st.earliest, st.minTime = v, at
			} else if at > st.maxTime {
				st.latest, st.maxTime = v, at
			}
			st.last = v
			switch v.Kind {
			case vec.KindInt:
				st.numbers = append(st.numbers, float64(v.Int))
				st.ints++
				st.sumInt += v.Int
				st.minInt, st.maxInt = min(st.minInt, v.Int), max(st.maxInt, v.Int)
			case vec.KindFloat:
				st.numbers = append(st.numbers, v.Float)
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
	case exec.First:
		return st.first
	case exec.Last:
		return st.last
	case exec.Earliest:
		return st.earliest
	case exec.Latest:
		return st.latest
	}
	if n == 0 {
		return exec.Value{}
	}
	switch a.Func {
	case exec.Avg:
		return floatValue((float64(st.sumInt) + st.sumFloat) / float64(n))
	case exec.Var, exec.Stdev:
		if n < 2 {
			return exec.Value{}
		}
		mean, spread := 0.0, 0.0
		for _, x := range st.numbers {
			mean += x
		}
		mean /= float64(n)
		for _, x := range st.numbers {
			spread += (x - mean) * (x - mean)
		}
		if a.Func == exec.Var {
			return floatValue(spread / float64(n-1))
		}

		return floatValue(math.Sqrt(spread / float64(n-1)))
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

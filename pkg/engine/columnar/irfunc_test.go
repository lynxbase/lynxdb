package columnar

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irfunc"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// notNative lists the scalar functions without a native implementation and
// why: they stay with the bridge to the row engine or with the row
// operators.
var notNative = map[string]string{
	"sysdate":   "volatile: the executor computes a function once per distinct argument",
	"rand":      "volatile: the executor computes a function once per distinct argument",
	"forall":    "takes a lambda",
	"exists":    "takes a lambda",
	"filter":    "takes a lambda",
	"transform": "takes a lambda",
	"reduce":    "takes a lambda",
}

// pendingNative lists the categories whose functions may still lack a
// native implementation; those run through the bridge to the row engine.
var pendingNative = map[string]bool{"collection": true}

func strValue(s string) exec.Value { return exec.Value{Kind: vec.KindString, Bytes: []byte(s)} }
func intValue(n int64) exec.Value  { return exec.Value{Kind: vec.KindInt, Int: n} }
func fltValue(f float64) exec.Value {
	return exec.Value{Kind: vec.KindFloat, Float: f}
}

func variantValue(text string) exec.Value {
	var x any
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	if err := dec.Decode(&x); err != nil {
		panic(err)
	}
	var b []byte
	var enc func(x any)
	enc = func(x any) {
		switch t := x.(type) {
		case nil:
			b = variant.AppendNull(b)
		case bool:
			b = variant.AppendBool(b, t)
		case string:
			b = variant.AppendString(b, []byte(t))
		case json.Number:
			if n, err := t.Int64(); err == nil {
				b = variant.AppendInt(b, n)
			} else {
				f, _ := t.Float64()
				b = variant.AppendFloat(b, f)
			}
		case []any:
			var mark int
			b, mark = variant.BeginArray(b)
			for _, e := range t {
				enc(e)
			}
			b = variant.End(b, mark)
		case map[string]any:
			keys := make([]string, 0, len(t))
			for k := range t {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			var mark int
			b, mark = variant.BeginObject(b)
			for _, k := range keys {
				b = variant.AppendKey(b, []byte(k))
				enc(t[k])
			}
			b = variant.End(b, mark)
		}
	}
	enc(x)

	return exec.Value{Kind: vec.KindVariant, Bytes: b}
}

var longText = strings.Repeat("Lorem ipsum dolor sit amet, ÄÖÜ ß 日本語 ", 9)

// anyValues are what a field of no declared type holds: every kind, nulls,
// missing values, boundary numbers and strings meant for each family.
var anyValues = func() []exec.Value {
	vals := []exec.Value{{}, {Kind: vec.KindNull}, {Kind: vec.KindBool, Bool: true}, {Kind: vec.KindBool}}
	for _, n := range []int64{0, 1, -1, 2, 7, 10, 16, 42, 100, 255, 365, 1999, 2024, 202402, 9912, 3600, -3600,
		1_700_000_000, 1_000_000_000_000, math.MaxInt64, math.MinInt64} {
		vals = append(vals, intValue(n))
	}
	for _, f := range []float64{0, math.Copysign(0, -1), 0.5, -2.5, 1.5, 2.675, 3.14159, 59.999, 1e21, 1e-7, 123456.789,
		-1234567.891, 1_700_000_000.25, math.NaN(), math.Inf(1), math.Inf(-1)} {
		vals = append(vals, fltValue(f))
	}
	for _, s := range []string{"", " ", "abc", "ABC def", "  padded \t", "42", " 42 ", "-17", "+8", "3.5", "1e3", ".5",
		"0x1F", "1F", "z", "1,234,567.89", "10kb", "12k", "3M", "2g", "1.5 GB", "true", "FALSE", "yes", "n/a", "NaN", "inf",
		"héllo wörld", "ǅungla ﬀ", "İstanbul", "ΣΑΣ", "\xff\xfe bad", "a\x00b", "日本語テキスト",
		"2024-02-29 23:59:59", "2023-12-31T23:59:59.999999999Z", "2024-01-01", "1999-12-31 23:59", "2021-01-03T10:00:00+05:00",
		"01/02/2006:15:04:05", "10/Oct/2000:13:55:36 -0700", "12:34:56", "12:34", "23:59:59.5", "25:00:00", "1+02:03:04",
		"02:03:04", "1:30.5", "45.25", "a,b,c", "a--b--c", "a%b_c", "abcabc", "aaa", "foo.bar.baz", "x*y*z", "*",
		`{"a":1,"b":[1,2,{"c":"x"}],"d":"t"}`, `[1,2,3]`, `[]`, `{}`, `"str"`, `{"a":`, "null",
		"%Y-%m-%d %H:%i:%s", "%d/%m/%Y %r", "%a %b %D %j %U %u %V %v %X %x %W %w %f %p %h %l %k %c %e %M %y %%",
		"%Y-%m-%dT%H:%M:%S %Z %z %s %N %F %T %C %A %B %I %e %u %j %Q %q", "%m/%d/%Y", "%d.%m.%Y %H.%i.%s",
		"+05:30", "-08:00", "+14:00", "+15:00", "UTC", "America/New_York", "Europe/Berlin", "Asia/Kolkata", "Invalid/Zone",
		"day", "DAY", "month", "hour", "year", "week", "quarter", "minute", "second", "microsecond", "millisecond", "fortnight",
		"date", "time", "datetime", "timestamp", "USA", "EUR", "ISO", "JIS", "INTERNAL",
		"192.168.1.10", "10.0.0.0/8", "192.168.1.0/24", "::1", "2001:db8::/32", "not an ip",
		"$.a", "$.b[1]", "$.b[*]", "b[-1].c", "$['d']", "$.*", "$.missing", "a", ".", "$[0]",
		longText} {
		vals = append(vals, strValue(s))
	}
	vals = append(vals, variantValue(`[1,"a",null,true,2.5,"10:00"]`), variantValue(`{"b":1,"a":"x","c":[1,2]}`),
		variantValue(`[]`), variantValue(`["x","y","x"]`))

	return vals
}()

func at(s string) int64 {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}

	return t.UnixNano()
}

// timeValues are times around day, month and year boundaries.
var timeValues = func() []exec.Value {
	vals := []exec.Value{{}, {Kind: vec.KindNull}}
	for _, s := range []string{"1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999999999Z", "2023-12-31T23:59:59.999999999Z",
		"2024-01-01T00:00:00Z", "2024-02-29T12:34:56.789Z", "2021-01-03T00:00:00Z", "2021-01-04T05:06:07Z",
		"2020-12-31T23:00:00Z", "1999-12-31T23:59:59Z", "2026-11-17T09:47:01.123456789Z", "2262-04-11T23:47:16Z",
		"1677-09-21T00:12:44Z", "2000-02-29T00:00:00Z", "2019-12-30T11:59:59Z"} {
		vals = append(vals, intValue(at(s)))
	}

	return vals
}()

var durationValues = func() []exec.Value {
	vals := []exec.Value{{}, {Kind: vec.KindNull}}
	for _, d := range []time.Duration{0, time.Second, 90061 * time.Second, -time.Hour, 12*time.Hour + 34*time.Minute + 56789*time.Millisecond,
		24*time.Hour - time.Nanosecond, 1500 * time.Microsecond} {
		vals = append(vals, intValue(int64(d)))
	}

	return vals
}()

// constValues lists the literals a constant parameter is tested with.
func constValues(f *ir.Func, p ir.Param) []any {
	switch f.Name + "." + p.Name {
	case "tostring.format":
		return []any{"hex", "binary", "commas", "duration", "", "HEX", "other", nil}
	case "tonumber.base":
		return []any{int64(10), int64(2), int64(16), int64(36), int64(1), int64(37), 8.0}
	case "like.pattern":
		return []any{"%a_c%", "abc", `a\%b%`, "%", "_", "h%", "%ß%", `\`}
	case "like.case_sensitive":
		return []any{true, false}
	case "regexp_match.pattern", "regexp_extract.pattern", "regexp_replace.pattern":
		return []any{"a(b+)", `(\d+)-?(\d*)`, "^$", "(?i)abc", `[a-z]+`, "é|ö", `(x)?y`}
	case "regexp_extract.group":
		return []any{int64(0), int64(1), int64(2), int64(5), int64(-1)}
	case "wildcard_replace.pattern":
		return []any{"a*c", "*", "ab", `\*`, "x*y*z", "", "*.bar"}
	case "wildcard_replace.replacement":
		return []any{"<*>", "", "*-*", `\*`, "q"}
	case "sha2.bits":
		return []any{int64(224), int64(256), int64(384), int64(512), int64(0)}
	case "extract.part":
		parts := []any{"YEAR", "month", "Day_Hour", "minute_second", "week", "quarter", "microsecond", "millisecond",
			"second_microsecond", "minute_microsecond", "hour_microsecond", "hour_second", "hour_minute",
			"day_microsecond", "day_second", "day_minute", "year_month", "hour", "second", "nope"}
		return parts
	case "get_format.type":
		return []any{"DATE", "time", "Datetime", "timestamp", "other"}
	case "get_format.locale":
		return []any{"USA", "eur", "ISO", "JIS", "INTERNAL", "XX"}
	case "date_add.unit":
		return []any{"day", "month", "year", "quarter", "week", "hour", "minute", "second", "microsecond", "millisecond", "DAY", "fortnight"}
	case "timestampadd.unit", "timestampdiff.unit":
		return []any{"DAY", "month", "year", "Quarter", "week", "hour", "minute", "second", "microsecond", "millisecond", "fortnight"}
	case "week.mode", "yearweek.mode":
		return []any{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(-1)}
	case "mvfind.regex":
		return []any{"^a", `\d`, "y$"}
	case "time_bucket.n":
		return []any{int64(1), int64(5), int64(0), int64(-1)}
	case "time_bucket.unit":
		return []any{"ms", "s", "m", "h", "d", "w", "mon", "q", "y", "x"}
	case "num_bucket.width":
		return []any{int64(10), 0.5, int64(0), int64(-3)}
	}
	switch p.Type.Kind {
	case ir.KindInt:
		return []any{int64(0), int64(2), int64(-1)}
	case ir.KindBool:
		return []any{true, false}
	case ir.KindFloat:
		return []any{1.5, int64(3)}
	}

	return []any{"abc", ""}
}

func literalValue(v any) exec.Value {
	switch v := v.(type) {
	case string:
		return strValue(v)
	case int64:
		return intValue(v)
	case float64:
		return fltValue(v)
	case bool:
		return exec.Value{Kind: vec.KindBool, Bool: v}
	}

	return exec.Value{Kind: vec.KindNull}
}

func literalType(v any) ir.Type {
	switch v.(type) {
	case string:
		return ir.String
	case int64:
		return ir.Int
	case float64:
		return ir.Float
	case bool:
		return ir.Bool
	}

	return ir.Null
}

// rowValue is a value of the executor as the row engine holds it, with the
// field's IR type telling a time or a duration from an integer.
func rowValue(v exec.Value, t ir.Type) (event.Value, bool) {
	switch {
	case v.Kind == exec.KindAny:
		return event.Value{}, false
	case v.Kind == vec.KindNull:
		return event.NullValue(), true
	case v.Kind == vec.KindInt && t.Kind == ir.KindTimestamp:
		return event.TimestampValue(time.Unix(0, v.Int).UTC()), true
	case v.Kind == vec.KindInt && t.Kind == ir.KindDuration:
		return event.DurationValue(time.Duration(v.Int)), true
	}

	return valueOf(v), true
}

// sameValue compares results, objects by content.
func sameValue(a, b exec.Value) bool {
	if a.Kind == exec.KindAny {
		a.Kind = vec.KindNull
	}
	if b.Kind == exec.KindAny {
		b.Kind = vec.KindNull
	}
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case vec.KindBool:
		return a.Bool == b.Bool
	case vec.KindInt:
		return a.Int == b.Int
	case vec.KindFloat:
		return a.Float == b.Float || (a.Float != a.Float && b.Float != b.Float)
	case vec.KindString:
		return bytes.Equal(a.Bytes, b.Bytes)
	case vec.KindVariant:
		return reflect.DeepEqual(decodeVariant(a.Bytes), decodeVariant(b.Bytes))
	}

	return true
}

func decodeVariant(b []byte) any {
	text, err := variant.AppendJSON(nil, b)
	if err != nil {
		return err.Error()
	}
	var x any
	if json.Unmarshal(text, &x) != nil {
		return string(text)
	}

	return x
}

func show(v exec.Value) string {
	switch v.Kind {
	case exec.KindAny:
		return "missing"
	case vec.KindNull:
		return "null"
	case vec.KindBool:
		return fmt.Sprint(v.Bool)
	case vec.KindInt:
		return fmt.Sprintf("int %d", v.Int)
	case vec.KindFloat:
		return fmt.Sprintf("float %v", v.Float)
	case vec.KindString:
		return fmt.Sprintf("string %q", v.Bytes)
	case vec.KindVariant:
		text, _ := variant.AppendJSON(nil, v.Bytes)
		return "variant " + string(text)
	}

	return "?"
}

// shape is one form of a call: the IR type of each field argument and the
// literal of each constant one.
type shape struct {
	types  []ir.Type
	consts []any // nil entry where the argument is a field
	isLit  []bool
}

func (s shape) call(f *ir.Func) *ir.Call {
	args := make([]ir.Expr, len(s.types))
	types := make([]ir.Type, len(s.types))
	for i := range args {
		if s.isLit[i] {
			args[i], types[i] = &ir.Literal{V: s.consts[i], T: literalType(s.consts[i])}, literalType(s.consts[i])
			continue
		}
		args[i], types[i] = &ir.FieldRef{Name: fmt.Sprintf("a%d", i), T: s.types[i]}, s.types[i]
	}
	t, _ := f.Check(types)

	return &ir.Call{Func: f, Args: args, T: t}
}

func valuesFor(t ir.Type) []exec.Value {
	switch t.Kind {
	case ir.KindTimestamp:
		return timeValues
	case ir.KindDuration:
		return durationValues
	}

	return anyValues
}

// shapes enumerates the forms a function is tested in: every arity, every
// combination of constant arguments, fields of no declared type, and one
// field at a time typed as a time or a duration.
func shapes(f *ir.Func) []shape {
	most := len(f.Params)
	if f.Variadic {
		most = max(most+1, 3)
	}
	var out []shape
	for n := f.MinArgs; n <= most; n++ {
		base := shape{types: make([]ir.Type, n), consts: make([]any, n), isLit: make([]bool, n)}
		var constPos []int
		for i := 0; i < n; i++ {
			p, _ := f.ParamFor(i)
			base.types[i] = ir.Any
			if p.Const {
				constPos = append(constPos, i)
			}
		}
		combos := [][]any{{}}
		for _, i := range constPos {
			p, _ := f.ParamFor(i)
			var next [][]any
			for _, c := range combos {
				for _, v := range constValues(f, p) {
					next = append(next, append(slices.Clone(c), v))
				}
			}
			combos = next
		}
		for _, combo := range combos {
			s := base
			s.types, s.consts, s.isLit = slices.Clone(base.types), slices.Clone(base.consts), slices.Clone(base.isLit)
			for k, i := range constPos {
				s.consts[i], s.isLit[i] = combo[k], true
			}
			out = append(out, s)
			for i := 0; i < n; i++ {
				if s.isLit[i] {
					continue
				}
				for _, t := range []ir.Type{ir.Timestamp, ir.Duration} {
					if t.Kind == ir.KindDuration && f.Category != "datetime" {
						continue
					}
					typed := s
					typed.types = slices.Clone(s.types)
					typed.types[i] = t
					out = append(out, typed)
				}
			}
		}
	}

	return out
}

// tuples returns argument values for the field positions of a shape: every
// combination when there are few, else a sample.
func tuples(s shape, limit int, r *rand.Rand) [][]exec.Value {
	lists := make([][]exec.Value, len(s.types))
	total := 1
	for i := range lists {
		if s.isLit[i] {
			lists[i] = []exec.Value{literalValue(s.consts[i])}
		} else {
			lists[i] = valuesFor(s.types[i])
		}
		total *= len(lists[i])
	}
	var out [][]exec.Value
	if total <= limit {
		idx := make([]int, len(lists))
		for {
			t := make([]exec.Value, len(lists))
			for i, l := range lists {
				t[i] = l[idx[i]]
			}
			out = append(out, t)
			i := 0
			for ; i < len(idx); i++ {
				if idx[i]++; idx[i] < len(lists[i]) {
					break
				}
				idx[i] = 0
			}
			if i == len(idx) {
				return out
			}
		}
	}
	for k := 0; k < limit; k++ {
		t := make([]exec.Value, len(lists))
		for i, l := range lists {
			t[i] = l[r.IntN(len(l))]
		}
		out = append(out, t)
	}

	return out
}

// rowEngineBugs lists where the row engine departs from the IR's
// documentation and whether the native functions do the same.
var rowEngineBugs = []struct {
	function, input, rowEngine, documented, native string
}{
	{"parse_json", "an array or object argument holding integers", "every integer inside becomes null",
		"the value the JSON denotes, with objects and arrays as nested values", "the same, for the conformance test"},
	{"substring", "a length that overflows int64 when added to the start", "fails, so the bridge gives null",
		"at most length characters: the rest of the string", "the same, for the conformance test"},
	{"regexp_extract", "a negative group", "fails, so the bridge gives null", "the text of the group, or NULL",
		"the same: NULL"},
	{"every conversion of a float to an integer", "a float beyond int64 or NaN, also inside ceil, floor, round, " +
		"sec_to_time, makedate, maketime, from_unixtime, strftime, ctime and casts",
		"the processor's conversion: saturated on arm64, MinInt64 on amd64", "not stated",
		"saturates to MaxInt64 or MinInt64 by sign and gives 0 for NaN, as arm64 does; " +
			"the conformance test compares these inputs only on arm64"},
	{"wildcard_replace", "a constant pattern with invalid UTF-8", "the compiler panics",
		"the pattern replaced in the text", "the builder refuses it"},
	{"json_extract", "a path with a wildcard over an object", "the members in random order",
		"the values at the path", "the members in key order; the conformance test compares them in any order"},
	{"json_extract", "an array or object argument and a path to an integer", "null",
		"the value at the path", "the same, for the conformance test"},
}

// sameInAnyOrder compares JSON texts with the elements of every array in
// any order.
func sameInAnyOrder(a, b exec.Value) bool {
	if a.Kind != vec.KindString || b.Kind != vec.KindString {
		return false
	}
	var x, y any
	if json.Unmarshal(a.Bytes, &x) != nil || json.Unmarshal(b.Bytes, &y) != nil {
		return false
	}

	return reflect.DeepEqual(sortedArrays(x), sortedArrays(y))
}

func sortedArrays(x any) any {
	switch t := x.(type) {
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = sortedArrays(e)
		}
		slices.SortFunc(out, func(a, b any) int {
			ta, _ := json.Marshal(a)
			tb, _ := json.Marshal(b)
			return strings.Compare(string(ta), string(tb))
		})
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = sortedArrays(e)
		}
		return out
	}

	return x
}

// wildcard reports a call of json_extract with a wildcard in a path.
func wildcard(call *ir.Call, args []exec.Value) bool {
	if call.Func.Name != "json_extract" {
		return false
	}
	for _, v := range args[1:] {
		if v.Kind == vec.KindString && bytes.Contains(v.Bytes, []byte("*")) {
			return true
		}
	}

	return false
}

// convertsByProcessor reports arguments of no declared type that may reach
// a conversion of a float beyond int64, which the row engine leaves to the
// processor.
func convertsByProcessor(s shape, args []exec.Value) bool {
	for i, v := range args {
		if !s.isLit[i] && s.types[i].Kind != ir.KindAny {
			continue
		}
		var f float64
		switch v.Kind {
		case vec.KindInt:
			f = float64(v.Int)
		case vec.KindFloat:
			f = v.Float
		case vec.KindString:
			parsed, err := strconv.ParseFloat(strings.TrimSpace(string(v.Bytes)), 64)
			if err != nil {
				continue
			}
			f = parsed
		default:
			continue
		}
		if f != f || math.Abs(f) >= 300 {
			return true
		}
	}

	return false
}

// against compares the native function of a call with the row engine's
// value of the call over argument tuples and returns the first difference.
func against(t testing.TB, call *ir.Call, b irfunc.Builder, s shape, rows [][]exec.Value) (string, bool) {
	newFunc, err := b(call, irfunc.Env{Now: testNow})
	if err != nil {
		return "", false
	}
	prog, err := vm.CompileExpr(call, vm.CompileOptions{Now: testNow})
	if err != nil {
		t.Logf("%s: the row engine does not compile it: %v", call, err)
		return "", true
	}
	native := newFunc()
	var machine vm.VM
	row := map[string]event.Value{}
	var buf []byte
	for _, args := range rows {
		if runtime.GOARCH != "arm64" && convertsByProcessor(s, args) {
			continue
		}
		clear(row)
		for i, v := range args {
			if s.isLit[i] {
				continue
			}
			if ev, ok := rowValue(v, s.types[i]); ok {
				row[fmt.Sprintf("a%d", i)] = ev
			}
		}
		want := exec.Value{Kind: vec.KindNull}
		if out, err := machine.Execute(prog, row); err == nil {
			want, _ = executorValue(out, nil)
		}
		var got exec.Value
		got, buf = native(args, buf[:0])
		if !sameValue(got, want) && !(wildcard(call, args) && sameInAnyOrder(got, want)) {
			in := make([]string, len(args))
			for i, v := range args {
				in[i] = show(v)
				if !s.isLit[i] && s.types[i].Kind != ir.KindAny {
					in[i] += " as " + s.types[i].String()
				}
			}
			return fmt.Sprintf("%s(%s): native %s, row engine %s", call.Func.Name, strings.Join(in, ", "), show(got), show(want)), true
		}
	}

	return "", true
}

func TestNativeFunctions(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 11))
	for _, f := range ir.Funcs() {
		if f.Kind != ir.ScalarFunc {
			continue
		}
		t.Run(f.Category+"/"+f.Name, func(t *testing.T) {
			b, ok := irfunc.Lookup(f.Name)
			if reason, listed := notNative[f.Name]; listed {
				if ok {
					t.Fatalf("listed as not native (%s) but has a builder", reason)
				}
				t.Skip(reason)
			}
			if !ok && pendingNative[f.Category] {
				t.Skip("pending")
			}
			if !ok {
				t.Fatal("no native implementation and not listed in notNative")
			}
			built := 0
			for _, s := range shapes(f) {
				call := s.call(f)
				diff, ok := against(t, call, b, s, tuples(s, 4000, r))
				if diff != "" {
					t.Fatal(diff)
				}
				if ok {
					built++
				} else {
					t.Logf("%s stays on the bridge", call)
				}
			}
			if built == 0 {
				t.Fatal("the builder accepts no form of the call")
			}
		})
	}
}

func TestNativeCasts(t *testing.T) {
	for _, to := range []ir.Type{ir.Int, ir.Float, ir.String, ir.Bool, ir.Timestamp, ir.Duration, ir.ArrayOf(ir.Any), ir.Object} {
		for _, from := range []ir.Type{ir.Any, ir.Timestamp, ir.Duration} {
			t.Run(to.String()+"/"+from.String(), func(t *testing.T) {
				cast := &ir.Cast{X: &ir.FieldRef{Name: "a0", T: from}, To: to}
				newFunc, err := irfunc.Cast(cast)
				if err != nil {
					t.Fatal(err)
				}
				prog, err := vm.CompileExpr(cast, vm.CompileOptions{Now: testNow})
				if err != nil {
					t.Fatal(err)
				}
				native := newFunc()
				var machine vm.VM
				for _, v := range valuesFor(from) {
					arg := shape{types: []ir.Type{from}, consts: []any{nil}, isLit: []bool{false}}
					if runtime.GOARCH != "arm64" && convertsByProcessor(arg, []exec.Value{v}) {
						continue
					}
					row := map[string]event.Value{}
					if ev, ok := rowValue(v, from); ok {
						row["a0"] = ev
					}
					want := exec.Value{Kind: vec.KindNull}
					if out, err := machine.Execute(prog, row); err == nil {
						want, _ = executorValue(out, nil)
					}
					if got, _ := native([]exec.Value{v}, nil); !sameValue(got, want) {
						t.Fatalf("CAST(%s AS %s): native %s, row engine %s", show(v), to, show(got), show(want))
					}
				}
			})
		}
	}
}

// fuzzFuncs are the string and conversion functions the fuzz target calls.
var fuzzFuncs = func() []*ir.Func {
	var out []*ir.Func
	for _, f := range ir.Funcs() {
		if f.Kind == ir.ScalarFunc && (f.Category == "string" || f.Category == "conversion") {
			if _, ok := irfunc.Lookup(f.Name); ok {
				out = append(out, f)
			}
		}
	}

	return out
}()

func FuzzNativeStrings(f *testing.F) {
	f.Add(uint8(0), "héllo", "l", int64(2), uint8(0))
	f.Add(uint8(3), " 12.5kb ", "%Y", int64(-1), uint8(5))
	f.Add(uint8(7), "a,b,,c", ",", int64(0), uint8(2))
	f.Fuzz(func(t *testing.T, which uint8, a, b string, n int64, kinds uint8) {
		fn := fuzzFuncs[int(which)%len(fuzzFuncs)]
		arity := fn.MinArgs + int(kinds>>6)%(max(fn.MaxArgs(), fn.MinArgs+1)-fn.MinArgs+1)
		if fn.MaxArgs() >= 0 {
			arity = min(arity, fn.MaxArgs())
		}
		s := shape{types: make([]ir.Type, arity), consts: make([]any, arity), isLit: make([]bool, arity)}
		args := make([]exec.Value, arity)
		for i := range args {
			p, _ := fn.ParamFor(i)
			s.types[i] = ir.Any
			switch (kinds >> (2 * (i % 3))) & 3 {
			case 0:
				args[i] = strValue(a)
			case 1:
				args[i] = strValue(b)
			case 2:
				args[i] = intValue(n)
			default:
				args[i] = fltValue(float64(n) / 7)
			}
			if p.Const {
				s.isLit[i] = true
				switch args[i].Kind {
				case vec.KindString:
					s.consts[i] = string(args[i].Bytes)
				case vec.KindInt:
					s.consts[i] = args[i].Int
				default:
					s.consts[i] = args[i].Float
				}
				if p.Type.Kind == ir.KindBool {
					s.consts[i] = n%2 == 0
					args[i] = literalValue(s.consts[i])
				}
			}
		}
		if diff, _ := against(t, s.call(fn), mustLookup(fn.Name), s, [][]exec.Value{args}); diff != "" {
			t.Fatal(diff)
		}
	})
}

func mustLookup(name string) irfunc.Builder {
	b, _ := irfunc.Lookup(name)
	return b
}

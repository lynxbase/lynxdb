package vm

import (
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Value conversions shared by the IR natives. Each returns false when the
// value cannot be interpreted, and the calling function then yields NULL.

func numFloat(v event.Value) (float64, bool) {
	switch v.Type() {
	case event.FieldTypeInt:
		return float64(v.AsInt()), true
	case event.FieldTypeFloat:
		return v.AsFloat(), true
	case event.FieldTypeString:
		f, err := strconv.ParseFloat(strings.TrimSpace(v.AsString()), 64)
		return f, err == nil
	case event.FieldTypeBool:
		if v.AsBool() {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func numInt(v event.Value) (int64, bool) {
	switch v.Type() {
	case event.FieldTypeInt:
		return v.AsInt(), true
	case event.FieldTypeFloat:
		f := v.AsFloat()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return toInt(f), true
	case event.FieldTypeString:
		s := strings.TrimSpace(v.AsString())
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return toInt(f), true
		}
	case event.FieldTypeBool:
		if v.AsBool() {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// parseNumber parses decimal text to an int when it has no fraction or
// exponent and to a float otherwise.
func parseNumber(s string) (event.Value, bool) {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return event.IntValue(n), true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return event.FloatValue(f), true
	}
	return event.NullValue(), false
}

// numericPair converts a string operand to a number when the other operand
// is a number, and to a timestamp when the other operand is a timestamp;
// strings that do not parse stay strings, so the operation yields NULL.
func numericPair(a, b event.Value) (event.Value, event.Value) {
	at, bt := a.Type(), b.Type()
	isNum := func(t event.FieldType) bool { return t == event.FieldTypeInt || t == event.FieldTypeFloat }
	if at == event.FieldTypeString && isNum(bt) {
		if n, ok := parseNumber(a.AsString()); ok {
			return n, b
		}
	}
	if bt == event.FieldTypeString && isNum(at) {
		if n, ok := parseNumber(b.AsString()); ok {
			return a, n
		}
	}
	if at == event.FieldTypeTimestamp && bt == event.FieldTypeString {
		if t, ok := timeOf(b); ok {
			return a, event.TimestampValue(t)
		}
	}
	if bt == event.FieldTypeTimestamp && at == event.FieldTypeString {
		if t, ok := timeOf(a); ok {
			return event.TimestampValue(t), b
		}
	}
	return a, b
}

func inStrictMode(val event.Value, items []event.Value, warnings *WarningCounters, numeric bool) event.Value {
	if !numeric {
		return inStrict(val, items, warnings)
	}
	if val.IsNull() {
		return event.NullValue()
	}
	hasNull := false
	for _, item := range items {
		a, b := numericPair(val, item)
		eq, isNull := strictEq(a, b, warnings)
		if isNull {
			hasNull = true
			continue
		}
		if eq {
			return event.BoolValue(true)
		}
	}
	if hasNull {
		return event.NullValue()
	}
	return event.BoolValue(false)
}

// TimestampLayout is the text form of timestamps converted to strings.
const TimestampLayout = "2006-01-02 15:04:05.999999999"

func formatFloat(f float64) string {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if strings.ContainsAny(s, "e") {
		return s
	}
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

func formatTimeOfDay(d time.Duration) string {
	neg := d < 0
	if neg {
		d = -d
	}
	h := int64(d / time.Hour)
	m := int64(d % time.Hour / time.Minute)
	s := int64(d % time.Minute / time.Second)
	ns := int64(d % time.Second)
	out := strconv.FormatInt(h, 10)
	if h < 10 {
		out = "0" + out
	}
	out += ":" + pad2(m) + ":" + pad2(s)
	if ns > 0 {
		frac := strings.TrimRight(strconv.FormatInt(ns+1e9, 10)[1:], "0")
		out += "." + frac
	}
	if neg {
		out = "-" + out
	}
	return out
}

func pad2(n int64) string {
	if n < 10 {
		return "0" + strconv.FormatInt(n, 10)
	}
	return strconv.FormatInt(n, 10)
}

// str converts a value to its canonical text: numbers in decimal, booleans
// as true/false, timestamps as yyyy-MM-dd HH:mm:ss[.fraction], durations as
// HH:mm:ss[.fraction], arrays and objects as JSON.
func str(v event.Value) (string, bool) {
	switch v.Type() {
	case event.FieldTypeNull:
		return "", false
	case event.FieldTypeString:
		return v.AsString(), true
	case event.FieldTypeInt:
		return strconv.FormatInt(v.AsInt(), 10), true
	case event.FieldTypeFloat:
		return formatFloat(v.AsFloat()), true
	case event.FieldTypeBool:
		return strconv.FormatBool(v.AsBool()), true
	case event.FieldTypeTimestamp:
		return v.AsTimestamp().UTC().Format(TimestampLayout), true
	case event.FieldTypeDuration:
		return formatTimeOfDay(v.AsDuration()), true
	case event.FieldTypeArray, event.FieldTypeObject:
		b, err := json.Marshal(toInterface(v))
		if err != nil {
			return "", false
		}
		return string(b), true
	}
	return v.String(), true
}

func toInterface(v event.Value) any {
	switch v.Type() {
	case event.FieldTypeArray:
		arr := v.AsArray()
		out := make([]any, len(arr))
		for i, e := range arr {
			out[i] = toInterface(e)
		}
		return out
	case event.FieldTypeObject:
		obj := v.AsObject()
		out := make(map[string]any, len(obj))
		for k, e := range obj {
			out[k] = toInterface(e)
		}
		return out
	case event.FieldTypeTimestamp, event.FieldTypeDuration:
		s, _ := str(v)
		return s
	}
	return v.Interface()
}

// fromInterface converts decoded JSON to a value; numbers keep int form when
// they have no fraction.
func fromInterface(x any) event.Value {
	switch t := x.(type) {
	case nil:
		return event.NullValue()
	case bool:
		return event.BoolValue(t)
	case string:
		return event.StringValue(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return event.IntValue(n)
		}
		f, _ := t.Float64()
		return event.FloatValue(f)
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
			return event.IntValue(int64(t))
		}
		return event.FloatValue(t)
	case []any:
		out := make([]event.Value, len(t))
		for i, e := range t {
			out[i] = fromInterface(e)
		}
		return event.ArrayValue(out)
	case map[string]any:
		out := make(map[string]event.Value, len(t))
		for k, e := range t {
			out[k] = fromInterface(e)
		}
		return event.ObjectValue(out)
	}
	return event.NullValue()
}

var timestampInputLayouts = []string{
	time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04", "2006-01-02", "01/02/2006:15:04:05", "02/Jan/2006:15:04:05 -0700",
}

// timeOf interprets a value as an instant: a timestamp, or a string in one
// of the accepted layouts (UTC unless it carries an offset).
func timeOf(v event.Value) (time.Time, bool) {
	switch v.Type() {
	case event.FieldTypeTimestamp:
		return v.AsTimestamp().UTC(), true
	case event.FieldTypeString:
		s := strings.TrimSpace(v.AsString())
		for _, l := range timestampInputLayouts {
			if t, err := time.Parse(l, s); err == nil {
				return t.UTC(), true
			}
		}
	}
	return time.Time{}, false
}

// timeOfDay interprets a value as a time of day: a duration, the clock part
// of a timestamp, or an HH:mm[:ss[.fraction]] string.
func timeOfDay(v event.Value) (time.Duration, bool) {
	switch v.Type() {
	case event.FieldTypeDuration:
		return v.AsDuration(), true
	case event.FieldTypeTimestamp:
		t := v.AsTimestamp().UTC()
		return t.Sub(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)), true
	case event.FieldTypeString:
		s := strings.TrimSpace(v.AsString())
		for _, l := range []string{"15:04:05.999999999", "15:04:05", "15:04"} {
			if t, err := time.Parse(l, s); err == nil {
				return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute +
					time.Duration(t.Second())*time.Second + time.Duration(t.Nanosecond()), true
			}
		}
		if t, ok := timeOf(v); ok {
			return t.Sub(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)), true
		}
	}
	return 0, false
}

func castFunc(to ir.Type) NativeFunc {
	switch to.Kind {
	case ir.KindInt:
		return func(a []event.Value) event.Value {
			if n, ok := numInt(a[0]); ok {
				return event.IntValue(n)
			}
			return event.NullValue()
		}
	case ir.KindFloat:
		return func(a []event.Value) event.Value {
			if f, ok := numFloat(a[0]); ok {
				return event.FloatValue(f)
			}
			return event.NullValue()
		}
	case ir.KindString:
		return func(a []event.Value) event.Value {
			if s, ok := str(a[0]); ok {
				return event.StringValue(s)
			}
			return event.NullValue()
		}
	case ir.KindBool:
		return func(a []event.Value) event.Value {
			switch a[0].Type() {
			case event.FieldTypeBool:
				return a[0]
			case event.FieldTypeInt, event.FieldTypeFloat:
				f, _ := numFloat(a[0])
				return event.BoolValue(f != 0)
			case event.FieldTypeString:
				if b, err := strconv.ParseBool(strings.TrimSpace(a[0].AsString())); err == nil {
					return event.BoolValue(b)
				}
			}
			return event.NullValue()
		}
	case ir.KindTimestamp:
		return func(a []event.Value) event.Value {
			if t, ok := timeOf(a[0]); ok {
				return event.TimestampValue(t)
			}
			if f, ok := numFloat(a[0]); ok && a[0].Type() != event.FieldTypeString && a[0].Type() != event.FieldTypeBool {
				sec, frac := math.Modf(f)
				return event.TimestampValue(time.Unix(int64(sec), int64(frac*1e9)).UTC())
			}
			return event.NullValue()
		}
	case ir.KindDuration:
		return func(a []event.Value) event.Value {
			if d, ok := timeOfDay(a[0]); ok && a[0].Type() != event.FieldTypeTimestamp {
				return event.DurationValue(d)
			}
			if f, ok := numFloat(a[0]); ok {
				return event.DurationValue(time.Duration(f * float64(time.Second)))
			}
			return event.NullValue()
		}
	case ir.KindArray:
		return func(a []event.Value) event.Value {
			if a[0].Type() == event.FieldTypeArray {
				return a[0]
			}
			return event.NullValue()
		}
	case ir.KindObject:
		return func(a []event.Value) event.Value {
			if a[0].Type() == event.FieldTypeObject {
				return a[0]
			}
			return event.NullValue()
		}
	}
	return func(a []event.Value) event.Value { return a[0] }
}

func nativeIsTrue(a []event.Value) event.Value {
	return event.BoolValue(a[0].Type() == event.FieldTypeBool && a[0].AsBool())
}

func nativeXor(a []event.Value) event.Value {
	if a[0].Type() != event.FieldTypeBool || a[1].Type() != event.FieldTypeBool {
		return event.NullValue()
	}
	return event.BoolValue(a[0].AsBool() != a[1].AsBool())
}

func isWordByte(r rune) bool {
	return r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r > 127
}

// phraseMatcher matches text containing the phrase at token boundaries,
// ignoring case.
func phraseMatcher(phrase string) NativeFunc {
	needle := strings.ToLower(phrase)
	first, _ := firstRune(needle)
	last, _ := lastRune(needle)
	return func(a []event.Value) event.Value {
		s, ok := str(a[0])
		if !ok {
			return event.NullValue()
		}
		s = strings.ToLower(s)
		for off := 0; ; {
			i := strings.Index(s[off:], needle)
			if i < 0 {
				return event.BoolValue(false)
			}
			i += off
			okStart := i == 0 || !isWordByte(first) || !isWordByte(prevRune(s, i))
			end := i + len(needle)
			okEnd := end == len(s) || !isWordByte(last) || !isWordByte(nextRune(s, end))
			if okStart && okEnd {
				return event.BoolValue(true)
			}
			off = i + 1
		}
	}
}

func firstRune(s string) (rune, bool) {
	for _, r := range s {
		return r, true
	}
	return 0, false
}

func lastRune(s string) (rune, bool) {
	rs := []rune(s)
	if len(rs) == 0 {
		return 0, false
	}
	return rs[len(rs)-1], true
}

func prevRune(s string, i int) rune {
	rs := []rune(s[:i])
	return rs[len(rs)-1]
}

func nextRune(s string, i int) rune {
	r, _ := firstRune(s[i:])
	return r
}

func regexMatcher(pattern string, caseSensitive bool) (NativeFunc, error) {
	if !caseSensitive {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return func(a []event.Value) event.Value {
		s, ok := str(a[0])
		if !ok {
			return event.NullValue()
		}
		return event.BoolValue(re.MatchString(s))
	}, nil
}

// toInt converts a float to an integer, saturating beyond int64, so that
// the result does not depend on the processor.
func toInt(f float64) int64 {
	switch {
	case f != f:
		return 0
	case f >= 1<<63:
		return math.MaxInt64
	case f < -(1 << 63):
		return math.MinInt64
	}

	return int64(f)
}

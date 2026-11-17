package irfunc

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// Arrays and objects are held decoded as the row engine holds them: []any
// and map[string]any whose leaves are nil, bool, int64, float64, string,
// time.Time and time.Duration.

// display writes an element as the row engine's String does.
func display(dst []byte, x any) []byte {
	switch t := x.(type) {
	case nil:
		return append(dst, "<null>"...)
	case string:
		return append(dst, t...)
	case int64:
		return strconv.AppendInt(dst, t, 10)
	case float64:
		return strconv.AppendFloat(dst, t, 'g', -1, 64)
	case bool:
		return strconv.AppendBool(dst, t)
	case time.Time:
		return t.UTC().AppendFormat(dst, time.RFC3339Nano)
	case time.Duration:
		return append(dst, t.String()...)
	}

	return jsonLike(dst, x)
}

func jsonLike(dst []byte, x any) []byte {
	switch t := x.(type) {
	case nil:
		return append(dst, "null"...)
	case string:
		return strconv.AppendQuote(dst, t)
	case time.Time, time.Duration:
		return strconv.AppendQuote(dst, string(display(nil, t)))
	case []any:
		dst = append(dst, '[')
		for i, e := range t {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = jsonLike(dst, e)
		}
		return append(dst, ']')
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		dst = append(dst, '{')
		for i, k := range keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = strconv.AppendQuote(dst, k)
			dst = append(dst, ':')
			dst = jsonLike(dst, t[k])
		}
		return append(dst, '}')
	}

	return display(dst, x)
}

func cmpInts[T int64 | float64 | time.Duration](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

// elemFloat is the row engine's ValueToFloat: strings parse untrimmed.
func elemFloat(x any) (float64, bool) {
	switch t := x.(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	}

	return 0, false
}

// compareElems is the row engine's three-way comparison of two values.
func compareElems(a, b any) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	switch x := a.(type) {
	case int64:
		if y, ok := b.(int64); ok {
			return cmpInts(x, y)
		}
	case float64:
		if y, ok := b.(float64); ok {
			return cmpInts(x, y)
		}
	case string:
		if y, ok := b.(string); ok {
			xf, err1 := strconv.ParseFloat(x, 64)
			yf, err2 := strconv.ParseFloat(y, 64)
			if err1 == nil && err2 == nil {
				return cmpInts(xf, yf)
			}
			return strings.Compare(x, y)
		}
	case bool:
		if y, ok := b.(bool); ok {
			return int(b2i(x) - b2i(y))
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Compare(y)
		}
	case time.Duration:
		if y, ok := b.(time.Duration); ok {
			return cmpInts(x, y)
		}
	case []any:
		if y, ok := b.([]any); ok {
			return compareArrays(x, y)
		}
	case map[string]any:
		if y, ok := b.(map[string]any); ok {
			if objectsEqual(x, y) {
				return 0
			}
			return strings.Compare(string(display(nil, x)), string(display(nil, y)))
		}
	}
	af, aok := elemFloat(a)
	bf, bok := elemFloat(b)
	if aok && bok {
		return cmpInts(af, bf)
	}

	return strings.Compare(string(display(nil, a)), string(display(nil, b)))
}

func compareArrays(a, b []any) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if c := compareElems(a[i], b[i]); c != 0 {
			return c
		}
	}

	return cmpInts(int64(len(a)), int64(len(b)))
}

// elemsEqual is the row engine's deep equality of two values.
func elemsEqual(a, b any) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	}
	switch x := a.(type) {
	case string:
		if y, ok := b.(string); ok {
			return x == y
		}
	case int64:
		if y, ok := b.(int64); ok {
			return x == y
		}
	case float64:
		if y, ok := b.(float64); ok {
			return x == y
		}
	case bool:
		if y, ok := b.(bool); ok {
			return x == y
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Equal(y)
		}
	case time.Duration:
		if y, ok := b.(time.Duration); ok {
			return x == y
		}
	case []any:
		if y, ok := b.([]any); ok {
			if len(x) != len(y) {
				return false
			}
			for i := range x {
				if !elemsEqual(x[i], y[i]) {
					return false
				}
			}
			return true
		}
	case map[string]any:
		if y, ok := b.(map[string]any); ok {
			return objectsEqual(x, y)
		}
	}
	af, aok := elemFloat(a)
	bf, bok := elemFloat(b)
	if aok && bok {
		return af == bf
	}

	return string(display(nil, a)) == string(display(nil, b))
}

func objectsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !elemsEqual(av, bv) {
			return false
		}
	}

	return true
}

// numericPair converts a string operand to a number when the other operand
// is a number, and to a time when the other is a time.
func numericPair(a, b val) (val, val) {
	switch {
	case a.k == kStr && b.isNum():
		if n, ok := parseNumber(a.s); ok {
			return n, b
		}
	case b.k == kStr && a.isNum():
		if n, ok := parseNumber(b.s); ok {
			return a, n
		}
	case a.k == kTime && b.k == kStr:
		if t, ok := timeOf(b); ok {
			return a, timeVal(t)
		}
	case b.k == kTime && a.k == kStr:
		if t, ok := timeOf(a); ok {
			return timeVal(t), b
		}
	}

	return a, b
}

// strictCompare compares values of one kind, or numbers of either kind;
// null reports values that do not compare.
func strictCompare(a, b val) (cmp int, null bool) {
	if a.k == kNull || b.k == kNull {
		return 0, true
	}
	if a.k == b.k {
		switch a.k {
		case kStr:
			return strings.Compare(a.s, b.s), false
		case kInt, kTime, kDur:
			return cmpInts(a.n, b.n), false
		case kFloat:
			return cmpInts(a.f, b.f), false
		case kBool:
			return int(a.n - b.n), false
		case kArray:
			x, _ := tree(a).([]any)
			y, _ := tree(b).([]any)
			return compareArrays(x, y), false
		case kObject:
			x, _ := tree(a).(map[string]any)
			y, _ := tree(b).(map[string]any)
			if objectsEqual(x, y) {
				return 0, false
			}
			return strings.Compare(string(display(nil, x)), string(display(nil, y))), false
		}
	}
	if a.isNum() && b.isNum() {
		af, _ := numFloat(a)
		bf, _ := numFloat(b)
		return cmpInts(af, bf), false
	}

	return 0, true
}

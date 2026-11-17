package exec

import (
	"bytes"
	"math"
	"strconv"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A Value of kind KindAny stands for a missing value and one of kind
// vec.KindNull for a null.

var (
	typeNames = [kindSlots + 1][]byte{
		KindAny:         []byte("missing"),
		vec.KindNull:    []byte("null"),
		vec.KindBool:    []byte("bool"),
		vec.KindInt:     []byte("int"),
		vec.KindFloat:   []byte("float"),
		vec.KindString:  []byte("string"),
		vec.KindVariant: []byte("array"),
	}
	trueText, falseText = []byte("true"), []byte("false")
)

func boolValue(b bool) Value { return Value{Kind: vec.KindBool, Bool: b} }

// compareScalar orders two values of one sort: numbers, strings or
// booleans; ok is false for any other pair and for a NaN.
func compareScalar(a, b Value) (c int, ok bool) {
	switch {
	case a.Kind == vec.KindInt && b.Kind == vec.KindInt:
		return order(a.Int, b.Int), true
	case a.Kind == vec.KindString && b.Kind == vec.KindString:
		return bytes.Compare(a.Bytes, b.Bytes), true
	case a.Kind == vec.KindBool && b.Kind == vec.KindBool:
		return btoi(a.Bool) - btoi(b.Bool), true
	}
	x, okA := number(a)
	y, okB := number(b)
	if !okA || !okB || x != x || y != y {
		return 0, false
	}

	return order(x, y), true
}

func holds(op Op, c int) bool {
	switch op {
	case OpEq:
		return c == 0
	case OpNe:
		return c != 0
	case OpLt:
		return c < 0
	case OpLe:
		return c <= 0
	case OpGt:
		return c > 0
	}

	return c >= 0
}

// floorTo returns the largest multiple of width that is not above x.
func floorTo(x, width int64) int64 {
	q := x / width
	if x%width != 0 && x < 0 {
		q--
	}

	return q * width
}

// parseInt reads a decimal integer without allocating.
func parseInt(s []byte) (int64, bool) {
	neg := false
	if len(s) > 0 && (s[0] == '-' || s[0] == '+') {
		neg, s = s[0] == '-', s[1:]
	}
	if len(s) == 0 || len(s) > 19 {
		return 0, false
	}
	var n uint64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}
	switch {
	case neg && n <= 1<<63:
		return -int64(n), true
	case !neg && n < 1<<63:
		return int64(n), true
	}

	return 0, false
}

// parseFloat reads a decimal float; text that holds anything but digits,
// signs, a point and an exponent is refused before the parser allocates an
// error for it.
func parseFloat(s []byte) (float64, bool) {
	if len(s) == 0 || len(s) > 64 {
		return 0, false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && c != '+' && c != '-' && c != '.' && c != 'e' && c != 'E' {
			return 0, false
		}
	}
	f, err := strconv.ParseFloat(unsafe.String(unsafe.SliceData(s), len(s)), 64)

	return f, err == nil
}

func appendCase(dst, s []byte, upper bool) []byte {
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case upper && 'a' <= c && c <= 'z':
				c -= 'a' - 'A'
			case !upper && 'A' <= c && c <= 'Z':
				c += 'a' - 'A'
			}
			dst = append(dst, c)
			i++

			continue
		}
		r, size := utf8.DecodeRune(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, c)
			i++

			continue
		}
		if upper {
			r = unicode.ToUpper(r)
		} else {
			r = unicode.ToLower(r)
		}
		dst = utf8.AppendRune(dst, r)
		i += size
	}

	return dst
}

// substr returns count characters of s from character start, which counts
// from the end when negative; a negative count takes the rest.
func substr(s []byte, start, count int64) []byte {
	n := int64(utf8.RuneCount(s))
	if start < 0 {
		start = max(start+n, 0)
	}
	if start >= n || count == 0 {
		return s[:0]
	}
	end := n
	if count > 0 && count < n-start {
		end = start + count
	}
	if n == int64(len(s)) {
		return s[start:end]
	}
	from, at := 0, int64(0)
	for i := 0; i < len(s); at++ {
		if at == start {
			from = i
		}
		if at == end {
			return s[from:i]
		}
		_, size := utf8.DecodeRune(s[i:])
		i += size
	}

	return s[from:]
}

func appendReplaced(dst, s, from, to []byte) []byte {
	if len(from) == 0 {
		return append(dst, s...)
	}
	for {
		i := bytes.Index(s, from)
		if i < 0 {
			return append(dst, s...)
		}
		dst = append(append(dst, s[:i]...), to...)
		s = s[i+len(from):]
	}
}

func truth(v Value) (is, known bool) { return v.Bool, v.Kind == vec.KindBool }

// scalarValue applies a scalar function to the values of one row. The bytes
// of a string it computes are appended to buf, which it returns; the result
// may also alias an argument.
func scalarValue(op Op, pat *tokenfilter.Pattern, args []Value, buf []byte) (Value, []byte) {
	a := args[0]
	var b Value
	if len(args) > 1 {
		b = args[1]
	}
	str := func(start int) Value { return Value{Kind: vec.KindString, Bytes: buf[start:]} }
	switch op {
	case OpAdd:
		if a.Kind == vec.KindString && b.Kind == vec.KindString {
			start := len(buf)
			buf = append(append(buf, a.Bytes...), b.Bytes...)

			return str(start), buf
		}

		return arith(op, a, b), buf
	case OpSub, OpMul, OpDiv:
		return arith(op, a, b), buf
	case OpMod:
		if a.Kind != vec.KindInt || b.Kind != vec.KindInt || b.Int == 0 {
			return Value{}, buf
		}
		if b.Int == -1 {
			return Value{Kind: vec.KindInt}, buf
		}

		return Value{Kind: vec.KindInt, Int: a.Int % b.Int}, buf
	case OpHourOfDay:
		if a.Kind != vec.KindInt {
			return Value{}, buf
		}

		return Value{Kind: vec.KindInt, Int: hourOfDay(a.Int)}, buf
	case OpBin:
		if a.Kind == vec.KindInt && b.Kind == vec.KindInt {
			if b.Int <= 0 {
				return Value{}, buf
			}

			return Value{Kind: vec.KindInt, Int: floorTo(a.Int, b.Int)}, buf
		}
		x, okA := number(a)
		w, okB := number(b)
		if !okA || !okB || !(w > 0) {
			return Value{}, buf
		}

		return Value{Kind: vec.KindFloat, Float: math.Floor(x/w) * w}, buf
	case OpToInt:
		switch a.Kind {
		case vec.KindInt:
			return a, buf
		case vec.KindBool:
			return Value{Kind: vec.KindInt, Int: int64(btoi(a.Bool))}, buf
		case vec.KindFloat:
			if a.Float >= -9223372036854775808.0 && a.Float < 9223372036854775808.0 {
				return Value{Kind: vec.KindInt, Int: int64(a.Float)}, buf
			}
		case vec.KindString:
			if n, ok := parseInt(a.Bytes); ok {
				return Value{Kind: vec.KindInt, Int: n}, buf
			}
			if f, ok := parseFloat(a.Bytes); ok && f >= -9223372036854775808.0 && f < 9223372036854775808.0 {
				return Value{Kind: vec.KindInt, Int: int64(f)}, buf
			}
		}

		return Value{}, buf
	case OpToFloat:
		switch a.Kind {
		case vec.KindFloat:
			return a, buf
		case vec.KindInt:
			return Value{Kind: vec.KindFloat, Float: float64(a.Int)}, buf
		case vec.KindBool:
			return Value{Kind: vec.KindFloat, Float: float64(btoi(a.Bool))}, buf
		case vec.KindString:
			if f, ok := parseFloat(a.Bytes); ok {
				return Value{Kind: vec.KindFloat, Float: f}, buf
			}
		}

		return Value{}, buf
	case OpToString:
		start := len(buf)
		switch a.Kind {
		case vec.KindString:
			return a, buf
		case vec.KindInt:
			buf = strconv.AppendInt(buf, a.Int, 10)
		case vec.KindFloat:
			buf = strconv.AppendFloat(buf, a.Float, 'g', -1, 64)
		case vec.KindBool:
			buf = strconv.AppendBool(buf, a.Bool)
		default:
			return Value{}, buf
		}

		return str(start), buf
	case OpToBool:
		switch a.Kind {
		case vec.KindBool:
			return a, buf
		case vec.KindInt:
			return boolValue(a.Int != 0), buf
		case vec.KindFloat:
			return boolValue(a.Float != 0), buf
		case vec.KindString:
			if bytes.EqualFold(a.Bytes, trueText) || bytes.EqualFold(a.Bytes, falseText) {
				return boolValue(len(a.Bytes) == len(trueText)), buf
			}
		}

		return Value{}, buf
	case OpIf:
		switch is, known := truth(a); {
		case !known:
			return Value{}, buf
		case is:
			return b, buf
		}

		return args[2], buf
	case OpCase:
		for i := 0; i+1 < len(args); i += 2 {
			if is, known := truth(args[i]); known && is {
				return args[i+1], buf
			}
		}
		if len(args)%2 == 1 {
			return args[len(args)-1], buf
		}

		return Value{}, buf
	case OpCoalesce:
		for _, v := range args {
			if !v.IsNull() {
				return v, buf
			}
		}

		return Value{}, buf
	case OpNullIf:
		if c, ok := compareScalar(a, b); ok && c == 0 {
			return Value{}, buf
		}

		return a, buf
	case OpExists:
		return boolValue(a.Kind != KindAny), buf
	case OpIsNull:
		return boolValue(a.Kind == vec.KindNull), buf
	case OpIsMissing:
		return boolValue(a.Kind == KindAny), buf
	case OpTypeOf:
		return Value{Kind: vec.KindString, Bytes: typeNames[a.Kind]}, buf
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		c, ok := compareScalar(a, b)
		if !ok {
			return Value{}, buf
		}

		return boolValue(holds(op, c)), buf
	case OpAnd, OpOr:
		// The value that settles the outcome: false for And, true for Or.
		settles, unknown := op == OpOr, false
		for _, v := range args {
			switch is, known := truth(v); {
			case !known:
				unknown = true
			case is == settles:
				return boolValue(settles), buf
			}
		}
		if unknown {
			return Value{}, buf
		}

		return boolValue(!settles), buf
	case OpNot:
		if is, known := truth(a); known {
			return boolValue(!is), buf
		}

		return Value{}, buf
	}
	// The rest are functions of strings.
	for _, v := range args {
		if v.Kind != vec.KindString && (op != OpSubstr || v.Kind != vec.KindInt) {
			return Value{}, buf
		}
	}
	if a.Kind != vec.KindString {
		return Value{}, buf
	}
	start := len(buf)
	switch op {
	case OpMatch:
		return boolValue(pat.Match(a.Bytes)), buf
	case OpLength:
		return Value{Kind: vec.KindInt, Int: int64(utf8.RuneCount(a.Bytes))}, buf
	case OpLower, OpUpper:
		buf = appendCase(buf, a.Bytes, op == OpUpper)
	case OpSubstr:
		count := int64(-1)
		if len(args) > 2 {
			count = max(args[2].Int, 0)
		}
		if b.Kind != vec.KindInt || (len(args) > 2 && args[2].Kind != vec.KindInt) {
			return Value{}, buf
		}

		return Value{Kind: vec.KindString, Bytes: substr(a.Bytes, b.Int, count)}, buf
	case OpConcat:
		for _, v := range args {
			buf = append(buf, v.Bytes...)
		}
	case OpStartsWith:
		return boolValue(bytes.HasPrefix(a.Bytes, b.Bytes)), buf
	case OpEndsWith:
		return boolValue(bytes.HasSuffix(a.Bytes, b.Bytes)), buf
	case OpReplace:
		buf = appendReplaced(buf, a.Bytes, b.Bytes, args[2].Bytes)
	default:
		return Value{}, buf
	}

	return str(start), buf
}

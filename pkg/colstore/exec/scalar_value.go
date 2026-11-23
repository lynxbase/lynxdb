package exec

import (
	"bytes"
	"math"
	"strconv"
	"unicode/utf8"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// A Value of kind KindAny stands for a missing value and one of kind
// vec.KindNull for a null.

func boolValue(b bool) Value { return Value{Kind: vec.KindBool, Bool: b} }

// compareScalar orders two values of one sort: numbers, strings or
// booleans, where a string compared with a number is read as the number it
// holds; ok is false for any other pair, for a string that holds no number
// and for a NaN.
func compareScalar(a, b Value) (c int, ok bool) {
	switch {
	case a.Kind == vec.KindInt && b.Kind == vec.KindInt:
		return order(a.Int, b.Int), true
	case a.Kind == vec.KindString && b.Kind == vec.KindString:
		return bytes.Compare(a.Bytes, b.Bytes), true
	case a.Kind == vec.KindBool && b.Kind == vec.KindBool:
		return btoi(a.Bool) - btoi(b.Bool), true
	}
	if a, ok = numeric(a); !ok {
		return 0, false
	}
	if b, ok = numeric(b); !ok {
		return 0, false
	}
	if a.Kind == vec.KindInt && b.Kind == vec.KindInt {
		return order(a.Int, b.Int), true
	}
	x, _ := number(a)
	y, _ := number(b)
	if x != x || y != y {
		return 0, false
	}

	return order(x, y), true
}

// numberText reads text that holds a number, with white space around it: an
// integer when it is one, else a float.
func numberText(s []byte) (Value, bool) {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	if n, ok := parseInt(s); ok {
		return Value{Kind: vec.KindInt, Int: n}, true
	}
	if f, ok := parseFloat(s); ok {
		return Value{Kind: vec.KindFloat, Float: f}, true
	}

	return Value{}, false
}

// numeric returns a value as a number: an integer or a float as it is, and a
// string as the number it holds.
func numeric(v Value) (Value, bool) {
	switch v.Kind {
	case vec.KindInt, vec.KindFloat:
		return v, true
	case vec.KindString:
		return numberText(v.Bytes)
	}

	return Value{}, false
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
		return arith(op, a, b), buf
	case OpHourOfDay:
		if a.Kind != vec.KindInt {
			return Value{}, buf
		}

		return Value{Kind: vec.KindInt, Int: hourOfDay(a.Int)}, buf
	case OpBin:
		if x, ok := numeric(a); ok {
			a = x
		}
		if w, ok := numeric(b); ok {
			b = w
		}
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
	case OpIsNull:
		return boolValue(a.Kind == vec.KindNull), buf
	case OpIsMissing:
		return boolValue(a.Kind == KindAny), buf
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
	if a.Kind != vec.KindString {
		return Value{}, buf
	}
	switch op {
	case OpMatch:
		return boolValue(pat.Match(a.Bytes)), buf
	case OpIndexOf:
		if b.Kind != vec.KindString {
			return Value{}, buf
		}
		at := bytes.Index(a.Bytes, b.Bytes)
		if at < 0 {
			return Value{}, buf
		}

		return Value{Kind: vec.KindInt, Int: int64(utf8.RuneCount(a.Bytes[:at]))}, buf
	}

	return Value{}, buf
}

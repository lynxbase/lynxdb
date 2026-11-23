package irfunc

import (
	"math"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// leaf converts a value to an element of an array or object.
func leaf(v val) any {
	switch v.k {
	case kInt:
		return v.n
	case kFloat:
		return v.f
	case kStr:
		return strings.Clone(v.s)
	case kBool:
		return v.n != 0
	case kTime:
		return v.time()
	case kDur:
		return v.dur()
	case kArray, kObject:
		return tree(v)
	}

	return nil
}

// interfaceForm gives times and durations their text, as JSON holds them.
func interfaceForm(x any) any {
	switch t := x.(type) {
	case time.Time:
		return t.UTC().Format(timestampLayout)
	case time.Duration:
		return string(appendTimeOfDay(nil, t))
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = interfaceForm(e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = interfaceForm(e)
		}
		return out
	}

	return x
}

// Index returns the function of an element access: the element of an array
// at a 0-based index, which counts from the end when negative, or the member
// of an object of a key; NULL for an index out of range, a key that the
// object lacks and anything else.
func Index(e *ir.Index) func() exec.Func {
	call := &ir.Call{Args: []ir.Expr{e.X, e.Key}}

	return adapt(call, func() impl {
		return func(c *ctx, a []val) val {
			switch x, k := a[0], a[1]; x.k {
			case kArray:
				var i int64
				switch {
				case k.k == kInt:
					i = k.n
				case k.k == kFloat && k.f == math.Trunc(k.f) && math.Abs(k.f) < 1<<53:
					i = int64(k.f)
				default:
					return null
				}
				arr := elements(x)
				if i < 0 {
					i += int64(len(arr))
				}
				if i < 0 || i >= int64(len(arr)) {
					return null
				}

				return c.elem(arr[i])
			case kObject:
				key, ok := c.str(k)
				obj, isObj := tree(x).(map[string]any)
				if !ok || !isObj {
					return null
				}
				if m, found := obj[key]; found {
					return c.elem(m)
				}
			}

			return null
		}
	})
}

// Elements returns the elements of an array, with their bytes of their own,
// and false for a value that is not an array.
func Elements(v exec.Value) ([]exec.Value, bool) {
	x := in(v, argPlain)
	if x.k != kArray {
		return nil, false
	}
	arr := elements(x)
	out := make([]exec.Value, len(arr))
	c := &ctx{}
	for i, e := range arr {
		c.out = c.out[:0]
		out[i], _ = outValue(c.elem(e), nil)
	}

	return out, true
}

// Array returns the array of values, with its bytes in buf.
func Array(elems []exec.Value, buf []byte) (exec.Value, []byte) {
	start := len(buf)
	buf, mark := variant.BeginArray(buf)
	for _, e := range elems {
		switch e.Kind {
		case vec.KindBool:
			buf = variant.AppendBool(buf, e.Bool)
		case vec.KindInt:
			buf = variant.AppendInt(buf, e.Int)
		case vec.KindFloat:
			buf = variant.AppendFloat(buf, e.Float)
		case vec.KindString:
			buf = variant.AppendString(buf, e.Bytes)
		case vec.KindVariant:
			buf = append(buf, e.Bytes...)
		default:
			buf = variant.AppendNull(buf)
		}
	}
	buf = variant.End(buf, mark)

	return exec.Value{Kind: vec.KindVariant, Bytes: buf[start:]}, buf
}

// outValue is out with bytes of their own.
func outValue(v val, buf []byte) (exec.Value, []byte) {
	x, buf := out(v, buf)
	if x.Bytes != nil {
		x.Bytes = append([]byte(nil), x.Bytes...)
	}

	return x, buf
}

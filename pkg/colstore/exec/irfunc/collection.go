package irfunc

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// elem converts an element of an array or object to a value, with arrays
// and objects encoded in c.out.
func (c *ctx) elem(x any) val {
	switch t := x.(type) {
	case bool:
		return boolVal(t)
	case int64:
		return intVal(t)
	case float64:
		return floatVal(t)
	case string:
		return strVal(t)
	case time.Time:
		return timeVal(t)
	case time.Duration:
		return durVal(t)
	case []any:
		return c.encoded(kArray, x)
	case map[string]any:
		return c.encoded(kObject, x)
	}

	return null
}

func (c *ctx) encoded(k kind, x any) val {
	start := len(c.out)
	c.out = appendElem(c.out, x)

	return val{k: k, s: bytesString(c.out[start:])}
}

// appendElem encodes an element in the variant encoding as the row engine
// writes its values: a time or a duration as its nanoseconds.
func appendElem(dst []byte, x any) []byte {
	switch t := x.(type) {
	case bool:
		return variant.AppendBool(dst, t)
	case int64:
		return variant.AppendInt(dst, t)
	case float64:
		return variant.AppendFloat(dst, t)
	case string:
		return variant.AppendString(dst, stringBytes(t))
	case time.Time:
		return variant.AppendInt(dst, t.UnixNano())
	case time.Duration:
		return variant.AppendInt(dst, int64(t))
	case []any:
		dst, mark := variant.BeginArray(dst)
		for _, e := range t {
			dst = appendElem(dst, e)
		}
		return variant.End(dst, mark)
	case map[string]any:
		dst, mark := variant.BeginObject(dst)
		for k, e := range t {
			dst = appendElem(variant.AppendKey(dst, stringBytes(k)), e)
		}
		return variant.End(dst, mark)
	}

	return variant.AppendNull(dst)
}

// appendElemText writes the canonical text of an element, false for a
// null.
func appendElemText(dst []byte, x any) ([]byte, bool) {
	switch t := x.(type) {
	case nil:
		return dst, false
	case string:
		return append(dst, t...), true
	case int64:
		return strconv.AppendInt(dst, t, 10), true
	case float64:
		return appendFloat(dst, t), true
	case bool:
		return strconv.AppendBool(dst, t), true
	case time.Time:
		return t.UTC().AppendFormat(dst, timestampLayout), true
	case time.Duration:
		return appendTimeOfDay(dst, t), true
	}
	b, err := json.Marshal(interfaceForm(x))
	if err != nil {
		return dst, false
	}

	return append(dst, b...), true
}

// elements returns the elements of an array, or the value itself as the
// one element of anything else.
func elements(v val) []any {
	if v.k == kArray {
		arr, _ := tree(v).([]any)
		return arr
	}

	return []any{leaf(v)}
}

func init() {
	fixed("array", func(c *ctx, a []val) val {
		out := make([]any, len(a))
		for i, v := range a {
			out[i] = leaf(v)
		}
		return c.encoded(kArray, out)
	})
	fixed("array_length", func(_ *ctx, a []val) val {
		if a[0].k != kArray {
			return null
		}
		return intVal(int64(len(elements(a[0]))))
	})
	fixed("mvjoin", strict(func(c *ctx, a []val) val {
		sep, ok := c.str(a[1])
		if !ok {
			return null
		}
		start := len(c.out)
		n := 0
		for _, e := range elements(a[0]) {
			mark := len(c.out)
			if n > 0 {
				c.out = append(c.out, sep...)
			}
			var ok bool
			if c.out, ok = appendElemText(c.out, e); !ok {
				c.out = c.out[:mark]
				continue
			}
			n++
		}
		return c.result(start)
	}))
	fixed("mvappend", func(c *ctx, a []val) val {
		out := []any{}
		for _, v := range a {
			switch v.k {
			case kNull:
			case kArray:
				for _, e := range elements(v) {
					if e != nil {
						out = append(out, e)
					}
				}
			default:
				out = append(out, leaf(v))
			}
		}
		return c.encoded(kArray, out)
	})
	fixed("split", strict(func(c *ctx, a []val) val {
		s, ok := c.str(a[0])
		sep, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		parts := strings.Split(s, sep)
		out := make([]any, len(parts))
		for i, p := range parts {
			out[i] = p
		}
		return c.encoded(kArray, out)
	}))
	fixed("mvdedup", strict(func(c *ctx, a []val) val {
		if a[0].k != kArray {
			return a[0]
		}
		arr := elements(a[0])
		out := make([]any, 0, len(arr))
		for _, e := range arr {
			found := false
			for _, seen := range out {
				if elemsEqual(e, seen) {
					found = true
					break
				}
			}
			if !found {
				out = append(out, e)
			}
		}
		return c.encoded(kArray, out)
	}))
	def("mvfind", func(call *ir.Call) (func() impl, error) {
		pattern, ok := constString(call, 1)
		if !ok {
			return nil, errorf("mvfind needs a constant regex")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		f := strict(func(c *ctx, a []val) val {
			for i, e := range elements(a[0]) {
				mark := len(c.text)
				var ok bool
				if c.text, ok = appendElemText(c.text, e); ok && re.Match(c.text[mark:]) {
					return intVal(int64(i))
				}
			}
			return null
		})
		return func() impl { return first(f) }, nil
	})
	fixed("mvindex", strict(func(c *ctx, a []val) val {
		var elems []any
		if a[0].k == kArray {
			elems = elements(a[0])
		}
		n := int64(len(elems))
		if a[0].k != kArray {
			n = 1
		}
		norm := func(v val) (int64, bool) {
			i, ok := numInt(v)
			if !ok {
				return 0, false
			}
			if i < 0 {
				i += n
			}
			return i, true
		}
		start, ok := norm(a[1])
		if !ok {
			return null
		}
		if len(a) == 2 {
			if start < 0 || start >= n {
				return null
			}
			if a[0].k != kArray {
				return a[0]
			}
			return c.elem(elems[start])
		}
		end, ok := norm(a[2])
		if !ok {
			return null
		}
		if start < 0 {
			start = 0
		}
		if end >= n {
			end = n - 1
		}
		if start > end {
			return null
		}
		if a[0].k != kArray {
			elems = []any{leaf(a[0])}
		}
		return c.encoded(kArray, elems[start:end+1])
	}))
	fixed("mvzip", strict(func(c *ctx, a []val) val {
		sep := ","
		if len(a) > 2 {
			s, ok := c.str(a[2])
			if !ok {
				return null
			}
			sep = s
		}
		l, r := elements(a[0]), elements(a[1])
		n := min(len(l), len(r))
		out := make([]any, n)
		var text []byte
		for i := 0; i < n; i++ {
			text, _ = appendElemText(text[:0], l[i])
			text = append(text, sep...)
			text, _ = appendElemText(text, r[i])
			out[i] = string(text)
		}
		return c.encoded(kArray, out)
	}))
}

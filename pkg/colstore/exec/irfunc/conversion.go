package irfunc

import (
	"bytes"
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

func init() {
	def("tostring", func(call *ir.Call) (func() impl, error) {
		format, _ := constString(call, 1)
		format = strings.ToLower(format)
		return func() impl { return first(toString(format)) }, nil
	})
	def("tonumber", func(call *ir.Call) (func() impl, error) {
		base := int64(10)
		if b, ok := constInt(call, 1); ok {
			base = b
		}
		return func() impl { return first(toNumber(base)) }, nil
	})
	fixed("parse_json", strict(func(c *ctx, a []val) val {
		x, ok := decodeJSON(a[0])
		if !ok {
			return null
		}
		return c.fromInterface(x)
	}))
	text := func(name string, f func(c *ctx, s string) val) {
		fixed(name, strict(func(c *ctx, a []val) val {
			if a[0].isNum() {
				return a[0]
			}
			s, ok := c.str(a[0])
			if !ok {
				return null
			}
			return f(c, s)
		}))
	}
	text("convert_auto", func(_ *ctx, s string) val {
		v, _ := parseNumber(s)
		return v
	})
	text("rmcomma", func(_ *ctx, s string) val {
		v, _ := parseNumber(strings.ReplaceAll(s, ",", ""))
		return v
	})
	text("rmunit", func(_ *ctx, s string) val {
		m := leadingNumber.FindStringSubmatch(s)
		if m == nil {
			return null
		}
		v, _ := parseNumber(m[1])
		return v
	})
	text("convert_num", func(_ *ctx, s string) val {
		m := leadingNumber.FindStringSubmatch(strings.ReplaceAll(s, ",", ""))
		if m == nil {
			return null
		}
		v, _ := parseNumber(m[1])
		return v
	})
	text("memk", func(_ *ctx, s string) val {
		s = strings.TrimSpace(s)
		mult := 1.0
		if s != "" {
			switch s[len(s)-1] {
			case 'k', 'K':
				s = s[:len(s)-1]
			case 'm', 'M':
				s, mult = s[:len(s)-1], 1024
			case 'g', 'G':
				s, mult = s[:len(s)-1], 1024*1024
			}
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return null
		}
		return numberVal(f * mult)
	})
	text("dur2sec", func(_ *ctx, s string) val {
		s = strings.TrimSpace(s)
		days := 0
		if d, rest, ok := strings.Cut(s, "+"); ok {
			n, err := strconv.Atoi(d)
			if err != nil {
				return null
			}
			days, s = n, rest
		}
		var total int64
		parts := 0
		for p := range strings.SplitSeq(s, ":") {
			n, err := strconv.Atoi(p)
			if err != nil {
				return null
			}
			total = total*60 + int64(n)
			parts++
		}
		if parts != 3 {
			return null
		}
		return intVal(int64(days)*86400 + total)
	})
	text("mstime", func(_ *ctx, s string) val {
		s = strings.TrimSpace(s)
		minutes := 0.0
		if m, rest, ok := strings.Cut(s, ":"); ok {
			n, err := strconv.ParseFloat(m, 64)
			if err != nil {
				return null
			}
			minutes, s = n, rest
		}
		sec, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return null
		}
		return floatVal(minutes*60 + sec)
	})
	fixed("ctime", strict(func(c *ctx, a []val) val {
		f, ok := numFloat(a[0])
		format, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		sec, frac := math.Modf(f)
		start := len(c.out)
		c.out = posixFormat(c.out, time.Unix(int64(sec), int64(frac*1e9)).UTC(), format)
		return c.result(start)
	}))
	fixed("mktime", strict(func(c *ctx, a []val) val {
		s, ok := c.str(a[0])
		format, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		t, ok := mysqlParse(s, posixToMySQL.Replace(format))
		if !ok {
			return null
		}
		return floatVal(float64(t.Unix()))
	}))
}

// Cast makes the function of a cast, which converts with the IR's rules.
func Cast(e *ir.Cast) (func() exec.Func, error) {
	f, ok := castTo(e.To)
	if !ok {
		return nil, errorf("a cast to %s", e.To)
	}

	return adapt(&ir.Call{Args: []ir.Expr{e.X}, T: e.To}, func() impl { return f }), nil
}

func castTo(to ir.Type) (impl, bool) {
	switch to.Kind {
	case ir.KindInt:
		return func(_ *ctx, a []val) val {
			if n, ok := numInt(a[0]); ok {
				return intVal(n)
			}
			return null
		}, true
	case ir.KindFloat:
		return func(_ *ctx, a []val) val {
			if f, ok := numFloat(a[0]); ok {
				return floatVal(f)
			}
			return null
		}, true
	case ir.KindString:
		return func(c *ctx, a []val) val {
			if s, ok := c.str(a[0]); ok {
				return strVal(s)
			}
			return null
		}, true
	case ir.KindBool:
		return func(_ *ctx, a []val) val {
			switch a[0].k {
			case kBool:
				return a[0]
			case kInt, kFloat:
				f, _ := numFloat(a[0])
				return boolVal(f != 0)
			case kStr:
				if b, err := strconv.ParseBool(strings.TrimSpace(a[0].s)); err == nil {
					return boolVal(b)
				}
			}
			return null
		}, true
	case ir.KindTimestamp:
		return func(_ *ctx, a []val) val {
			if t, ok := timeOf(a[0]); ok {
				return timeVal(t)
			}
			if f, ok := numFloat(a[0]); ok && a[0].k != kStr && a[0].k != kBool {
				return timeVal(secondsTime(f))
			}
			return null
		}, true
	case ir.KindDuration:
		return func(_ *ctx, a []val) val {
			if d, ok := timeOfDay(a[0]); ok && a[0].k != kTime {
				return durVal(d)
			}
			if f, ok := numFloat(a[0]); ok {
				return durVal(time.Duration(f * float64(time.Second)))
			}
			return null
		}, true
	case ir.KindArray:
		return func(_ *ctx, a []val) val {
			if a[0].k == kArray {
				return a[0]
			}
			return null
		}, true
	case ir.KindObject:
		return func(_ *ctx, a []val) val {
			if a[0].k == kObject {
				return a[0]
			}
			return null
		}, true
	}

	return nil, false
}

// first makes f see only its first argument, as the functions that take
// their other arguments as constants do.
func first(f impl) impl {
	return func(c *ctx, a []val) val { return f(c, a[:1]) }
}

func toString(format string) impl {
	return strict(func(c *ctx, a []val) val {
		start := len(c.out)
		switch format {
		case "hex":
			if n, ok := numInt(a[0]); ok {
				c.out = append(c.out, "0x"...)
				c.out = strconv.AppendInt(c.out, n, 16)
				return c.result(start)
			}
		case "binary":
			if n, ok := numInt(a[0]); ok {
				c.out = strconv.AppendInt(c.out, n, 2)
				return c.result(start)
			}
		case "commas":
			if f, ok := numFloat(a[0]); ok {
				c.out = appendCommas(c.out, f, a[0].k == kInt)
				return c.result(start)
			}
		case "duration":
			if n, ok := numInt(a[0]); ok {
				d := n
				if d < 0 {
					c.out = append(c.out, '-')
					d = -d
				}
				c.out = appendPad2(c.out, d/3600)
				c.out = append(c.out, ':')
				c.out = appendPad2(c.out, d%3600/60)
				c.out = append(c.out, ':')
				c.out = appendPad2(c.out, d%60)
				return c.result(start)
			}
		}
		s, ok := c.str(a[0])
		if !ok {
			return null
		}
		return strVal(s)
	})
}

func toNumber(base int64) impl {
	return strict(func(c *ctx, a []val) val {
		if a[0].isNum() {
			return a[0]
		}
		s, ok := c.str(a[0])
		if !ok {
			return null
		}
		if base == 10 {
			v, _ := parseNumber(s)
			return v
		}
		if base < 2 || base > 36 {
			return null
		}
		n, err := strconv.ParseInt(strings.TrimSpace(s), int(base), 64)
		if err != nil {
			return null
		}
		return intVal(n)
	})
}

// appendCommas writes a number with thousands separators, two decimals for
// a float.
func appendCommas(dst []byte, f float64, isInt bool) []byte {
	if f < 0 {
		dst = append(dst, '-')
		f = -f
	}
	var digits [64]byte
	var intPart, frac []byte
	if isInt {
		intPart = strconv.AppendInt(digits[:0], int64(f), 10)
	} else {
		s := strconv.AppendFloat(digits[:0], f, 'f', 2, 64)
		intPart = s
		if dot := bytes.IndexByte(s, '.'); dot >= 0 {
			intPart, frac = s[:dot], s[dot+1:]
		}
	}
	for i, d := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, d)
	}
	if len(frac) > 0 {
		dst = append(dst, '.')
		dst = append(dst, frac...)
	}

	return dst
}

var leadingNumber = regexp.MustCompile(`^\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)`)

// numberVal gives an integer for a whole float within 2^53.
func numberVal(f float64) val {
	if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return intVal(int64(f))
	}

	return floatVal(f)
}

var posixToMySQL = strings.NewReplacer("%M", "%i", "%S", "%s", "%I", "%h", "%B", "%M")

// decodeJSON parses JSON text, or takes an array or object, into Go values;
// numbers of text are json.Number.
func decodeJSON(v val) (any, bool) {
	switch v.k {
	case kArray, kObject:
		x := tree(v)
		return x, x != nil
	case kStr:
		dec := json.NewDecoder(strings.NewReader(v.s))
		dec.UseNumber()
		var out any
		if err := dec.Decode(&out); err != nil {
			return nil, false
		}
		if dec.More() {
			return nil, false
		}
		return out, true
	}

	return nil, false
}

// fromInterface converts decoded JSON to a value: a scalar as itself, an
// array or object in the variant encoding in c.out. Numbers keep the int
// form when they have no fraction; an int64, which only an array or object
// argument holds, is a null, as in the row engine.
func (c *ctx) fromInterface(x any) val {
	switch t := x.(type) {
	case nil:
		return null
	case bool:
		return boolVal(t)
	case string:
		return strVal(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return intVal(n)
		}
		f, _ := t.Float64()
		return floatVal(f)
	case float64:
		return numberVal(t)
	case []any, map[string]any:
		start := len(c.out)
		c.out = appendVariant(c.out, x)
		k := kArray
		if _, ok := x.(map[string]any); ok {
			k = kObject
		}
		return val{k: k, s: bytesString(c.out[start:])}
	}

	return null
}

// appendVariant encodes decoded JSON in the variant encoding with the
// conversions of fromInterface.
func appendVariant(dst []byte, x any) []byte {
	switch t := x.(type) {
	case nil:
		return variant.AppendNull(dst)
	case bool:
		return variant.AppendBool(dst, t)
	case string:
		return variant.AppendString(dst, stringBytes(t))
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return variant.AppendInt(dst, n)
		}
		f, _ := t.Float64()
		return variant.AppendFloat(dst, f)
	case float64:
		if v := numberVal(t); v.k == kInt {
			return variant.AppendInt(dst, v.n)
		}
		return variant.AppendFloat(dst, t)
	case []any:
		dst, mark := variant.BeginArray(dst)
		for _, e := range t {
			dst = appendVariant(dst, e)
		}
		return variant.End(dst, mark)
	case map[string]any:
		dst, mark := variant.BeginObject(dst)
		for k, e := range t {
			dst = appendVariant(variant.AppendKey(dst, stringBytes(k)), e)
		}
		return variant.End(dst, mark)
	}

	return variant.AppendNull(dst)
}

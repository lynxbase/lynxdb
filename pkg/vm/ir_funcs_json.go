package vm

import (
	"bytes"
	"encoding/json"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// decodeJSON parses JSON text (or passes through an array or object value)
// into Go values with json.Number numbers.
func decodeJSON(v event.Value) (any, bool) {
	switch v.Type() {
	case event.FieldTypeArray, event.FieldTypeObject:
		return toInterface(v), true
	case event.FieldTypeString:
		dec := json.NewDecoder(strings.NewReader(v.AsString()))
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

func encodeJSON(x any) event.Value {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(x); err != nil {
		return event.NullValue()
	}
	return event.StringValue(strings.TrimRight(buf.String(), "\n"))
}

// jsonArg converts an argument to a value to embed in JSON text.
func jsonArg(v event.Value) any {
	switch v.Type() {
	case event.FieldTypeNull:
		return nil
	case event.FieldTypeInt:
		return v.AsInt()
	case event.FieldTypeFloat:
		return v.AsFloat()
	case event.FieldTypeBool:
		return v.AsBool()
	}
	return toInterface(v)
}

type pathStep struct {
	key   string
	index int
	all   bool
	isKey bool
}

var pathToken = regexp.MustCompile(`^(?:\.([A-Za-z0-9_@$-]+)|\.\*|\[\*\]|\[(-?\d+)\]|\['([^']*)'\]|\["([^"]*)"\])`)

// parsePath parses a JSONPath subset: $, .name, ['name'], [n], [*], .*;
// a path without a leading $ is read relative to the root.
func parsePath(p string) ([]pathStep, bool) {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "$") {
		p = p[1:]
	} else if p != "" && p[0] != '.' && p[0] != '[' {
		p = "." + p
	}
	var steps []pathStep
	for p != "" {
		m := pathToken.FindStringSubmatch(p)
		if m == nil {
			return nil, false
		}
		switch {
		case m[1] != "":
			steps = append(steps, pathStep{key: m[1], isKey: true})
		case m[2] != "":
			n, _ := strconv.Atoi(m[2])
			steps = append(steps, pathStep{index: n})
		case strings.HasPrefix(m[0], "['") || strings.HasPrefix(m[0], "[\""):
			steps = append(steps, pathStep{key: m[3] + m[4], isKey: true})
		default:
			steps = append(steps, pathStep{all: true})
		}
		p = p[len(m[0]):]
	}
	return steps, true
}

// evalPath returns the values a path selects and whether it used a
// wildcard.
func evalPath(root any, steps []pathStep) ([]any, bool) {
	cur := []any{root}
	wild := false
	for _, s := range steps {
		var next []any
		for _, x := range cur {
			switch {
			case s.all:
				wild = true
				switch t := x.(type) {
				case []any:
					next = append(next, t...)
				case map[string]any:
					for _, v := range t {
						next = append(next, v)
					}
				}
			case s.isKey:
				if m, ok := x.(map[string]any); ok {
					if v, ok := m[s.key]; ok {
						next = append(next, v)
					}
				}
			default:
				if a, ok := x.([]any); ok {
					i := s.index
					if i < 0 {
						i += len(a)
					}
					if i >= 0 && i < len(a) {
						next = append(next, a[i])
					}
				}
			}
		}
		cur = next
	}
	return cur, wild
}

// scalarOrJSON returns strings, numbers and booleans as values and objects
// and arrays as JSON text.
func scalarOrJSON(x any) event.Value {
	switch x.(type) {
	case map[string]any, []any:
		return encodeJSON(x)
	}
	return fromInterface(x)
}

// mutatePath applies f to the parent container of the path's last step,
// creating intermediate objects when create is set.
func mutatePath(root any, steps []pathStep, create bool, f func(parent any, last pathStep) any) any {
	if len(steps) == 0 {
		return root
	}
	if len(steps) == 1 {
		return f(root, steps[0])
	}
	s := steps[0]
	switch t := root.(type) {
	case map[string]any:
		if !s.isKey {
			return root
		}
		child, ok := t[s.key]
		if !ok {
			if !create {
				return root
			}
			child = map[string]any{}
		}
		t[s.key] = mutatePath(child, steps[1:], create, f)
	case []any:
		if s.isKey {
			return root
		}
		for i := range t {
			if s.all || i == s.index || (s.index < 0 && i == len(t)+s.index) {
				t[i] = mutatePath(t[i], steps[1:], create, f)
			}
		}
	}
	return root
}

func jsonEditor(edit func(parent any, last pathStep, value any) any, takesValue bool) NativeFunc {
	return func(a []event.Value) event.Value {
		if a[0].IsNull() {
			return event.NullValue()
		}
		root, ok := decodeJSON(a[0])
		if !ok {
			return event.NullValue()
		}
		stride := 1
		if takesValue {
			stride = 2
		}
		for i := 1; i+stride-1 < len(a); i += stride {
			p, ok := str(a[i])
			if !ok {
				return event.NullValue()
			}
			steps, ok := parsePath(p)
			if !ok {
				return event.NullValue()
			}
			var val any
			if takesValue {
				val = jsonArg(a[i+1])
			}
			root = mutatePath(root, steps, takesValue, func(parent any, last pathStep) any { return edit(parent, last, val) })
		}
		return encodeJSON(root)
	}
}

func registerJSONFuncs() {
	irFuncs["json"] = natively(func(a []event.Value) event.Value {
		if _, ok := decodeJSON(a[0]); ok && a[0].Type() == event.FieldTypeString {
			return a[0]
		}
		if a[0].Type() == event.FieldTypeArray || a[0].Type() == event.FieldTypeObject {
			s, _ := str(a[0])
			return event.StringValue(s)
		}
		return event.NullValue()
	})
	irFuncs["json_valid"] = natively(func(a []event.Value) event.Value {
		if a[0].Type() != event.FieldTypeString {
			return event.BoolValue(false)
		}
		_, ok := decodeJSON(a[0])
		return event.BoolValue(ok)
	})
	irFuncs["parse_json"] = natively(strict(func(a []event.Value) event.Value {
		x, ok := decodeJSON(a[0])
		if !ok {
			return event.NullValue()
		}
		return fromInterface(x)
	}))
	irFuncs["json_object"] = natively(func(a []event.Value) event.Value {
		if len(a)%2 != 0 {
			return event.NullValue()
		}
		out := map[string]any{}
		for i := 0; i < len(a); i += 2 {
			k, ok := str(a[i])
			if !ok {
				return event.NullValue()
			}
			out[k] = jsonArg(a[i+1])
		}
		return encodeJSON(out)
	})
	irFuncs["json_array"] = natively(func(a []event.Value) event.Value {
		out := make([]any, len(a))
		for i, v := range a {
			out[i] = jsonArg(v)
		}
		return encodeJSON(out)
	})
	irFuncs["json_array_length"] = natively(strict(func(a []event.Value) event.Value {
		x, ok := decodeJSON(a[0])
		arr, isArr := x.([]any)
		if !ok || !isArr {
			return event.NullValue()
		}
		return event.IntValue(int64(len(arr)))
	}))
	irFuncs["json_keys"] = natively(strict(func(a []event.Value) event.Value {
		x, ok := decodeJSON(a[0])
		obj, isObj := x.(map[string]any)
		if !ok || !isObj {
			return event.NullValue()
		}
		keys := make([]any, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sortAny(keys)
		return encodeJSON(keys)
	}))
	irFuncs["json_extract"] = natively(strict(func(a []event.Value) event.Value {
		root, ok := decodeJSON(a[0])
		if !ok {
			return event.NullValue()
		}
		var results []any
		for _, pv := range a[1:] {
			p, _ := str(pv)
			steps, ok := parsePath(p)
			if !ok {
				return event.NullValue()
			}
			vals, wild := evalPath(root, steps)
			switch {
			case len(a) == 2 && !wild:
				if len(vals) == 0 {
					return event.NullValue()
				}
				return scalarOrJSON(vals[0])
			case len(a) == 2:
				return encodeJSON(vals)
			}
			if len(vals) == 1 && !wild {
				results = append(results, vals[0])
			} else if len(vals) > 0 {
				results = append(results, vals)
			} else {
				results = append(results, nil)
			}
		}
		return encodeJSON(results)
	}))
	irFuncs["json_delete"] = natively(jsonEditor(func(parent any, last pathStep, _ any) any {
		switch t := parent.(type) {
		case map[string]any:
			if last.isKey {
				delete(t, last.key)
			}
		case []any:
			if !last.isKey && !last.all {
				i := last.index
				if i < 0 {
					i += len(t)
				}
				if i >= 0 && i < len(t) {
					return append(append([]any(nil), t[:i]...), t[i+1:]...)
				}
			}
		}
		return parent
	}, false))
	irFuncs["json_set"] = natively(jsonEditor(func(parent any, last pathStep, v any) any {
		switch t := parent.(type) {
		case map[string]any:
			if last.isKey {
				t[last.key] = v
			}
		case []any:
			i := last.index
			if i < 0 {
				i += len(t)
			}
			if !last.isKey && i >= 0 && i < len(t) {
				t[i] = v
			}
		}
		return parent
	}, true))
	appendTo := func(extend bool) NativeFunc {
		return jsonEditor(func(parent any, last pathStep, v any) any {
			m, ok := parent.(map[string]any)
			if !ok || !last.isKey {
				return parent
			}
			arr, ok := m[last.key].([]any)
			if !ok {
				return parent
			}
			if extend {
				items, isArr := v.([]any)
				if !isArr {
					if s, isStr := v.(string); isStr {
						if x, ok := decodeJSON(event.StringValue(s)); ok {
							items, isArr = x.([]any)
						}
					}
				}
				if isArr {
					m[last.key] = append(arr, items...)
					return parent
				}
			}
			m[last.key] = append(arr, v)
			return parent
		}, true)
	}
	irFuncs["json_append"] = natively(appendTo(false))
	irFuncs["json_extend"] = natively(appendTo(true))
}

func sortAny(xs []any) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1].(string) > xs[j].(string); j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}

var leadingNumber = regexp.MustCompile(`^\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)`)

func numberValue(f float64) event.Value {
	if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return event.IntValue(int64(f))
	}
	return event.FloatValue(f)
}

func registerConvertCommandFuncs() {
	text := func(f func(s string) event.Value) irEmitter {
		return natively(strict(func(a []event.Value) event.Value {
			if a[0].Type() == event.FieldTypeInt || a[0].Type() == event.FieldTypeFloat {
				return a[0]
			}
			s, ok := str(a[0])
			if !ok {
				return event.NullValue()
			}
			return f(s)
		}))
	}
	irFuncs["convert_auto"] = text(func(s string) event.Value {
		if v, ok := parseNumber(s); ok {
			return v
		}
		return event.NullValue()
	})
	irFuncs["rmcomma"] = text(func(s string) event.Value {
		if v, ok := parseNumber(strings.ReplaceAll(s, ",", "")); ok {
			return v
		}
		return event.NullValue()
	})
	irFuncs["rmunit"] = text(func(s string) event.Value {
		m := leadingNumber.FindStringSubmatch(s)
		if m == nil {
			return event.NullValue()
		}
		v, _ := parseNumber(m[1])
		return v
	})
	irFuncs["convert_num"] = text(func(s string) event.Value {
		m := leadingNumber.FindStringSubmatch(strings.ReplaceAll(s, ",", ""))
		if m == nil {
			return event.NullValue()
		}
		v, _ := parseNumber(m[1])
		return v
	})
	irFuncs["memk"] = text(func(s string) event.Value {
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
			return event.NullValue()
		}
		return numberValue(f * mult)
	})
	irFuncs["dur2sec"] = text(func(s string) event.Value {
		s = strings.TrimSpace(s)
		days := 0
		if d, rest, ok := strings.Cut(s, "+"); ok {
			n, err := strconv.Atoi(d)
			if err != nil {
				return event.NullValue()
			}
			days, s = n, rest
		}
		parts := strings.Split(s, ":")
		if len(parts) != 3 {
			return event.NullValue()
		}
		var total int64
		for _, p := range parts {
			n, err := strconv.Atoi(p)
			if err != nil {
				return event.NullValue()
			}
			total = total*60 + int64(n)
		}
		return event.IntValue(int64(days)*86400 + total)
	})
	irFuncs["mstime"] = text(func(s string) event.Value {
		s = strings.TrimSpace(s)
		minutes := 0.0
		if m, rest, ok := strings.Cut(s, ":"); ok {
			n, err := strconv.ParseFloat(m, 64)
			if err != nil {
				return event.NullValue()
			}
			minutes, s = n, rest
		}
		sec, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return event.NullValue()
		}
		return event.FloatValue(minutes*60 + sec)
	})
	irFuncs["ctime"] = natively(strict(func(a []event.Value) event.Value {
		f, ok := numFloat(a[0])
		format, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		sec, frac := math.Modf(f)
		return event.StringValue(posixFormat(time.Unix(int64(sec), int64(frac*1e9)).UTC(), format))
	}))
	irFuncs["mktime"] = natively(strict(func(a []event.Value) event.Value {
		s, ok := str(a[0])
		format, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		t, ok := mysqlParse(s, posixToMySQL(format))
		if !ok {
			return event.NullValue()
		}
		return event.FloatValue(float64(t.Unix()))
	}))
}

// posixToMySQL rewrites the strftime specifiers mktime accepts into their
// MySQL equivalents for mysqlParse.
func posixToMySQL(f string) string {
	r := strings.NewReplacer("%M", "%i", "%S", "%s", "%I", "%h", "%B", "%M")
	return r.Replace(f)
}

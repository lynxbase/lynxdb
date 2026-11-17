package irfunc

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// encodeJSON writes x as JSON text without escaping HTML, as the row engine
// does; NaN and infinities have no JSON form and give false.
func encodeJSON(dst []byte, x any) ([]byte, bool) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(x); err != nil {
		return dst, false
	}

	return append(dst, bytes.TrimRight(buf.Bytes(), "\n")...), true
}

func (c *ctx) encodeJSON(x any) val {
	start := len(c.out)
	var ok bool
	if c.out, ok = encodeJSON(c.out, x); !ok {
		return null
	}

	return c.result(start)
}

// jsonArg converts a value to embed in JSON text.
func jsonArg(v val) any {
	switch v.k {
	case kNull:
		return nil
	case kInt:
		return v.n
	case kFloat:
		return v.f
	case kBool:
		return v.n != 0
	}

	return interfaceForm(leaf(v))
}

type pathStep struct {
	key   string
	index int
	all   bool
	isKey bool
}

var pathToken = regexp.MustCompile(`^(?:\.([A-Za-z0-9_@$-]+)|\.\*|\[\*\]|\[(-?\d+)\]|\['([^']*)'\]|\["([^"]*)"\])`)

// parsePath parses a JSONPath subset: $, .name, ['name'], [n], [*], .*; a
// path without a leading $ is read relative to the root.
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
					// The row engine takes the members in random order.
					keys := make([]string, 0, len(t))
					for k := range t {
						keys = append(keys, k)
					}
					sortStrings(keys)
					for _, k := range keys {
						next = append(next, t[k])
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

// scalarOrJSON gives strings, numbers and booleans as values and objects
// and arrays as JSON text.
func (c *ctx) scalarOrJSON(x any) val {
	switch x.(type) {
	case map[string]any, []any:
		return c.encodeJSON(x)
	}

	return c.fromInterface(x)
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

func jsonEditor(edit func(parent any, last pathStep, value any) any, takesValue bool) impl {
	return func(c *ctx, a []val) val {
		if a[0].k == kNull {
			return null
		}
		root, ok := decodeJSON(a[0])
		if !ok {
			return null
		}
		stride := 1
		if takesValue {
			stride = 2
		}
		for i := 1; i+stride-1 < len(a); i += stride {
			p, ok := c.str(a[i])
			if !ok {
				return null
			}
			steps, ok := parsePath(p)
			if !ok {
				return null
			}
			var value any
			if takesValue {
				value = jsonArg(a[i+1])
			}
			root = mutatePath(root, steps, takesValue, func(parent any, last pathStep) any { return edit(parent, last, value) })
		}
		return c.encodeJSON(root)
	}
}

func init() {
	fixed("json", func(c *ctx, a []val) val {
		if _, ok := decodeJSON(a[0]); ok && a[0].k == kStr {
			return a[0]
		}
		if a[0].k == kArray || a[0].k == kObject {
			s, _ := c.str(a[0])
			return strVal(s)
		}
		return null
	})
	fixed("json_valid", func(_ *ctx, a []val) val {
		if a[0].k != kStr {
			return boolVal(false)
		}
		_, ok := decodeJSON(a[0])
		return boolVal(ok)
	})
	fixed("json_object", func(c *ctx, a []val) val {
		if len(a)%2 != 0 {
			return null
		}
		out := map[string]any{}
		for i := 0; i < len(a); i += 2 {
			k, ok := c.str(a[i])
			if !ok {
				return null
			}
			out[strings.Clone(k)] = jsonArg(a[i+1])
		}
		return c.encodeJSON(out)
	})
	fixed("json_array", func(c *ctx, a []val) val {
		out := make([]any, len(a))
		for i, v := range a {
			out[i] = jsonArg(v)
		}
		return c.encodeJSON(out)
	})
	fixed("json_array_length", strict(func(_ *ctx, a []val) val {
		x, ok := decodeJSON(a[0])
		arr, isArr := x.([]any)
		if !ok || !isArr {
			return null
		}
		return intVal(int64(len(arr)))
	}))
	fixed("json_keys", strict(func(c *ctx, a []val) val {
		x, ok := decodeJSON(a[0])
		obj, isObj := x.(map[string]any)
		if !ok || !isObj {
			return null
		}
		keys := make([]string, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sortStrings(keys)
		out := make([]any, len(keys))
		for i, k := range keys {
			out[i] = k
		}
		return c.encodeJSON(out)
	}))
	fixed("json_extract", strict(func(c *ctx, a []val) val {
		root, ok := decodeJSON(a[0])
		if !ok {
			return null
		}
		var results []any
		for _, pv := range a[1:] {
			p, _ := c.str(pv)
			steps, ok := parsePath(p)
			if !ok {
				return null
			}
			vals, wild := evalPath(root, steps)
			switch {
			case len(a) == 2 && !wild:
				if len(vals) == 0 {
					return null
				}
				return c.scalarOrJSON(vals[0])
			case len(a) == 2:
				return c.encodeJSON(vals)
			}
			if len(vals) == 1 && !wild {
				results = append(results, vals[0])
			} else if len(vals) > 0 {
				results = append(results, vals)
			} else {
				results = append(results, nil)
			}
		}
		return c.encodeJSON(results)
	}))
	fixed("json_delete", jsonEditor(func(parent any, last pathStep, _ any) any {
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
	fixed("json_set", jsonEditor(func(parent any, last pathStep, v any) any {
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
	appendTo := func(extend bool) impl {
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
						if x, ok := decodeJSON(strVal(s)); ok {
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
	fixed("json_append", appendTo(false))
	fixed("json_extend", appendTo(true))
}

// sortStrings sorts by insertion, as the row engine does; the order is the
// same as any other sort's.
func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j-1] > xs[j]; j-- {
			xs[j-1], xs[j] = xs[j], xs[j-1]
		}
	}
}

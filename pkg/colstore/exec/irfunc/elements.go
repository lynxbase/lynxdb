package irfunc

import (
	"strings"
	"time"
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

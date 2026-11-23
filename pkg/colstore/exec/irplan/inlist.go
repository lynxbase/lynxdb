package irplan

import (
	"math"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// listedComparisons is the most constants of a test for membership that
// become one comparison each; a longer list is looked up in sets.
const listedComparisons = 16

// listed holds the constants of a test for membership as sets, which give
// what the comparisons of a value with each would: a number equals the
// numbers and the strings that hold it, a string equals its bytes and,
// when it holds a number, the numbers equal to that.
type listed struct {
	strs    map[string]struct{}
	numbers map[numberKey]struct{}
	// texts holds the numbers that strings of the list hold.
	texts map[numberKey]struct{}
	bools [2]bool
	// What a value compares with as unknown: a null, a string that holds
	// no number, a number, a boolean, an array or an object.
	null, text, number, string, boolean, other bool
}

// numberKey is a number as an integer when it is one, else as the bits of
// the float.
type numberKey struct {
	n     int64
	float bool
}

func keyOf(v exec.Value) (numberKey, bool) {
	switch v.Kind {
	case vec.KindInt:
		return numberKey{n: v.Int}, true
	case vec.KindFloat:
		if v.Float != v.Float {
			return numberKey{}, false
		}
		if v.Float == math.Trunc(v.Float) && math.Abs(v.Float) < 1<<63 {
			return numberKey{n: int64(v.Float)}, true
		}

		return numberKey{n: int64(math.Float64bits(v.Float)), float: true}, true
	}

	return numberKey{}, false
}

// inList returns the test of x for membership among the literals of list,
// or nil for a list that is short or holds more than literals.
func (t *translator) inList(x ir.Expr, list []ir.Expr, not bool) (*exec.Expr, error) {
	if len(list) <= listedComparisons {
		return nil, nil
	}
	l := &listed{strs: map[string]struct{}{}, numbers: map[numberKey]struct{}{}, texts: map[numberKey]struct{}{}}
	for _, e := range list {
		lit, ok := e.(*ir.Literal)
		if !ok {
			return nil, nil
		}
		c, err := literal(lit)
		if err != nil {
			return nil, nil
		}
		switch v := c.Value; v.Kind {
		case vec.KindNull, exec.KindAny:
			l.null = true
		case vec.KindBool:
			l.bools[btoi(v.Bool)], l.boolean = true, true
		case vec.KindInt, vec.KindFloat:
			if k, ok := keyOf(v); ok {
				l.numbers[k] = struct{}{}
			}
			l.number = true
		case vec.KindString:
			l.strs[string(v.Bytes)], l.string = struct{}{}, true
			if n, ok := exec.NumberText(v.Bytes); ok {
				if k, ok := keyOf(n); ok {
					l.texts[k] = struct{}{}
				}
			} else {
				l.text = true
			}
		default:
			l.other = true
		}
	}
	arg, err := t.expr(x)
	if err != nil {
		return nil, err
	}

	return exec.Call(func() exec.Func {
		return func(args []exec.Value, buf []byte) (exec.Value, []byte) {
			is, known := l.holds(args[0])
			if !known {
				return exec.Value{Kind: vec.KindNull}, buf
			}

			return exec.Value{Kind: vec.KindBool, Bool: is != not}, buf
		}
	}, arg), nil
}

func btoi(b bool) int {
	if b {
		return 1
	}

	return 0
}

// holds reports whether the list holds v, and false for known where the
// outcome is unknown: no constant equals v and one compares with it as
// unknown.
func (l *listed) holds(v exec.Value) (is, known bool) {
	unknown := l.null || l.other
	switch v.Kind {
	case vec.KindInt, vec.KindFloat:
		k, ok := keyOf(v)
		if !ok {
			return false, false
		}
		if _, hit := l.numbers[k]; hit {
			return true, true
		}
		if _, hit := l.texts[k]; hit {
			return true, true
		}
		unknown = unknown || l.text || l.boolean
	case vec.KindString:
		if _, hit := l.strs[string(v.Bytes)]; hit {
			return true, true
		}
		n, number := exec.NumberText(v.Bytes)
		if k, ok := keyOf(n); number && ok {
			if _, hit := l.numbers[k]; hit {
				return true, true
			}
		}
		unknown = unknown || l.boolean || (l.number && !number)
	case vec.KindBool:
		if l.bools[btoi(v.Bool)] {
			return true, true
		}
		unknown = unknown || l.number || l.string
	default:
		return false, false
	}

	return false, !unknown
}

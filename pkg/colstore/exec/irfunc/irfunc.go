// Package irfunc implements the scalar functions of the IR as functions of
// the columnar executor, with the semantics the row engine gives them.
package irfunc

import (
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Builder makes the function of a call. It may specialize on the call's
// constant arguments and on the IR types of its arguments; the function
// receives the values of all the call's arguments in order.
type Builder func(call *ir.Call) (func() exec.Func, error)

// Lookup returns the builder of an IR function.
func Lookup(name string) (Builder, bool) {
	b, ok := builders[name]

	return b, ok
}

// Names returns the functions that have a builder.
func Names() []string {
	out := make([]string, 0, len(builders))
	for name := range builders {
		out = append(out, name)
	}

	return out
}

var builders = map[string]Builder{}

// ctx is the memory of one function: text forms of its arguments and the
// bytes of a result it builds. Both are reset for every row.
type ctx struct {
	text []byte
	out  []byte
	rs   []rune
}

// impl computes a function from the values of its arguments.
type impl func(c *ctx, a []val) val

// str returns the canonical text of a value, false for a null.
func (c *ctx) str(v val) (string, bool) {
	switch v.k {
	case kNull:
		return "", false
	case kStr:
		return v.s, true
	}
	start := len(c.text)
	c.text = appendText(c.text, v)

	return bytesString(c.text[start:]), true
}

// result returns the bytes appended to c.out since start as a string.
func (c *ctx) result(start int) val { return strVal(bytesString(c.out[start:])) }

// def registers a function whose implementation is made per call.
func def(name string, build func(call *ir.Call) (func() impl, error)) {
	if _, dup := builders[name]; dup {
		panic("irfunc: duplicate function " + name)
	}
	builders[name] = func(call *ir.Call) (func() exec.Func, error) {
		mk, err := build(call)
		if err != nil {
			return nil, err
		}

		return adapt(call, mk), nil
	}
}

// fixed registers a function that does not depend on the call.
func fixed(name string, f impl) {
	def(name, func(*ir.Call) (func() impl, error) { return func() impl { return f }, nil })
}

// strict makes f give a null when any argument is null.
func strict(f impl) impl {
	return func(c *ctx, a []val) val {
		for _, v := range a {
			if v.k == kNull {
				return null
			}
		}
		return f(c, a)
	}
}

// adapt turns an implementation into a function of the executor's values.
func adapt(call *ir.Call, mk func() impl) func() exec.Func {
	kinds := make([]argKind, len(call.Args))
	for i, a := range call.Args {
		switch a.Type().Kind {
		case ir.KindTimestamp:
			kinds[i] = argTime
		case ir.KindDuration:
			kinds[i] = argDur
		}
	}

	return func() exec.Func {
		f := mk()
		c := &ctx{}
		vals := make([]val, len(kinds))

		return func(args []exec.Value, buf []byte) (exec.Value, []byte) {
			c.text, c.out = c.text[:0], c.out[:0]
			for i := range vals {
				if i < len(args) {
					vals[i] = in(args[i], kinds[i])
				}
			}

			return out(f(c, vals), buf)
		}
	}
}

// constString returns the string literal of argument i.
func constString(call *ir.Call, i int) (string, bool) {
	if i >= len(call.Args) {
		return "", false
	}
	lit, ok := call.Args[i].(*ir.Literal)
	if !ok {
		return "", false
	}
	s, ok := lit.V.(string)

	return s, ok
}

// constInt returns the integer literal of argument i; a float literal is
// truncated.
func constInt(call *ir.Call, i int) (int64, bool) {
	if i >= len(call.Args) {
		return 0, false
	}
	lit, ok := call.Args[i].(*ir.Literal)
	if !ok {
		return 0, false
	}
	switch v := lit.V.(type) {
	case int64:
		return v, true
	case float64:
		return int64(v), true
	}

	return 0, false
}

func errorf(format string, args ...any) error { return fmt.Errorf("irfunc: "+format, args...) }

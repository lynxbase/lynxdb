package irplan

import (
	"fmt"
	"slices"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irfunc"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// lambdaCall translates a call of a function that takes lambdas: forall,
// exists, filter, transform and reduce. The executor's function of the call
// takes the call's other arguments, then the columns that the lambdas read
// besides their parameters, and computes each lambda for an element as an
// expression of a row that holds the parameters and those columns.
func (t *translator) lambdaCall(c *ir.Call) (*exec.Expr, error) {
	var args []*exec.Expr
	var bodies []*exec.Expr
	var params [][]string
	for _, a := range c.Args {
		l, ok := a.(*ir.Lambda)
		if !ok {
			e, err := t.expr(a)
			if err != nil {
				return nil, err
			}
			args = append(args, e)

			continue
		}
		paths := make([]string, len(l.Params))
		for i, p := range l.Params {
			paths[i] = fmt.Sprintf("\x00param%d", len(t.params))
			t.params[p] = paths[i]
		}
		body, err := t.expr(l.Body)
		if err != nil {
			return nil, err
		}
		bodies, params = append(bodies, body), append(params, paths)
	}
	// The columns the bodies read follow the call's own arguments.
	own := len(args)
	var outer []string
	var walk func(e *exec.Expr)
	walk = func(e *exec.Expr) {
		if e.Op == exec.OpColumn && !slices.ContainsFunc(params, func(ps []string) bool { return slices.Contains(ps, e.Path) }) &&
			!slices.Contains(outer, e.Path) {
			outer = append(outer, e.Path)
			args = append(args, e)
		}
		for _, a := range e.Args {
			walk(a)
		}
	}
	for _, b := range bodies {
		walk(b)
	}
	if len(args) == 0 {
		args = append(args, exec.Bool(true))
	}
	name := c.Func.Name
	if len(bodies) == 0 || (name != "reduce" && own != 1) || (name == "reduce" && own != 2) {
		return nil, fmt.Errorf("irplan: %s takes an array and a lambda", c)
	}

	return exec.Call(func() exec.Func {
		l := &lambdas{outer: outer}
		for i, b := range bodies {
			l.bodies = append(l.bodies, exec.NewRowExpr(b))
			l.params = append(l.params, params[i])
		}

		return func(vals []exec.Value, buf []byte) (exec.Value, []byte) {
			l.vals = vals[own:]

			return l.call(name, vals[:own], buf)
		}
	}, args...), nil
}

// lambdas computes the lambdas of one call; it is the row of their
// expressions.
type lambdas struct {
	bodies []*exec.RowExpr
	params [][]string
	outer  []string
	// vals holds the values of the outer columns, bound the values of the
	// parameters of the lambda being computed.
	vals  []exec.Value
	at    int
	bound []exec.Value
}

func (l *lambdas) Field(path string) exec.Value {
	if i := slices.Index(l.params[l.at], path); i >= 0 && i < len(l.bound) {
		return l.bound[i]
	}
	if i := slices.Index(l.outer, path); i >= 0 {
		return l.vals[i]
	}

	return exec.Value{}
}

func (l *lambdas) Values(fn func(exec.Value) bool) {
	for _, v := range l.bound {
		if !fn(v) {
			return
		}
	}
	for _, v := range l.vals {
		if !fn(v) {
			return
		}
	}
}

// apply computes lambda i for the values of its parameters; the value has
// bytes of its own.
func (l *lambdas) apply(i int, bound ...exec.Value) exec.Value {
	l.at, l.bound = i, bound
	v := l.bodies[i].Value(l)
	if v.Bytes != nil {
		v.Bytes = append([]byte(nil), v.Bytes...)
	}

	return v
}

func truth(v exec.Value) (is, known bool) { return v.Bool, v.Kind == vec.KindBool }

func (l *lambdas) call(name string, args []exec.Value, buf []byte) (exec.Value, []byte) {
	null := exec.Value{Kind: vec.KindNull}
	elems, ok := irfunc.Elements(args[0])
	if !ok {
		return null, buf
	}
	switch name {
	case "forall", "exists":
		// The value that settles the outcome: false for forall, true for
		// exists; an unknown element leaves it unknown otherwise.
		settles, unknown := name == "exists", false
		for _, e := range elems {
			switch is, known := truth(l.apply(0, e)); {
			case !known:
				unknown = true
			case is == settles:
				return exec.Value{Kind: vec.KindBool, Bool: settles}, buf
			}
		}
		if unknown {
			return null, buf
		}

		return exec.Value{Kind: vec.KindBool, Bool: !settles}, buf
	case "filter":
		kept := elems[:0]
		for _, e := range elems {
			if is, known := truth(l.apply(0, e)); known && is {
				kept = append(kept, e)
			}
		}

		return irfunc.Array(kept, buf)
	case "transform":
		out := make([]exec.Value, len(elems))
		for i, e := range elems {
			out[i] = l.apply(0, e, exec.Value{Kind: vec.KindInt, Int: int64(i)})
		}

		return irfunc.Array(out, buf)
	}
	acc := args[1]
	for _, e := range elems {
		acc = l.apply(0, acc, e)
	}
	if len(l.bodies) > 1 {
		acc = l.apply(1, acc)
	}
	if acc.IsNull() {
		return null, buf
	}
	start := len(buf)
	buf = append(buf, acc.Bytes...)
	if acc.Bytes != nil {
		acc.Bytes = buf[start:]
	}

	return acc, buf
}

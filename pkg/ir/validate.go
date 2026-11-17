package ir

import (
	"errors"
	"fmt"
)

// ValidationError reports one violated IR invariant.
type ValidationError struct {
	Node string
	Msg  string
}

func (e *ValidationError) Error() string { return "ir: " + e.Node + ": " + e.Msg }

// Validate checks the structural and typing invariants of a plan: operator
// shapes, column resolution against closed input schemas, function kinds,
// arity and static argument types, aggregate placement, and lambda scoping.
func Validate(p *Plan) error {
	if p == nil || p.Root == nil {
		return &ValidationError{Node: "plan", Msg: "empty plan"}
	}
	v := &validator{}
	v.node(p.Root)
	return errors.Join(v.errs...)
}

type validator struct {
	errs []error
}

func (v *validator) fail(n Node, format string, args ...any) {
	v.errs = append(v.errs, &ValidationError{Node: n.Label(), Msg: fmt.Sprintf(format, args...)})
}

// node validates n and its inputs and returns the output schema of n; ok
// is false when n has a nil input somewhere below it.
func (v *validator) node(n Node) (out Schema, ok bool) {
	var nodeBuf [2]Node
	ins := AppendInputs(nodeBuf[:0], n)
	var schemaBuf [2]Schema
	schemas := schemaBuf[:0]
	ok = true
	for _, in := range ins {
		if in == nil {
			v.fail(n, "nil input")
			return Schema{}, false
		}
		s, inOK := v.node(in)
		schemas = append(schemas, s)
		ok = ok && inOK
	}
	if !ok {
		for i, in := range ins {
			schemas[i] = in.Schema()
		}
	}
	var in Schema
	if len(ins) == 1 {
		in = schemas[0]
	}
	v.check(n, in, schemas)
	return DeriveSchema(n, schemas...), true
}

// check validates the operator n itself against its input schemas.
func (v *validator) check(n Node, in Schema, inputs []Schema) {
	scalarIn := func(e Expr) { v.expr(n, e, in, false, nil) }

	switch x := n.(type) {
	case *Scan:
		for _, e := range x.Prune {
			v.expr(n, e, x.Out, false, nil)
		}
	case *Values:
		for i, r := range x.Rows {
			if len(r) != len(x.Out.Columns) {
				v.fail(n, "row %d has %d values for %d columns", i, len(r), len(x.Out.Columns))
			}
			for _, e := range r {
				if _, ok := e.(*Literal); !ok {
					v.fail(n, "row %d holds a non-literal %s", i, e)
				}
			}
		}
	case *Filter:
		scalarIn(x.Cond)
		v.boolean(n, x.Cond)
	case *Compute:
		var b SchemaBuilder
		b.Init(in, len(x.Assigns))
		for _, a := range x.Assigns {
			if a.Name == "" {
				v.fail(n, "assignment without a name")
			}
			v.expr(n, a.Expr, b.Schema(), false, nil)
			b.With(Column{Name: a.Name, Type: a.Expr.Type(), Nullable: true})
		}
	case *Project:
		if len(x.Names) == 0 {
			v.fail(n, "no columns")
		}
		if !in.Open {
			var known map[string]bool
			if len(in.Columns) > wideSchema && len(x.Names) > 8 {
				known = make(map[string]bool, len(in.Columns))
				for _, c := range in.Columns {
					known[c.Name] = true
				}
			}
			for _, name := range x.Names {
				if !IsNamePattern(name) && !known[name] {
					if _, ok := in.Resolve(name); !ok {
						v.fail(n, "unknown column %s", QuoteName(name))
					}
				}
			}
		}
	case *Aggregate:
		names := map[string]bool{}
		for _, g := range x.Groups {
			scalarIn(g.Expr)
			v.unique(n, names, g.Name)
		}
		for _, a := range x.Aggs {
			v.unique(n, names, a.Name)
			v.agg(n, a.Agg, in, AggregateFunc)
		}
		if len(x.Aggs) == 0 && len(x.Groups) == 0 {
			v.fail(n, "no groups and no aggregates")
		}
	case *Window:
		for _, g := range x.PartitionBy {
			scalarIn(g.Expr)
		}
		names := map[string]bool{}
		for _, f := range x.Funcs {
			v.unique(n, names, f.Name)
			v.agg(n, f.Agg, in, WindowFunc)
		}
		if x.Frame.Kind != FramePartition && x.Frame.Kind != FrameRunning {
			v.fail(n, "invalid frame kind")
		}
	case *Sort:
		if len(x.Keys) == 0 {
			v.fail(n, "no sort keys")
		}
		for _, k := range x.Keys {
			scalarIn(k.Expr)
		}
	case *TopN:
		if len(x.Keys) == 0 || x.Count < 0 {
			v.fail(n, "TopN needs keys and a non-negative count")
		}
		for _, k := range x.Keys {
			scalarIn(k.Expr)
		}
	case *Limit:
		if x.Count < -1 || x.Offset < 0 {
			v.fail(n, "invalid count %d or offset %d", x.Count, x.Offset)
		}
	case *Dedup:
		if len(x.Keys) == 0 || x.Count < 1 {
			v.fail(n, "dedup needs keys and a count of at least 1")
		}
		for _, k := range x.Keys {
			scalarIn(k)
		}
	case *Join:
		if len(x.LeftKeys) != len(x.RightKeys) {
			v.fail(n, "%d left keys for %d right keys", len(x.LeftKeys), len(x.RightKeys))
		}
		ls, rs := inputs[0], inputs[1]
		for _, k := range x.LeftKeys {
			v.expr(n, k, ls, false, nil)
		}
		for _, k := range x.RightKeys {
			v.expr(n, k, rs, false, nil)
		}
		if x.Cond != nil {
			both := UnionSchemas(ls, rs)
			v.expr(n, x.Cond, both, false, nil)
			v.boolean(n, x.Cond)
		}
		if x.Kind != JoinCross && len(x.LeftKeys) == 0 && x.Cond == nil {
			v.fail(n, "%s join without a condition", x.Kind)
		}
	case *Union:
		if len(x.Sources) == 0 {
			v.fail(n, "no inputs")
		}
	case *Extract:
		scalarIn(x.Source)
		if len(x.Outputs) == 0 {
			v.fail(n, "no output columns")
		}
		if x.Pattern == "" {
			v.fail(n, "empty pattern")
		}
	case *Patterns:
		scalarIn(x.Source)
	case *FillNull:
		for _, a := range x.Fields {
			scalarIn(a.Expr)
		}
		if len(x.Fields) == 0 && x.Default == nil {
			v.fail(n, "nothing to fill")
		}
		if x.Default != nil {
			scalarIn(x.Default)
		}
	case *Unnest:
		if x.Field == "" {
			v.fail(n, "no field")
		}
	case *Transpose:
		if x.MaxRows < 1 {
			v.fail(n, "max rows must be positive")
		}
	}
}

func (v *validator) unique(n Node, seen map[string]bool, name string) {
	if name == "" {
		v.fail(n, "output without a name")
		return
	}
	if seen[name] {
		v.fail(n, "duplicate output column %s", QuoteName(name))
	}
	seen[name] = true
}

func (v *validator) boolean(n Node, e Expr) {
	switch e.Type().Kind {
	case KindBool, KindAny, KindNull:
	default:
		v.fail(n, "condition %s has type %s, want bool", e, e.Type())
	}
}

func (v *validator) agg(n Node, a *AggCall, in Schema, place FuncKind) {
	if a == nil || a.Func == nil {
		v.fail(n, "aggregate without a function")
		return
	}
	switch {
	case a.Func.Kind == AggregateFunc:
	case a.Func.Kind == WindowFunc && place == WindowFunc:
	default:
		v.fail(n, "%s is a %s function", a.Func.Name, a.Func.Kind)
	}
	types := make([]Type, len(a.Args))
	for i, arg := range a.Args {
		v.expr(n, arg, in, false, nil)
		types[i] = arg.Type()
		if prm, ok := a.Func.ParamFor(i); ok && prm.Const {
			if _, lit := arg.(*Literal); !lit {
				v.fail(n, "%s: argument %d must be a constant", a.Func.Name, i+1)
			}
		}
	}
	if _, err := a.Func.Check(types); err != nil {
		v.fail(n, "%v", err)
	}
	if a.Filter != nil {
		v.expr(n, a.Filter, in, false, nil)
		v.boolean(n, a.Filter)
	}
}

func (v *validator) expr(n Node, e Expr, in Schema, inLambdaArg bool, scope []*LambdaParam) {
	if e == nil {
		v.fail(n, "nil expression")
		return
	}
	switch x := e.(type) {
	case *FieldRef:
		if _, ok := in.Resolve(x.Name); !ok {
			v.fail(n, "unknown column %s", QuoteName(x.Name))
		}
	case *LambdaParam:
		for _, p := range scope {
			if p == x || p.Name == x.Name {
				return
			}
		}
		v.fail(n, "lambda parameter %s outside its lambda", x.Name)
	case *Lambda:
		if !inLambdaArg {
			v.fail(n, "lambda %s outside a function argument", x)
		}
		v.expr(n, x.Body, in, false, append(append([]*LambdaParam(nil), scope...), x.Params...))
		return
	case *AggCall:
		v.fail(n, "aggregate %s used as a scalar expression", x)
	case *Call:
		if x.Func == nil {
			v.fail(n, "call without a function")
			return
		}
		if x.Func.Kind != ScalarFunc {
			v.fail(n, "%s is a %s function", x.Func.Name, x.Func.Kind)
		}
		types := make([]Type, len(x.Args))
		for i, a := range x.Args {
			prm, _ := x.Func.ParamFor(i)
			if prm.Lambda > 0 {
				l, ok := a.(*Lambda)
				if !ok {
					v.fail(n, "%s: argument %d must be a lambda", x.Func.Name, i+1)
				} else if len(l.Params) > prm.Lambda {
					v.fail(n, "%s: argument %d takes at most %d lambda parameters", x.Func.Name, i+1, prm.Lambda)
				}
			}
			if prm.Const {
				if _, lit := a.(*Literal); !lit {
					v.fail(n, "%s: argument %d must be a constant", x.Func.Name, i+1)
				}
			}
			v.expr(n, a, in, prm.Lambda > 0, scope)
			types[i] = a.Type()
		}
		if _, err := x.Func.Check(types); err != nil {
			v.fail(n, "%v", err)
		}
		return
	case *Subquery:
		if x.Plan == nil {
			v.fail(n, "subquery without a plan")
		} else {
			s, ok := v.node(x.Plan)
			if !ok {
				s = x.Plan.Schema()
			}
			if x.Mode == SubqueryScalar && !s.Open && len(s.Columns) != 1 {
				v.fail(n, "scalar subquery returns %d columns", len(s.Columns))
			}
			if x.Mode == SubqueryIn && !s.Open && len(s.Columns) != len(x.Args) {
				v.fail(n, "IN subquery returns %d columns for %d values", len(s.Columns), len(x.Args))
			}
		}
	case *Search:
		if x.Pattern == "" {
			v.fail(n, "empty search pattern")
		}
	}
	for _, c := range ExprChildren(e) {
		v.expr(n, c, in, false, scope)
	}
}

package columnar

import (
	"context"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// listedValues is the most values of a subquery that a membership test
// names one by one; a longer answer is left to the row operators.
const listedValues = 1024

// resolved returns the plan under n with every test for membership among
// the rows of a subquery, in a filter or a computed column, replaced by a
// test among the values that the subquery gives, so that the executor can
// take the plan. A subquery of several columns, of other values than
// scalars or of too many rows stays as it is.
func resolved(ctx context.Context, n ir.Node, tables Tables, opts Options) (ir.Node, error) {
	var failed error
	listed := func(e ir.Expr) ir.Expr {
		sq, ok := e.(*ir.Subquery)
		if !ok || failed != nil || sq.Mode != ir.SubqueryIn || len(sq.Args) != 1 {
			return e
		}
		names := sq.Plan.Schema().Names()
		if len(names) != 1 {
			return e
		}
		res, err := Run(ctx, &ir.Plan{Root: &ir.Limit{Input: sq.Plan, Count: listedValues + 1}}, tables, opts)
		if err != nil {
			failed = err

			return e
		}
		if res.Len() > listedValues {
			return e
		}
		list := make([]ir.Expr, 0, res.Len())
		for _, row := range res.Maps() {
			lit, ok := literalOf(row[names[0]])
			if !ok {
				return e
			}
			list = append(list, lit)
		}

		return &ir.InList{X: sq.Args[0], List: list, Not: sq.Not}
	}
	out := ir.Transform(n, func(n ir.Node) ir.Node {
		switch n := n.(type) {
		case *ir.Filter:
			if cond := ir.RewriteExpr(n.Cond, listed); cond != n.Cond {
				return &ir.Filter{Input: n.Input, Cond: cond}
			}
		case *ir.Compute:
			assigns, changed := make([]ir.Assign, len(n.Assigns)), false
			for i, a := range n.Assigns {
				assigns[i] = ir.Assign{Name: a.Name, Expr: ir.RewriteExpr(a.Expr, listed)}
				changed = changed || assigns[i].Expr != a.Expr
			}
			if changed {
				return &ir.Compute{Input: n.Input, Assigns: assigns}
			}
		}

		return n
	})

	return out, failed
}

// literalOf returns a scalar value of a row as a literal of a plan.
func literalOf(v event.Value) (*ir.Literal, bool) {
	switch v.Type() {
	case event.FieldTypeNull:
		return ir.NullLit(), true
	case event.FieldTypeBool:
		return ir.BoolLit(v.AsBool()), true
	case event.FieldTypeInt:
		return ir.IntLit(v.AsInt()), true
	case event.FieldTypeFloat:
		return &ir.Literal{V: v.AsFloat(), T: ir.Float}, true
	case event.FieldTypeString:
		return ir.StringLit(v.AsString()), true
	}

	return nil, false
}

package columnar

import (
	"context"
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// resolved returns the plan under n with every subquery replaced by what it
// gives, after running it once: an EXISTS by whether it has a row, a scalar
// subquery by the value of its row, and a test for membership among its
// rows by the test among the values of those rows.
func resolved(ctx context.Context, n ir.Node, tables Tables, opts Options) (ir.Node, error) {
	var failed error
	answer := func(e ir.Expr) ir.Expr {
		sq, ok := e.(*ir.Subquery)
		if !ok || failed != nil {
			return e
		}
		out, err := answered(ctx, sq, tables, opts)
		if err != nil {
			failed = err

			return e
		}

		return out
	}
	out := ir.Transform(n, func(n ir.Node) ir.Node { return ir.RewriteNodeExprs(n, answer) })

	return out, failed
}

// answered runs a subquery and returns the expression that stands for it.
func answered(ctx context.Context, sq *ir.Subquery, tables Tables, opts Options) (ir.Expr, error) {
	plan := sq.Plan
	switch sq.Mode {
	case ir.SubqueryExists:
		plan = &ir.Limit{Input: plan, Count: 1}
	case ir.SubqueryScalar:
		plan = &ir.Limit{Input: plan, Count: 2}
	}
	names := sq.Plan.Schema().Names()
	if sq.Mode != ir.SubqueryExists && len(names) == 0 {
		return nil, fmt.Errorf("subquery: its columns are not known before it runs; end it with fields or stats")
	}
	res, err := Run(ctx, &ir.Plan{Root: plan}, tables, opts)
	if err != nil {
		return nil, fmt.Errorf("subquery: %w", err)
	}
	rows := res.Maps()
	switch sq.Mode {
	case ir.SubqueryExists:
		return ir.BoolLit(len(rows) > 0), nil
	case ir.SubqueryScalar:
		switch len(rows) {
		case 0:
			return ir.NullLit(), nil
		case 1:
			return literalOf(rows[0][names[0]])
		}

		return nil, fmt.Errorf("subquery: a scalar subquery gives more than one row")
	}
	if len(names) != len(sq.Args) {
		return nil, fmt.Errorf("subquery: %d columns for a test of %d values", len(names), len(sq.Args))
	}
	if len(sq.Args) == 1 {
		list := make([]ir.Expr, len(rows))
		for i, row := range rows {
			if list[i], err = literalOf(row[names[0]]); err != nil {
				return nil, err
			}
		}
		if len(list) == 0 {
			return ir.BoolLit(sq.Not), nil
		}

		return &ir.InList{X: sq.Args[0], List: list, Not: sq.Not}, nil
	}
	// A tuple is among the rows where it equals one of them in every value.
	equal := make([]ir.Expr, len(rows))
	for i, row := range rows {
		all := make([]ir.Expr, len(names))
		for j, name := range names {
			lit, err := literalOf(row[name])
			if err != nil {
				return nil, err
			}
			all[j] = &ir.Binary{Op: ir.OpEq, L: sq.Args[j], R: lit, T: ir.Bool}
		}
		equal[i] = ir.And(all...)
	}
	if len(equal) == 0 {
		return ir.BoolLit(sq.Not), nil
	}
	in := equal[0]
	for _, e := range equal[1:] {
		in = &ir.Binary{Op: ir.OpOr, L: in, R: e, T: ir.Bool}
	}
	if sq.Not {
		in = &ir.Unary{Op: ir.OpNot, X: in, T: ir.Bool}
	}

	return in, nil
}

// literalOf returns a scalar value of a row as a literal of a plan.
func literalOf(v event.Value) (*ir.Literal, error) {
	switch v.Type() {
	case event.FieldTypeNull:
		return ir.NullLit(), nil
	case event.FieldTypeBool:
		return ir.BoolLit(v.AsBool()), nil
	case event.FieldTypeInt:
		return ir.IntLit(v.AsInt()), nil
	case event.FieldTypeFloat:
		return &ir.Literal{V: v.AsFloat(), T: ir.Float}, nil
	case event.FieldTypeString:
		return ir.StringLit(v.AsString()), nil
	case event.FieldTypeTimestamp:
		return &ir.Literal{V: v.AsTimestamp(), T: ir.Timestamp}, nil
	case event.FieldTypeDuration:
		return &ir.Literal{V: v.AsDuration(), T: ir.Duration}, nil
	}

	return nil, fmt.Errorf("subquery: a value of type %s is not a value to compare with", v.Type())
}

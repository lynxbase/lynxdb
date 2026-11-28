package query

import (
	"slices"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// orderSubqueries gives a head inside a scalar or membership subquery its
// event order. The enclosing expression consumes values independently of order.
func orderSubqueries(n ir.Node) ir.Node {
	return ir.RewriteNodeExprs(n, func(e ir.Expr) ir.Expr {
		s, ok := e.(*ir.Subquery)
		if !ok {
			return e
		}
		c := *s
		c.Plan = eventOrder(ir.Transform(s.Plan, orderSubqueries), false)
		return &c
	})
}

// eventOrder supplies the input order of an event search and of a head
// within a larger query. A sort, aggregation or combining command owns
// its output order. Projections can remove or replace _time, so order the
// original events below them.
func eventOrder(n ir.Node, needed bool) ir.Node {
	switch n.(type) {
	case *ir.Limit:
		needed = true
	case *ir.Scan, *ir.Filter:
		if needed {
			input := n
			for {
				f, ok := input.(*ir.Filter)
				if !ok {
					break
				}
				input = f.Input
			}
			if _, ok := input.(*ir.Scan); ok {
				return &ir.Sort{Input: n, Keys: []ir.SortKey{{Expr: &ir.FieldRef{Name: ir.TimeColumn, T: ir.Timestamp}, Desc: true}}}
			}
		}
	case *ir.Project, *ir.Compute, *ir.RenameColumns, *ir.Extract, *ir.FillNull,
		*ir.Unnest, *ir.FlattenObject, *ir.Reverse, *ir.Dedup, *ir.Window:
	default:
		needed = false
	}
	ins := n.Inputs()
	var changed []ir.Node
	for i, in := range ins {
		out := eventOrder(in, needed)
		if out != in {
			if changed == nil {
				changed = slices.Clone(ins)
			}
			changed[i] = out
		}
	}
	if changed != nil {
		return n.WithInputs(changed)
	}
	return n
}

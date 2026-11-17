package columnar

import (
	"slices"
	"strconv"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// collected rewrites an aggregate that collects the distinct values of one
// expression in each group as two aggregates: one over the events, by the
// groups and the expression, which the executor takes, and one over its
// rows, far fewer than the events, that collects the values and adds up
// what the first counted. It returns n itself for any other node, and for an
// aggregate with a function that does not add up this way.
func collected(n ir.Node) ir.Node {
	agg, ok := n.(*ir.Aggregate)
	if !ok || agg.Phase != ir.AggComplete {
		return n
	}
	var value ir.Expr
	for _, a := range agg.Aggs {
		call := a.Agg
		if call.Filter != nil || call.Distinct {
			return n
		}
		switch call.Func.Name {
		case "values":
			if len(call.Args) != 1 || (value != nil && value.String() != call.Args[0].String()) {
				return n
			}
			value = call.Args[0]
		case "count":
			// Without groups the count of no rows is zero, which no sum of
			// counts gives.
			if len(agg.Groups) == 0 || len(call.Args) > 1 {
				return n
			}
		case "sum", "min", "max":
			if len(call.Args) != 1 {
				return n
			}
		default:
			return n
		}
	}
	sum, ok := ir.LookupFunc("sum")
	if value == nil || !ok {
		return n
	}
	taken := make([]string, 0, len(agg.Groups)+len(agg.Aggs))
	for _, g := range agg.Groups {
		taken = append(taken, g.Name)
	}
	for _, a := range agg.Aggs {
		taken = append(taken, a.Name)
	}
	// name returns a column name that neither aggregate gives otherwise.
	name := func() string {
		for i := len(taken); ; i++ {
			if s := "part" + strconv.Itoa(i); !slices.Contains(taken, s) {
				taken = append(taken, s)

				return s
			}
		}
	}
	valueName := name()
	first := &ir.Aggregate{Input: agg.Input, Groups: append(slices.Clone(agg.Groups), ir.NamedExpr{Name: valueName, Expr: value})}
	second := &ir.Aggregate{
		Input: first, DropNullGroups: agg.DropNullGroups, Phase: agg.Phase, Limit: agg.Limit, OrderBy: agg.OrderBy,
	}
	for _, g := range agg.Groups {
		second.Groups = append(second.Groups, ir.NamedExpr{Name: g.Name, Expr: &ir.FieldRef{Name: g.Name, T: g.Expr.Type()}})
	}
	for _, a := range agg.Aggs {
		call := a.Agg
		if call.Func.Name == "values" {
			arg := &ir.FieldRef{Name: valueName, T: value.Type()}
			second.Aggs = append(second.Aggs, ir.NamedAgg{Name: a.Name, Agg: &ir.AggCall{Func: call.Func, Args: []ir.Expr{arg}, T: call.T}})

			continue
		}
		part := name()
		first.Aggs = append(first.Aggs, ir.NamedAgg{Name: part, Agg: call})
		whole := call.Func
		if whole.Name == "count" {
			whole = sum
		}
		arg := &ir.FieldRef{Name: part, T: call.T}
		second.Aggs = append(second.Aggs, ir.NamedAgg{Name: a.Name, Agg: &ir.AggCall{Func: whole, Args: []ir.Expr{arg}, T: call.T}})
	}

	return second
}

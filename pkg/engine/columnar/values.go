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
// what the first counted; a mean is the sum over the count. It returns n
// itself for any other node, and for an aggregate with a function that does
// not add up this way.
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
		case "avg":
			if len(agg.Groups) == 0 || len(call.Args) != 1 {
				return n
			}
		default:
			return n
		}
	}
	sum, ok := ir.LookupFunc("sum")
	count, ok2 := ir.LookupFunc("count")
	if value == nil || !ok || !ok2 {
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
	// added gives the first aggregate call under a name of its own and
	// returns the sum of it over the rows of a group in the second.
	added := func(call *ir.AggCall, as string) ir.NamedAgg {
		part := name()
		first.Aggs = append(first.Aggs, ir.NamedAgg{Name: part, Agg: call})
		whole := call.Func
		if whole.Name == "count" {
			whole = sum
		}

		return ir.NamedAgg{Name: as, Agg: &ir.AggCall{Func: whole, Args: []ir.Expr{&ir.FieldRef{Name: part, T: call.T}}, T: call.T}}
	}
	var means []ir.Assign
	for _, a := range agg.Aggs {
		call := a.Agg
		switch call.Func.Name {
		case "values":
			arg := &ir.FieldRef{Name: valueName, T: value.Type()}
			second.Aggs = append(second.Aggs, ir.NamedAgg{Name: a.Name, Agg: &ir.AggCall{Func: call.Func, Args: []ir.Expr{arg}, T: call.T}})
		case "avg":
			total := added(&ir.AggCall{Func: sum, Args: call.Args, T: ir.Float}, name())
			rows := added(&ir.AggCall{Func: count, Args: call.Args, T: ir.Int}, name())
			second.Aggs = append(second.Aggs, total, rows)
			means = append(means, ir.Assign{Name: a.Name, Expr: &ir.Binary{
				Op: ir.OpDiv, T: ir.Float,
				L: &ir.Cast{X: &ir.FieldRef{Name: total.Name, T: ir.Float}, To: ir.Float},
				R: &ir.FieldRef{Name: rows.Name, T: ir.Int},
			}})
		default:
			second.Aggs = append(second.Aggs, added(call, a.Name))
		}
	}
	if means == nil {
		return second
	}
	// The means are computed from their sums and counts, and the columns
	// are those of the aggregate again, in its order.
	names := make([]string, 0, len(agg.Groups)+len(agg.Aggs))
	for _, g := range agg.Groups {
		names = append(names, g.Name)
	}
	for _, a := range agg.Aggs {
		names = append(names, a.Name)
	}

	return &ir.Project{Input: &ir.Compute{Input: second, Assigns: means}, Names: names}
}

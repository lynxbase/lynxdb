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
	if !ok {
		return n
	}
	var value ir.Expr
	for _, a := range agg.Aggs {
		if call := a.Agg; call.Func.Name == "values" && len(call.Args) == 1 {
			value = call.Args[0]
		}
	}
	if value == nil {
		return n
	}
	if two, ok := inTwo(agg, agg.Input, value, nil); ok {
		return two
	}

	return n
}

// premapped rewrites an aggregate over columns that are computed from
// themselves alone and serve only as its groups, such as the labels that a
// chart gives its series: the executor aggregates by the columns as they are
// stored, the columns are computed for its rows, one for each of their
// values, and a second aggregate adds those rows up.
func premapped(n ir.Node) ir.Node {
	agg, ok := n.(*ir.Aggregate)
	if !ok {
		return n
	}
	var chain []*ir.Compute
	input := agg.Input
	for {
		c, ok := input.(*ir.Compute)
		if !ok {
			break
		}
		chain, input = append(chain, c), c.Input
	}
	if len(chain) == 0 {
		return n
	}
	var mapped []string
	for _, c := range chain {
		for _, a := range c.Assigns {
			if read := ir.ColumnsRead(a.Expr); len(read) > 1 || (len(read) == 1 && read[0] != a.Name) || varies(a.Expr) {
				return n
			}
			mapped = append(mapped, a.Name)
		}
	}
	// A computed column is a group under its own name, and nothing else of
	// the aggregate reads it.
	for _, g := range agg.Groups {
		ref, plain := g.Expr.(*ir.FieldRef)
		for _, name := range ir.ColumnsRead(g.Expr) {
			if slices.Contains(mapped, name) && (!plain || ref.Name != g.Name) {
				return n
			}
		}
	}
	for _, name := range mapped {
		if !slices.ContainsFunc(agg.Groups, func(g ir.NamedExpr) bool { return g.Name == name }) {
			return n
		}
	}
	for _, a := range agg.Aggs {
		for _, arg := range a.Agg.Args {
			for _, name := range ir.ColumnsRead(arg) {
				if slices.Contains(mapped, name) {
					return n
				}
			}
		}
	}
	between := func(first ir.Node) ir.Node {
		for i := len(chain) - 1; i >= 0; i-- {
			first = &ir.Compute{Input: first, Assigns: chain[i].Assigns}
		}

		return first
	}
	// The executor groups by the columns as stored, whatever type the
	// computed ones have.
	stored := *agg
	stored.Groups = slices.Clone(agg.Groups)
	for i, g := range stored.Groups {
		if slices.Contains(mapped, g.Name) {
			stored.Groups[i].Expr = &ir.FieldRef{Name: g.Name, T: ir.Any}
		}
	}
	if two, ok := inTwo(&stored, input, nil, between); ok {
		return two
	}

	return n
}

// varies reports whether e calls a function that can give another value
// for the same arguments.
func varies(e ir.Expr) bool {
	found := false
	ir.WalkExpr(e, func(x ir.Expr) bool {
		if call, ok := x.(*ir.Call); ok && call.Func.Volatile {
			found = true
		}

		return !found
	})

	return found
}

// inTwo returns agg as two aggregates that give its rows: a first over
// input, by the groups of agg and by value where agg collects the values of
// it, and a second over the rows of the first, with between applied to them
// when it is set, that adds up what the first counted; a mean is the sum
// over the count. It reports false for an aggregate with a call that does
// not add up this way.
func inTwo(agg *ir.Aggregate, input ir.Node, value ir.Expr, between func(ir.Node) ir.Node) (ir.Node, bool) {
	for _, a := range agg.Aggs {
		call := a.Agg
		if call.Filter != nil {
			return nil, false
		}
		switch call.Func.Name {
		case "values":
			if value == nil || len(call.Args) != 1 || value.String() != call.Args[0].String() {
				return nil, false
			}
		case "count", "avg":
			// Without groups the count of no rows is zero, which no sum of
			// counts gives.
			if len(agg.Groups) == 0 || len(call.Args) > 1 || (call.Func.Name == "avg" && len(call.Args) != 1) {
				return nil, false
			}
		case "sum", "min", "max":
			if len(call.Args) != 1 {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	sum, ok := ir.LookupFunc("sum")
	count, ok2 := ir.LookupFunc("count")
	if !ok || !ok2 {
		return nil, false
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
	first := &ir.Aggregate{Input: input, Groups: slices.Clone(agg.Groups)}
	valueName := ""
	if value != nil {
		valueName = name()
		first.Groups = append(first.Groups, ir.NamedExpr{Name: valueName, Expr: value})
	}
	second := &ir.Aggregate{DropNullGroups: agg.DropNullGroups}
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
	second.Input = first
	if between != nil {
		second.Input = between(first)
	}
	if means == nil {
		return second, true
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

	return &ir.Project{Input: &ir.Compute{Input: second, Assigns: means}, Names: names}, true
}

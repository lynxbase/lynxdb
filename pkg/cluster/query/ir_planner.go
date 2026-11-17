package query

import (
	"errors"
	"slices"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// ErrNotDistributable reports a plan that is not a single chain of
// operators over one scan, which the coordinator cannot split.
var ErrNotDistributable = errors.New("cluster query: only a single pipeline over one scan runs distributed; joins, unions and subqueries do not")

// IRDistributedPlan describes how a plan is split between shards and the
// coordinator. Shards compile the same query text and derive the same split,
// so only the text travels to them.
type IRDistributedPlan struct {
	// ShardPlan is the row-local prefix every shard executes.
	ShardPlan *ir.Plan
	// CoordNodes are the operators the coordinator applies, in pipeline
	// order, to the merged shard rows.
	CoordNodes []ir.Node
	Strategy   MergeStrategy
	// PartialAggSpec describes the aggregation shards compute as mergeable
	// states for MergePartialAgg and MergeTopK.
	PartialAggSpec *pipeline.PartialAggSpec
	TopK           int
	TopKSortFields []pipeline.SortField
	// SplitIndex is the number of chain operators executed on shards.
	SplitIndex int
}

// PlanDistributedQueryIR splits a plan into a shard prefix and coordinator
// operators.
func PlanDistributedQueryIR(plan *ir.Plan) (*IRDistributedPlan, error) {
	if plan == nil || plan.Root == nil {
		return &IRDistributedPlan{Strategy: MergeConcat}, nil
	}
	nodes, err := linearizeIR(plan.Root)
	if err != nil {
		return nil, err
	}
	split := 1
	for split < len(nodes) && isRowLocal(nodes[split]) {
		split++
	}
	result := &IRDistributedPlan{
		ShardPlan:  &ir.Plan{Root: nodes[split-1]},
		CoordNodes: nodes[split:],
		Strategy:   MergeConcat,
		SplitIndex: split,
	}
	if len(result.CoordNodes) == 0 {
		return result, nil
	}
	agg, ok := result.CoordNodes[0].(*ir.Aggregate)
	if !ok {
		return result, nil
	}
	spec := partialAggSpec(agg)
	if spec == nil {
		return result, nil
	}
	result.PartialAggSpec = spec
	result.Strategy = MergePartialAgg
	rest := result.CoordNodes[1:]
	result.CoordNodes = rest
	if len(rest) == 0 {
		return result, nil
	}
	// A column projection between the aggregate and a TopN commutes with
	// the TopN when it keeps the sort columns.
	var lead []ir.Node
	if len(rest) > 1 {
		if p, ok := rest[0].(*ir.Project); ok && !p.Drop {
			lead, rest = rest[:1], rest[1:]
		}
	}
	if top, ok := rest[0].(*ir.TopN); ok && top.Offset == 0 {
		if fields, ok := sortFields(top.Keys); ok && keepsColumns(lead, fields) {
			result.Strategy = MergeTopK
			result.TopK = int(top.Count)
			result.TopKSortFields = fields
			result.CoordNodes = append(append([]ir.Node(nil), lead...), rest[1:]...)
		}
	}
	return result, nil
}

func keepsColumns(lead []ir.Node, fields []pipeline.SortField) bool {
	for _, n := range lead {
		p := n.(*ir.Project)
		for _, f := range fields {
			if !slices.Contains(p.Names, f.Name) {
				return false
			}
		}
	}
	return true
}

// linearizeIR returns the operators from the scan up to the root, or
// ErrNotDistributable when the plan is not one chain.
func linearizeIR(root ir.Node) ([]ir.Node, error) {
	var chain []ir.Node
	for n := root; ; {
		for _, e := range ir.NodeExprs(n) {
			hasSubquery := false
			ir.WalkExpr(e, func(x ir.Expr) bool {
				if _, ok := x.(*ir.Subquery); ok {
					hasSubquery = true
				}
				return !hasSubquery
			})
			if hasSubquery {
				return nil, ErrNotDistributable
			}
		}
		chain = append(chain, n)
		in := n.Inputs()
		if len(in) == 0 {
			if _, ok := n.(*ir.Scan); !ok {
				return nil, ErrNotDistributable
			}
			break
		}
		if len(in) != 1 {
			return nil, ErrNotDistributable
		}
		n = in[0]
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

// isRowLocal reports whether an operator maps each row independently, so it
// gives the same result on every shard's rows as on all rows.
func isRowLocal(n ir.Node) bool {
	switch n.(type) {
	case *ir.Filter, *ir.Compute, *ir.Project, *ir.RenameColumns, *ir.Extract,
		*ir.Unnest, *ir.FlattenObject:
		return true
	}
	return false
}

// partialAggSpec maps an aggregate onto mergeable partial states, or returns
// nil when a group key or aggregate has no partial form.
func partialAggSpec(agg *ir.Aggregate) *pipeline.PartialAggSpec {
	if agg.Phase != ir.AggComplete || agg.Limit > 0 || agg.DropNullGroups {
		return nil
	}
	spec := &pipeline.PartialAggSpec{}
	for _, g := range agg.Groups {
		f, ok := g.Expr.(*ir.FieldRef)
		if !ok || f.Name != g.Name {
			return nil
		}
		spec.GroupBy = append(spec.GroupBy, g.Name)
	}
	for _, a := range agg.Aggs {
		call := a.Agg
		if call.Distinct || call.Filter != nil {
			return nil
		}
		fn := pipeline.PartialAggFunc{Name: call.Func.Name, Alias: a.Name}
		switch call.Func.Name {
		case "count":
			if len(call.Args) > 1 {
				return nil
			}
		case "sum", "avg", "min", "max":
			if len(call.Args) != 1 {
				return nil
			}
		default:
			return nil
		}
		if len(call.Args) == 1 {
			f, ok := call.Args[0].(*ir.FieldRef)
			if !ok {
				return nil
			}
			fn.Field = f.Name
		}
		spec.Funcs = append(spec.Funcs, fn)
	}
	return spec
}

func sortFields(keys []ir.SortKey) ([]pipeline.SortField, bool) {
	out := make([]pipeline.SortField, len(keys))
	for i, k := range keys {
		f, ok := k.Expr.(*ir.FieldRef)
		if !ok {
			return nil, false
		}
		out[i] = pipeline.SortField{Name: f.Name, Desc: k.Desc}
	}
	return out, true
}

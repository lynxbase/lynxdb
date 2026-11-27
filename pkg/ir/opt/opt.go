// Package opt rewrites canonical IR plans into cheaper equivalent plans.
//
// Every rule preserves the plan's result: rules that hand predicates to a
// scan for data skipping keep the predicates in the plan above the scan, so
// a storage engine may use any subset of them.
package opt

import (
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Options configures Optimize.
type Options struct {
	// Fold evaluates a constant expression; when nil, constant folding is
	// skipped.
	Fold func(ir.Expr) (*ir.Literal, bool)
}

// Applied records how often a rule fired.
type Applied struct {
	Rule  string
	Count int
}

type rule struct {
	name  string
	apply func(ir.Node, *Options) (ir.Node, bool)
}

var rules = [...]rule{
	{"fold-constants", foldConstants},
	{"merge-filters", mergeFilters},
	{"remove-true-filter", removeTrueFilter},
	{"prune-scan", pruneScan},
	{"time-range-to-scan", timeRangeToScan},
	{"sort-limit-to-topn", sortLimitToTopN},
	{"limit-into-scan", limitIntoScan},
}

// Optimize applies the rewrite rules until the plan stops changing, at
// most a fixed number of passes.
func Optimize(p *ir.Plan, opts Options) (*ir.Plan, []Applied) {
	var counts [len(rules)]int
	root := p.Root
	for pass := 0; pass < 8; pass++ {
		changed := false
		for i := range rules {
			apply := rules[i].apply
			root = ir.Transform(root, func(n ir.Node) ir.Node {
				out, ok := apply(n, &opts)
				if ok {
					counts[i]++
					changed = true
					return out
				}
				return n
			})
		}
		if !changed {
			break
		}
	}
	root = columnPruning(root)
	var applied []Applied
	for i, r := range rules {
		if c := counts[i]; c > 0 {
			applied = append(applied, Applied{Rule: r.name, Count: c})
		}
	}
	return &ir.Plan{Root: root}, applied
}

func foldConstants(n ir.Node, o *Options) (ir.Node, bool) {
	if o.Fold == nil {
		return n, false
	}
	changed := false
	fold := func(e ir.Expr) ir.Expr {
		if !hasFoldable(e) {
			return e
		}
		return ir.RewriteExpr(e, func(x ir.Expr) ir.Expr {
			if !foldable(x) {
				return x
			}
			lit, ok := o.Fold(x)
			if !ok {
				return x
			}
			changed = true
			return lit
		})
	}
	switch x := n.(type) {
	case *ir.Filter:
		c := fold(x.Cond)
		if changed {
			return &ir.Filter{Input: x.Input, Cond: c}, true
		}
	case *ir.Compute:
		var assigns []ir.Assign
		for i, a := range x.Assigns {
			e := fold(a.Expr)
			if e != a.Expr && assigns == nil {
				assigns = make([]ir.Assign, len(x.Assigns))
				copy(assigns, x.Assigns[:i])
			}
			if assigns != nil {
				assigns[i] = ir.Assign{Name: a.Name, Expr: e}
			}
		}
		if changed {
			return &ir.Compute{Input: x.Input, Assigns: assigns}, true
		}
	}
	return n, false
}

// hasFoldable reports whether e or one of its sub-expressions is foldable.
func hasFoldable(e ir.Expr) bool {
	if e == nil {
		return false
	}
	if foldable(e) {
		return true
	}
	var buf [4]ir.Expr
	for _, c := range ir.AppendExprChildren(buf[:0], e) {
		if hasFoldable(c) {
			return true
		}
	}
	return false
}

// foldable reports whether e is a deterministic computation over literals
// that is not already a literal.
func foldable(e ir.Expr) bool {
	switch x := e.(type) {
	case *ir.Literal, *ir.FieldRef, *ir.LambdaParam, *ir.Lambda, *ir.Subquery, *ir.AggCall, *ir.Search, *ir.RelativeTime:
		return false
	case *ir.Call:
		if !x.Func.Foldable() {
			return false
		}
	}
	var buf [4]ir.Expr
	for _, c := range ir.AppendExprChildren(buf[:0], e) {
		if _, ok := c.(*ir.Literal); !ok {
			return false
		}
	}
	return true
}

func mergeFilters(n ir.Node, _ *Options) (ir.Node, bool) {
	f, ok := n.(*ir.Filter)
	if !ok {
		return n, false
	}
	inner, ok := f.Input.(*ir.Filter)
	if !ok {
		return n, false
	}
	return &ir.Filter{Input: inner.Input, Cond: ir.And(inner.Cond, f.Cond)}, true
}

func removeTrueFilter(n ir.Node, _ *Options) (ir.Node, bool) {
	f, ok := n.(*ir.Filter)
	if !ok {
		return n, false
	}
	if lit, ok := f.Cond.(*ir.Literal); ok {
		if b, isBool := lit.V.(bool); isBool && b {
			return f.Input, true
		}
	}
	return n, false
}

// pruneScan copies the indexable conjuncts of a filter directly above a
// scan into the scan's pruning predicates.
func pruneScan(n ir.Node, _ *Options) (ir.Node, bool) {
	f, ok := n.(*ir.Filter)
	if !ok {
		return n, false
	}
	scan, ok := f.Input.(*ir.Scan)
	if !ok {
		return n, false
	}
	var add []ir.Expr
	var buf [8]ir.Expr
	known := pruneSet{list: scan.Prune}
	for _, c := range ir.AppendConjuncts(buf[:0], f.Cond) {
		if indexable(c) && !known.contains(c) {
			add = append(add, c)
		}
	}
	if len(add) == 0 {
		return n, false
	}
	cp := *scan
	cp.Prune = append(append([]ir.Expr(nil), scan.Prune...), add...)
	return &ir.Filter{Input: &cp, Cond: f.Cond}, true
}

// pruneSet answers whether a predicate is already among a scan's pruning
// predicates, comparing rendered expressions. Predicates the rule added
// earlier are the same values, so identity settles most lookups without
// rendering.
type pruneSet struct {
	list    []ir.Expr
	ptrs    map[ir.Expr]bool
	strings map[string]bool
}

func (s *pruneSet) contains(e ir.Expr) bool {
	if len(s.list) == 0 {
		return false
	}
	if len(s.list) <= 8 {
		for _, x := range s.list {
			if x == e {
				return true
			}
		}
	} else {
		if s.ptrs == nil {
			s.ptrs = make(map[ir.Expr]bool, len(s.list))
			for _, x := range s.list {
				s.ptrs[x] = true
			}
		}
		if s.ptrs[e] {
			return true
		}
	}
	if s.strings == nil {
		s.strings = make(map[string]bool, len(s.list))
		for _, x := range s.list {
			s.strings[x.String()] = true
		}
	}
	return s.strings[e.String()]
}

// indexable reports whether a predicate can be answered or narrowed by the
// term index, blooms or zone maps: raw-text searches, a column equal to a
// string literal, and disjunctions of raw-text words.
func indexable(e ir.Expr) bool {
	switch x := e.(type) {
	case *ir.Search:
		return x.Field == "" && x.Mode != ir.SearchRegex && x.Mode != ir.SearchSubstring
	case *ir.Binary:
		switch x.Op {
		case ir.OpEq:
			_, isField := x.L.(*ir.FieldRef)
			lit, isLit := x.R.(*ir.Literal)
			if isField && isLit {
				_, isStr := lit.V.(string)
				return isStr
			}
		case ir.OpOr:
			// A disjunction of raw-text words is indexable exactly when
			// both sides are words, which makes them indexable too.
			return isWord(x)
		}
	}
	return false
}

func isWord(e ir.Expr) bool {
	if b, ok := e.(*ir.Binary); ok && b.Op == ir.OpOr {
		return isWord(b.L) && isWord(b.R)
	}
	s, ok := e.(*ir.Search)
	return ok && s.Mode == ir.SearchWord && s.Field == ""
}

// timeRangeToScan narrows a scan's time range by comparisons of the time
// column with constants in a filter directly above it. The comparisons
// stay in the filter.
func timeRangeToScan(n ir.Node, _ *Options) (ir.Node, bool) {
	f, ok := n.(*ir.Filter)
	if !ok {
		return n, false
	}
	scan, ok := f.Input.(*ir.Scan)
	if !ok {
		return n, false
	}
	tr := ir.TimeRange{}
	if scan.Time != nil {
		tr = *scan.Time
	}
	changed := false
	var buf [8]ir.Expr
	for _, c := range ir.AppendConjuncts(buf[:0], f.Cond) {
		b, ok := c.(*ir.Binary)
		if !ok {
			continue
		}
		field, okf := b.L.(*ir.FieldRef)
		if !okf || field.Name != ir.TimeColumn {
			continue
		}
		bound := constantBound(b.R)
		if bound == nil || bound.Abs == nil {
			continue
		}
		switch b.Op {
		case ir.OpGe, ir.OpGt:
			if tr.Earliest == nil || (tr.Earliest.Abs != nil && bound.Abs.After(*tr.Earliest.Abs)) {
				tr.Earliest, changed = bound, true
			}
		case ir.OpLt:
			if tr.Latest == nil || (tr.Latest.Abs != nil && bound.Abs.Before(*tr.Latest.Abs)) {
				tr.Latest, changed = bound, true
			}
		case ir.OpLe:
			next := bound.Abs.Add(1)
			if tr.Latest == nil || (tr.Latest.Abs != nil && next.Before(*tr.Latest.Abs)) {
				tr.Latest, changed = ir.AbsoluteBound(next), true
			}
		}
	}
	if !changed {
		return n, false
	}
	cp := *scan
	cp.Time = &tr
	return &ir.Filter{Input: &cp, Cond: f.Cond}, true
}

func constantBound(e ir.Expr) *ir.TimeBound {
	lit, ok := e.(*ir.Literal)
	if !ok || lit.T.Kind != ir.KindTimestamp {
		return nil
	}
	if t, ok := lit.V.(interface{ IsZero() bool }); ok && t.IsZero() {
		return nil
	}
	return timestampBound(lit)
}

func sortLimitToTopN(n ir.Node, _ *Options) (ir.Node, bool) {
	l, ok := n.(*ir.Limit)
	if !ok || l.Count < 0 {
		return n, false
	}
	top, ok := BoundSort(l.Input, l.Count+l.Offset)
	if !ok {
		return n, false
	}
	if l.Offset > 0 {
		return l.WithInputs([]ir.Node{top}), true
	}
	return top, true
}

// BoundSort caps the nearest sort beneath operators that preserve both
// order and row count. Sort keys are evaluated before any projection that
// hides or replaces them.
func BoundSort(input ir.Node, count int64) (ir.Node, bool) {
	switch n := input.(type) {
	case *ir.Sort:
		if count == 0 {
			return &ir.Limit{Input: n.Input}, true
		}
		return &ir.TopN{Input: n.Input, Keys: n.Keys, Count: count}, true
	case *ir.TopN:
		if n.Offset == 0 && count < n.Count {
			if count == 0 {
				return &ir.Limit{Input: n.Input}, true
			}
			cp := *n
			cp.Count = count
			return &cp, true
		}
	case *ir.Project, *ir.Compute, *ir.RenameColumns, *ir.FillNull, *ir.Extract:
		if child, ok := BoundSort(input.Inputs()[0], count); ok {
			return input.WithInputs([]ir.Node{child}), true
		}
	}
	return input, false
}

// limitIntoScan tells a scan how many rows the operators above it read
// when only row-preserving operators separate a limit from the scan.
func limitIntoScan(n ir.Node, _ *Options) (ir.Node, bool) {
	l, ok := n.(*ir.Limit)
	if !ok || l.Count < 0 {
		return n, false
	}
	bound := l.Count + l.Offset
	var path []ir.Node
	cur := l.Input
	for {
		switch x := cur.(type) {
		case *ir.Compute, *ir.Project, *ir.RenameColumns:
			path = append(path, x)
			cur = x.Inputs()[0]
			continue
		case *ir.Scan:
			if x.Limit > 0 && x.Limit <= bound {
				return n, false
			}
			cp := *x
			cp.Limit = bound
			var rebuilt ir.Node = &cp
			for i := len(path) - 1; i >= 0; i-- {
				rebuilt = path[i].WithInputs([]ir.Node{rebuilt})
			}
			return l.WithInputs([]ir.Node{rebuilt}), true
		}
		return n, false
	}
}

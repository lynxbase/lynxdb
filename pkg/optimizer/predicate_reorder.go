package optimizer

import (
	"sort"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// predicateReorderingRule reorders AND-connected predicates in WHERE clauses
// by estimated selectivity/cost. Cheaper, more selective predicates go first
// for short-circuit evaluation.
//
// When stats is non-nil, uses cardinality-based selectivity estimation:
//   - equality: selectivity = 1/cardinality
//   - range: estimated from min/max spread
//   - !=: selectivity = 1 - 1/cardinality
type predicateReorderingRule struct {
	stats map[string]FieldStatInfo // optional: per-field stats for selectivity
}

func (r *predicateReorderingRule) Name() string { return "PredicateReordering" }
func (r *predicateReorderingRule) Description() string {
	return "Reorders AND predicates by selectivity for short-circuit evaluation"
}

func (r *predicateReorderingRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false
	for i, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		// Flatten AND-tree.
		preds := flattenAND(w.Expr)
		if len(preds) < 2 {
			continue
		}
		// Sort by estimated cost.
		sort.SliceStable(preds, func(a, b int) bool {
			return r.costOf(preds[a]) < r.costOf(preds[b])
		})
		// Rebuild AND-tree.
		newExpr := preds[0]
		for _, p := range preds[1:] {
			newExpr = &spl2.BinaryExpr{Left: newExpr, Op: "and", Right: p}
		}
		if newExpr.String() != w.Expr.String() {
			q.Commands[i] = &spl2.WhereCommand{Expr: newExpr}
			changed = true
		}
	}

	return q, changed
}

// costOf estimates the cost of evaluating a predicate.
// When field stats are available, uses selectivity-based cost.
func (r *predicateReorderingRule) costOf(expr spl2.Expr) float64 {
	if r.stats != nil {
		return r.costWithStats(expr)
	}

	return float64(predicateCost(expr))
}

// costWithStats uses field cardinality to estimate selectivity.
// EffectiveCost = baseCost * selectivity
// Lower cost → evaluated first (cheaper and more selective).
func (r *predicateReorderingRule) costWithStats(expr spl2.Expr) float64 {
	cmp, ok := expr.(*spl2.CompareExpr)
	if !ok {
		return float64(predicateCost(expr))
	}
	field, isField := cmp.Left.(*spl2.FieldExpr)
	_, isLit := cmp.Right.(*spl2.LiteralExpr)
	if !isField || !isLit {
		return float64(predicateCost(expr))
	}

	info, hasStats := r.stats[field.Name]
	if !hasStats || info.Cardinality <= 0 {
		return float64(predicateCost(expr))
	}

	baseCost := float64(predicateCost(expr))
	var selectivity float64

	switch cmp.Op {
	case "=", "==":
		selectivity = 1.0 / float64(info.Cardinality)
	case "!=":
		selectivity = 1.0 - 1.0/float64(info.Cardinality)
	case ">", ">=", "<", "<=":
		// Rough estimate: range predicates select ~33% of data.
		selectivity = 0.33
	default:
		selectivity = 0.5
	}

	return baseCost * selectivity
}

// flattenAND collects all leaves of an AND-tree into a flat slice.
func flattenAND(expr spl2.Expr) []spl2.Expr {
	b, ok := expr.(*spl2.BinaryExpr)
	if !ok || !strings.EqualFold(b.Op, "and") {
		return []spl2.Expr{expr}
	}
	left := flattenAND(b.Left)
	right := flattenAND(b.Right)

	return append(left, right...)
}

// predicateCost estimates the cost of evaluating a predicate.
// Lower cost = cheaper/more selective = should be evaluated first.
func predicateCost(expr spl2.Expr) int {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		_, isField := e.Left.(*spl2.FieldExpr)
		_, isLit := e.Right.(*spl2.LiteralExpr)
		if isField && isLit {
			// Equality on indexed field (cheapest).
			if e.Op == "=" || e.Op == "==" {
				field := e.Left.(*spl2.FieldExpr).Name
				if isIndexedField(field) {
					return 10
				}

				return 20 // equality on non-indexed field
			}
			// Range comparison.
			if e.Op == ">" || e.Op == ">=" || e.Op == "<" || e.Op == "<=" {
				return 30
			}
			// Not-equal.
			if e.Op == "!=" {
				return 40
			}
		}

		return 50 // field-to-field or complex comparison
	case *spl2.FuncCallExpr:
		name := strings.ToLower(e.Name)
		// Regex/like are expensive.
		if name == "match" || name == "like" {
			return 80
		}

		return 70 // function calls
	case *spl2.NotExpr:
		return predicateCost(e.Expr) + 5
	case *spl2.BinaryExpr:
		// OR is more expensive than a single predicate.
		if strings.EqualFold(e.Op, "or") {
			return 60
		}

		return 50
	default:
		return 50
	}
}

// isIndexedField returns true for fields that typically have bloom/inverted indexes.
func isIndexedField(name string) bool {
	switch name {
	case "_raw", "host", "source", "_source", "sourcetype", "_sourcetype", "index":
		return true
	default:
		return false
	}
}

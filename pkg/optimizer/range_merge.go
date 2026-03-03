package optimizer

import (
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// rangeMergeRule detects field >= A AND field < B and emits rangePredicates annotation.
type rangeMergeRule struct{}

func (r *rangeMergeRule) Name() string { return "RangeMerge" }
func (r *rangeMergeRule) Description() string {
	return "Detects field >= A AND field < B and emits range predicate annotations"
}

func (r *rangeMergeRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	if q.Annotations != nil {
		if _, done := q.Annotations["rangePredicates"]; done {
			return q, false
		}
	}

	var ranges []spl2.RangePredicate
	for _, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		extractRanges(w.Expr, &ranges)
	}
	if len(ranges) == 0 {
		return q, false
	}
	q.Annotate("rangePredicates", ranges)

	return q, true
}

func extractRanges(expr spl2.Expr, ranges *[]spl2.RangePredicate) {
	preds := flattenAND(expr)
	// Group predicates by field.
	fieldBounds := make(map[string]*spl2.RangePredicate)
	for _, p := range preds {
		cmp, ok := p.(*spl2.CompareExpr)
		if !ok {
			continue
		}
		field, ok := cmp.Left.(*spl2.FieldExpr)
		if !ok || field.Name == "_time" {
			continue // time handled separately
		}
		lit, ok := cmp.Right.(*spl2.LiteralExpr)
		if !ok {
			continue
		}
		rp, exists := fieldBounds[field.Name]
		if !exists {
			rp = &spl2.RangePredicate{Field: field.Name}
			fieldBounds[field.Name] = rp
		}
		switch cmp.Op {
		case ">=", ">":
			rp.Min = lit.Value
		case "<=", "<":
			rp.Max = lit.Value
		}
	}
	for _, rp := range fieldBounds {
		if rp.Min != "" && rp.Max != "" {
			*ranges = append(*ranges, *rp)
		}
	}
}

// inListRewriteRule detects f=A OR f=B OR f=C and rewrites to InExpr.
type inListRewriteRule struct{}

func (r *inListRewriteRule) Name() string { return "InListRewrite" }
func (r *inListRewriteRule) Description() string {
	return "Rewrites f=A OR f=B OR f=C into IN(A,B,C) for hash-based evaluation"
}

func (r *inListRewriteRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false
	for i, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		newExpr, rewritten := rewriteORtoIN(w.Expr)
		if rewritten {
			q.Commands[i] = &spl2.WhereCommand{Expr: newExpr}
			changed = true
		}
	}

	return q, changed
}

// rewriteORtoIN detects f=A OR f=B OR f=C patterns and rewrites to InExpr.
func rewriteORtoIN(expr spl2.Expr) (spl2.Expr, bool) {
	orLeaves := flattenOR(expr)
	if len(orLeaves) < 3 {
		return expr, false
	}

	// Check if all OR leaves are field=literal on the same field.
	var fieldName string
	var values []spl2.Expr
	for _, leaf := range orLeaves {
		cmp, ok := leaf.(*spl2.CompareExpr)
		if !ok {
			return expr, false
		}
		if cmp.Op != "=" && cmp.Op != "==" {
			return expr, false
		}
		field, ok := cmp.Left.(*spl2.FieldExpr)
		if !ok {
			return expr, false
		}
		lit, ok := cmp.Right.(*spl2.LiteralExpr)
		if !ok {
			return expr, false
		}
		if fieldName == "" {
			fieldName = field.Name
		} else if field.Name != fieldName {
			return expr, false
		}
		values = append(values, lit)
	}

	return &spl2.InExpr{
		Field:  &spl2.FieldExpr{Name: fieldName},
		Values: values,
	}, true
}

// flattenOR collects all leaves of an OR-tree into a flat slice.
func flattenOR(expr spl2.Expr) []spl2.Expr {
	b, ok := expr.(*spl2.BinaryExpr)
	if !ok || !strings.EqualFold(b.Op, "or") {
		return []spl2.Expr{expr}
	}
	left := flattenOR(b.Left)
	right := flattenOR(b.Right)

	return append(left, right...)
}

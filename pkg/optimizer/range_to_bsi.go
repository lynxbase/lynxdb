package optimizer

import (
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

const rangePredicatesAnnotation = "rangePredicates"

// SegmentSet provides the segment readers considered by a query scan.
type SegmentSet interface {
	Segments() []*segment.Reader
}

// LowerRangeToBSI marks range predicates whose field has a range BSI in every
// segment in the scan set. It leaves predicates unmarked for empty or mixed
// scan sets so the row VM remains the source of truth.
func LowerRangeToBSI(q *spl2.Query, segs SegmentSet) (*spl2.Query, bool) {
	if q == nil || segs == nil {
		return q, false
	}
	ann, ok := q.GetAnnotation(rangePredicatesAnnotation)
	if !ok {
		return q, false
	}
	preds, ok := ann.([]spl2.RangePredicate)
	if !ok || len(preds) == 0 {
		return q, false
	}

	readers := segs.Segments()
	if len(readers) == 0 {
		return q, false
	}

	changed := false
	loweredFields := make(map[string]struct{}, len(preds))
	for i := range preds {
		field := normalizeRangeField(preds[i].Field)
		lowered := everySegmentHasRangeBSI(readers, field)
		if preds[i].LoweredToBSI != lowered {
			preds[i].LoweredToBSI = lowered
			changed = true
		}
		if lowered {
			loweredFields[field] = struct{}{}
		}
	}
	if len(loweredFields) > 0 {
		if markLoweredCompareExprs(q, loweredFields) {
			changed = true
		}
	}
	if changed {
		q.Annotate(rangePredicatesAnnotation, preds)
	}

	return q, changed
}

type rangeToBSIRule struct {
	segs SegmentSet
}

func (r *rangeToBSIRule) Name() string { return "RangeToBSI" }
func (r *rangeToBSIRule) Description() string {
	return "Marks range predicates handled by range BSI across the scan set"
}
func (r *rangeToBSIRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	return LowerRangeToBSI(q, r.segs)
}

func everySegmentHasRangeBSI(readers []*segment.Reader, field string) bool {
	for _, r := range readers {
		if r == nil || !r.HasRangeBSI() || r.IndexProfile(field) != segment.IndexProfileRangeBSI {
			return false
		}
	}

	return true
}

func markLoweredCompareExprs(q *spl2.Query, fields map[string]struct{}) bool {
	changed := false
	for _, cmd := range q.Commands {
		w, ok := cmd.(*spl2.WhereCommand)
		if !ok {
			continue
		}
		if markLoweredCompareExpr(w.Expr, fields) {
			changed = true
		}
	}

	return changed
}

func markLoweredCompareExpr(expr spl2.Expr, fields map[string]struct{}) bool {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		field, ok := e.Left.(*spl2.FieldExpr)
		if !ok || !isRangeOp(e.Op) {
			return false
		}
		if _, ok := fields[normalizeRangeField(field.Name)]; !ok || e.LoweredToBSI {
			return false
		}
		e.LoweredToBSI = true
		return true
	case *spl2.BinaryExpr:
		left := markLoweredCompareExpr(e.Left, fields)
		right := markLoweredCompareExpr(e.Right, fields)
		return left || right
	case *spl2.NotExpr:
		return markLoweredCompareExpr(e.Expr, fields)
	default:
		return false
	}
}

func isRangeOp(op string) bool {
	switch op {
	case ">", ">=", "<", "<=":
		return true
	default:
		return false
	}
}

func normalizeRangeField(name string) string {
	switch name {
	case "source":
		return "_source"
	case "sourcetype":
		return "_sourcetype"
	default:
		return name
	}
}

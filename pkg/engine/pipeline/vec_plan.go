package pipeline

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// vecNode is a node in the vectorized filter execution plan tree.
// Each node evaluates a batch and produces a boolean bitmap indicating
// which rows match the predicate. The second return value indicates
// whether the vectorized path was successfully applied (false = fallback to VM).
type vecNode interface {
	evalBitmap(batch *Batch) ([]bool, bool)
}

// vecCompareNode handles simple field op literal comparisons.
// Replaces the flat vecField/vecOp/vecValue pattern from the original
// FilterIterator, now composable within AND/OR trees.
type vecCompareNode struct {
	field string
	op    string
	value string // raw literal string
}

// vecInNode handles field IN (v1, v2, ...) with hashset lookup.
type vecInNode struct {
	field    string
	negated  bool
	intSet   map[int64]struct{}   // populated if all values parse as int64
	floatSet map[float64]struct{} // populated if all values parse as float64
	strSet   map[string]struct{}  // always populated as fallback
}

// vecNullCheckNode handles isnull(field) and isnotnull(field).
type vecNullCheckNode struct {
	field    string
	wantNull bool // true = isnull, false = isnotnull
}

// vecLikeNode handles field LIKE pattern with fast-path dispatch.
type vecLikeNode struct {
	field   string
	pattern string // original pattern (for general LIKE)
	kind    string // "prefix", "suffix", "contains", "exact", "general"
	literal string // lowercased literal core
}

// vecRangeNode is a fused BETWEEN: field >= min AND field <= max in one pass.
type vecRangeNode struct {
	field  string
	minVal string // literal min
	maxVal string // literal max
	minOp  string // ">=" or ">"
	maxOp  string // "<=" or "<"
}

// vecAndNode composes two child bitmaps with element-wise AND.
type vecAndNode struct {
	left  vecNode
	right vecNode
}

// vecOrNode composes two child bitmaps with element-wise OR.
type vecOrNode struct {
	left  vecNode
	right vecNode
}

// vecNotNode inverts a child bitmap.
type vecNotNode struct {
	child vecNode
}

// analyzeVecExpr recursively walks an expression AST and builds a vecNode tree.
// Returns nil if any subtree is not vectorizable (all-or-nothing fallback to VM).
// Called once at FilterIterator construction time — not on the hot path.
func analyzeVecExpr(expr spl2.Expr) vecNode {
	switch e := expr.(type) {
	case *spl2.CompareExpr:
		return analyzeCompare(e)

	case *spl2.BinaryExpr:
		return analyzeBinary(e)

	case *spl2.NotExpr:
		child := analyzeVecExpr(e.Expr)
		if child == nil {
			return nil
		}
		return &vecNotNode{child: child}

	case *spl2.InExpr:
		return analyzeIn(e)

	case *spl2.FuncCallExpr:
		return analyzeFunc(e)

	default:
		return nil // expression type not vectorizable
	}
}

// analyzeCompare handles CompareExpr: field op literal.
func analyzeCompare(e *spl2.CompareExpr) vecNode {
	field, ok := e.Left.(*spl2.FieldExpr)
	if !ok {
		return nil
	}
	lit, ok := e.Right.(*spl2.LiteralExpr)
	if !ok {
		return nil
	}

	// Reject glob wildcards — these should use regex matching.
	if strings.ContainsAny(lit.Value, "*?") {
		return nil
	}

	// LIKE operator gets its own node with fast-path classification.
	if e.Op == "like" {
		return analyzeLike(field.Name, lit.Value)
	}

	return &vecCompareNode{
		field: field.Name,
		op:    e.Op,
		value: lit.Value,
	}
}

// analyzeLike creates a vecLikeNode with pattern classification.
func analyzeLike(fieldName, pattern string) vecNode {
	kind, literal := classifyLikePatternForVec(pattern)
	return &vecLikeNode{
		field:   fieldName,
		pattern: pattern,
		kind:    kind,
		literal: literal,
	}
}

// classifyLikePatternForVec classifies a LIKE pattern for vectorized dispatch.
// Same logic as vm.ClassifyLikePattern but kept local to avoid import cycle issues.
func classifyLikePatternForVec(pattern string) (kind, literal string) {
	lower := strings.ToLower(pattern)

	if !strings.ContainsAny(lower, "%_") {
		return "exact", lower
	}
	if lower == "%" {
		return "general", ""
	}

	hasLeading := lower[0] == '%'
	hasTrailing := lower[len(lower)-1] == '%'

	if hasLeading && hasTrailing && len(lower) >= 3 {
		inner := lower[1 : len(lower)-1]
		if !strings.ContainsAny(inner, "%_") {
			return "contains", inner
		}
	}
	if hasTrailing && !hasLeading {
		prefix := lower[:len(lower)-1]
		if !strings.ContainsAny(prefix, "%_") {
			return "prefix", prefix
		}
	}
	if hasLeading && !hasTrailing {
		suffix := lower[1:]
		if !strings.ContainsAny(suffix, "%_") {
			return "suffix", suffix
		}
	}

	return "general", lower
}

// analyzeBinary handles AND/OR with optional BETWEEN fusion.
func analyzeBinary(e *spl2.BinaryExpr) vecNode {
	if strings.EqualFold(e.Op, "and") {
		// Try BETWEEN fusion: field >= A AND field <= B → vecRangeNode.
		if rn := tryFuseRange(e.Left, e.Right); rn != nil {
			return rn
		}

		left := analyzeVecExpr(e.Left)
		right := analyzeVecExpr(e.Right)
		if left == nil || right == nil {
			return nil
		}
		return &vecAndNode{left: left, right: right}
	}

	if strings.EqualFold(e.Op, "or") {
		left := analyzeVecExpr(e.Left)
		right := analyzeVecExpr(e.Right)
		if left == nil || right == nil {
			return nil
		}
		return &vecOrNode{left: left, right: right}
	}

	return nil
}

// analyzeIn handles IN expressions with hashset construction.
func analyzeIn(e *spl2.InExpr) vecNode {
	field, ok := e.Field.(*spl2.FieldExpr)
	if !ok {
		return nil
	}

	node := &vecInNode{
		field:   field.Name,
		negated: e.Negated,
		strSet:  make(map[string]struct{}, len(e.Values)),
	}

	allInt := true
	allFloat := true
	intVals := make([]int64, 0, len(e.Values))
	floatVals := make([]float64, 0, len(e.Values))

	for _, v := range e.Values {
		lit, ok := v.(*spl2.LiteralExpr)
		if !ok {
			return nil // non-literal in IN list → not vectorizable
		}
		node.strSet[lit.Value] = struct{}{}

		if allInt {
			n, err := strconv.ParseInt(lit.Value, 10, 64)
			if err != nil {
				allInt = false
			} else {
				intVals = append(intVals, n)
			}
		}
		if allFloat {
			f, err := strconv.ParseFloat(lit.Value, 64)
			if err != nil {
				allFloat = false
			} else {
				floatVals = append(floatVals, f)
			}
		}
	}

	if allInt && len(intVals) > 0 {
		node.intSet = make(map[int64]struct{}, len(intVals))
		for _, v := range intVals {
			node.intSet[v] = struct{}{}
		}
	}
	if allFloat && len(floatVals) > 0 {
		node.floatSet = make(map[float64]struct{}, len(floatVals))
		for _, v := range floatVals {
			node.floatSet[v] = struct{}{}
		}
	}

	return node
}

// analyzeFunc handles function calls: isnull(field), isnotnull(field).
func analyzeFunc(e *spl2.FuncCallExpr) vecNode {
	name := strings.ToLower(e.Name)
	switch name {
	case "isnull":
		if len(e.Args) != 1 {
			return nil
		}
		field, ok := e.Args[0].(*spl2.FieldExpr)
		if !ok {
			return nil
		}
		return &vecNullCheckNode{field: field.Name, wantNull: true}

	case "isnotnull":
		if len(e.Args) != 1 {
			return nil
		}
		field, ok := e.Args[0].(*spl2.FieldExpr)
		if !ok {
			return nil
		}
		return &vecNullCheckNode{field: field.Name, wantNull: false}
	}

	return nil // unknown function → not vectorizable
}

// tryFuseRange detects patterns like field >= A AND field <= B and fuses
// them into a single vecRangeNode that evaluates the range check in one pass.
func tryFuseRange(left, right spl2.Expr) *vecRangeNode {
	lCmp, lOk := left.(*spl2.CompareExpr)
	rCmp, rOk := right.(*spl2.CompareExpr)
	if !lOk || !rOk {
		return nil
	}

	lField, lOk := lCmp.Left.(*spl2.FieldExpr)
	rField, rOk := rCmp.Left.(*spl2.FieldExpr)
	if !lOk || !rOk || lField.Name != rField.Name {
		return nil
	}

	lLit, lOk := lCmp.Right.(*spl2.LiteralExpr)
	rLit, rOk := rCmp.Right.(*spl2.LiteralExpr)
	if !lOk || !rOk {
		return nil
	}

	// Detect lower-bound op on left, upper-bound op on right.
	var minOp, maxOp, minVal, maxVal string

	switch lCmp.Op {
	case ">=", ">":
		minOp = lCmp.Op
		minVal = lLit.Value
	case "<=", "<":
		maxOp = lCmp.Op
		maxVal = lLit.Value
	default:
		return nil
	}

	switch rCmp.Op {
	case ">=", ">":
		if minOp != "" {
			return nil // both are lower-bound → not a range
		}
		minOp = rCmp.Op
		minVal = rLit.Value
	case "<=", "<":
		if maxOp != "" {
			return nil // both are upper-bound → not a range
		}
		maxOp = rCmp.Op
		maxVal = rLit.Value
	default:
		return nil
	}

	if minOp == "" || maxOp == "" {
		return nil
	}

	return &vecRangeNode{
		field:  lField.Name,
		minVal: minVal,
		maxVal: maxVal,
		minOp:  minOp,
		maxOp:  maxOp,
	}
}

// detectColumnType returns the FieldType of the first non-null value in a column.
func detectColumnType(col []event.Value) event.FieldType {
	for _, v := range col {
		if !v.IsNull() {
			return v.Type()
		}
	}
	return event.FieldTypeNull
}

// extractInt64Column extracts int64 values from a column, returning the typed
// array and a null mask. Null positions get zero values in the typed array
// and true in the null mask.
func extractInt64Column(col []event.Value) ([]int64, []bool) {
	n := len(col)
	out := make([]int64, n)
	nulls := make([]bool, n)
	for i, v := range col {
		if v.IsNull() {
			nulls[i] = true
		} else {
			out[i] = v.AsInt()
		}
	}
	return out, nulls
}

// extractFloat64Column extracts float64 values from a column.
func extractFloat64Column(col []event.Value) ([]float64, []bool) {
	n := len(col)
	out := make([]float64, n)
	nulls := make([]bool, n)
	for i, v := range col {
		if v.IsNull() {
			nulls[i] = true
		} else {
			out[i] = v.AsFloat()
		}
	}
	return out, nulls
}

// extractStringColumn extracts string values from a column.
func extractStringColumn(col []event.Value) ([]string, []bool) {
	n := len(col)
	out := make([]string, n)
	nulls := make([]bool, n)
	for i, v := range col {
		if v.IsNull() {
			nulls[i] = true
		} else {
			out[i] = v.AsString()
		}
	}
	return out, nulls
}

// applyNullMask zeros out bitmap positions where nullMask is true.
// Implements SQL NULL semantics: NULL compared to anything = false.
func applyNullMask(bitmap, nullMask []bool) {
	for i, isNull := range nullMask {
		if isNull && i < len(bitmap) {
			bitmap[i] = false
		}
	}
}

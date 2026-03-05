package pipeline

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

func (n *vecCompareNode) evalBitmap(batch *Batch) ([]bool, bool) {
	col, ok := batch.Columns[n.field]
	if !ok || len(col) == 0 {
		// Missing column: all values are null → all comparisons return false.
		return make([]bool, batch.Len), true
	}

	colType := detectColumnType(col)

	switch colType {
	case event.FieldTypeInt:
		threshold, err := strconv.ParseInt(n.value, 10, 64)
		if err != nil {
			return nil, false
		}
		intCol, nulls := extractInt64Column(col)
		bitmap := filterInt64ByOp(intCol, n.op, threshold)
		if bitmap == nil {
			return nil, false
		}
		applyNullMask(bitmap, nulls)
		return bitmap, true

	case event.FieldTypeFloat:
		threshold, err := strconv.ParseFloat(n.value, 64)
		if err != nil {
			return nil, false
		}
		fCol, nulls := extractFloat64Column(col)
		bitmap := filterFloat64ByOp(fCol, n.op, threshold)
		if bitmap == nil {
			return nil, false
		}
		applyNullMask(bitmap, nulls)
		return bitmap, true

	case event.FieldTypeString:
		sCol, nulls := extractStringColumn(col)
		bitmap := filterStringByOp(sCol, n.op, n.value)
		if bitmap == nil {
			return nil, false
		}
		applyNullMask(bitmap, nulls)
		return bitmap, true

	default:
		return nil, false
	}
}

// filterInt64ByOp dispatches to the appropriate typed filter function.
func filterInt64ByOp(col []int64, op string, val int64) []bool {
	switch op {
	case ">":
		return FilterInt64GT(col, val)
	case ">=":
		return FilterInt64GTE(col, val)
	case "<":
		return FilterInt64LT(col, val)
	case "<=":
		return FilterInt64LTE(col, val)
	case "=", "==":
		return FilterInt64EQ(col, val)
	case "!=":
		return FilterInt64NE(col, val)
	default:
		return nil
	}
}

// filterFloat64ByOp dispatches to the appropriate typed filter function.
func filterFloat64ByOp(col []float64, op string, val float64) []bool {
	switch op {
	case ">":
		return FilterFloat64GT(col, val)
	case ">=":
		return FilterFloat64GTE(col, val)
	case "<":
		return FilterFloat64LT(col, val)
	case "<=":
		return FilterFloat64LTE(col, val)
	case "=", "==":
		return FilterFloat64EQ(col, val)
	case "!=":
		return FilterFloat64NE(col, val)
	default:
		return nil
	}
}

// filterStringByOp dispatches to the appropriate typed filter function.
func filterStringByOp(col []string, op string, val string) []bool {
	switch op {
	case "=", "==":
		return FilterStringEQ(col, val)
	case "!=":
		return FilterStringNE(col, val)
	case ">":
		return FilterStringGT(col, val)
	case ">=":
		return FilterStringGTE(col, val)
	case "<":
		return FilterStringLT(col, val)
	case "<=":
		return FilterStringLTE(col, val)
	default:
		return nil
	}
}

func (n *vecAndNode) evalBitmap(batch *Batch) ([]bool, bool) {
	left, ok := n.left.evalBitmap(batch)
	if !ok {
		return nil, false
	}
	right, ok := n.right.evalBitmap(batch)
	if !ok {
		return nil, false
	}
	return AndBitmaps(left, right), true
}

func (n *vecOrNode) evalBitmap(batch *Batch) ([]bool, bool) {
	left, ok := n.left.evalBitmap(batch)
	if !ok {
		return nil, false
	}
	right, ok := n.right.evalBitmap(batch)
	if !ok {
		return nil, false
	}
	return OrBitmaps(left, right), true
}

func (n *vecNotNode) evalBitmap(batch *Batch) ([]bool, bool) {
	child, ok := n.child.evalBitmap(batch)
	if !ok {
		return nil, false
	}
	return NotBitmap(child), true
}

func (n *vecInNode) evalBitmap(batch *Batch) ([]bool, bool) {
	col, ok := batch.Columns[n.field]
	if !ok || len(col) == 0 {
		// Missing column: all null → IN returns false, NOT IN returns true.
		bitmap := make([]bool, batch.Len)
		if n.negated {
			for i := range bitmap {
				bitmap[i] = true
			}
		}
		return bitmap, true
	}

	colType := detectColumnType(col)

	var bitmap []bool
	switch colType {
	case event.FieldTypeInt:
		if n.intSet != nil {
			intCol, nulls := extractInt64Column(col)
			bitmap = FilterInt64InSet(intCol, n.intSet)
			applyNullMask(bitmap, nulls)
		} else {
			return nil, false
		}
	case event.FieldTypeFloat:
		if n.floatSet != nil {
			fCol, nulls := extractFloat64Column(col)
			bitmap = FilterFloat64InSet(fCol, n.floatSet)
			applyNullMask(bitmap, nulls)
		} else {
			return nil, false
		}
	case event.FieldTypeString:
		sCol, nulls := extractStringColumn(col)
		bitmap = FilterStringInSet(sCol, n.strSet)
		applyNullMask(bitmap, nulls)
	default:
		return nil, false
	}

	if n.negated {
		bitmap = NotBitmap(bitmap)
	}

	return bitmap, true
}

func (n *vecNullCheckNode) evalBitmap(batch *Batch) ([]bool, bool) {
	col, ok := batch.Columns[n.field]
	bitmap := make([]bool, batch.Len)

	if !ok {
		// Column doesn't exist → all values are null.
		if n.wantNull {
			for i := range bitmap {
				bitmap[i] = true
			}
		}
		return bitmap, true
	}

	for i := 0; i < batch.Len && i < len(col); i++ {
		isNull := col[i].IsNull()
		if n.wantNull {
			bitmap[i] = isNull
		} else {
			bitmap[i] = !isNull
		}
	}

	// If column is shorter than batch, remaining positions are null.
	if n.wantNull {
		for i := len(col); i < batch.Len; i++ {
			bitmap[i] = true
		}
	}

	return bitmap, true
}

func (n *vecLikeNode) evalBitmap(batch *Batch) ([]bool, bool) {
	col, ok := batch.Columns[n.field]
	if !ok || len(col) == 0 {
		return make([]bool, batch.Len), true
	}

	sCol, nulls := extractStringColumn(col)
	bitmap := filterStringLike(sCol, n.kind, n.literal, n.pattern)
	applyNullMask(bitmap, nulls)
	return bitmap, true
}

// filterStringLike dispatches LIKE evaluation based on pattern classification.
// For common patterns (prefix%, %suffix, %contains%), it uses fast string
// functions instead of the general LIKE matcher.
func filterStringLike(col []string, kind, literal, pattern string) []bool {
	bitmap := make([]bool, len(col))

	switch kind {
	case "prefix":
		for i, v := range col {
			bitmap[i] = strings.HasPrefix(strings.ToLower(v), literal)
		}
	case "suffix":
		for i, v := range col {
			bitmap[i] = strings.HasSuffix(strings.ToLower(v), literal)
		}
	case "contains":
		for i, v := range col {
			bitmap[i] = strings.Contains(strings.ToLower(v), literal)
		}
	case "exact":
		for i, v := range col {
			bitmap[i] = strings.ToLower(v) == literal
		}
	default:
		// General LIKE — use the VM's matchLike implementation.
		for i, v := range col {
			bitmap[i] = vm.MatchLike(v, pattern)
		}
	}

	return bitmap
}

func (n *vecRangeNode) evalBitmap(batch *Batch) ([]bool, bool) {
	col, ok := batch.Columns[n.field]
	if !ok || len(col) == 0 {
		return make([]bool, batch.Len), true
	}

	colType := detectColumnType(col)

	switch colType {
	case event.FieldTypeInt:
		minVal, err := strconv.ParseInt(n.minVal, 10, 64)
		if err != nil {
			return nil, false
		}
		maxVal, err := strconv.ParseInt(n.maxVal, 10, 64)
		if err != nil {
			return nil, false
		}
		intCol, nulls := extractInt64Column(col)
		bitmap := FilterInt64Range(intCol, minVal, maxVal, n.minOp == ">", n.maxOp == "<")
		applyNullMask(bitmap, nulls)
		return bitmap, true

	case event.FieldTypeFloat:
		minVal, err := strconv.ParseFloat(n.minVal, 64)
		if err != nil {
			return nil, false
		}
		maxVal, err := strconv.ParseFloat(n.maxVal, 64)
		if err != nil {
			return nil, false
		}
		fCol, nulls := extractFloat64Column(col)
		bitmap := FilterFloat64Range(fCol, minVal, maxVal, n.minOp == ">", n.maxOp == "<")
		applyNullMask(bitmap, nulls)
		return bitmap, true

	case event.FieldTypeString:
		sCol, nulls := extractStringColumn(col)
		bitmap := filterStringRange(sCol, n.minVal, n.maxVal, n.minOp == ">", n.maxOp == "<")
		applyNullMask(bitmap, nulls)
		return bitmap, true

	default:
		return nil, false
	}
}

// filterStringRange applies a range filter on string columns (lexicographic).
func filterStringRange(col []string, minVal, maxVal string, minExclusive, maxExclusive bool) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		var aboveMin, belowMax bool
		if minExclusive {
			aboveMin = v > minVal
		} else {
			aboveMin = v >= minVal
		}
		if maxExclusive {
			belowMax = v < maxVal
		} else {
			belowMax = v <= maxVal
		}
		bitmap[i] = aboveMin && belowMax
	}
	return bitmap
}

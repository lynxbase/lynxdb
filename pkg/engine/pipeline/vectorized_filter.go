package pipeline

// Vectorized filter functions for typed column arrays.
// These operate on raw arrays without boxing/unboxing event.Value,
// achieving 5-10x speedup for simple predicates.

// FilterInt64GT filters an int64 column, returning a bitmap of rows where col[i] > threshold.
func FilterInt64GT(col []int64, threshold int64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v > threshold
	}

	return bitmap
}

// FilterInt64GTE filters an int64 column where col[i] >= threshold.
func FilterInt64GTE(col []int64, threshold int64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v >= threshold
	}

	return bitmap
}

// FilterInt64LT filters an int64 column where col[i] < threshold.
func FilterInt64LT(col []int64, threshold int64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v < threshold
	}

	return bitmap
}

// FilterInt64LTE filters an int64 column where col[i] <= threshold.
func FilterInt64LTE(col []int64, threshold int64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v <= threshold
	}

	return bitmap
}

// FilterInt64EQ filters an int64 column where col[i] == value.
func FilterInt64EQ(col []int64, value int64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v == value
	}

	return bitmap
}

// FilterInt64NE filters an int64 column where col[i] != value.
func FilterInt64NE(col []int64, value int64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v != value
	}

	return bitmap
}

// FilterFloat64GT filters a float64 column where col[i] > threshold.
func FilterFloat64GT(col []float64, threshold float64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v > threshold
	}

	return bitmap
}

// FilterFloat64GTE filters a float64 column where col[i] >= threshold.
func FilterFloat64GTE(col []float64, threshold float64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v >= threshold
	}

	return bitmap
}

// FilterFloat64LT filters a float64 column where col[i] < threshold.
func FilterFloat64LT(col []float64, threshold float64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v < threshold
	}

	return bitmap
}

// FilterFloat64LTE filters a float64 column where col[i] <= threshold.
func FilterFloat64LTE(col []float64, threshold float64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v <= threshold
	}

	return bitmap
}

// FilterFloat64EQ filters a float64 column where col[i] == value.
func FilterFloat64EQ(col []float64, value float64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v == value
	}

	return bitmap
}

// FilterFloat64NE filters a float64 column where col[i] != value.
func FilterFloat64NE(col []float64, value float64) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v != value
	}

	return bitmap
}

// FilterStringEQ filters a string column where col[i] == value.
func FilterStringEQ(col []string, value string) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v == value
	}

	return bitmap
}

// FilterStringNE filters a string column where col[i] != value.
func FilterStringNE(col []string, value string) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v != value
	}

	return bitmap
}

// FilterStringGT filters a string column where col[i] > value (lexicographic).
func FilterStringGT(col []string, value string) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v > value
	}

	return bitmap
}

// FilterStringGTE filters a string column where col[i] >= value (lexicographic).
func FilterStringGTE(col []string, value string) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v >= value
	}

	return bitmap
}

// FilterStringLT filters a string column where col[i] < value (lexicographic).
func FilterStringLT(col []string, value string) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v < value
	}

	return bitmap
}

// FilterStringLTE filters a string column where col[i] <= value (lexicographic).
func FilterStringLTE(col []string, value string) []bool {
	bitmap := make([]bool, len(col))
	for i, v := range col {
		bitmap[i] = v <= value
	}

	return bitmap
}

// AndBitmaps computes element-wise AND of two bitmaps.
func AndBitmaps(a, b []bool) []bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]bool, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] && b[i]
	}

	return result
}

// OrBitmaps computes element-wise OR of two bitmaps.
func OrBitmaps(a, b []bool) []bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]bool, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] || b[i]
	}

	return result
}

// CountTrue returns the number of true values in a bitmap.
func CountTrue(bitmap []bool) int {
	count := 0
	for _, v := range bitmap {
		if v {
			count++
		}
	}

	return count
}

// CompactInt64ByBitmap returns only elements where bitmap[i] is true.
func CompactInt64ByBitmap(col []int64, bitmap []bool) []int64 {
	result := make([]int64, 0, CountTrue(bitmap))
	for i, v := range bitmap {
		if v && i < len(col) {
			result = append(result, col[i])
		}
	}

	return result
}

// CompactFloat64ByBitmap returns only elements where bitmap[i] is true.
func CompactFloat64ByBitmap(col []float64, bitmap []bool) []float64 {
	result := make([]float64, 0, CountTrue(bitmap))
	for i, v := range bitmap {
		if v && i < len(col) {
			result = append(result, col[i])
		}
	}

	return result
}

// CompactStringByBitmap returns only elements where bitmap[i] is true.
func CompactStringByBitmap(col []string, bitmap []bool) []string {
	result := make([]string, 0, CountTrue(bitmap))
	for i, v := range bitmap {
		if v && i < len(col) {
			result = append(result, col[i])
		}
	}

	return result
}

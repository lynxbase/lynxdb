package pipeline

// Vectorized evaluation functions for batch-level arithmetic.

// AddInt64Columns adds two int64 columns element-wise.
func AddInt64Columns(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]int64, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] + b[i]
	}

	return result
}

// MulInt64Column multiplies an int64 column by a scalar.
func MulInt64Column(col []int64, scalar int64) []int64 {
	result := make([]int64, len(col))
	for i, v := range col {
		result[i] = v * scalar
	}

	return result
}

// AddFloat64Columns adds two float64 columns element-wise.
func AddFloat64Columns(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] + b[i]
	}

	return result
}

// MulFloat64Column multiplies a float64 column by a scalar.
func MulFloat64Column(col []float64, scalar float64) []float64 {
	result := make([]float64, len(col))
	for i, v := range col {
		result[i] = v * scalar
	}

	return result
}

// DivFloat64Column divides a float64 column by a scalar.
func DivFloat64Column(col []float64, scalar float64) []float64 {
	result := make([]float64, len(col))
	if scalar == 0 {
		return result // all zeros
	}
	for i, v := range col {
		result[i] = v / scalar
	}

	return result
}

// SubInt64Columns subtracts b from a element-wise.
func SubInt64Columns(a, b []int64) []int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]int64, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] - b[i]
	}

	return result
}

// SubFloat64Columns subtracts b from a element-wise.
func SubFloat64Columns(a, b []float64) []float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	result := make([]float64, n)
	for i := 0; i < n; i++ {
		result[i] = a[i] - b[i]
	}

	return result
}

// Int64ToFloat64 converts an int64 column to float64.
func Int64ToFloat64(col []int64) []float64 {
	result := make([]float64, len(col))
	for i, v := range col {
		result[i] = float64(v)
	}

	return result
}

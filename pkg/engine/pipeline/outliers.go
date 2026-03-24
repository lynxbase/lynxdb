package pipeline

import (
	"context"
	"math"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// OutliersIterator is a blocking operator that identifies outlier rows using
// statistical methods (IQR, Z-score, or MAD). It accumulates all rows from
// its child, computes statistics, then emits all rows with added _outlier
// (bool) and _score (float64) columns.
type OutliersIterator struct {
	child     Iterator
	field     string
	method    string
	threshold float64

	// Accumulation phase.
	done bool

	// Emission phase.
	output *Batch
	offset int
}

// NewOutliersIterator creates a new outliers iterator.
func NewOutliersIterator(child Iterator, field, method string, threshold float64) *OutliersIterator {
	return &OutliersIterator{
		child:     child,
		field:     field,
		method:    method,
		threshold: threshold,
	}
}

func (o *OutliersIterator) Init(ctx context.Context) error {
	return o.child.Init(ctx)
}

func (o *OutliersIterator) Next(ctx context.Context) (*Batch, error) {
	// Phase 1: Accumulate all rows from the child.
	if !o.done {
		o.done = true

		var allRows []map[string]event.Value
		for {
			batch, err := o.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			if batch == nil {
				break
			}
			for i := 0; i < batch.Len; i++ {
				allRows = append(allRows, batch.Row(i))
			}
		}

		// Phase 2: Compute outlier scores and build output batch.
		o.output = o.computeOutliers(allRows)
	}

	// Phase 3: Emit in batches (the full output is already computed).
	if o.output == nil || o.offset >= o.output.Len {
		return nil, nil
	}

	end := o.offset + DefaultBatchSize
	if end > o.output.Len {
		end = o.output.Len
	}

	result := o.output.Slice(o.offset, end)
	o.offset = end

	return result, nil
}

func (o *OutliersIterator) Close() error {
	return o.child.Close()
}

func (o *OutliersIterator) Schema() []FieldInfo {
	schema := o.child.Schema()
	// Add _outlier and _score columns.
	return append(schema,
		FieldInfo{Name: "_outlier", Type: "bool"},
		FieldInfo{Name: "_score", Type: "float"},
	)
}

// computeOutliers applies the selected outlier detection method and returns
// a batch enriched with _outlier and _score columns.
func (o *OutliersIterator) computeOutliers(rows []map[string]event.Value) *Batch {
	if len(rows) == 0 {
		return NewBatch(0)
	}

	// Extract numeric values from the field.
	values := make([]float64, len(rows))
	for i, row := range rows {
		if v, ok := row[o.field]; ok {
			if f, ok := vm.ValueToFloat(v); ok {
				values[i] = f
			} else {
				values[i] = math.NaN()
			}
		} else {
			values[i] = math.NaN()
		}
	}

	// Compute scores based on method.
	var scores []float64
	var isOutlier []bool

	switch o.method {
	case "iqr":
		scores, isOutlier = o.computeIQR(values)
	case "zscore":
		scores, isOutlier = o.computeZScore(values)
	case "mad":
		scores, isOutlier = o.computeMAD(values)
	default:
		scores, isOutlier = o.computeIQR(values)
	}

	// Build output batch.
	b := NewBatch(len(rows))
	for i, row := range rows {
		row["_outlier"] = event.BoolValue(isOutlier[i])
		row["_score"] = event.FloatValue(scores[i])
		b.AddRow(row)
	}

	return b
}

// computeIQR computes outlier scores using the Interquartile Range method.
func (o *OutliersIterator) computeIQR(values []float64) ([]float64, []bool) {
	valid := filterNaN(values)
	scores := make([]float64, len(values))
	isOutlier := make([]bool, len(values))

	if len(valid) < 4 {
		return scores, isOutlier
	}

	sort.Float64s(valid)
	q1 := percentileFloat64(valid, 25)
	q3 := percentileFloat64(valid, 75)
	iqr := q3 - q1

	if iqr == 0 {
		return scores, isOutlier
	}

	lower := q1 - o.threshold*iqr
	upper := q3 + o.threshold*iqr

	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if v < lower {
			scores[i] = (lower - v) / iqr
			isOutlier[i] = true
		} else if v > upper {
			scores[i] = (v - upper) / iqr
			isOutlier[i] = true
		}
	}

	return scores, isOutlier
}

// computeZScore computes outlier scores using Z-score method.
func (o *OutliersIterator) computeZScore(values []float64) ([]float64, []bool) {
	scores := make([]float64, len(values))
	isOutlier := make([]bool, len(values))

	// Welford's algorithm for mean and stdev.
	var count int
	var mean, m2 float64
	for _, v := range values {
		if math.IsNaN(v) {
			continue
		}
		count++
		delta := v - mean
		mean += delta / float64(count)
		delta2 := v - mean
		m2 += delta * delta2
	}

	if count < 2 {
		return scores, isOutlier
	}

	stdev := math.Sqrt(m2 / float64(count-1))
	if stdev == 0 {
		return scores, isOutlier
	}

	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		z := math.Abs((v - mean) / stdev)
		scores[i] = z
		isOutlier[i] = z > o.threshold
	}

	return scores, isOutlier
}

// computeMAD computes outlier scores using Median Absolute Deviation.
func (o *OutliersIterator) computeMAD(values []float64) ([]float64, []bool) {
	valid := filterNaN(values)
	scores := make([]float64, len(values))
	isOutlier := make([]bool, len(values))

	if len(valid) < 3 {
		return scores, isOutlier
	}

	sorted := make([]float64, len(valid))
	copy(sorted, valid)
	sort.Float64s(sorted)
	median := percentileFloat64(sorted, 50)

	deviations := make([]float64, len(valid))
	for i, v := range valid {
		deviations[i] = math.Abs(v - median)
	}
	sort.Float64s(deviations)
	mad := percentileFloat64(deviations, 50)

	if mad == 0 {
		return scores, isOutlier
	}

	const consistencyConstant = 0.6745
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		score := consistencyConstant * math.Abs(v-median) / mad
		scores[i] = score
		isOutlier[i] = score > o.threshold
	}

	return scores, isOutlier
}

// filterNaN returns only non-NaN values from the input.
func filterNaN(values []float64) []float64 {
	valid := make([]float64, 0, len(values))
	for _, v := range values {
		if !math.IsNaN(v) {
			valid = append(valid, v)
		}
	}

	return valid
}

// percentileFloat64 computes the p-th percentile of a sorted slice.
func percentileFloat64(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}

	rank := p / 100 * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))

	if lower == upper {
		return sorted[lower]
	}

	frac := rank - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

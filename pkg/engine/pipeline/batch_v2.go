package pipeline

import (
	"github.com/lynxbase/lynxdb/pkg/event"
)

// TypedBatch holds typed column arrays for vectorized execution.
type TypedBatch struct {
	Int64Cols   map[string][]int64
	Float64Cols map[string][]float64
	StringCols  map[string][]string
	BoolCols    map[string][]bool
	Len         int
}

// NewTypedBatch creates an empty TypedBatch.
func NewTypedBatch(capacity int) *TypedBatch {
	return &TypedBatch{
		Int64Cols:   make(map[string][]int64),
		Float64Cols: make(map[string][]float64),
		StringCols:  make(map[string][]string),
		BoolCols:    make(map[string][]bool),
		Len:         0,
	}
}

// FromBatch converts a legacy Batch to a TypedBatch.
// Infers types from the first non-null value in each column.
func FromBatch(b *Batch) *TypedBatch {
	tb := NewTypedBatch(b.Len)
	tb.Len = b.Len

	for name, col := range b.Columns {
		if len(col) == 0 {
			continue
		}
		// Detect type from first non-null value.
		var colType event.FieldType
		for _, v := range col {
			if !v.IsNull() {
				colType = v.Type()

				break
			}
		}
		// Use error-returning AsXE() variants instead of panic-prone AsX() to
		// handle mixed-type columns gracefully (e.g., string "N/A" in an int column).
		switch colType {
		case event.FieldTypeInt:
			ints := make([]int64, len(col))
			for i, v := range col {
				if !v.IsNull() {
					if n, err := v.AsIntE(); err == nil {
						ints[i] = n
					}
				}
			}
			tb.Int64Cols[name] = ints
		case event.FieldTypeFloat:
			floats := make([]float64, len(col))
			for i, v := range col {
				if !v.IsNull() {
					if f, err := v.AsFloatE(); err == nil {
						floats[i] = f
					}
				}
			}
			tb.Float64Cols[name] = floats
		case event.FieldTypeBool:
			bools := make([]bool, len(col))
			for i, v := range col {
				if !v.IsNull() {
					if b, err := v.AsBoolE(); err == nil {
						bools[i] = b
					}
				}
			}
			tb.BoolCols[name] = bools
		case event.FieldTypeString:
			strs := make([]string, len(col))
			for i, v := range col {
				if !v.IsNull() {
					if s, err := v.AsStringE(); err == nil {
						strs[i] = s
					}
				}
			}
			tb.StringCols[name] = strs
		default:
			// Skip non-primitive types (timestamp, etc.) — not vectorizable.
			continue
		}
	}

	return tb
}

// ToBatch converts a TypedBatch back to a legacy Batch.
func (tb *TypedBatch) ToBatch() *Batch {
	b := NewBatch(tb.Len)
	for name, col := range tb.Int64Cols {
		vals := make([]event.Value, len(col))
		for i, v := range col {
			vals[i] = event.IntValue(v)
		}
		b.Columns[name] = vals
	}
	for name, col := range tb.Float64Cols {
		vals := make([]event.Value, len(col))
		for i, v := range col {
			vals[i] = event.FloatValue(v)
		}
		b.Columns[name] = vals
	}
	for name, col := range tb.StringCols {
		vals := make([]event.Value, len(col))
		for i, v := range col {
			vals[i] = event.StringValue(v)
		}
		b.Columns[name] = vals
	}
	for name, col := range tb.BoolCols {
		vals := make([]event.Value, len(col))
		for i, v := range col {
			vals[i] = event.BoolValue(v)
		}
		b.Columns[name] = vals
	}
	b.Len = tb.Len

	return b
}

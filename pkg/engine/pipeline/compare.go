package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// CompareIterator is a blocking operator that re-executes the child pipeline
// with a time shift and merges current + previous results side by side.
// Output columns: group columns + each numeric column X as X + previous_X + change_X (%).
type CompareIterator struct {
	child     Iterator
	shift     time.Duration
	reExec    func(ctx context.Context) (Iterator, error)
	batchSize int

	// Accumulation phases.
	currentDone  bool
	previousDone bool
	currentRows  []map[string]event.Value
	previousRows []map[string]event.Value

	// Emission phase.
	output *Batch
	offset int
	done   bool
}

// NewCompareIterator creates a new compare iterator.
func NewCompareIterator(child Iterator, shift time.Duration, reExec func(ctx context.Context) (Iterator, error), batchSize int) *CompareIterator {
	return &CompareIterator{
		child:     child,
		shift:     shift,
		reExec:    reExec,
		batchSize: batchSize,
	}
}

func (c *CompareIterator) Init(ctx context.Context) error {
	return c.child.Init(ctx)
}

func (c *CompareIterator) Next(ctx context.Context) (*Batch, error) {
	// Phase 1: Accumulate current rows.
	if !c.currentDone {
		c.currentDone = true
		for {
			batch, err := c.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			if batch == nil {
				break
			}
			for i := 0; i < batch.Len; i++ {
				c.currentRows = append(c.currentRows, batch.Row(i))
			}
		}
	}

	// Phase 2: Re-execute with time shift (if reExec is available).
	if !c.previousDone && c.reExec != nil {
		c.previousDone = true

		prevIter, err := c.reExec(ctx)
		if err == nil && prevIter != nil {
			if err := prevIter.Init(ctx); err == nil {
				for {
					batch, err := prevIter.Next(ctx)
					if err != nil {
						break
					}
					if batch == nil {
						break
					}
					for i := 0; i < batch.Len; i++ {
						c.previousRows = append(c.previousRows, batch.Row(i))
					}
				}
				prevIter.Close()
			}
		}
	}

	// Phase 3: Merge and emit.
	if c.output == nil {
		c.output = c.mergeRows()
	}

	if c.offset >= c.output.Len {
		return nil, nil
	}

	end := c.offset + c.batchSize
	if end > c.output.Len {
		end = c.output.Len
	}

	result := c.output.Slice(c.offset, end)
	c.offset = end

	return result, nil
}

func (c *CompareIterator) Close() error {
	return c.child.Close()
}

func (c *CompareIterator) Schema() []FieldInfo {
	// Dynamic schema — derived from child schema with added previous/change columns.
	schema := c.child.Schema()
	result := make([]FieldInfo, 0, len(schema)*2)
	for _, f := range schema {
		result = append(result, f)
		if isNumericType(f.Type) {
			result = append(result,
				FieldInfo{Name: "previous_" + f.Name, Type: f.Type},
				FieldInfo{Name: "change_" + f.Name, Type: "float"},
			)
		}
	}

	return result
}

// mergeRows merges current and previous rows into a single output batch.
// For each current row, it looks up a matching previous row by non-numeric
// (group) columns and adds previous_X and change_X columns.
func (c *CompareIterator) mergeRows() *Batch {
	if len(c.currentRows) == 0 {
		return NewBatch(0)
	}

	// Classify columns from first current row.
	var groupCols []string
	var numericCols []string
	for k, v := range c.currentRows[0] {
		if isNumericValue(v) {
			numericCols = append(numericCols, k)
		} else if k != "_outlier" && k != "_score" {
			groupCols = append(groupCols, k)
		}
	}

	prevLookup := make(map[string]map[string]event.Value, len(c.previousRows))
	for _, row := range c.previousRows {
		key := buildGroupKey(row, groupCols)
		prevLookup[key] = row
	}

	b := NewBatch(len(c.currentRows))
	for _, row := range c.currentRows {
		key := buildGroupKey(row, groupCols)
		prevRow, hasPrev := prevLookup[key]

		merged := make(map[string]event.Value, len(row)+len(numericCols)*2)
		for k, v := range row {
			merged[k] = v
		}

		for _, col := range numericCols {
			if hasPrev {
				if pv, ok := prevRow[col]; ok {
					merged["previous_"+col] = pv
					// Compute % change.
					curF, curOk := vm.ValueToFloat(row[col])
					prevF, prevOk := vm.ValueToFloat(pv)
					if curOk && prevOk && prevF != 0 {
						merged["change_"+col] = event.FloatValue(((curF - prevF) / prevF) * 100)
					} else {
						merged["change_"+col] = event.NullValue()
					}
				} else {
					merged["previous_"+col] = event.NullValue()
					merged["change_"+col] = event.NullValue()
				}
			} else {
				merged["previous_"+col] = event.NullValue()
				merged["change_"+col] = event.NullValue()
			}
		}

		b.AddRow(merged)
	}

	return b
}

// buildGroupKey creates a string key from group columns for lookup.
func buildGroupKey(row map[string]event.Value, groupCols []string) string {
	if len(groupCols) == 0 {
		return "_all"
	}
	var key string
	for _, col := range groupCols {
		if v, ok := row[col]; ok {
			key += col + "=" + v.String() + "\x00"
		}
	}

	return key
}

// isNumericValue returns true if the value is numeric.
func isNumericValue(v event.Value) bool {
	switch v.Type() {
	case event.FieldTypeInt, event.FieldTypeFloat:
		return true
	case event.FieldTypeString:
		// Try to parse as number.
		s := v.AsString()
		if len(s) > 0 && (s[0] >= '0' && s[0] <= '9' || s[0] == '-' || s[0] == '.') {
			var f float64
			_, err := fmt.Sscanf(s, "%f", &f)
			return err == nil
		}
	}

	return false
}

// isNumericType returns true if the type string represents a numeric type.
func isNumericType(t string) bool {
	return t == "int" || t == "float" || t == "number"
}

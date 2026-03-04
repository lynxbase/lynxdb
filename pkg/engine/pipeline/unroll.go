package pipeline

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// UnrollIterator explodes a JSON array field into multiple rows.
// For each input row, if the target field contains a JSON array:
//   - Each array element becomes a separate output row
//   - Object elements are flattened with dot-notation (field.key)
//   - Scalar elements replace the field value directly
//
// Non-array values pass through unchanged.
type UnrollIterator struct {
	child     Iterator
	field     string
	batchSize int

	// Buffer for expanded rows from the current batch.
	buffer []map[string]event.Value
	offset int
}

// NewUnrollIterator creates an unroll operator that explodes the given field.
func NewUnrollIterator(child Iterator, field string, batchSize int) *UnrollIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &UnrollIterator{
		child:     child,
		field:     field,
		batchSize: batchSize,
	}
}

func (u *UnrollIterator) Init(ctx context.Context) error {
	return u.child.Init(ctx)
}

func (u *UnrollIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		// Emit buffered rows first.
		if u.offset < len(u.buffer) {
			end := u.offset + u.batchSize
			if end > len(u.buffer) {
				end = len(u.buffer)
			}
			batch := BatchFromRows(u.buffer[u.offset:end])
			u.offset = end

			return batch, nil
		}

		childBatch, err := u.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		if childBatch == nil {
			return nil, nil
		}

		u.buffer = u.buffer[:0]
		u.offset = 0

		for i := 0; i < childBatch.Len; i++ {
			row := childBatch.Row(i)
			fieldVal, ok := row[u.field]
			if !ok || fieldVal.IsNull() {
				// No field or null — pass through.
				u.buffer = append(u.buffer, row)

				continue
			}

			s := strings.TrimSpace(fieldVal.String())
			if len(s) == 0 || s[0] != '[' {
				// Not a JSON array — pass through.
				u.buffer = append(u.buffer, row)

				continue
			}

			// Try to parse as JSON array.
			var arr []json.RawMessage
			if err := json.Unmarshal([]byte(s), &arr); err != nil {
				// Not valid JSON array — pass through.
				u.buffer = append(u.buffer, row)

				continue
			}

			if len(arr) == 0 {
				// Empty array — pass through (preserves the row with empty array).
				u.buffer = append(u.buffer, row)

				continue
			}

			// Explode: one output row per array element.
			for _, elem := range arr {
				newRow := cloneRow(row)
				elemStr := strings.TrimSpace(string(elem))

				if len(elemStr) > 0 && elemStr[0] == '{' {
					// Object element: flatten keys with dot-notation prefix.
					var obj map[string]json.RawMessage
					if err := json.Unmarshal(elem, &obj); err == nil {
						newRow[u.field] = event.StringValue(elemStr)
						for k, v := range obj {
							dotKey := u.field + "." + k
							newRow[dotKey] = jsonRawToValue(v)
						}
					} else {
						newRow[u.field] = event.StringValue(elemStr)
					}
				} else {
					// Scalar or array element: replace the field value.
					newRow[u.field] = jsonRawToValue(elem)
				}

				u.buffer = append(u.buffer, newRow)
			}
		}
	}
}

func (u *UnrollIterator) Close() error {
	return u.child.Close()
}

func (u *UnrollIterator) Schema() []FieldInfo {
	return u.child.Schema()
}

// cloneRow creates a shallow copy of a row.
func cloneRow(row map[string]event.Value) map[string]event.Value {
	clone := make(map[string]event.Value, len(row))
	for k, v := range row {
		clone[k] = v
	}

	return clone
}

// jsonRawToValue converts a json.RawMessage to a typed event.Value.
func jsonRawToValue(raw json.RawMessage) event.Value {
	s := strings.TrimSpace(string(raw))
	if len(s) == 0 {
		return event.NullValue()
	}

	switch s[0] {
	case 'n':
		if s == "null" {
			return event.NullValue()
		}

		return event.StringValue(s)
	case 't':
		if s == "true" {
			return event.BoolValue(true)
		}

		return event.StringValue(s)
	case 'f':
		if s == "false" {
			return event.BoolValue(false)
		}

		return event.StringValue(s)
	case '"':
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return event.StringValue(s)
		}

		return event.StringValue(str)
	case '{', '[':
		return event.StringValue(s)
	default:
		// Number: try integer first, then float.
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()

		var num json.Number
		if err := dec.Decode(&num); err == nil {
			if n, err := num.Int64(); err == nil {
				return event.IntValue(n)
			}
			if f, err := num.Float64(); err == nil {
				return event.FloatValue(f)
			}
		}

		return event.StringValue(s)
	}
}

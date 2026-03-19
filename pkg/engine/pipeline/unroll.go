package pipeline

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// UnrollIterator explodes JSON array fields into multiple rows.
// For each input row, if the target field contains a JSON array:
//   - Each array element becomes a separate output row
//   - Object elements are flattened with dot-notation (field.key)
//   - Scalar elements replace the field value directly
//
// When multiple fields are specified, they are zip-expanded: all fields must
// contain JSON arrays of the same length, and element i from each field is
// placed into the same output row. If any field is not an array or lengths
// differ, the row passes through unchanged.
//
// Non-array values pass through unchanged.
type UnrollIterator struct {
	child     Iterator
	fields    []string
	batchSize int

	// Buffer for expanded rows from the current batch.
	buffer []map[string]event.Value
	offset int
}

// NewUnrollIterator creates an unroll operator that explodes the given fields.
// When len(fields) == 1, behavior is identical to the original single-field unroll.
func NewUnrollIterator(child Iterator, fields []string, batchSize int) *UnrollIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &UnrollIterator{
		child:     child,
		fields:    fields,
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

			if len(u.fields) == 1 {
				u.unrollSingle(row, u.fields[0])
			} else {
				u.unrollMulti(row)
			}
		}
	}
}

// unrollSingle is the original single-field expansion logic.
func (u *UnrollIterator) unrollSingle(row map[string]event.Value, field string) {
	fieldVal, ok := row[field]
	if !ok || fieldVal.IsNull() {
		u.buffer = append(u.buffer, row)

		return
	}

	s := strings.TrimSpace(fieldVal.String())
	if len(s) == 0 || s[0] != '[' {
		u.buffer = append(u.buffer, row)

		return
	}

	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		u.buffer = append(u.buffer, row)

		return
	}

	if len(arr) == 0 {
		u.buffer = append(u.buffer, row)

		return
	}

	for _, elem := range arr {
		newRow := cloneRow(row)
		elemStr := strings.TrimSpace(string(elem))

		if len(elemStr) > 0 && elemStr[0] == '{' {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(elem, &obj); err == nil {
				newRow[field] = event.StringValue(elemStr)
				for k, v := range obj {
					dotKey := field + "." + k
					newRow[dotKey] = jsonRawToValue(v)
				}
			} else {
				newRow[field] = event.StringValue(elemStr)
			}
		} else {
			newRow[field] = jsonRawToValue(elem)
		}

		u.buffer = append(u.buffer, newRow)
	}
}

// unrollMulti does zip-expansion across multiple fields.
// All fields must contain JSON arrays of the same length; otherwise the row
// passes through unchanged.
func (u *UnrollIterator) unrollMulti(row map[string]event.Value) {
	// Parse all fields as JSON arrays.
	arrays := make([][]json.RawMessage, len(u.fields))
	arrLen := -1

	for i, field := range u.fields {
		fieldVal, ok := row[field]
		if !ok || fieldVal.IsNull() {
			u.buffer = append(u.buffer, row)

			return
		}

		s := strings.TrimSpace(fieldVal.String())
		if len(s) == 0 || s[0] != '[' {
			u.buffer = append(u.buffer, row)

			return
		}

		var arr []json.RawMessage
		if err := json.Unmarshal([]byte(s), &arr); err != nil {
			u.buffer = append(u.buffer, row)

			return
		}

		if arrLen == -1 {
			arrLen = len(arr)
		} else if len(arr) != arrLen {
			// Mismatched lengths — pass through.
			u.buffer = append(u.buffer, row)

			return
		}

		arrays[i] = arr
	}

	if arrLen <= 0 {
		u.buffer = append(u.buffer, row)

		return
	}

	// Zip-expand: for each index, create a row with element[i] from each field.
	for idx := 0; idx < arrLen; idx++ {
		newRow := cloneRow(row)

		for fi, field := range u.fields {
			elem := arrays[fi][idx]
			elemStr := strings.TrimSpace(string(elem))

			if len(elemStr) > 0 && elemStr[0] == '{' {
				var obj map[string]json.RawMessage
				if err := json.Unmarshal(elem, &obj); err == nil {
					newRow[field] = event.StringValue(elemStr)
					for k, v := range obj {
						dotKey := field + "." + k
						newRow[dotKey] = jsonRawToValue(v)
					}
				} else {
					newRow[field] = event.StringValue(elemStr)
				}
			} else {
				newRow[field] = jsonRawToValue(elem)
			}
		}

		u.buffer = append(u.buffer, newRow)
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

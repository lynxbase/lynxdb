package pipeline

import (
	"context"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// UnpackIterator performs format-specific field extraction per batch.
// Uses a callback-based FormatParser to avoid intermediate map allocations.
type UnpackIterator struct {
	child        Iterator
	parser       unpack.FormatParser
	sourceField  string              // field to read from (default: "_raw")
	fields       map[string]struct{} // if non-nil, only extract these fields
	prefix       string              // prefix for output field names
	keepOriginal bool                // don't overwrite existing non-null fields
}

// NewUnpackIterator creates a format-specific field extraction operator.
func NewUnpackIterator(
	child Iterator,
	parser unpack.FormatParser,
	sourceField string,
	fields []string,
	prefix string,
	keepOriginal bool,
) *UnpackIterator {
	var fieldSet map[string]struct{}
	if len(fields) > 0 {
		fieldSet = make(map[string]struct{}, len(fields))
		for _, f := range fields {
			fieldSet[f] = struct{}{}
		}
	}

	return &UnpackIterator{
		child:        child,
		parser:       parser,
		sourceField:  sourceField,
		fields:       fieldSet,
		prefix:       prefix,
		keepOriginal: keepOriginal,
	}
}

func (u *UnpackIterator) Init(ctx context.Context) error {
	return u.child.Init(ctx)
}

func (u *UnpackIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := u.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	srcCol := batch.Columns[u.sourceField]
	if srcCol == nil {
		return batch, nil
	}

	for i := 0; i < batch.Len; i++ {
		if i >= len(srcCol) {
			break
		}
		src := srcCol[i]
		if src.IsNull() {
			continue
		}

		rowIdx := i // capture for closure
		u.parser.Parse(src.String(), func(key string, val event.Value) bool {
			if u.fields != nil {
				if _, ok := u.fields[key]; !ok {
					return true // skip this field, continue parsing
				}
			}

			outKey := key
			if u.prefix != "" {
				outKey = u.prefix + key
			}

			col, exists := batch.Columns[outKey]
			if !exists {
				col = make([]event.Value, batch.Len)
				batch.Columns[outKey] = col
			} else if len(col) < batch.Len {
				extended := make([]event.Value, batch.Len)
				copy(extended, col)
				col = extended
				batch.Columns[outKey] = col
			}

			// Don't overwrite existing non-null values in keep_original mode.
			if u.keepOriginal && !col[rowIdx].IsNull() {
				return true
			}

			// strings.Clone: prevent memory retention of full source string
			// backing array. Without Clone, extracted substrings share the
			// backing array with the source field, preventing GC of unused bytes.
			if val.Type() == event.FieldTypeString {
				col[rowIdx] = event.StringValue(strings.Clone(val.String()))
			} else {
				col[rowIdx] = val
			}

			return true
		})
	}

	return batch, nil
}

func (u *UnpackIterator) Close() error { return u.child.Close() }

func (u *UnpackIterator) Schema() []FieldInfo { return u.child.Schema() }

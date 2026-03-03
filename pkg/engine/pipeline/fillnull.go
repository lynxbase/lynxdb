package pipeline

import (
	"context"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// FillnullIterator replaces null values with a specified value.
type FillnullIterator struct {
	child  Iterator
	value  string
	fields []string // empty = all fields
}

// NewFillnullIterator creates a fillnull iterator.
func NewFillnullIterator(child Iterator, value string, fields []string) *FillnullIterator {
	return &FillnullIterator{
		child:  child,
		value:  value,
		fields: fields,
	}
}

func (f *FillnullIterator) Init(ctx context.Context) error {
	return f.child.Init(ctx)
}

func (f *FillnullIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := f.child.Next(ctx)
	if err != nil || batch == nil {
		return batch, err
	}

	fillVal := event.StringValue(f.value)

	if len(f.fields) > 0 {
		// Fill only specified fields.
		for _, field := range f.fields {
			col, exists := batch.Columns[field]
			if !exists {
				// Create the column with fill values.
				col = make([]event.Value, batch.Len)
				for i := range col {
					col[i] = fillVal
				}
				batch.Columns[field] = col
			} else {
				for i := 0; i < batch.Len; i++ {
					if col[i].IsNull() {
						col[i] = fillVal
					}
				}
			}
		}
	} else {
		// Fill all columns.
		for field, col := range batch.Columns {
			_ = field
			for i := 0; i < batch.Len; i++ {
				if col[i].IsNull() {
					col[i] = fillVal
				}
			}
		}
	}

	return batch, nil
}

func (f *FillnullIterator) Close() error        { return f.child.Close() }
func (f *FillnullIterator) Schema() []FieldInfo { return f.child.Schema() }

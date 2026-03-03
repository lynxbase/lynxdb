package pipeline

import (
	"context"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// ProjectIterator selects/removes columns from passing batches.
type ProjectIterator struct {
	child  Iterator
	fields []string
	remove bool // true = remove listed fields, false = keep only listed fields
}

// NewProjectIterator creates a column selector (FIELDS/TABLE).
func NewProjectIterator(child Iterator, fields []string, remove bool) *ProjectIterator {
	return &ProjectIterator{child: child, fields: fields, remove: remove}
}

func (p *ProjectIterator) Init(ctx context.Context) error {
	return p.child.Init(ctx)
}

func (p *ProjectIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := p.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	result := &Batch{
		Columns: make(map[string][]event.Value, len(p.fields)),
		Len:     batch.Len,
	}

	if p.remove {
		removeSet := make(map[string]bool, len(p.fields))
		for _, f := range p.fields {
			removeSet[f] = true
		}
		for k, v := range batch.Columns {
			if !removeSet[k] {
				result.Columns[k] = v
			}
		}
	} else {
		for _, f := range p.fields {
			if col, ok := batch.Columns[f]; ok {
				result.Columns[f] = col
			}
		}
	}

	return result, nil
}

func (p *ProjectIterator) Close() error {
	return p.child.Close()
}

func (p *ProjectIterator) Schema() []FieldInfo {
	var schema []FieldInfo
	for _, f := range p.fields {
		schema = append(schema, FieldInfo{Name: f, Type: "any"})
	}

	return schema
}

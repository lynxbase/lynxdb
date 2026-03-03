package pipeline

import (
	"context"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// RenameIterator remaps column names without copying data.
type RenameIterator struct {
	child   Iterator
	renames map[string]string // old → new
}

// NewRenameIterator creates a field renaming operator.
func NewRenameIterator(child Iterator, renames map[string]string) *RenameIterator {
	return &RenameIterator{child: child, renames: renames}
}

func (r *RenameIterator) Init(ctx context.Context) error {
	return r.child.Init(ctx)
}

func (r *RenameIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := r.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	result := &Batch{
		Columns: make(map[string][]event.Value, len(batch.Columns)),
		Len:     batch.Len,
	}
	for k, v := range batch.Columns {
		if newName, ok := r.renames[k]; ok {
			result.Columns[newName] = v
		} else {
			result.Columns[k] = v
		}
	}

	return result, nil
}

func (r *RenameIterator) Close() error {
	return r.child.Close()
}

func (r *RenameIterator) Schema() []FieldInfo {
	return r.child.Schema()
}

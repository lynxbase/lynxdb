package pipeline

import (
	"context"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// ReverseIterator materializes input rows and emits them in reverse order.
type ReverseIterator struct {
	child     Iterator
	rows      []map[string]event.Value
	emitted   bool
	offset    int
	batchSize int
}

// NewReverseIterator creates an operator that emits its input in reverse order.
func NewReverseIterator(child Iterator, batchSize int) *ReverseIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &ReverseIterator{
		child:     child,
		batchSize: batchSize,
	}
}

func (r *ReverseIterator) Init(ctx context.Context) error { return r.child.Init(ctx) }

func (r *ReverseIterator) Next(ctx context.Context) (*Batch, error) {
	if !r.emitted {
		if err := r.materialize(ctx); err != nil {
			return nil, err
		}
		r.emitted = true
	}

	if r.offset >= len(r.rows) {
		return nil, nil
	}

	end := r.offset + r.batchSize
	if end > len(r.rows) {
		end = len(r.rows)
	}
	batch := BatchFromRows(r.rows[r.offset:end])
	r.offset = end

	return batch, nil
}

func (r *ReverseIterator) materialize(ctx context.Context) error {
	for {
		batch, err := r.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			r.rows = append(r.rows, batch.Row(i))
		}
	}
	reverseRows(r.rows)

	return nil
}

func reverseRows(rows []map[string]event.Value) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func (r *ReverseIterator) Close() error {
	return r.child.Close()
}

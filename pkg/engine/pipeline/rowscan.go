package pipeline

import (
	"context"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// RowScanIterator scans from pre-materialized pipeline rows ([]map[string]event.Value).
// Used for CTE variable resolution where results are already in pipeline format.
type RowScanIterator struct {
	rows      []map[string]event.Value
	offset    int
	batchSize int
}

// NewRowScanIterator creates a scan iterator over pre-materialized rows.
func NewRowScanIterator(rows []map[string]event.Value, batchSize int) *RowScanIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &RowScanIterator{rows: rows, batchSize: batchSize}
}

func (r *RowScanIterator) Init(ctx context.Context) error { return nil }

func (r *RowScanIterator) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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

func (r *RowScanIterator) Close() error        { return nil }
func (r *RowScanIterator) Schema() []FieldInfo { return nil }

package pipeline

import (
	"context"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// AppendcolsIterator appends subsearch fields to current rows by row position.
type AppendcolsIterator struct {
	child    Iterator
	sub      Iterator
	override bool
	maxout   int
	batch    int
	output   Iterator
}

// NewAppendcolsIterator creates a row-wise appendcols operator.
func NewAppendcolsIterator(child, sub Iterator, override bool, maxout int, batchSize int) *AppendcolsIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &AppendcolsIterator{child: child, sub: sub, override: override, maxout: maxout, batch: batchSize}
}

func (a *AppendcolsIterator) Init(ctx context.Context) error {
	if err := a.child.Init(ctx); err != nil {
		return err
	}
	return a.sub.Init(ctx)
}

func (a *AppendcolsIterator) Next(ctx context.Context) (*Batch, error) {
	if a.output == nil {
		if err := a.materialize(ctx); err != nil {
			return nil, err
		}
	}
	return a.output.Next(ctx)
}

func (a *AppendcolsIterator) materialize(ctx context.Context) error {
	mainRows, err := CollectAll(ctx, a.child)
	if err != nil {
		return err
	}
	subRows, err := CollectAll(ctx, a.sub)
	if err != nil {
		return err
	}
	if a.maxout >= 0 && len(subRows) > a.maxout {
		subRows = subRows[:a.maxout]
	}

	out := cloneAppendcolsRows(mainRows)
	for i := range out {
		if i >= len(subRows) {
			break
		}
		for k, v := range subRows[i] {
			if strings.HasPrefix(k, "_") {
				continue
			}
			if _, exists := out[i][k]; !exists || a.override {
				out[i][k] = v
			}
		}
	}
	a.output = NewRowScanIterator(out, a.batch)
	return a.output.Init(ctx)
}

func (a *AppendcolsIterator) Close() error {
	if a.output != nil {
		_ = a.output.Close()
	}
	_ = a.sub.Close()
	return a.child.Close()
}

func (a *AppendcolsIterator) Schema() []FieldInfo {
	return nil
}

func cloneAppendcolsRows(rows []map[string]event.Value) []map[string]event.Value {
	out := make([]map[string]event.Value, len(rows))
	for i, row := range rows {
		cp := make(map[string]event.Value, len(row))
		for k, v := range row {
			cp[k] = v
		}
		out[i] = cp
	}
	return out
}

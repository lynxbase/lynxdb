package pipeline

import (
	"context"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// EvalIterator computes columns of every row of a batch.
type EvalIterator struct {
	child       Iterator
	assignments []EvalAssignment
}

// EvalAssignment is a column and the expression of its values; a row whose
// expression reads a field it does not hold does not hold the column.
type EvalAssignment struct {
	Field string
	Expr  *Expr
}

// NewEvalIterator returns the rows of child with the columns of assignments,
// each computed after those before it.
func NewEvalIterator(child Iterator, assignments []EvalAssignment) *EvalIterator {
	return &EvalIterator{child: child, assignments: assignments}
}

func (e *EvalIterator) Init(ctx context.Context) error {
	return e.child.Init(ctx)
}

func (e *EvalIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := e.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}
	rows := make([]map[string]event.Value, batch.Len)
	for i := range rows {
		row := batch.Row(i)
		for _, a := range e.assignments {
			if v, ok := a.Expr.Value(row); ok {
				row[a.Field] = v
			} else {
				delete(row, a.Field)
			}
		}
		rows[i] = row
	}

	return BatchFromRows(rows), nil
}

func (e *EvalIterator) Close() error {
	return e.child.Close()
}

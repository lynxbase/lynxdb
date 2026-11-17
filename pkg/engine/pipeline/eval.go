package pipeline

import (
	"context"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// EvalIterator runs compiled EVAL assignments on each row in a batch.
type EvalIterator struct {
	child       Iterator
	assignments []EvalAssignment
	vmInst      vm.VM
}

// EvalAssignment pairs a field name with its compiled program.
type EvalAssignment struct {
	Field   string
	Program *vm.Program
}

// NewEvalIterator creates an eval operator with pre-compiled assignments.
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

	// Pre-allocate output columns for assignment targets.
	for _, assign := range e.assignments {
		if _, exists := batch.Columns[assign.Field]; !exists {
			batch.Columns[assign.Field] = make([]event.Value, batch.Len)
		}
	}

	// Reuse one map across all rows — update values in place.
	row := make(map[string]event.Value, len(batch.Columns)+len(e.assignments))
	for i := 0; i < batch.Len; i++ {
		batch.RowInto(i, row)
		for _, assign := range e.assignments {
			result, execErr := e.vmInst.Execute(assign.Program, row)
			if execErr != nil {
				continue
			}
			row[assign.Field] = result
			batch.Columns[assign.Field][i] = result
			batch.MarkPresent(assign.Field, i)
		}
	}

	return batch, nil
}

func (e *EvalIterator) Close() error {
	return e.child.Close()
}

func (e *EvalIterator) Schema() []FieldInfo {
	return e.child.Schema()
}

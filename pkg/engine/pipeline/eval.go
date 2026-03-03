package pipeline

import (
	"context"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/vm"
)

// EvalIterator runs compiled EVAL assignments on each row in a batch.
type EvalIterator struct {
	child       Iterator
	assignments []EvalAssignment
	vmInst      vm.VM
	// VM profiling (trace level only).
	profileVM bool
	vmCalls   int64
	vmTimeNS  int64
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
		// Populate row from columnar data (reuses map buckets).
		for k, col := range batch.Columns {
			if i < len(col) {
				row[k] = col[i]
			}
		}
		for _, assign := range e.assignments {
			var result event.Value
			var execErr error
			if e.profileVM {
				start := time.Now()
				result, execErr = e.vmInst.Execute(assign.Program, row)
				e.vmTimeNS += time.Since(start).Nanoseconds()
				e.vmCalls++
			} else {
				result, execErr = e.vmInst.Execute(assign.Program, row)
			}
			if execErr != nil {
				continue
			}
			row[assign.Field] = result
			batch.Columns[assign.Field][i] = result
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

// SetProfileVM enables per-call VM timing collection (trace level).
func (e *EvalIterator) SetProfileVM(enable bool) {
	e.profileVM = enable
}

// VMStats returns the accumulated VM execution metrics.
func (e *EvalIterator) VMStats() (calls, timeNS int64) {
	return e.vmCalls, e.vmTimeNS
}

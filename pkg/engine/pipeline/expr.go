package pipeline

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irplan"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Expr is an expression of the IR that the columnar executor's operations
// compute for one row at a time, so that a row operator gives it the
// meaning the executor does. It is not safe for concurrent use.
type Expr struct {
	x      *exec.RowExpr
	kind   ir.Kind
	fields fields
}

// NewExpr prepares a value; now is the time that relative times count from.
func NewExpr(e ir.Expr, now time.Time) (*Expr, error) {
	x, err := irplan.Expr(e, irplan.Options{Now: now})
	if err != nil {
		return nil, err
	}

	return &Expr{x: exec.NewRowExpr(x), kind: e.Type().Kind}, nil
}

// NewCond prepares a condition, which a row passes as it passes the filter
// of a scan.
func NewCond(e ir.Expr, now time.Time) (*Expr, error) {
	x, err := irplan.Cond(e, irplan.Options{Now: now})
	if err != nil {
		return nil, err
	}

	return &Expr{x: exec.NewRowExpr(x), kind: ir.KindBool}, nil
}

// Value returns the value for row, and false where the expression is a
// field that the row does not hold.
func (e *Expr) Value(row map[string]event.Value) (event.Value, bool) {
	v := e.Exec(row)
	if v.Kind == exec.KindAny {
		return event.Value{}, false
	}

	return RowValue(v, e.kind), true
}

// Exec returns the value for row as the executor holds it; its bytes stay
// valid until the next call.
func (e *Expr) Exec(row map[string]event.Value) exec.Value {
	e.fields.reset(row)

	return e.x.Value(&e.fields)
}

// True reports whether row passes the condition.
func (e *Expr) True(row map[string]event.Value) bool {
	e.fields.reset(row)

	return e.x.True(&e.fields)
}

package columnar

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// opaque returns what gives the columnar executor a function for an
// expression it has no operations for: the expression compiled for the row
// engine and run over the fields it reads. An expression that gives another
// value each time it is computed is left to the row operators, since the
// executor computes a function once for each distinct argument.
func opaque(now time.Time) func(e ir.Expr, fields []*ir.FieldRef) (func() exec.Func, bool) {
	return func(e ir.Expr, fields []*ir.FieldRef) (func() exec.Func, bool) {
		pure := true
		ir.WalkExpr(e, func(x ir.Expr) bool {
			if call, ok := x.(*ir.Call); ok && call.Func.Volatile {
				pure = false
			}

			return pure
		})
		if !pure {
			return nil, false
		}
		prog := compile(e, now)
		if prog == nil {
			return nil, false
		}
		names := make([]string, len(fields))
		times := make([]bool, len(fields))
		for i, f := range fields {
			names[i], times[i] = f.Name, f.Name == ir.TimeColumn || f.T.Kind == ir.KindTimestamp
		}

		return func() exec.Func {
			var machine vm.VM
			row := make(map[string]event.Value, len(names))

			return func(args []exec.Value, buf []byte) (exec.Value, []byte) {
				for i, name := range names {
					switch v := args[i]; {
					case v.Kind == exec.KindAny:
						delete(row, name)
					case v.Kind == vec.KindNull:
						row[name] = event.NullValue()
					case times[i] && v.Kind == vec.KindInt:
						row[name] = event.TimestampValue(time.Unix(0, v.Int).UTC())
					default:
						row[name] = valueOf(v)
					}
				}
				out, err := machine.Execute(prog, row)
				if err != nil {
					return exec.Value{Kind: vec.KindNull}, buf
				}

				return executorValue(out, buf)
			}
		}, true
	}
}

// compile compiles an expression for the row engine, or returns nil for one
// that it does not take; a panic of the compiler is such a refusal.
func compile(e ir.Expr, now time.Time) (prog *vm.Program) {
	defer func() {
		if recover() != nil {
			prog = nil
		}
	}()
	prog, err := vm.CompileExpr(e, vm.CompileOptions{Now: now})
	if err != nil {
		return nil
	}

	return prog
}

// executorValue turns a value of a row into one of the executor's, with its
// bytes in buf: a time or a duration as its nanoseconds, an array or an
// object as a variant.
func executorValue(v event.Value, buf []byte) (exec.Value, []byte) {
	switch v.Type() {
	case event.FieldTypeBool:
		return exec.Value{Kind: vec.KindBool, Bool: v.AsBool()}, buf
	case event.FieldTypeInt:
		return exec.Value{Kind: vec.KindInt, Int: v.AsInt()}, buf
	case event.FieldTypeFloat:
		return exec.Value{Kind: vec.KindFloat, Float: v.AsFloat()}, buf
	case event.FieldTypeString:
		buf = append(buf, v.AsString()...)

		return exec.Value{Kind: vec.KindString, Bytes: buf}, buf
	case event.FieldTypeTimestamp:
		return exec.Value{Kind: vec.KindInt, Int: v.AsTimestamp().UnixNano()}, buf
	case event.FieldTypeDuration:
		return exec.Value{Kind: vec.KindInt, Int: int64(v.AsDuration())}, buf
	case event.FieldTypeArray, event.FieldTypeObject:
		buf = appendVariant(buf, v)

		return exec.Value{Kind: vec.KindVariant, Bytes: buf}, buf
	}

	return exec.Value{Kind: vec.KindNull}, buf
}

// appendVariant appends a value of a row in the variant encoding.
func appendVariant(dst []byte, v event.Value) []byte {
	switch v.Type() {
	case event.FieldTypeBool:
		return variant.AppendBool(dst, v.AsBool())
	case event.FieldTypeInt:
		return variant.AppendInt(dst, v.AsInt())
	case event.FieldTypeFloat:
		return variant.AppendFloat(dst, v.AsFloat())
	case event.FieldTypeString:
		return variant.AppendString(dst, []byte(v.AsString()))
	case event.FieldTypeTimestamp:
		return variant.AppendInt(dst, v.AsTimestamp().UnixNano())
	case event.FieldTypeDuration:
		return variant.AppendInt(dst, int64(v.AsDuration()))
	case event.FieldTypeArray:
		dst, mark := variant.BeginArray(dst)
		for _, e := range v.AsArray() {
			dst = appendVariant(dst, e)
		}

		return variant.End(dst, mark)
	case event.FieldTypeObject:
		dst, mark := variant.BeginObject(dst)
		for k, e := range v.AsObject() {
			dst = appendVariant(variant.AppendKey(dst, []byte(k)), e)
		}

		return variant.End(dst, mark)
	}

	return variant.AppendNull(dst)
}

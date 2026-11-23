package exec

import (
	"encoding/binary"
	"math"

	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Row gives an expression that is computed for one row at a time the values
// of the row's fields.
type Row interface {
	// Field returns the value at path: the zero Value where the row holds
	// none, and a Value of vec.KindNull where it holds a null.
	Field(path string) Value
	// Values calls fn with the value of every field of the row until fn
	// returns false.
	Values(fn func(Value) bool)
}

// RowExpr computes an expression for one row at a time with the semantics
// that the executor gives it over blocks: what the scalar functions give
// row by row, and for a condition, what a filter of a scan keeps. It is not
// safe for concurrent use.
type RowExpr struct {
	root *rowNode
}

// rowNode is a node of an expression with the memory of its function.
type rowNode struct {
	e    *Expr
	args []*rowNode
	vals []Value
	buf  []byte
	fn   Func
	// number and numbered say what number a free-text word of digits
	// spells, which the numbers of a row match as well.
	number   int64
	numbered bool
}

// NewRowExpr prepares e for rows.
func NewRowExpr(e *Expr) *RowExpr { return &RowExpr{root: newRowNode(e)} }

func newRowNode(e *Expr) *rowNode {
	n := &rowNode{e: e, vals: make([]Value, len(e.Args))}
	for _, a := range e.Args {
		n.args = append(n.args, newRowNode(a))
	}
	switch e.Op {
	case OpFunc:
		n.fn = e.NewFunc()
	case OpMatchAny:
		n.number, n.numbered = e.Pattern.Number()
	}

	return n
}

// Value returns what the expression computes for row: the zero Value where
// it reads a field the row does not hold, and a Value of vec.KindNull where
// it gives no value. The bytes of the value stay valid until the next call.
func (x *RowExpr) Value(row Row) Value { return x.root.value(row) }

// True reports whether row passes the expression as a filter: And, Or and
// Not are two-valued there, and anything else passes where it is true.
func (x *RowExpr) True(row Row) bool { return x.root.test(row) }

func (n *rowNode) test(row Row) bool {
	switch n.e.Op {
	case OpAnd:
		for _, a := range n.args {
			if !a.test(row) {
				return false
			}
		}

		return true
	case OpOr:
		for _, a := range n.args {
			if a.test(row) {
				return true
			}
		}

		return false
	case OpNot:
		return !n.args[0].test(row)
	}
	v := n.value(row)

	return v.Kind == vec.KindBool && v.Bool
}

func (n *rowNode) value(row Row) Value {
	e := n.e
	switch e.Op {
	case OpColumn:
		v := row.Field(e.Path)
		if e.Kind != KindAny && v.Kind != e.Kind {
			return Value{}
		}

		return v
	case OpLiteral:
		if e.Value.IsNull() {
			return Value{Kind: vec.KindNull}
		}

		return e.Value
	case OpMatchAny:
		return boolValue(n.matchAny(row))
	}
	for i, a := range n.args {
		n.vals[i] = a.value(row)
	}
	var v Value
	if e.Op == OpFunc {
		v, n.buf = n.fn(n.vals, n.buf[:0])
	} else {
		v, n.buf = scalarValue(e.Op, &e.Pattern, n.vals, n.buf[:0])
	}
	// What a function computes is a null where it has no value.
	if v.Kind == KindAny {
		v.Kind = vec.KindNull
	}

	return v
}

// matchAny reports whether a string of the row, also one inside an array or
// an object, holds the pattern, or for a word of digits, whether a number of
// the row is that number or has it as its whole part.
func (n *rowNode) matchAny(row Row) bool {
	pat, hit := &n.e.Pattern, false
	row.Values(func(v Value) bool {
		switch v.Kind {
		case vec.KindString:
			hit = pat.Match(v.Bytes)
		case vec.KindVariant:
			variant.ForEachString(v.Bytes, func(s []byte) { hit = hit || pat.Match(s) })
		case vec.KindInt:
			hit = n.numbered && (v.Int == n.number || v.Int == -n.number)
		case vec.KindFloat:
			hit = n.numbered && math.Trunc(math.Abs(v.Float)) == float64(n.number)
		}

		return !hit
	})

	return hit
}

// Compare orders two values as a sort of the executor does: in the order
// of compareValues, reversed with desc, and nulls last, or first with
// nullsFirst, in either direction.
func Compare(a, b Value, desc, nullsFirst bool) int {
	return compareOrdered(a, b, orderSpec{desc: desc, nullsFirst: nullsFirst})
}

// AppendKey appends v as one part of a key whose values are equal exactly
// where they form one group: a number is one key whether it is an integer
// or a float, a string and a number are two, and a null and a missing value
// are one.
func AppendKey(dst []byte, v Value) []byte {
	switch v.Kind {
	case vec.KindBool:
		return appendKeyBool(dst, v.Bool)
	case vec.KindInt:
		return appendKeyInt(dst, v.Int)
	case vec.KindFloat:
		return appendKeyFloat(dst, v.Float)
	case vec.KindString, vec.KindVariant:
		return appendKeyBytes(dst, bytesTag(v.Kind), v.Bytes, false)
	}

	return append(dst, tagNull)
}

// KeyValue returns the value that a key of AppendKey holds at its start and
// the rest of the key; the bytes of the value alias the key.
func KeyValue(key []byte) (Value, []byte) {
	tag := key[0]
	key = key[1:]
	switch tag {
	case tagFalse, tagTrue:
		return Value{Kind: vec.KindBool, Bool: tag == tagTrue}, key
	case tagInt:
		return Value{Kind: vec.KindInt, Int: int64(binary.LittleEndian.Uint64(key))}, key[8:]
	case tagFloat:
		return Value{Kind: vec.KindFloat, Float: math.Float64frombits(binary.LittleEndian.Uint64(key))}, key[8:]
	case tagString, tagVariant:
		n, used := binary.Uvarint(key)
		v := Value{Kind: vec.KindString, Bytes: key[used : used+int(n)]}
		if tag == tagVariant {
			v.Kind = vec.KindVariant
		}

		return v, key[used+int(n):]
	}

	return Value{Kind: vec.KindNull}, key
}

// Number returns the number that a value is, or holds as a string, as a
// float.
func Number(v Value) (float64, bool) { return number(v) }

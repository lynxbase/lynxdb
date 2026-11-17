// Package exec runs logical query plans over a snapshot of a columnar table,
// one block at a time on every core.
//
// A path of the table may hold values of several kinds. A column reference
// names the kind it reads, and then sees only the values of that kind, or
// KindAny, and then sees every value. A row whose path holds no value of the
// referenced kind is missing for that reference; a JSON null is missing too.
//
// Filters are two-valued: a comparison or a text match with a missing value
// is false, and Not inverts that. A string literal compares with string
// values only, a numeric literal with integers and floats, a boolean literal
// with booleans.
//
// Group keys keep the kind of every value, so 1 and "1" form two groups, and
// missing keys form one group whose key is null.
//
// Aggregates: Count without an argument counts rows; with one it counts the
// rows where the argument has a value. CountDistinct counts the distinct
// values of the argument, telling kinds apart and identifying a value by its
// 128-bit hash. Sum, Min, Max and Avg read integers and floats and skip
// values of every other kind; their result is an integer when every value
// read was one, a float otherwise, and Avg is always a float. An aggregate
// that read no value is null.
package exec

import (
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// KindAny is the kind of a column reference that reads values of every kind.
const KindAny vec.Kind = 0

// Op identifies what an expression node computes.
type Op uint8

// The operations of an expression tree.
const (
	OpColumn Op = iota + 1
	OpLiteral
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
	OpNot
	OpMatch
	OpHourOfDay
	OpAdd
	OpSub
	OpMul
	OpDiv
)

// Expr is a node of an expression tree; which fields it uses depends on Op.
type Expr struct {
	Op Op
	// Path and Kind name the column of an OpColumn node.
	Path string
	Kind vec.Kind
	// Value is the constant of an OpLiteral node.
	Value Value
	// Pattern is what an OpMatch node searches for in its argument.
	Pattern tokenfilter.Pattern
	Args    []*Expr
}

// Value is one scalar; the zero Value is null.
type Value struct {
	Kind  vec.Kind
	Int   int64
	Float float64
	Bool  bool
	Bytes []byte
}

// IsNull reports whether v holds no value.
func (v Value) IsNull() bool { return v.Kind == KindAny || v.Kind == vec.KindNull }

// Col refers to the values of kind at path; in a plan node above an
// aggregate or a projection, path names one of its input's columns.
func Col(path string, kind vec.Kind) *Expr { return &Expr{Op: OpColumn, Path: path, Kind: kind} }

// Time refers to the row timestamp in nanoseconds.
func Time() *Expr { return Col(part.TimePath, vec.KindInt) }

// Str is a string literal.
func Str(s string) *Expr {
	return &Expr{Op: OpLiteral, Value: Value{Kind: vec.KindString, Bytes: []byte(s)}}
}

// Int is an integer literal.
func Int(v int64) *Expr { return &Expr{Op: OpLiteral, Value: Value{Kind: vec.KindInt, Int: v}} }

// Float is a float literal.
func Float(v float64) *Expr {
	return &Expr{Op: OpLiteral, Value: Value{Kind: vec.KindFloat, Float: v}}
}

// Bool is a boolean literal.
func Bool(v bool) *Expr { return &Expr{Op: OpLiteral, Value: Value{Kind: vec.KindBool, Bool: v}} }

func pair(op Op, a, b *Expr) *Expr { return &Expr{Op: op, Args: []*Expr{a, b}} }

// Eq is a == b.
func Eq(a, b *Expr) *Expr { return pair(OpEq, a, b) }

// Ne is a != b, false when either side is missing.
func Ne(a, b *Expr) *Expr { return pair(OpNe, a, b) }

// Lt is a < b.
func Lt(a, b *Expr) *Expr { return pair(OpLt, a, b) }

// Le is a <= b.
func Le(a, b *Expr) *Expr { return pair(OpLe, a, b) }

// Gt is a > b.
func Gt(a, b *Expr) *Expr { return pair(OpGt, a, b) }

// Ge is a >= b.
func Ge(a, b *Expr) *Expr { return pair(OpGe, a, b) }

// And is true when every argument is.
func And(args ...*Expr) *Expr { return &Expr{Op: OpAnd, Args: args} }

// Or is true when any argument is.
func Or(args ...*Expr) *Expr { return &Expr{Op: OpOr, Args: args} }

// Not inverts a.
func Not(a *Expr) *Expr { return &Expr{Op: OpNot, Args: []*Expr{a}} }

// Match is true when the string a holds the pattern.
func Match(a *Expr, p tokenfilter.Pattern) *Expr {
	return &Expr{Op: OpMatch, Pattern: p, Args: []*Expr{a}}
}

// HourOfDay is the UTC hour, 0 to 23, of a time in nanoseconds.
func HourOfDay(t *Expr) *Expr { return &Expr{Op: OpHourOfDay, Args: []*Expr{t}} }

// Add is a + b.
func Add(a, b *Expr) *Expr { return pair(OpAdd, a, b) }

// Sub is a - b.
func Sub(a, b *Expr) *Expr { return pair(OpSub, a, b) }

// Mul is a * b.
func Mul(a, b *Expr) *Expr { return pair(OpMul, a, b) }

// Div is a / b, truncated for integers and missing when b is zero.
func Div(a, b *Expr) *Expr { return pair(OpDiv, a, b) }

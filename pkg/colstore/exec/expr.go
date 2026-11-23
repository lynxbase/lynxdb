// Package exec runs logical query plans over a snapshot of a columnar table,
// one block at a time on every core.
//
// A path of the table may hold values of several kinds. A column reference
// names the kind it reads, and then sees only the values of that kind, or
// KindAny, and then sees every value. A row whose path holds no value of the
// referenced kind is missing for that reference; a JSON null is missing too.
//
// Filters are two-valued: a comparison or a text match with a missing value
// is false, and Not inverts that. A string compared with a number is read as
// the number it holds, and the comparison is false for a string that holds
// none; a boolean literal compares with booleans.
//
// Scalar functions compute a value per row. A function of a value it cannot
// take, of a null or of a missing one gives a null, which IsNull, IsMissing,
// Coalesce, If and Case tell apart from a value as their comments say; the
// functions of the IR come as Funcs. Arithmetic reads a string as the number
// it holds, keeps integers, gives a float when either side is one, and gives
// no value when dividing by zero or for a string that holds no number; Add
// joins two strings.
// Inside a scalar function, comparisons, And, Or and Not are three-valued:
// they give no value where the outcome depends on a value that is not there.
//
// Group keys keep the kind of every value, so 1 and "1" form two groups,
// while a number is one key whether it is stored as an integer or a float.
// The rows where a key is null or missing form one more group, whose key is
// null.
//
// Aggregates: Count without an argument counts rows; with one it counts the
// rows where the argument has a value. CountDistinct counts the distinct
// values of the argument, telling kinds apart as group keys do and
// identifying a value by its 128-bit hash. Sum, Min, Max, Avg, Var, Stdev
// and Percentile read integers, floats and the numbers that strings hold,
// and skip values of every other kind; the result of the first three is an
// integer when every number read was one, a float otherwise, and the others
// are always floats. Min and Max read the other strings too, which order
// after every number as in a sort: the Min of a group is a string only when
// the group holds no number, and its Max is a string when it holds one. An
// aggregate that read no value is null.
//
// A group's state is a row of 64-bit words, and for the aggregates that need
// more, states of variable size that the words refer to: the extreme string
// of a Min or a Max, as long as that string, the set of hashes or the sketch
// of an ApproxDistinct, of at most 4 KB, and the two stores of a Percentile,
// which grow with the range of the values to at most 16 KB each.
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
	OpMod
	OpBin
	OpIf
	OpCase
	OpCoalesce
	OpIsNull
	OpIsMissing
	OpMatchAny
	OpIndexOf
	OpTotal
	OpFunc

	// opPlace is where a row is in the table, for reading its fields once
	// the rows of the result are known.
	opPlace
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
	// NewFunc gives an OpFunc node its function. The executor calls it once
	// for every goroutine that computes the node, so a function may keep
	// memory of its own.
	NewFunc func() Func
	Args    []*Expr
}

// Func computes a value from the values of its arguments in one row; an
// argument without a value is a null, or the zero Value where the row does
// not hold the field. It gives the same value for the same arguments, so it
// may be computed once for each of their combinations. A value that holds
// bytes may be built in buf, which is returned for the next call; the value
// is used before that call.
type Func func(args []Value, buf []byte) (Value, []byte)

// Call computes a function of at least one argument row by row.
func Call(newFunc func() Func, args ...*Expr) *Expr {
	return &Expr{Op: OpFunc, NewFunc: newFunc, Args: args}
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

// MatchAny is true when a string that the row holds at any path, also inside
// an array or an object, holds the pattern, or for a pattern of a word of
// digits, when a number of the row is that number or has it as its whole
// part.
func MatchAny(p tokenfilter.Pattern) *Expr { return &Expr{Op: OpMatchAny, Pattern: p} }

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

// Mod is the remainder of the integer division a / b.
func Mod(a, b *Expr) *Expr { return pair(OpMod, a, b) }

func call(op Op, args ...*Expr) *Expr { return &Expr{Op: op, Args: args} }

// Bin is the start of the bucket of the given width that holds x: the
// largest multiple of width that is not above x.
func Bin(x, width *Expr) *Expr { return call(OpBin, x, width) }

// If is a where cond is true and b where it is false.
func If(cond, a, b *Expr) *Expr { return call(OpIf, cond, a, b) }

// Case takes conditions and values in turn, then optionally a default: it
// is the value after the first condition that is true, or else the default.
func Case(args ...*Expr) *Expr { return call(OpCase, args...) }

// Coalesce is the first argument that has a value.
func Coalesce(args ...*Expr) *Expr { return call(OpCoalesce, args...) }

// IsNull is true where a holds a null.
func IsNull(a *Expr) *Expr { return call(OpIsNull, a) }

// IsMissing is true where a holds neither a value nor a null.
func IsMissing(a *Expr) *Expr { return call(OpIsMissing, a) }

// IndexOf is how many characters precede the first occurrence of the string
// needle in s, and has no value when there is none.
func IndexOf(s, needle *Expr) *Expr { return call(OpIndexOf, s, needle) }

// Total is the sum of the numbers that a column of an aggregate holds over
// its groups: all of them in the filter above the aggregate, and those that
// pass that filter in the projection. The groups are then all held until it
// is known.
func Total(column *Expr) *Expr { return call(OpTotal, column) }

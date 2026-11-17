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
// Scalar functions compute a value per row. A function of a value it cannot
// take, of a null or of a missing one gives a null, which Exists, IsNull,
// IsMissing, TypeOf, Coalesce, If and Case tell apart from a value as their
// comments say. Arithmetic keeps integers, gives a float when either side is
// one, and gives no value when dividing by zero; Add joins two strings.
// Inside a scalar function, comparisons, And, Or and Not are three-valued:
// they give no value where the outcome depends on a value that is not there.
//
// Group keys keep the kind of every value, so 1 and "1" form two groups. The
// rows where a key is null and the rows where it is missing form two more
// groups, whose keys are both null in the result: a reference of KindAny is
// null where the path holds a JSON null, a function is null where it gives
// no value, and anything else without a value is missing.
//
// Aggregates: Count without an argument counts rows; with one it counts the
// rows where the argument has a value. CountDistinct counts the distinct
// values of the argument, telling kinds apart and identifying a value by its
// 128-bit hash. Sum, Min, Max, Avg, Var and Stdev read integers and floats
// and skip values of every other kind; the result of the first three is an
// integer when every value read was one, a float otherwise, and the others
// are always floats. Min and Max read strings too, which order after every
// number as in a sort: the Min of a group is a string only when the group
// holds no number, and its Max is a string when it holds one. An aggregate
// that read no value is null.
//
// A group's state is a row of 64-bit words, and for the aggregates that need
// more, states of variable size that the words refer to: the extreme string
// of a Min or a Max, as long as that string, and the set of hashes or the
// sketch of an ApproxDistinct, of at most 4 KB.
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
	OpToInt
	OpToFloat
	OpToString
	OpToBool
	OpIf
	OpCase
	OpCoalesce
	OpNullIf
	OpExists
	OpIsNull
	OpIsMissing
	OpTypeOf
	OpLength
	OpLower
	OpUpper
	OpSubstr
	OpConcat
	OpStartsWith
	OpEndsWith
	OpReplace
	OpMatchAny
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

// MatchAny is true when a string that the row holds at any path holds the
// pattern; strings inside arrays are not looked at.
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

// ToInt converts a number, a boolean or the decimal text of a number to an
// integer, truncating a fraction.
func ToInt(a *Expr) *Expr { return call(OpToInt, a) }

// ToFloat converts a number, a boolean or the text of a number to a float.
func ToFloat(a *Expr) *Expr { return call(OpToFloat, a) }

// ToString is the text of a string, a number or a boolean.
func ToString(a *Expr) *Expr { return call(OpToString, a) }

// ToBool converts a boolean, a number, which is true unless zero, or the
// text true or false.
func ToBool(a *Expr) *Expr { return call(OpToBool, a) }

// If is a where cond is true and b where it is false.
func If(cond, a, b *Expr) *Expr { return call(OpIf, cond, a, b) }

// Case takes conditions and values in turn, then optionally a default: it
// is the value after the first condition that is true, or else the default.
func Case(args ...*Expr) *Expr { return call(OpCase, args...) }

// Coalesce is the first argument that has a value.
func Coalesce(args ...*Expr) *Expr { return call(OpCoalesce, args...) }

// NullIf is a unless it equals b.
func NullIf(a, b *Expr) *Expr { return call(OpNullIf, a, b) }

// Exists is true where a holds a value or a null.
func Exists(a *Expr) *Expr { return call(OpExists, a) }

// IsNull is true where a holds a null.
func IsNull(a *Expr) *Expr { return call(OpIsNull, a) }

// IsMissing is true where a holds neither a value nor a null.
func IsMissing(a *Expr) *Expr { return call(OpIsMissing, a) }

// TypeOf names what a holds: string, int, float, bool, array, null or
// missing.
func TypeOf(a *Expr) *Expr { return call(OpTypeOf, a) }

// Length is the number of characters of a string.
func Length(a *Expr) *Expr { return call(OpLength, a) }

// Lower is a string in lower case.
func Lower(a *Expr) *Expr { return call(OpLower, a) }

// Upper is a string in upper case.
func Upper(a *Expr) *Expr { return call(OpUpper, a) }

// Substr is the characters of s from start, counted from zero or, when
// negative, from the end; args may hold how many characters to take.
func Substr(s, start *Expr, args ...*Expr) *Expr {
	return call(OpSubstr, append([]*Expr{s, start}, args...)...)
}

// Concat joins strings.
func Concat(args ...*Expr) *Expr { return call(OpConcat, args...) }

// StartsWith is true where the string s begins with prefix.
func StartsWith(s, prefix *Expr) *Expr { return call(OpStartsWith, s, prefix) }

// EndsWith is true where the string s ends with suffix.
func EndsWith(s, suffix *Expr) *Expr { return call(OpEndsWith, s, suffix) }

// Replace is s with every occurrence of the string from replaced by to.
func Replace(s, from, to *Expr) *Expr { return call(OpReplace, s, from, to) }

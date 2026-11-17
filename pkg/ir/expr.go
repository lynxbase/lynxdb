package ir

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Expr is a typed scalar expression.
type Expr interface {
	// Type returns the static result type.
	Type() Type
	// String renders the expression in the canonical IR notation used by
	// EXPLAIN and golden tests.
	String() string
	isExpr()
}

// Literal is a constant value. V holds one of: nil, bool, int64, float64,
// string, time.Time, time.Duration, []any (array) or map[string]any (object);
// array and object elements use the same representation.
type Literal struct {
	V any
	T Type
}

// NullLit returns the NULL literal.
func NullLit() *Literal { return &Literal{T: Null} }

// BoolLit returns a boolean literal.
func BoolLit(b bool) *Literal { return &Literal{V: b, T: Bool} }

// IntLit returns an integer literal.
func IntLit(n int64) *Literal { return &Literal{V: n, T: Int} }

// FloatLit returns a float literal.
func FloatLit(f float64) *Literal { return &Literal{V: f, T: Float} }

// StringLit returns a string literal.
func StringLit(s string) *Literal { return &Literal{V: s, T: String} }

// TimestampLit returns a timestamp literal.
func TimestampLit(t time.Time) *Literal { return &Literal{V: t.UTC(), T: Timestamp} }

// DurationLit returns a duration literal.
func DurationLit(d time.Duration) *Literal { return &Literal{V: d, T: Duration} }

func (l *Literal) Type() Type { return l.T }
func (*Literal) isExpr()      {}

// IsNull reports whether the literal is NULL.
func (l *Literal) IsNull() bool { return l.V == nil }

func (l *Literal) String() string { return formatValue(l.V) }

func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return fmt.Sprintf("float(%q)", strconv.FormatFloat(x, 'g', -1, 64))
		}
		s := strconv.FormatFloat(x, 'g', -1, 64)
		if !strings.ContainsAny(s, ".eE") {
			s += ".0"
		}
		return s
	case string:
		return strconv.Quote(x)
	case time.Time:
		return "timestamp(" + strconv.Quote(x.UTC().Format(time.RFC3339Nano)) + ")"
	case time.Duration:
		return "duration(" + strconv.Quote(x.String()) + ")"
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = formatValue(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = strconv.Quote(k) + ": " + formatValue(x[k])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprintf("%v", x)
	}
}

// FieldRef reads a column of the current row. Name is the column name; a
// dotted name resolves to the column with exactly that name when the row has
// one, and otherwise walks object members from the first segment.
type FieldRef struct {
	Name string
	T    Type
}

// Field returns a reference to a column of type Any.
func Field(name string) *FieldRef { return &FieldRef{Name: name, T: Any} }

func (f *FieldRef) Type() Type { return f.T }
func (*FieldRef) isExpr()      {}
func (f *FieldRef) String() string {
	return QuoteName(f.Name)
}

// QuoteName renders a column name, quoting it with backticks unless it is a
// plain dotted identifier.
func QuoteName(name string) string {
	if isPlainName(name) {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func isPlainName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		switch {
		case r == '_' || r == '@' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case (r >= '0' && r <= '9') || r == '.':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return !strings.HasSuffix(name, ".") && !strings.Contains(name, "..")
}

// UnaryOp is a unary operator.
type UnaryOp uint8

const (
	// OpNeg is arithmetic negation; NULL yields NULL.
	OpNeg UnaryOp = iota + 1
	// OpNot is Kleene negation; NULL yields NULL.
	OpNot
)

// Unary applies a unary operator.
type Unary struct {
	Op UnaryOp
	X  Expr
	T  Type
}

func (u *Unary) Type() Type { return u.T }
func (*Unary) isExpr()      {}
func (u *Unary) String() string {
	if u.Op == OpNot {
		return "NOT " + paren(u.X)
	}
	return "-" + paren(u.X)
}

// BinaryOp is a binary operator.
type BinaryOp uint8

const (
	OpAdd BinaryOp = iota + 1
	OpSub
	OpMul
	// OpDiv divides; integer operands give an integer quotient truncated
	// toward zero, and division by zero yields NULL.
	OpDiv
	// OpMod is the remainder with the sign of the dividend; modulo by zero
	// yields NULL.
	OpMod
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpAnd
	OpOr
	OpXor
)

var binaryOpText = map[BinaryOp]string{
	OpAdd: "+", OpSub: "-", OpMul: "*", OpDiv: "/", OpMod: "%",
	OpEq: "=", OpNe: "!=", OpLt: "<", OpLe: "<=", OpGt: ">", OpGe: ">=",
	OpAnd: "AND", OpOr: "OR", OpXor: "XOR",
}

func (op BinaryOp) String() string { return binaryOpText[op] }

// IsComparison reports whether op is one of = != < <= > >=.
func (op BinaryOp) IsComparison() bool { return op >= OpEq && op <= OpGe }

// IsArithmetic reports whether op is one of + - * / %.
func (op BinaryOp) IsArithmetic() bool { return op >= OpAdd && op <= OpMod }

// IsLogical reports whether op is AND, OR or XOR.
func (op BinaryOp) IsLogical() bool { return op >= OpAnd }

// Commute returns the operator obtained by swapping the operands of a
// comparison (a < b is b > a).
func (op BinaryOp) Commute() BinaryOp {
	switch op {
	case OpLt:
		return OpGt
	case OpLe:
		return OpGe
	case OpGt:
		return OpLt
	case OpGe:
		return OpLe
	}
	return op
}

// Binary applies a binary operator. Arithmetic on two ints is int, on an int
// and a float is float; timestamp - timestamp is a duration and timestamp ±
// duration is a timestamp. Adding two strings concatenates them. Wherever a
// string meets a number, in arithmetic or in a comparison, the string is
// converted to a number, and wherever a string meets a timestamp it is
// converted to a timestamp; the result is NULL when it does not parse.
// Comparisons between other incompatible types yield NULL.
type Binary struct {
	Op BinaryOp
	L  Expr
	R  Expr
	T  Type
}

func (b *Binary) Type() Type { return b.T }
func (*Binary) isExpr()      {}
func (b *Binary) String() string {
	return paren(b.L) + " " + b.Op.String() + " " + paren(b.R)
}

// Call invokes a registered scalar function.
type Call struct {
	Func *Func
	Args []Expr
	T    Type
}

func (c *Call) Type() Type     { return c.T }
func (*Call) isExpr()          {}
func (c *Call) String() string { return c.Func.Name + "(" + joinExprs(c.Args) + ")" }

// When is one branch of a Case.
type When struct {
	Cond Expr
	Then Expr
}

// Case evaluates the first branch whose condition is TRUE; with no such
// branch it yields Else, or NULL when Else is nil.
type Case struct {
	Whens []When
	Else  Expr
	T     Type
}

func (c *Case) Type() Type { return c.T }
func (*Case) isExpr()      {}
func (c *Case) String() string {
	var b strings.Builder
	b.WriteString("CASE")
	for _, w := range c.Whens {
		b.WriteString(" WHEN " + w.Cond.String() + " THEN " + w.Then.String())
	}
	if c.Else != nil {
		b.WriteString(" ELSE " + c.Else.String())
	}
	b.WriteString(" END")
	return b.String()
}

// Cast converts a value to another type. A value that cannot be converted
// yields NULL; NULL stays NULL. Conversion rules are listed in
// docs/site/docs/ppl/ir.md.
type Cast struct {
	X  Expr
	To Type
}

func (c *Cast) Type() Type { return c.To }
func (*Cast) isExpr()      {}
func (c *Cast) String() string {
	return "CAST(" + c.X.String() + " AS " + c.To.String() + ")"
}

// Index reads an array element (0-based; negative counts from the end) or an
// object member by key. An out-of-range index or absent key yields NULL.
type Index struct {
	X   Expr
	Key Expr
	T   Type
}

func (i *Index) Type() Type     { return i.T }
func (*Index) isExpr()          {}
func (i *Index) String() string { return paren(i.X) + "[" + i.Key.String() + "]" }

// Lambda is an anonymous function passed to a higher-order function such as
// array filter or transform. It is valid only as a Call argument.
type Lambda struct {
	Params []*LambdaParam
	Body   Expr
}

func (l *Lambda) Type() Type { return l.Body.Type() }
func (*Lambda) isExpr()      {}
func (l *Lambda) String() string {
	names := make([]string, len(l.Params))
	for i, p := range l.Params {
		names[i] = p.Name
	}
	return "(" + strings.Join(names, ", ") + ") -> " + l.Body.String()
}

// LambdaParam references a parameter of the innermost enclosing Lambda that
// declares it.
type LambdaParam struct {
	Name string
	T    Type
}

func (p *LambdaParam) Type() Type     { return p.T }
func (*LambdaParam) isExpr()          {}
func (p *LambdaParam) String() string { return "$" + p.Name }

// RelativeTime is the instant a time bound denotes for the query's
// reference time. It is constant within one execution.
type RelativeTime struct {
	Bound *TimeBound
}

func (*RelativeTime) Type() Type       { return Timestamp }
func (*RelativeTime) isExpr()          {}
func (r *RelativeTime) String() string { return "TIME(" + r.Bound.String() + ")" }

// InList tests membership in a list of values with = semantics: TRUE on a
// match, NULL when there is no match and X or any element is NULL, FALSE
// otherwise. Not negates the result under three-valued logic.
type InList struct {
	X    Expr
	List []Expr
	Not  bool
}

func (*InList) Type() Type { return Bool }
func (*InList) isExpr()    {}
func (in *InList) String() string {
	op := " IN ("
	if in.Not {
		op = " NOT IN ("
	}
	return paren(in.X) + op + joinExprs(in.List) + ")"
}

// SearchMode selects how a Search predicate matches text.
type SearchMode uint8

const (
	// SearchWord matches when the text contains every token of Pattern as a
	// whole token. It is answered by the term index.
	SearchWord SearchMode = iota + 1
	// SearchPrefix matches when the text contains a token starting with
	// Pattern. It is answered by a term-index range scan.
	SearchPrefix
	// SearchGlob matches when a token of the text matches the wildcard
	// pattern ('*' any run, '?' one character). It is answered by a bounded
	// term-index expansion.
	SearchGlob
	// SearchPhrase matches when the text contains Pattern as a substring that
	// starts and ends on token boundaries. The term index prunes on the
	// phrase's tokens; the substring is verified per row.
	SearchPhrase
	// SearchSubstring matches when the text contains Pattern anywhere. It is
	// verified per row; only token-free pruning applies.
	SearchSubstring
	// SearchRegex matches when the text matches the RE2 regular expression.
	SearchRegex
)

var searchModeNames = map[SearchMode]string{
	SearchWord: "word", SearchPrefix: "prefix", SearchGlob: "glob",
	SearchPhrase: "phrase", SearchSubstring: "substring", SearchRegex: "regex",
}

func (m SearchMode) String() string { return searchModeNames[m] }

// Search is a text-search predicate over one column or over the event's raw
// text. Field "" means the raw text of the event, which contains every field
// of the event as ingested. The result is NULL when the searched column is
// NULL or not a string.
type Search struct {
	Mode    SearchMode
	Field   string
	Pattern string
}

func (*Search) Type() Type { return Bool }
func (*Search) isExpr()    {}
func (s *Search) String() string {
	field := "_raw"
	if s.Field != "" {
		field = QuoteName(s.Field)
	}
	return "SEARCH_" + strings.ToUpper(s.Mode.String()) + "(" + field + ", " + strconv.Quote(s.Pattern) + ")"
}

// SubqueryMode selects how a Subquery is consumed.
type SubqueryMode uint8

const (
	// SubqueryExists is TRUE when the subquery returns at least one row.
	SubqueryExists SubqueryMode = iota + 1
	// SubqueryIn tests whether the tuple Args is among the subquery's rows,
	// with InList NULL semantics.
	SubqueryIn
	// SubqueryScalar yields the single column of the single row the
	// subquery returns, NULL for no rows, and fails for more than one row.
	SubqueryScalar
)

// Subquery embeds an uncorrelated plan in an expression.
type Subquery struct {
	Mode SubqueryMode
	Plan Node
	Args []Expr
	Not  bool
	T    Type
}

func (s *Subquery) Type() Type { return s.T }
func (*Subquery) isExpr()      {}
func (s *Subquery) String() string {
	not := ""
	if s.Not {
		not = "NOT "
	}
	switch s.Mode {
	case SubqueryExists:
		return not + "EXISTS(subquery)"
	case SubqueryIn:
		return "(" + joinExprs(s.Args) + ") " + not + "IN (subquery)"
	default:
		return "SCALAR(subquery)"
	}
}

// AggCall is an aggregate or window function application. It appears only
// in Aggregate and Window operators.
type AggCall struct {
	Func *Func
	Args []Expr
	// Filter restricts the rows the aggregate consumes; nil consumes all.
	Filter Expr
	T      Type
}

func (a *AggCall) Type() Type { return a.T }
func (*AggCall) isExpr()      {}
func (a *AggCall) String() string {
	var b strings.Builder
	b.WriteString(a.Func.Name + "(")
	b.WriteString(joinExprs(a.Args) + ")")
	if a.Filter != nil {
		b.WriteString(" FILTER (WHERE " + a.Filter.String() + ")")
	}
	return b.String()
}

func joinExprs(es []Expr) string {
	parts := make([]string, len(es))
	for i, e := range es {
		parts[i] = e.String()
	}
	return strings.Join(parts, ", ")
}

func paren(e Expr) string {
	switch e.(type) {
	case *Binary, *InList:
		return "(" + e.String() + ")"
	}
	return e.String()
}

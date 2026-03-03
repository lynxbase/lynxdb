package spl2

import (
	"fmt"
	"strings"
)

// SearchExpr is the AST for search command expressions.
// Search expressions have different semantics from WHERE expressions:
// - OR binds tighter than AND (opposite of standard)
// - Implicit AND between adjacent terms
// - Bare words search _raw field
// - Case-insensitive by default.
type SearchExpr interface {
	searchExpr()
	String() string
}

// SearchAndExpr represents: left AND right (including implicit AND).
type SearchAndExpr struct {
	Left, Right SearchExpr
}

func (*SearchAndExpr) searchExpr() {}
func (e *SearchAndExpr) String() string {
	return fmt.Sprintf("(%s AND %s)", e.Left, e.Right)
}

// SearchOrExpr represents: left OR right.
type SearchOrExpr struct {
	Left, Right SearchExpr
}

func (*SearchOrExpr) searchExpr() {}
func (e *SearchOrExpr) String() string {
	return fmt.Sprintf("(%s OR %s)", e.Left, e.Right)
}

// SearchNotExpr represents: NOT operand.
type SearchNotExpr struct {
	Operand SearchExpr
}

func (*SearchNotExpr) searchExpr() {}
func (e *SearchNotExpr) String() string {
	return fmt.Sprintf("NOT %s", e.Operand)
}

// SearchKeywordExpr represents a bare word or quoted phrase that searches _raw.
type SearchKeywordExpr struct {
	Value         string
	HasWildcard   bool
	CaseSensitive bool // CASE() directive
	IsTermMatch   bool // TERM() directive
}

func (*SearchKeywordExpr) searchExpr() {}
func (e *SearchKeywordExpr) String() string {
	if e.IsTermMatch {
		return fmt.Sprintf("TERM(%s)", e.Value)
	}
	if e.CaseSensitive {
		return fmt.Sprintf("CASE(%s)", e.Value)
	}
	if strings.Contains(e.Value, " ") {
		return fmt.Sprintf("%q", e.Value)
	}

	return e.Value
}

// CompareOp represents comparison operators in search expressions.
type CompareOp int

const (
	OpEq    CompareOp = iota // =
	OpNotEq                  // !=
	OpLt                     // <
	OpLte                    // <=
	OpGt                     // >
	OpGte                    // >=
	OpLike                   // LIKE
)

func (op CompareOp) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNotEq:
		return "!="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpLike:
		return " LIKE "
	default:
		return "?"
	}
}

// SearchCompareExpr represents: field op value.
type SearchCompareExpr struct {
	Field         string
	Op            CompareOp
	Value         string
	HasWildcard   bool
	CaseSensitive bool // value wrapped in CASE()
}

func (*SearchCompareExpr) searchExpr() {}
func (e *SearchCompareExpr) String() string {
	val := e.Value
	if e.CaseSensitive {
		val = fmt.Sprintf("CASE(%s)", val)
	}

	return fmt.Sprintf("%s%s%s", e.Field, e.Op, val)
}

// SearchInExpr represents: field IN (value, value, ...).
type SearchInExpr struct {
	Field  string
	Values []SearchInValue
}

// SearchInValue is a single value in an IN() value list.
type SearchInValue struct {
	Value       string
	HasWildcard bool
}

func (*SearchInExpr) searchExpr() {}
func (e *SearchInExpr) String() string {
	vals := make([]string, len(e.Values))
	for i, v := range e.Values {
		vals[i] = v.Value
	}

	return fmt.Sprintf("%s IN (%s)", e.Field, strings.Join(vals, ", "))
}

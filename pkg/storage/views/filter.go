package views

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// predicate is a single field comparison.
type predicate struct {
	field string
	op    string // "=", "!=", ">", ">=", "<", "<="
	value string
}

// Filter evaluates event predicates for MV dispatch.
type Filter struct {
	predicates []predicate
}

// Compile parses a filter expression like "source=nginx status>=500" into predicates.
// Multiple predicates are space-separated and all must match (AND logic).
// Empty expression matches everything.
func Compile(expr string) (*Filter, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return &Filter{}, nil
	}

	f := &Filter{}
	// Split on spaces, handling quoted values.
	parts := splitFilterExpr(expr)
	for _, part := range parts {
		p, err := parsePredicate(part)
		if err != nil {
			return nil, fmt.Errorf("views: filter: %w", err)
		}
		f.predicates = append(f.predicates, p)
	}

	return f, nil
}

// Match returns true if the event matches all filter predicates.
func (f *Filter) Match(e *event.Event) bool {
	for _, p := range f.predicates {
		if !matchPredicate(e, p) {
			return false
		}
	}

	return true
}

func parsePredicate(s string) (predicate, error) {
	// Try two-char operators first.
	for _, op := range []string{">=", "<=", "!="} {
		idx := strings.Index(s, op)
		if idx > 0 {
			return predicate{
				field: s[:idx],
				op:    op,
				value: s[idx+len(op):],
			}, nil
		}
	}
	// Single-char operators.
	for _, op := range []string{"=", ">", "<"} {
		idx := strings.Index(s, op)
		if idx > 0 {
			return predicate{
				field: s[:idx],
				op:    op,
				value: s[idx+len(op):],
			}, nil
		}
	}

	return predicate{}, fmt.Errorf("invalid predicate: %q", s)
}

func matchPredicate(e *event.Event, p predicate) bool {
	val := e.GetField(p.field)
	if val.IsNull() {
		return false
	}

	eventStr := val.String()

	// Try numeric comparison.
	eventNum, errE := strconv.ParseFloat(eventStr, 64)
	predNum, errP := strconv.ParseFloat(p.value, 64)
	if errE == nil && errP == nil {
		return compareNumeric(eventNum, p.op, predNum)
	}

	// Fall back to string comparison.
	return compareString(eventStr, p.op, p.value)
}

func compareNumeric(a float64, op string, b float64) bool {
	switch op {
	case "=":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	}

	return false
}

func compareString(a, op, b string) bool {
	switch op {
	case "=":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case ">=":
		return a >= b
	case "<":
		return a < b
	case "<=":
		return a <= b
	}

	return false
}

// splitFilterExpr splits on spaces but respects quoted values.
func splitFilterExpr(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for _, r := range s {
		switch {
		case r == '"':
			inQuote = !inQuote
		case r == ' ' && !inQuote:
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

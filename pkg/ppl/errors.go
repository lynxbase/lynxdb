package ppl

import (
	"errors"
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/ppl/ast"
	"github.com/lynxbase/lynxdb/pkg/ppl/lexer"
	"github.com/lynxbase/lynxdb/pkg/ppl/parser"
)

// ErrorKind classifies query errors for API responses.
type ErrorKind string

const (
	// ErrSyntax is a query that does not match the PPL grammar.
	ErrSyntax ErrorKind = "syntax"
	// ErrSemantic is a well-formed query that refers to unknown fields or
	// uses a command inconsistently.
	ErrSemantic ErrorKind = "semantic"
	// ErrType is an expression whose operand types are incompatible.
	ErrType ErrorKind = "type"
	// ErrUnsupportedCommand is a PPL command LynxDB does not execute.
	ErrUnsupportedCommand ErrorKind = "unsupported_command"
	// ErrUnsupportedFunction is a PPL function LynxDB does not execute.
	ErrUnsupportedFunction ErrorKind = "unsupported_function"
	// ErrUnsupportedFeature is a command option or language feature LynxDB
	// does not execute.
	ErrUnsupportedFeature ErrorKind = "unsupported_feature"
)

// Error is a PPL compilation error with its source position.
type Error struct {
	Kind ErrorKind
	Pos  lexer.Pos
	End  lexer.Pos
	Msg  string
	// Hint suggests a fix; it may be empty.
	Hint string
	// Expected lists the tokens a syntax error would have accepted.
	Expected []string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("%s error at line %d, column %d: %s", e.Kind, e.Pos.Line, e.Pos.Col, e.Msg)
	if e.Hint != "" {
		s += " (" + e.Hint + ")"
	}
	return s
}

// AsError returns the *Error inside err, converting parser syntax errors.
func AsError(err error) (*Error, bool) {
	var pe *Error
	if errors.As(err, &pe) {
		return pe, true
	}
	var se *parser.SyntaxError
	if errors.As(err, &se) {
		return &Error{Kind: ErrSyntax, Pos: se.Pos, End: se.End, Msg: se.Msg, Expected: se.Expected}, true
	}
	return nil, false
}

func errAt(kind ErrorKind, n ast.Node, format string, args ...any) *Error {
	e := &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
	if n != nil {
		sp := n.NodeSpan()
		e.Pos, e.End = sp.Start, sp.End
	}
	return e
}

func (e *Error) withHint(h string) *Error {
	e.Hint = h
	return e
}

func semanticErr(n ast.Node, format string, args ...any) *Error {
	return errAt(ErrSemantic, n, format, args...)
}

func typeErr(n ast.Node, format string, args ...any) *Error {
	return errAt(ErrType, n, format, args...)
}

func unsupportedFeature(n ast.Node, format string, args ...any) *Error {
	return errAt(ErrUnsupportedFeature, n, format, args...)
}

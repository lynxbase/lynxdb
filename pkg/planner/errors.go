package planner

import "errors"

// ParseError wraps an SPL2 parse failure with a user-facing suggestion.
type ParseError struct {
	Message    string
	Suggestion string
	Wrapped    error
}

func (e *ParseError) Error() string { return e.Message }
func (e *ParseError) Unwrap() error { return e.Wrapped }

// IsParseError reports whether err is (or wraps) a *ParseError.
func IsParseError(err error) bool {
	var pe *ParseError

	return errors.As(err, &pe)
}

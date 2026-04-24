package spl2

// ErrorCode is a stable, searchable identifier for parse and runtime errors.
// Codes follow the pattern LF-E### where the digit groups encode severity:
//
//	LF-E1xx — Unknown names (commands, functions, fields)
//	LF-E2xx — Type and value errors
//	LF-E3xx — Syntax structure errors (quotes, parens, pipes)
//	LF-E4xx — Aggregation-specific errors
//	LF-E5xx — Complexity and resource limits
//	LF-E6xx — Format and parser-specific errors
type ErrorCode string

const (
	// Unknown name errors (E1xx).
	ErrUnknownCommand  ErrorCode = "LF-E101" // Unknown command name
	ErrUnknownFunction ErrorCode = "LF-E102" // Unknown function name
	ErrUnknownField    ErrorCode = "LF-E103" // Unknown field reference
	ErrMissingPipe     ErrorCode = "LF-E104" // Known command without pipe

	// Type and value errors (E2xx).
	ErrTypeMismatch ErrorCode = "LF-E201" // Type mismatch in comparison/expression

	// Syntax structure errors (E3xx).
	ErrSyntaxError     ErrorCode = "LF-E301" // General syntax error
	ErrQuoteMismatch   ErrorCode = "LF-E302" // Unterminated string literal
	ErrParenMismatch   ErrorCode = "LF-E303" // Unmatched parenthesis
	ErrBracketMismatch ErrorCode = "LF-E304" // Unmatched bracket
	ErrEmptyPipeline   ErrorCode = "LF-E305" // Trailing pipe with no command
	ErrClauseAsCommand ErrorCode = "LF-E306" // Clause keyword used as command

	// Aggregation errors (E4xx).
	ErrMissingAgg ErrorCode = "LF-E401" // stats/timechart missing aggregation function
	ErrMissingBy  ErrorCode = "LF-E402" // stats field without BY keyword

	// Complexity errors (E5xx).
	ErrCodeQueryTooComplex ErrorCode = "LF-E501" // Expression nesting depth exceeded

	// Format errors (E6xx).
	ErrParseFormat    ErrorCode = "LF-E601"
	ErrMissingCompute ErrorCode = "LF-E602" // group/every without compute clause
)

// ParseDiagnostic carries structured error information from the parser.
// It is used to produce Rust-style error messages with caret positioning,
// suggestions, and deep-dive references.
type ParseDiagnostic struct {
	Code       ErrorCode
	Message    string // one-line summary
	Position   int    // byte offset in query (-1 if unknown)
	Length     int    // token length for caret span (>= 1)
	Suggestion string // "Did you mean: ..." or "Hint: ..."
}

// ExplainRef returns the command to run for a detailed explanation of this error.
func (d *ParseDiagnostic) ExplainRef() string {
	return "lynxdb explain-error " + string(d.Code)
}

// ClassifyError examines an error message string and returns the most
// specific ErrorCode that applies. This avoids touching 200+ parser
// call sites — existing errorf() strings are pattern-matched here.
func ClassifyError(msg string) ErrorCode {
	lower := toLower(msg)

	// Unknown command.
	if contains(lower, "unexpected command") {
		return ErrUnknownCommand
	}

	// Unknown function (token in expression context that isn't a command).
	if contains(lower, "unexpected token ident") {
		return ErrUnknownFunction
	}

	// Missing pipe — known command used without preceding pipe.
	if contains(lower, "missing pipe") || (contains(lower, "without pipe")) {
		return ErrMissingPipe
	}

	// Clause used as standalone command.
	if contains(lower, "is a clause") || (contains(lower, "clause") && contains(lower, "command")) {
		return ErrClauseAsCommand
	}

	// Stats/timechart aggregation errors.
	if contains(lower, "stats") && (contains(lower, "aggregat") || contains(lower, "function") || contains(lower, "expected")) {
		if !contains(lower, "by") {
			return ErrMissingAgg
		}
	}

	// Empty pipeline (trailing pipe).
	if contains(lower, "eof") && (contains(lower, "expected") || contains(lower, "unexpected")) {
		return ErrEmptyPipeline
	}

	// Quote / string errors.
	if contains(lower, "unterminated string") || contains(lower, "unclosed string") {
		return ErrQuoteMismatch
	}

	// Parenthesis mismatch.
	if contains(lower, "expected )") || contains(lower, `expected ")"`) {
		return ErrParenMismatch
	}

	// Bracket mismatch.
	if contains(lower, "expected ]") || contains(lower, `expected "]"`) {
		return ErrBracketMismatch
	}

	// Type mismatch.
	if contains(lower, "type mismatch") || contains(lower, "cannot compare") {
		return ErrTypeMismatch
	}

	// Query too complex.
	if contains(lower, "too complex") || contains(lower, "nesting depth") {
		return ErrCodeQueryTooComplex
	}

	if contains(lower, "parse") && contains(lower, "format") {
		return ErrParseFormat
	}

	// Fallback.
	return ErrSyntaxError
}

// toLower is a simple ASCII lowercase for error classification.
func toLower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 32
		}
	}

	return string(b)
}

// contains is a convenience wrapper around strings.Contains.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}

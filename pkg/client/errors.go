// Package client provides a typed Go HTTP client for the LynxDB API.
package client

import (
	"errors"
	"fmt"
)

// ErrorCode is a machine-readable SCREAMING_SNAKE error code returned by the server.
type ErrorCode string

const (
	ErrCodeInvalidJSON         ErrorCode = "INVALID_JSON"
	ErrCodeInvalidRequest      ErrorCode = "INVALID_REQUEST"
	ErrCodeValidationError     ErrorCode = "VALIDATION_ERROR"
	ErrCodeNotFound            ErrorCode = "NOT_FOUND"
	ErrCodeTooManyRequests     ErrorCode = "TOO_MANY_REQUESTS"
	ErrCodeAuthRequired        ErrorCode = "AUTH_REQUIRED"
	ErrCodeInvalidToken        ErrorCode = "INVALID_TOKEN"
	ErrCodeForbidden           ErrorCode = "FORBIDDEN"
	ErrCodeLastRootKey         ErrorCode = "LAST_ROOT_KEY"
	ErrCodeInternalError       ErrorCode = "INTERNAL_ERROR"
	ErrCodeQueryMemoryExceeded ErrorCode = "QUERY_MEMORY_EXCEEDED"
	ErrCodeSyntaxError         ErrorCode = "SYNTAX_ERROR"
	ErrCodeSemanticError       ErrorCode = "SEMANTIC_ERROR"
	ErrCodeTypeError           ErrorCode = "TYPE_ERROR"
	ErrCodeUnsupportedCommand  ErrorCode = "UNSUPPORTED_COMMAND"
	ErrCodeUnsupportedFunction ErrorCode = "UNSUPPORTED_FUNCTION"
	ErrCodeUnsupportedFeature  ErrorCode = "UNSUPPORTED_FEATURE"
	ErrCodePlanningError       ErrorCode = "PLANNING_ERROR"
	ErrCodeExecutionError      ErrorCode = "EXECUTION_ERROR"
)

// APIError represents a structured error response from the LynxDB API.
type APIError struct {
	// HTTPStatus is the HTTP status code returned by the server.
	HTTPStatus int
	// Code is the machine-readable error code.
	Code ErrorCode
	// Message is the human-readable error message.
	Message string
	// Suggestion is an optional hint for how to fix the error.
	Suggestion string
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("lynxdb: %s: %s (HTTP %d)", e.Code, e.Message, e.HTTPStatus)
	}

	return fmt.Sprintf("lynxdb: %s (HTTP %d)", e.Message, e.HTTPStatus)
}

// IsNotFound reports whether the error is a NOT_FOUND API error.
func IsNotFound(err error) bool {
	return hasCode(err, ErrCodeNotFound)
}

// IsAuthRequired reports whether the error is an AUTH_REQUIRED API error.
func IsAuthRequired(err error) bool {
	return hasCode(err, ErrCodeAuthRequired)
}

// IsRateLimited reports whether the error is a TOO_MANY_REQUESTS API error.
func IsRateLimited(err error) bool {
	return hasCode(err, ErrCodeTooManyRequests)
}

// IsInvalidQuery reports whether the server rejected the query text: a
// syntax, semantic, type, unsupported or planning error.
func IsInvalidQuery(err error) bool {
	for _, code := range []ErrorCode{
		ErrCodeSyntaxError, ErrCodeSemanticError, ErrCodeTypeError,
		ErrCodeUnsupportedCommand, ErrCodeUnsupportedFunction, ErrCodeUnsupportedFeature, ErrCodePlanningError,
	} {
		if hasCode(err, code) {
			return true
		}
	}
	return false
}

// IsValidationError reports whether the error is a VALIDATION_ERROR API error.
func IsValidationError(err error) bool {
	return hasCode(err, ErrCodeValidationError)
}

// IsQueryMemoryExceeded reports whether the error is a QUERY_MEMORY_EXCEEDED API error.
func IsQueryMemoryExceeded(err error) bool {
	return hasCode(err, ErrCodeQueryMemoryExceeded)
}

func hasCode(err error, code ErrorCode) bool {
	var apiErr *APIError

	return errors.As(err, &apiErr) && apiErr.Code == code
}

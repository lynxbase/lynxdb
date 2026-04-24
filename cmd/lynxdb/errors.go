package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"syscall"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// queryError wraps an error with the original query string so that
// renderError can display a caret pointing at the error position.
type queryError struct {
	inner error
	query string
}

func (e *queryError) Error() string { return e.inner.Error() }
func (e *queryError) Unwrap() error { return e.inner }

// isConnectionError reports whether err is a network connection error.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return true
	}

	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	msg := err.Error()

	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "dial tcp")
}

// isTimeoutError reports whether err is a timeout or deadline exceeded error.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	msg := err.Error()

	return strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "timeout exceeded") ||
		strings.Contains(msg, "query timed out") ||
		strings.Contains(msg, "Client.Timeout")
}

// isAuthError reports whether err is an authentication or authorization error.
func isAuthError(err error) bool {
	if err == nil {
		return false
	}

	if client.IsAuthRequired(err) {
		return true
	}

	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case client.ErrCodeAuthRequired, client.ErrCodeInvalidToken, client.ErrCodeForbidden:
			return true
		}
	}

	return false
}

// isQueryParseError reports whether err is a query parse error.
func isQueryParseError(err error) bool {
	if err == nil {
		return false
	}

	if client.IsInvalidQuery(err) {
		return true
	}

	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		return apiErr.HTTPStatus == 400
	}

	msg := err.Error()

	return strings.Contains(msg, "parse error") ||
		strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "syntax error") ||
		strings.Contains(msg, "spl2: ") ||
		strings.Contains(msg, "search: unexpected token")
}

// renderError prints a formatted error to stderr.
func renderError(err error) {
	if err == nil {
		return
	}

	var qe *queryError
	if errors.As(err, &qe) {
		var apiErr *client.APIError
		if errors.As(qe.inner, &apiErr) && apiErr.Code == client.ErrCodeInvalidQuery {
			renderQueryParseError(apiErr, qe.query)

			return
		}

		// Not a parse error — fall through to standard rendering
		// with the inner error (unwrap the queryError wrapper).
		renderError(qe.inner)

		return
	}

	if isMemoryError(err) {
		renderMemoryError(err)

		return
	}

	if isConnectionError(err) {
		renderConnectionError()

		return
	}

	if isTimeoutError(err) {
		renderTimeoutError()

		return
	}

	if isAuthError(err) {
		renderAuthError(err)

		return
	}

	if isRequiredFlagError(err) {
		renderRequiredFlagError(err)

		return
	}

	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		renderAPIError(apiErr)

		return
	}

	ui.Stderr.RenderError(err)
}

// isMemoryError reports whether err is a query memory or pool exhaustion error.
func isMemoryError(err error) bool {
	return client.IsQueryMemoryExceeded(err) || client.IsQueryPoolExhausted(err)
}

// renderMemoryError prints a memory-related query error with actionable next steps.
func renderMemoryError(err error) {
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		renderAPIError(apiErr)

		if client.IsQueryMemoryExceeded(err) {
			printNextSteps(
				"lynxdb query '... | head 1000'         Limit results",
				"lynxdb query '...' --since 1h          Narrow time range",
				"lynxdb explain '...'                    Check query cost",
			)
		}

		return
	}

	ui.Stderr.RenderError(err)
}

// renderConnectionError prints a connection error with helpful suggestions.
func renderConnectionError() {
	ui.Stderr.RenderConnectionError(globalServer)
}

// renderTimeoutError prints a timeout error with actionable next steps.
func renderTimeoutError() {
	ui.Stderr.RenderError(fmt.Errorf("query timed out"))
	printNextSteps(
		"lynxdb query '...' --timeout 10m     Increase timeout",
		"lynxdb query '... | head 1000'       Limit result count",
		"lynxdb explain '...'                 Check query cost first",
		"lynxdb mv create <name> '...'        Materialize for repeated queries",
	)
}

// renderAuthError prints an authentication error with next steps.
func renderAuthError(err error) {
	msg := "authentication failed"

	var apiErr *client.APIError
	if errors.As(err, &apiErr) && apiErr.Message != "" {
		msg = apiErr.Message
	}

	ui.Stderr.RenderError(fmt.Errorf("%s", msg))
	printNextSteps(
		"lynxdb login                         Authenticate with server",
		"lynxdb query --token <key> '...'     Pass token directly",
		"export LYNXDB_TOKEN=<key>            Set token via environment",
	)
}

// renderAPIError prints an API error with code, message, and optional suggestion.
// For INVALID_QUERY errors, attempts field name fuzzy matching.
func renderAPIError(ae *client.APIError) {
	code := string(ae.Code)
	if code == "" {
		code = fmt.Sprintf("HTTP_%d", ae.HTTPStatus)
	}

	suggestion := ae.Suggestion

	// Fuzzy-match field names when the server returns an invalid-query error without a suggestion.
	if suggestion == "" && ae.Code == client.ErrCodeInvalidQuery {
		if fieldSuggestion := tryFieldNameSuggestion(ae.Message); fieldSuggestion != "" {
			suggestion = fieldSuggestion
		}
	}

	ui.Stderr.RenderServerError(code, ae.Message, suggestion)
}

// tryFieldNameSuggestion attempts to extract a field name from an error message
// and suggest a similar known field name via fuzzy matching.
func tryFieldNameSuggestion(message string) string {
	// Look for patterns like "unknown field 'xyz'" or "field 'xyz' not found".
	fieldName := extractFieldFromError(message)
	if fieldName == "" {
		return ""
	}

	fields := fetchFieldCatalog()
	if len(fields) == 0 {
		return ""
	}

	suggestion := suggestFieldName(fieldName, fields)
	if suggestion == "" {
		return ""
	}

	return fmt.Sprintf("Did you mean: %s? Run 'lynxdb fields' to see all known fields", suggestion)
}

// extractFieldFromError tries to extract a field name from a parse error message.
func extractFieldFromError(message string) string {
	lower := strings.ToLower(message)

	// Common patterns: "unknown field 'X'", "field 'X' not found", "no such field 'X'".
	patterns := []string{"unknown field '", "field '", "no such field '"}

	for _, p := range patterns {
		idx := strings.Index(lower, p)
		if idx < 0 {
			continue
		}

		start := idx + len(p)
		end := strings.IndexByte(message[start:], '\'')

		if end > 0 {
			return message[start : start+end]
		}
	}

	return ""
}

// renderQueryError prints a query parse error with caret positioning.
func renderQueryError(query string, position, length int, message, suggestion string) {
	ui.Stderr.RenderQueryError(query, position, length, message, suggestion)
}

// renderQueryParseError renders an INVALID_QUERY API error with caret display
// by extracting position information from the error message and classifying
// the error code.
func renderQueryParseError(ae *client.APIError, query string) {
	pos, length := extractPositionFromError(ae.Message)

	suggestion := ae.Suggestion
	if suggestion == "" {
		fieldNames := fetchFieldNames()
		suggestion = spl2.SuggestFix(ae.Message, fieldNames)
	}

	if suggestion == "" {
		if fieldSuggestion := tryFieldNameSuggestion(ae.Message); fieldSuggestion != "" {
			suggestion = fieldSuggestion
		}
	}

	code := string(spl2.ClassifyError(ae.Message))
	ui.Stderr.RenderQueryErrorWithCode(query, pos, length, ae.Message, suggestion, code)
	if code != "" {
		printHint("Run: lynxdb explain-error %s", code)
	}
}

// isRequiredFlagError reports whether err is a cobra "required flag(s) not set" error.
func isRequiredFlagError(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	return strings.Contains(msg, "required flag(s)") && strings.Contains(msg, "not set")
}

// parseRequiredFlags extracts flag names from a cobra required-flag error message.
// Cobra formats: `required flag(s) "name" not set` or `required flag(s) "name", "query" not set`.
func parseRequiredFlags(err error) []string {
	if err == nil {
		return nil
	}

	msg := err.Error()

	start := strings.Index(msg, `"`)
	end := strings.LastIndex(msg, `"`)

	if start < 0 || end <= start {
		return nil
	}

	// Extract the substring between first and last quote: name", "query
	inner := msg[start+1 : end]

	// Split on `", "` for multiple flags, otherwise single flag.
	parts := strings.Split(inner, `", "`)

	flags := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			flags = append(flags, p)
		}
	}

	return flags
}

// renderRequiredFlagError prints a formatted error for missing required flags,
// resolving the target command to display its usage line and examples.
func renderRequiredFlagError(err error) {
	flags := parseRequiredFlags(err)
	if len(flags) == 0 {
		// Fallback: unrecognized format, render generic.
		ui.Stderr.RenderError(err)

		return
	}

	var usageLine, example string

	// Resolve the command that failed by walking os.Args through the command tree.
	if cmd, _, findErr := rootCmd.Find(os.Args[1:]); findErr == nil && cmd != nil {
		usageLine = cmd.UseLine()
		example = cmd.Example
	}

	ui.Stderr.RenderRequiredFlagError(flags, usageLine, example)
}

// extractPositionFromError parses "at position N" from an error message and
// extracts the token literal length for the caret span.
// Returns (position, length) where position is -1 if not found.
func extractPositionFromError(msg string) (int, int) {
	const marker = "at position "
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return -1, 1
	}

	rest := msg[idx+len(marker):]
	pos := 0
	hasDigit := false

	for _, c := range rest {
		if c >= '0' && c <= '9' {
			pos = pos*10 + int(c-'0')
			hasDigit = true
		} else {
			break
		}
	}

	if !hasDigit {
		return -1, 1
	}

	// Try to extract the token literal length from the quoted token in the message.
	// Error messages often contain: IDENT "level" or STRING "foo"
	length := 1
	if qIdx := strings.LastIndex(msg, "\""); qIdx > 0 {
		// Find the opening quote before the last closing quote.
		sub := msg[:qIdx]
		if oIdx := strings.LastIndex(sub, "\""); oIdx >= 0 {
			tokenLit := msg[oIdx+1 : qIdx]
			if tokenLit != "" && len(tokenLit) < 100 {
				length = len(tokenLit)
			}
		}
	}

	return pos, length
}

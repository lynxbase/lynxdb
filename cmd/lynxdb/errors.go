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
)

// queryError wraps an error with the original query string so that
// renderError can display a caret pointing at the error position.
type queryError struct {
	inner error
	query string
}

func (e *queryError) Error() string { return e.inner.Error() }
func (e *queryError) Unwrap() error { return e.inner }

type noFilesMatchingError struct {
	pattern string
}

func (e noFilesMatchingError) Error() string {
	return "no files matching: " + e.pattern
}

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
		strings.Contains(msg, "semantic error") ||
		strings.Contains(msg, "type error") ||
		strings.Contains(msg, "unsupported_")
}

// renderError prints a formatted error to stderr.
func renderError(err error) {
	renderErrorWithLynx(err, true)
}

func renderErrorWithLynx(err error, showLynx bool) {
	if err == nil {
		return
	}

	if showLynx {
		renderSadLynxForError()
	}

	var qe *queryError
	if errors.As(err, &qe) {
		var apiErr *client.APIError
		if errors.As(qe.inner, &apiErr) && client.IsInvalidQuery(apiErr) {
			renderQueryParseError(apiErr, qe.query)

			return
		}

		// Not a parse error — fall through to standard rendering
		// with the inner error (unwrap the queryError wrapper).
		renderErrorWithLynx(qe.inner, false)

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

	var noFiles noFilesMatchingError
	if errors.As(err, &noFiles) {
		renderNoFilesMatchingError(noFiles)

		return
	}

	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		renderAPIError(apiErr)

		return
	}

	ui.Stderr.RenderError(err)
}

func renderSadLynxForError() {
	if globalQuiet || !isTTY() {
		return
	}

	ui.WriteLynxMark(os.Stderr, ui.Stderr, ui.LynxSad, shouldAnimateLynx())
}

func renderNoFilesMatchingError(err noFilesMatchingError) {
	t := ui.Stderr
	cwd, cwdErr := os.Getwd()
	if cwdErr != nil {
		cwd = "."
	}

	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s %s\n", t.Info.Render("i"), t.Bold.Render("No files matched"))
	fmt.Fprintf(os.Stderr, "    Pattern: %s\n", t.Info.Render(err.pattern))
	fmt.Fprintf(os.Stderr, "    Directory: %s\n", t.Dim.Render(cwd))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "    %s\n", t.Dim.Render("Try:"))
	fmt.Fprintf(os.Stderr, "      %s\n", t.Info.Render("lynxdb query --file './*.log' 'search source=main error'"))
	fmt.Fprintf(os.Stderr, "      %s\n", t.Info.Render("cat app.log | lynxdb query 'search source=main error'"))
	fmt.Fprintf(os.Stderr, "      %s\n", t.Info.Render("ls -la"))
	fmt.Fprintln(os.Stderr)
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
func renderAPIError(ae *client.APIError) {
	code := string(ae.Code)
	if code == "" {
		code = fmt.Sprintf("HTTP_%d", ae.HTTPStatus)
	}

	ui.Stderr.RenderServerError(code, ae.Message, ae.Suggestion)
}

// renderQueryError prints a query parse error with caret positioning.
func renderQueryError(query string, position, length int, message, suggestion string) {
	ui.Stderr.RenderQueryError(query, position, length, message, suggestion)
}

// renderQueryParseError renders a query compile error from the server with
// a caret under the position named in its message.
func renderQueryParseError(ae *client.APIError, query string) {
	pos := extractPositionFromError(ae.Message)
	ui.Stderr.RenderQueryErrorWithCode(query, pos, 1, ae.Message, ae.Suggestion, string(ae.Code))
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

// extractPositionFromError returns the 0-based column of a compile error at
// line 1, or -1 when the message names no such position.
func extractPositionFromError(msg string) int {
	_, rest, ok := strings.Cut(msg, "at line 1, column ")
	if !ok {
		return -1
	}
	col := 0
	for _, c := range rest {
		if c < '0' || c > '9' {
			break
		}
		col = col*10 + int(c-'0')
	}

	return col - 1
}

package ui

import (
	"fmt"
	"strings"
)

// PrintSuccess prints a green "check" message to the theme's writer.
func (t *Theme) PrintSuccess(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(t.w, "  %s %s\n", t.StatusOK.Render("\u2714"), msg)
}

// PrintWarning prints a yellow warning message to the theme's writer.
func (t *Theme) PrintWarning(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(t.w, "  %s  %s\n", t.StatusWarn.Render("\u26a0"), msg)
}

// PrintHint prints a dim hint to the theme's writer (TTY-only).
func (t *Theme) PrintHint(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(t.w, "  %s\n", t.Dim.Render(msg))
}

// PrintMeta prints metadata to the theme's writer.
func (t *Theme) PrintMeta(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	fmt.Fprintf(t.w, format+"\n", args...)
}

// PrintNextSteps prints dim "Next steps:" hints to the theme's writer.
func (t *Theme) PrintNextSteps(quiet bool, steps ...string) {
	if quiet || len(steps) == 0 {
		return
	}
	fmt.Fprintln(t.w)
	fmt.Fprintf(t.w, "  %s\n", t.Dim.Render("Next steps:"))
	for _, s := range steps {
		fmt.Fprintf(t.w, "  %s\n", t.Dim.Render("  "+s))
	}
}

// IconOK returns a green check mark.
func (t *Theme) IconOK() string { return t.StatusOK.Render("\u2714") }

// IconWarn returns a yellow warning sign.
func (t *Theme) IconWarn() string { return t.StatusWarn.Render("\u26a0") }

// IconError returns a red cross mark.
func (t *Theme) IconError() string { return t.StatusErr.Render("\u2716") }

// KeyValue returns a formatted "  Label:  Value" string.
func (t *Theme) KeyValue(key, value string) string {
	return fmt.Sprintf("  %-14s %s", t.Bold.Render(key), value)
}

// HRule returns a horizontal rule of the given width, styled dim.
func (t *Theme) HRule(width int) string {
	if width <= 0 {
		width = 50
	}

	return t.Rule.Render(strings.Repeat("\u2500", width))
}

// RenderError prints a formatted generic error to the theme's writer.
func (t *Theme) RenderError(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(t.w, "\n  %s %s\n\n", t.IconError(), err.Error())
}

// RenderConnectionError prints a connection error with helpful suggestions.
func (t *Theme) RenderConnectionError(server string) {
	fmt.Fprintf(t.w, "\n  %s Cannot connect to %s\n\n", t.IconError(), server)
	fmt.Fprintln(t.w, `    Is the server running? Try:
      lynxdb server              Start the server
      lynxdb doctor              Check environment

    Or query local files without a server:
      lynxdb query --file app.log 'level=error'

    Using a different server? Set --server:
      lynxdb query --server http://logs:3100 'level=error'`)
	fmt.Fprintln(t.w)
}

// RenderServerError prints a server error with code, message, and optional suggestion.
func (t *Theme) RenderServerError(code, message, suggestion string) {
	fmt.Fprintf(t.w, "\n  %s %s: %s\n", t.IconError(), code, message)
	if suggestion != "" {
		fmt.Fprintf(t.w, "    %s\n", t.Info.Render(suggestion))
	}
	fmt.Fprintln(t.w)
}

// RenderRequiredFlagError prints a formatted error for missing required CLI flags,
// with usage and example hints matching the style of other error renderers.
func (t *Theme) RenderRequiredFlagError(flags []string, usageLine, example string) {
	noun := "flag"
	if len(flags) > 1 {
		noun = "flags"
	}

	formatted := make([]string, len(flags))
	for i, f := range flags {
		formatted[i] = "--" + f
	}

	fmt.Fprintf(t.w, "\n  %s missing required %s: %s\n", t.IconError(), noun, strings.Join(formatted, ", "))

	if usageLine != "" {
		fmt.Fprintf(t.w, "\n  %s\n    %s\n", t.Bold.Render("Usage:"), usageLine)
	}

	if example != "" {
		fmt.Fprintf(t.w, "\n  %s\n%s\n", t.Bold.Render("Examples:"), example)
	}

	fmt.Fprintln(t.w)
}

// RenderQueryError prints a query parse error with caret positioning and error code.
func (t *Theme) RenderQueryError(query string, position, length int, message, suggestion string) {
	t.renderQueryErrorCode(query, position, length, message, suggestion, "")
}

// RenderQueryErrorWithCode prints a query parse error with an explicit error code prefix.
func (t *Theme) RenderQueryErrorWithCode(query string, position, length int, message, suggestion, code string) {
	t.renderQueryErrorCode(query, position, length, message, suggestion, code)
}

// renderQueryErrorCode prints a query parse error with caret positioning, error code, and suggestion.
func (t *Theme) renderQueryErrorCode(query string, position, length int, message, suggestion, code string) {
	prefix := "INVALID_QUERY"
	if code != "" {
		prefix = code
	}
	fmt.Fprintf(t.w, "\n  %s %s: %s\n\n", t.IconError(), prefix, message)
	fmt.Fprintf(t.w, "    %s\n", query)

	// Always show at least one caret character when position is valid.
	if length <= 0 {
		length = 1
	}

	if position >= 0 {
		caret := strings.Repeat(" ", position) + strings.Repeat("^", length)
		fmt.Fprintf(t.w, "    %s\n", t.Error.Render(caret))
	}

	if suggestion != "" {
		// Use "Hint:" prefix when the suggestion is a full sentence,
		// "Did you mean:" when it's a short replacement.
		prefix := "Hint: "
		if !strings.Contains(suggestion, " ") || len(suggestion) < 30 {
			prefix = "Did you mean: "
		}

		fmt.Fprintf(t.w, "    %s\n", t.Info.Render(prefix+suggestion))
	}

	fmt.Fprintln(t.w)
}

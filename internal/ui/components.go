package ui

import (
	"fmt"
	"strings"
)

// Metric is one value in a compact human-readable summary grid.
type Metric struct {
	Label string
	Value string
	Hint  string
	Warn  bool
}

// Diagnostic describes an actionable error panel.
type Diagnostic struct {
	Code       string
	Message    string
	Detail     string
	Suggestion string
	Commands   []string
}

// Section returns a titled block suitable for command/status output.
func (t *Theme) Section(title string, lines ...string) string {
	var b strings.Builder
	if title != "" {
		b.WriteString("  ")
		b.WriteString(t.SectionTitle.Render(title))
		b.WriteByte('\n')
	}
	for _, line := range lines {
		if line == "" {
			b.WriteByte('\n')
			continue
		}
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteByte('\n')
	}

	return strings.TrimRight(b.String(), "\n")
}

// MetricGrid renders label/value pairs in a dense, terminal-friendly grid.
func (t *Theme) MetricGrid(metrics []Metric, compact bool) string {
	if len(metrics) == 0 {
		return ""
	}

	labelWidth := 0
	for _, m := range metrics {
		if len(m.Label) > labelWidth {
			labelWidth = len(m.Label)
		}
	}
	if labelWidth > 18 {
		labelWidth = 18
	}

	var b strings.Builder
	for i, m := range metrics {
		valueStyle := t.MetricValue
		if m.Warn {
			valueStyle = t.Warning
		}
		fmt.Fprintf(&b, "  %-*s  %s", labelWidth, t.MetricLabel.Render(m.Label), valueStyle.Render(m.Value))
		if !compact && m.Hint != "" {
			fmt.Fprintf(&b, "  %s", t.Muted.Render(m.Hint))
		}
		if i < len(metrics)-1 {
			b.WriteByte('\n')
		}
	}

	return b.String()
}

// EmptyState renders a consistent empty result block with optional next steps.
func (t *Theme) EmptyState(title string, steps ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  %s %s\n", t.Info.Render("i"), t.Bold.Render(title))
	if len(steps) > 0 {
		fmt.Fprintf(&b, "\n  %s\n", t.Dim.Render("Next steps:"))
		for _, step := range steps {
			fmt.Fprintf(&b, "    %s\n", t.Dim.Render(step))
		}
	}

	return strings.TrimRight(b.String(), "\n")
}

// Diagnostic renders an actionable error block without changing exit behavior.
func (t *Theme) Diagnostic(d Diagnostic) string {
	var b strings.Builder
	code := d.Code
	if code == "" {
		code = "ERROR"
	}
	fmt.Fprintf(&b, "\n  %s %s: %s\n", t.IconError(), t.Error.Render(code), d.Message)
	if d.Detail != "" {
		fmt.Fprintf(&b, "    %s\n", d.Detail)
	}
	if d.Suggestion != "" {
		fmt.Fprintf(&b, "\n    %s %s\n", t.Hint.Render("Hint:"), d.Suggestion)
	}
	if len(d.Commands) > 0 {
		fmt.Fprintf(&b, "\n    %s\n", t.Dim.Render("Try:"))
		for _, cmd := range d.Commands {
			fmt.Fprintf(&b, "      %s\n", t.Info.Render(cmd))
		}
	}
	b.WriteByte('\n')

	return b.String()
}

func (t *Theme) printf(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(t.w, format, args...)
}

func (t *Theme) print(args ...interface{}) {
	_, _ = fmt.Fprint(t.w, args...)
}

func (t *Theme) println(args ...interface{}) {
	_, _ = fmt.Fprintln(t.w, args...)
}

// PrintSuccess prints a green "check" message to the theme's writer.
func (t *Theme) PrintSuccess(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	msg := fmt.Sprintf(format, args...)
	t.printf("  %s %s\n", t.StatusOK.Render("\u2714"), msg)
}

// PrintWarning prints a yellow warning message to the theme's writer.
func (t *Theme) PrintWarning(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	msg := fmt.Sprintf(format, args...)
	t.printf("  %s  %s\n", t.StatusWarn.Render("\u26a0"), msg)
}

// PrintHint prints a dim hint to the theme's writer (TTY-only).
func (t *Theme) PrintHint(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	msg := fmt.Sprintf(format, args...)
	t.printf("  %s\n", t.Dim.Render(msg))
}

// PrintMeta prints metadata to the theme's writer.
func (t *Theme) PrintMeta(quiet bool, format string, args ...interface{}) {
	if quiet {
		return
	}
	t.printf(format+"\n", args...)
}

// PrintNextSteps prints dim "Next steps:" hints to the theme's writer.
func (t *Theme) PrintNextSteps(quiet bool, steps ...string) {
	if quiet || len(steps) == 0 {
		return
	}
	t.println()
	t.printf("  %s\n", t.Dim.Render("Next steps:"))
	for _, s := range steps {
		t.printf("  %s\n", t.Dim.Render("  "+s))
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
	t.print(t.Diagnostic(Diagnostic{
		Code:    "ERROR",
		Message: err.Error(),
	}))
}

// RenderConnectionError prints a connection error with helpful suggestions.
func (t *Theme) RenderConnectionError(server string) {
	t.print(t.Diagnostic(Diagnostic{
		Code:       "CONNECTION",
		Message:    "Cannot connect to " + server,
		Suggestion: "Start the server, check the environment, or point --server at the right endpoint.",
		Commands: []string{
			"lynxdb server",
			"lynxdb query --file app.log 'search source=main error'",
			"lynxdb query --server http://logs:3100 'search source=main error'",
		},
	}))
}

// RenderServerError prints a server error with code, message, and optional suggestion.
func (t *Theme) RenderServerError(code, message, suggestion string) {
	t.print(t.Diagnostic(Diagnostic{
		Code:       code,
		Message:    message,
		Suggestion: suggestion,
	}))
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

	t.printf("\n  %s missing required %s: %s\n", t.IconError(), noun, strings.Join(formatted, ", "))

	if usageLine != "" {
		t.printf("\n  %s\n    %s\n", t.Bold.Render("Usage:"), usageLine)
	}

	if example != "" {
		t.printf("\n  %s\n%s\n", t.Bold.Render("Examples:"), example)
	}

	t.println()
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
	t.printf("\n  %s %s: %s\n\n", t.IconError(), prefix, message)
	t.printf("    %s\n", query)

	// Always show at least one caret character when position is valid.
	if length <= 0 {
		length = 1
	}

	if position >= 0 {
		caret := strings.Repeat(" ", position) + strings.Repeat("^", length)
		t.printf("    %s\n", t.Error.Render(caret))
	}

	if suggestion != "" {
		// Use "Hint:" prefix when the suggestion is a full sentence,
		// "Did you mean:" when it's a short replacement.
		prefix := "Hint: "
		if !strings.Contains(suggestion, " ") || len(suggestion) < 30 {
			prefix = "Did you mean: "
		}

		t.printf("    %s\n", t.Info.Render(prefix+suggestion))
	}

	t.println()
}

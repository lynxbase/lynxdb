package shell

import (
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"

	"github.com/lynxbase/lynxdb/internal/output"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

// ZeroResultContext provides data for zero-result guidance tips.
type ZeroResultContext struct {
	Session    *Session
	FieldInfos []client.FieldInfo
}

// Results wraps a viewport for scrollable query history and results.
type Results struct {
	viewport viewport.Model
	entries  []string
	theme    *ShellTheme
}

// NewResults creates a results viewport.
func NewResults(width, height int) Results {
	vp := viewport.New(viewport.WithWidth(width), viewport.WithHeight(height))

	return Results{
		viewport: vp,
		theme:    NewShellTheme(),
	}
}

// SetSize updates the viewport dimensions.
func (r *Results) SetSize(width, height int) {
	r.viewport.SetWidth(width)
	r.viewport.SetHeight(height)
}

func (r *Results) Update(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	r.viewport, cmd = r.viewport.Update(msg)

	return cmd
}

// AppendResult adds a query + result to the scrollback.
func (r *Results) AppendResult(query string, rows []map[string]interface{},
	elapsed time.Duration, err error, width int,
	format output.Format, showTiming bool, hints string,
	zeroCtx *ZeroResultContext) {
	t := ui.Stdout

	// Highlighted query text.
	styledQuery := fmt.Sprintf("  %s %s",
		t.Dim.Render(">"),
		HighlightSPL2(query, r.theme))
	r.entries = append(r.entries, styledQuery)

	// Compat hints (SPL1 → SPL2 suggestions).
	if hints != "" {
		r.entries = append(r.entries, "  "+t.Warning.Render(strings.TrimSpace(hints)))
	}

	if err != nil {
		r.appendStyledError(err.Error())
	} else if len(rows) == 0 {
		r.appendZeroGuidance(query, zeroCtx)
	} else {
		// Render results in the configured format.
		tableStr := renderResultRows(rows, width, format)
		r.entries = append(r.entries, tableStr)

		// Stats line.
		word := "rows"
		if len(rows) == 1 {
			word = "row"
		}

		if showTiming {
			statsLine := fmt.Sprintf("  %s", t.Dim.Render(
				fmt.Sprintf("%d %s \u2014 %s", len(rows), word, formatElapsedShell(elapsed))))
			r.entries = append(r.entries, statsLine, "")
		} else {
			statsLine := fmt.Sprintf("  %s", t.Dim.Render(
				fmt.Sprintf("%d %s", len(rows), word)))
			r.entries = append(r.entries, statsLine, "")
		}
	}

	r.updateContent()
}

// AppendText adds a plain text block to the scrollback.
func (r *Results) AppendText(text string) {
	r.entries = append(r.entries, text, "")
	r.updateContent()
}

// Clear removes all entries.
func (r *Results) Clear() {
	r.entries = nil
	r.updateContent()
}

// View renders the viewport.
func (r *Results) View() string {
	return r.viewport.View()
}

func (r *Results) updateContent() {
	content := strings.Join(r.entries, "\n")
	r.viewport.SetContent(content)
	r.viewport.GotoBottom()
}

// renderResultRows renders rows using the given format.
func renderResultRows(rows []map[string]interface{}, width int, format output.Format) string {
	if len(rows) == 0 {
		return ""
	}

	f := output.DetectFormat(format, rows, ui.Stdout)

	var b strings.Builder
	_ = f.Format(&b, rows)

	return b.String()
}

// appendZeroGuidance renders "No results." with contextual tips.
func (r *Results) appendZeroGuidance(query string, zeroCtx *ZeroResultContext) {
	t := ui.Stdout
	r.entries = append(r.entries, "  "+t.Dim.Render("No results."))

	if zeroCtx == nil {
		r.entries = append(r.entries, "")
		return
	}

	var tips []string

	// Time range hint.
	if zeroCtx.Session != nil && zeroCtx.Session.Since != "" {
		tips = append(tips, fmt.Sprintf("Time range: last %s. Try /since 24h or /since 7d",
			zeroCtx.Session.Since))
	}

	// Field value suggestions.
	if field, _, ok := detectFieldFilter(query); ok {
		for _, fi := range zeroCtx.FieldInfos {
			if strings.EqualFold(fi.Name, field) && len(fi.TopValues) > 0 {
				vals := topValueStrings(fi.TopValues, 5)
				tips = append(tips, fmt.Sprintf("Known values for %s: %s",
					field, strings.Join(vals, ", ")))

				break
			}
		}
	}

	for _, tip := range tips {
		r.entries = append(r.entries, "  "+t.Info.Render("\u2022 "+tip))
	}

	r.entries = append(r.entries, "")
}

// detectFieldFilter finds a field=value pattern in a query string.
func detectFieldFilter(query string) (field, value string, ok bool) {
	for _, part := range strings.Fields(query) {
		if idx := strings.Index(part, "="); idx > 0 {
			// Skip operators like !=, >=, <=.
			if prev := part[idx-1]; prev == '!' || prev == '>' || prev == '<' {
				continue
			}

			field = part[:idx]
			value = strings.Trim(part[idx+1:], "\"'")

			return field, value, true
		}
	}

	return "", "", false
}

// topValueStrings extracts string representations of top values.
func topValueStrings(tvs []client.TopValue, limit int) []string {
	result := make([]string, 0, limit)

	for i, tv := range tvs {
		if i >= limit {
			break
		}

		if s := fmt.Sprintf("%v", tv.Value); s != "" {
			result = append(result, s)
		}
	}

	return result
}

// appendStyledError renders a multi-line error (with caret + hint lines) into the scrollback.
func (r *Results) appendStyledError(errText string) {
	t := ui.Stdout
	lines := strings.Split(errText, "\n")

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		switch {
		case i == 0:
			r.entries = append(r.entries, fmt.Sprintf("  %s %s",
				t.Error.Render("Error:"), trimmed))
		case strings.HasPrefix(trimmed, "Hint:"):
			r.entries = append(r.entries, "  "+t.Info.Render(trimmed))
		default:
			r.entries = append(r.entries, "  "+line)
		}
	}

	r.entries = append(r.entries, "")
}

// formatElapsedShell returns a human-readable elapsed duration.
func formatElapsedShell(d time.Duration) string {
	if d < time.Millisecond {
		return "<1ms"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	mins := int(d.Minutes())
	secs := int(d.Seconds()) % 60

	return fmt.Sprintf("%dm %ds", mins, secs)
}

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// formatValueTTY formats a value for human-friendly TTY display.
// Integers get comma separators; large floats that are whole get commas too.
func formatValueTTY(v interface{}) string {
	if !isTTY() {
		return fmt.Sprint(v)
	}

	switch val := v.(type) {
	case float64:
		if val == float64(int64(val)) {
			return formatCount(int64(val))
		}

		return fmt.Sprintf("%.4g", val)
	case int64:
		return formatCount(val)
	case int:
		return formatCount(int64(val))
	default:
		return fmt.Sprint(v)
	}
}

// isTTY reports whether stdout is connected to a terminal.
func isTTY() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}

// isStdinPiped reports whether stdin is piped (not a terminal).
func isStdinPiped() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}

	return fi != nil && (fi.Mode()&os.ModeCharDevice) == 0
}

// formatCount returns a human-readable count string with commas.
func formatCount(n int64) string {
	if n < 0 {
		return "-" + formatCount(-n)
	}

	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var b strings.Builder
	b.Grow(len(s) + len(s)/3)

	offset := len(s) % 3
	if offset > 0 {
		b.WriteString(s[:offset])
	}

	for i := offset; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}

		b.WriteString(s[i : i+3])
	}

	return b.String()
}

// formatCountHuman returns human-readable counts with suffixes (1.2M, 3.4B) in TTY, plain otherwise.
func formatCountHuman(n int64) string {
	if !isTTY() {
		return formatCount(n)
	}
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}

	return formatCount(n)
}

// formatBytes returns a human-readable byte size string.
func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// formatDuration returns a human-readable duration from total seconds.
func formatDuration(totalSecs int64) string {
	days := totalSecs / 86400
	hours := (totalSecs % 86400) / 3600
	mins := (totalSecs % 3600) / 60
	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, mins)
	}

	return fmt.Sprintf("%dm", mins)
}

// formatElapsed returns a human-readable elapsed duration.
func formatElapsed(d time.Duration) string {
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

// ensureFromClause normalizes a query so it always has a FROM clause.
// Delegates to spl2.NormalizeQuery which handles all cases (pipe-prefixed,
// known commands, implicit search) and prepends "FROM main" as needed.
func ensureFromClause(query string) string {
	return spl2.NormalizeQuery(query)
}

// truncateStr truncates s to maxLen runes, appending "..." if truncated.
func truncateStr(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return string(runes[:maxLen])
	}

	return string(runes[:maxLen-3]) + "..."
}

// printSuccess prints a green success message to stderr (respects --quiet).
func printSuccess(format string, args ...interface{}) {
	ui.Stderr.PrintSuccess(globalQuiet, format, args...)
}

// printWarning prints a yellow warning message to stderr (respects --quiet).
func printWarning(format string, args ...interface{}) {
	ui.Stderr.PrintWarning(globalQuiet, format, args...)
}

// printHint prints a dim hint to stderr (TTY only, respects --quiet).
func printHint(format string, args ...interface{}) {
	if !isTTY() {
		return
	}

	ui.Stderr.PrintHint(globalQuiet, format, args...)
}

// printMeta prints metadata to stderr (respects --quiet).
func printMeta(format string, args ...interface{}) {
	ui.Stderr.PrintMeta(globalQuiet, format, args...)
}

// printNextSteps prints dim "Next steps" hints to stderr.
// Only shown in TTY mode and when --quiet is not set.
func printNextSteps(steps ...string) {
	if !isTTY() {
		return
	}

	ui.Stderr.PrintNextSteps(globalQuiet, steps...)
}

// isStdinTTY reports whether stdin is connected to a terminal.
func isStdinTTY() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}

// confirmAction prompts the user for confirmation on stderr/stdin.
func confirmAction(prompt string) bool {
	return ui.Stderr.Confirm(prompt)
}

// confirmDestructive prompts for typed confirmation of a destructive action.
func confirmDestructive(message, confirmValue string) bool {
	return ui.Stderr.ConfirmDestructive(message, confirmValue)
}

// fetchFieldTopValues fetches top values for a field from the server.
// Returns nil on any error (best-effort).
func fetchFieldTopValues(fieldName string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	result, err := apiClient().FieldValues(ctx, fieldName, 0)
	if err != nil {
		return nil
	}

	var values []string
	for _, v := range result.Values {
		s := fmt.Sprint(v.Value)
		if s != "" {
			values = append(values, s)
		}
	}

	return values
}

// extractFilterFields extracts field names from simple filter expressions.
// E.g., "level=error source=nginx" → map["level"]="error", map["source"]="nginx".
// Best-effort heuristic, not a full parser.
func extractFilterFields(query string) map[string]string {
	fields := make(map[string]string)

	// Look for field=value and field>=value patterns.
	for _, part := range strings.Fields(query) {
		// Skip SPL2 commands and pipes.
		if strings.HasPrefix(part, "|") || strings.HasPrefix(part, "FROM") {
			continue
		}

		for _, op := range []string{">=", "<=", "!=", "=", ">", "<"} {
			if idx := strings.Index(part, op); idx > 0 {
				field := part[:idx]
				value := strings.Trim(part[idx+len(op):], `"'`)

				// Skip internal fields and numeric comparisons without field names.
				if field != "" && !strings.HasPrefix(field, "_") && !strings.ContainsAny(field, " ()|") {
					fields[field] = value
				}

				break
			}
		}
	}

	return fields
}

// printEmptyResultGuidance prints suggestions when a query returns zero results.
// Best-effort: fetches field values from the server to suggest alternatives.
func printEmptyResultGuidance(query, since string) {
	if globalQuiet || !isTTY() {
		return
	}

	t := ui.Stderr

	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n\n", t.Bold.Render("No results found."))

	filterFields := extractFilterFields(query)
	suggestionsShown := false

	fmt.Fprintf(os.Stderr, "  %s\n", t.Dim.Render("Suggestions:"))

	bullet := t.Info.Render("\u2022")

	var knownFields []fieldEntry

	for field, usedValue := range filterFields {
		topValues := fetchFieldTopValues(field)
		if len(topValues) == 0 {
			if knownFields == nil {
				knownFields = fetchFieldCatalog()
			}

			if suggestion := suggestFieldName(field, knownFields); suggestion != "" {
				fmt.Fprintf(os.Stderr, "    %s Unknown field %q. Did you mean: %s?\n",
					bullet, field, suggestion)
				fmt.Fprintf(os.Stderr, "      %s\n", t.Dim.Render("Run 'lynxdb fields' to see all known fields"))

				suggestionsShown = true
			}

			continue
		}

		found := false

		for _, v := range topValues {
			if strings.EqualFold(v, usedValue) {
				found = true

				break
			}
		}

		if !found {
			displayed := topValues
			if len(displayed) > 5 {
				displayed = displayed[:5]
			}

			fmt.Fprintf(os.Stderr, "    %s Known values for '%s': %s\n",
				bullet, field, strings.Join(displayed, ", "))
			fmt.Fprintf(os.Stderr, "      %s\n", t.Dim.Render("Run: lynxdb fields "+field))

			suggestionsShown = true
		}
	}

	if since != "" {
		wider := suggestWiderTimeRange(since)
		if wider != "" {
			fmt.Fprintf(os.Stderr, "    %s Widen time range: lynxdb query '%s' --since %s\n",
				bullet, truncateStr(query, 40), wider)

			suggestionsShown = true
		}
	} else {
		fmt.Fprintf(os.Stderr, "    %s Try a broader time range: lynxdb query '%s' --since 1h\n",
			bullet, truncateStr(query, 40))

		suggestionsShown = true
	}

	if !suggestionsShown {
		fmt.Fprintf(os.Stderr, "    %s Try a broader search or check field names with: lynxdb fields\n",
			bullet)
	}

	fmt.Fprintln(os.Stderr)
}

// fetchFieldCatalog fetches the full field catalog from the server.
// Returns nil on any error (best-effort).
func fetchFieldCatalog() []fieldEntry {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fields, err := apiClient().Fields(ctx)
	if err != nil {
		return nil
	}

	entries := make([]fieldEntry, 0, len(fields))
	for _, f := range fields {
		entries = append(entries, fieldEntry{
			Name:     f.Name,
			Type:     f.Type,
			Coverage: f.Coverage,
		})
	}

	return entries
}

// suggestFieldName finds the closest known field name using fuzzy matching.
// Returns a formatted suggestion string, or empty if no close match found.
func suggestFieldName(name string, knownFields []fieldEntry) string {
	if len(knownFields) == 0 {
		return ""
	}

	type match struct {
		name     string
		typ      string
		coverage float64
		dist     int
	}

	lowerName := strings.ToLower(name)
	var best *match

	for _, f := range knownFields {
		d := levenshteinDistance(lowerName, strings.ToLower(f.Name))
		if d > 2 {
			continue
		}

		if best == nil || d < best.dist {
			best = &match{
				name:     f.Name,
				typ:      f.Type,
				coverage: f.Coverage,
				dist:     d,
			}
		}
	}

	if best == nil {
		return ""
	}

	return fmt.Sprintf("%s (%s, %.0f%% coverage)", best.name, best.typ, best.coverage)
}

// suggestMVHint returns a performance hint suggesting a materialized view
// when a query is slow (>5s), scans many events (>10M), and contains
// an aggregation with a group-by clause (MV-compatible pattern).
// Returns empty string if no suggestion is warranted.
func suggestMVHint(query string, elapsed time.Duration, rowsScanned int64) string {
	const minElapsed = 5 * time.Second
	const minRows = 10_000_000

	if elapsed < minElapsed || rowsScanned < minRows {
		return ""
	}

	// Check if the query contains stats/timechart with a group-by (MV-compatible).
	upper := strings.ToUpper(query)
	hasStat := strings.Contains(upper, "STATS ") || strings.Contains(upper, "TIMECHART ")
	hasByClause := strings.Contains(upper, " BY ")

	if !hasStat || !hasByClause {
		return ""
	}

	return fmt.Sprintf("This query scanned %s events in %s.\n"+
		"     A materialized view could make it ~100-400x faster:\n"+
		"     lynxdb mv create mv_<name> '%s'",
		formatCountHuman(rowsScanned), formatElapsed(elapsed), truncateStr(query, 80))
}

// suggestWiderTimeRange suggests a wider time range based on the current one.
func suggestWiderTimeRange(since string) string {
	switch since {
	case "5m", "10m", "15m":
		return "1h"
	case "30m":
		return "2h"
	case "1h":
		return "6h"
	case "2h", "3h":
		return "12h"
	case "6h":
		return "24h"
	case "12h":
		return "2d"
	case "24h", "1d":
		return "7d"
	case "7d":
		return "30d"
	default:
		return ""
	}
}

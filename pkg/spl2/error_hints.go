package spl2

import (
	"fmt"
	"strings"
)

// knownCommands is the list of all supported SPL2 command names.
var knownCommands = []string{
	"search", "where", "stats", "eval", "sort", "head", "tail",
	"timechart", "rex", "fields", "table", "dedup", "rename",
	"bin", "streamstats", "eventstats", "join", "append",
	"multisearch", "transaction", "xyseries", "top", "rare", "fillnull",
	"limit", "from", "materialize",
	"unpack_json", "unpack_logfmt", "unpack_syslog", "unpack_combined",
	"unpack_clf", "unpack_nginx_error", "unpack_cef", "unpack_kv",
	"unpack_docker", "unpack_redis", "unpack_apache_error",
	"unpack_postgres", "unpack_mysql_slow", "unpack_haproxy",
	"unpack_leef", "unpack_w3c", "unpack_pattern",
	"json", "unroll", "pack_json",
}

// knownFunctions is the list of all supported eval/aggregation functions.
var knownFunctions = []string{
	// Eval functions
	"if", "case", "match", "coalesce", "tonumber", "tostring", "tobool",
	"round", "substr", "lower", "upper", "len", "ln", "abs", "ceil",
	"floor", "sqrt", "mvjoin", "mvappend", "mvdedup", "mvcount",
	"isnotnull", "isnull", "null", "strftime", "max", "min",
	// Aggregation functions
	"count", "sum", "avg", "dc", "values", "stdev",
	"perc50", "perc75", "perc90", "perc95", "perc99",
	"earliest", "latest", "first", "last", "percentile",
	// JSON functions
	"json_extract", "json_valid", "json_keys", "json_array_length",
	"json_object", "json_array", "json_type", "json_set", "json_remove",
	"json_merge",
}

// SuggestFix examines a parse or execution error and returns a hint string
// that may help the user fix their query. If no suggestion is applicable,
// returns an empty string.
//
// Generator order matters — more specific detectors fire first, generic ones last.
// The former suggestSyntaxPosition call was removed from this chain because
// FormatParseError already shows a caret at the error position, making the
// "Syntax error at position N" string redundant and shadowing better hints.
func SuggestFix(errMsg string, knownFields []string) string {
	// Try each hint generator in priority order.
	if hint := suggestUnknownCommand(errMsg); hint != "" {
		return hint
	}
	if hint := suggestMissingPipe(errMsg); hint != "" {
		return hint
	}
	if hint := suggestUnknownFunction(errMsg); hint != "" {
		return hint
	}
	if hint := suggestMissingAgg(errMsg); hint != "" {
		return hint
	}
	if hint := suggestQuoteMismatch(errMsg); hint != "" {
		return hint
	}
	if hint := suggestTypeMismatch(errMsg); hint != "" {
		return hint
	}
	if hint := suggestMissingBy(errMsg); hint != "" {
		return hint
	}
	if hint := suggestEmptyPipeline(errMsg); hint != "" {
		return hint
	}

	return ""
}

// suggestUnknownCommand checks for "unexpected command" errors and suggests
// the closest known command name. When the unknown token looks like a field
// name (lowercase, at position 0), it suggests prepending the "search" keyword.
func suggestUnknownCommand(errMsg string) string {
	// Match pattern: "unexpected command IDENT "xxx""
	const prefix = "unexpected command"
	idx := strings.Index(errMsg, prefix)
	if idx < 0 {
		return ""
	}
	// Extract the command name from the quoted string.
	rest := errMsg[idx+len(prefix):]
	name := extractQuoted(rest)
	if name == "" {
		return ""
	}

	// Check if this looks like implicit search (field=value at start of query).
	// The token is at position 0 and is a lowercase identifier — likely a field name.
	if strings.Contains(errMsg, "at position 0") && name == strings.ToLower(name) {
		return fmt.Sprintf("Did you mean: search %s ...? Bare field=value syntax requires the \"search\" keyword.", name)
	}

	// SPL1 compatibility: spath → json / unpack_json.
	// "spath" has edit-distance 5 from "json" which won't match within maxDist=3,
	// so we handle it as a special case before generic fuzzy matching.
	if strings.ToLower(name) == "spath" {
		return `Unknown command "spath". In LynxDB, use: | json (quick extraction) or | unpack_json (full extraction with from/prefix/fields options).`
	}

	if match := ClosestMatch(name, knownCommands, 3); match != "" {
		return fmt.Sprintf("Unknown command %q. Did you mean: %s?", name, match)
	}

	return fmt.Sprintf("Unknown command %q. Available commands: %s", name, strings.Join(knownCommands, ", "))
}

// suggestUnknownFunction extracts an identifier from "unexpected token IDENT"
// errors and delegates to SuggestFunction for fuzzy matching against known
// function names. This wires up the previously-dead SuggestFunction code path.
func suggestUnknownFunction(errMsg string) string {
	// Match pattern: 'unexpected token IDENT "xxx"' — but not "unexpected command"
	// which is handled by suggestUnknownCommand.
	if strings.Contains(errMsg, "unexpected command") {
		return ""
	}

	const prefix = "unexpected token IDENT"
	idx := strings.Index(errMsg, prefix)
	if idx < 0 {
		return ""
	}

	rest := errMsg[idx+len(prefix):]
	name := extractQuoted(rest)
	if name == "" {
		return ""
	}

	// Skip known command names — those are handled by suggestMissingPipe.
	if isKnownCommand(strings.ToLower(name)) {
		return ""
	}

	return SuggestFunction(name)
}

// suggestMissingPipe detects when a known command name appears as an
// unexpected token, which usually means the user forgot the pipe (|).
// Example: "level=error stats count" -> suggests "| stats".
func suggestMissingPipe(errMsg string) string {
	// Look for an unexpected IDENT that is a known command name.
	const prefix = "unexpected token IDENT"
	idx := strings.Index(errMsg, prefix)
	if idx < 0 {
		return ""
	}

	rest := errMsg[idx+len(prefix):]
	name := extractQuoted(rest)
	if name == "" {
		return ""
	}

	if isKnownCommand(strings.ToLower(name)) {
		return fmt.Sprintf("Did you mean: | %s? Commands must be preceded by a pipe (|).", name)
	}

	return ""
}

// suggestMissingAgg checks for stats/timechart errors missing aggregation functions.
func suggestMissingAgg(errMsg string) string {
	lower := strings.ToLower(errMsg)
	if strings.Contains(lower, "stats") && (strings.Contains(lower, "expected") || strings.Contains(lower, "requires")) {
		if strings.Contains(lower, "aggregat") || strings.Contains(lower, "function") || strings.Contains(lower, "ident") {
			return "stats requires at least one aggregation. Example: | stats count by source"
		}
	}

	return ""
}

// suggestQuoteMismatch detects unclosed quotes, parentheses, and brackets.
func suggestQuoteMismatch(errMsg string) string {
	lower := strings.ToLower(errMsg)

	if strings.Contains(lower, "unterminated string") || strings.Contains(lower, "unclosed string") {
		return "Missing closing quote. Check for unmatched quotation marks."
	}

	// Missing closing paren at EOF.
	if strings.Contains(lower, "expected )") || strings.Contains(lower, "expected \")\"") {
		return "Missing closing parenthesis. Check for unmatched '(' in your expression."
	}

	// Missing closing bracket (subsearch).
	if strings.Contains(lower, "expected ]") || strings.Contains(lower, "expected \"]\"") {
		return "Missing closing bracket in subsearch. Check for unmatched '['."
	}

	return ""
}

// suggestTypeMismatch detects type comparison errors.
func suggestTypeMismatch(errMsg string) string {
	lower := strings.ToLower(errMsg)
	if strings.Contains(lower, "cannot compare") || strings.Contains(lower, "type mismatch") {
		if strings.Contains(lower, "string") && strings.Contains(lower, "int") {
			return "Cannot compare string to number. Use tonumber() to convert: | where tonumber(field) > 100"
		}

		return "Type mismatch in comparison. Use tonumber() or tostring() to convert types."
	}

	return ""
}

// suggestMissingBy detects when a field name follows an aggregation in stats
// without the required BY keyword.
// Example: "| stats count source" -> suggests "| stats count by source".
func suggestMissingBy(errMsg string) string {
	lower := strings.ToLower(errMsg)

	// Pattern: error from stats parsing where an IDENT appears after aggregation.
	if !strings.Contains(lower, "stats") {
		return ""
	}

	// Look for unexpected IDENT that is NOT a known function or command —
	// likely a field name that should be preceded by BY.
	const prefix = "unexpected token IDENT"
	idx := strings.Index(errMsg, prefix)
	if idx < 0 {
		return ""
	}

	rest := errMsg[idx+len(prefix):]
	name := extractQuoted(rest)
	if name == "" {
		return ""
	}

	nameLower := strings.ToLower(name)

	// If it's a known function or command, this isn't a missing-BY case.
	if isKnownCommand(nameLower) {
		return ""
	}
	for _, fn := range knownFunctions {
		if fn == nameLower {
			return ""
		}
	}

	return fmt.Sprintf("Missing 'by' keyword? Try: | stats ... by %s", name)
}

// suggestEmptyPipeline detects when a query ends with a trailing pipe
// and nothing after it.
// Example: "FROM main |" -> suggests adding a command.
func suggestEmptyPipeline(errMsg string) string {
	lower := strings.ToLower(errMsg)

	// Pattern: got EOF when a command/token was expected after a pipe.
	if strings.Contains(lower, "eof") && (strings.Contains(lower, "expected") || strings.Contains(lower, "unexpected")) {
		return "Incomplete query — add a command after the pipe. Example: | stats count"
	}

	return ""
}

// SuggestField suggests the closest known field name if the given field
// is not in the known set.
func SuggestField(name string, knownFields []string) string {
	for _, f := range knownFields {
		if f == name {
			return ""
		}
	}
	if match := ClosestMatch(name, knownFields, 3); match != "" {
		return fmt.Sprintf("Unknown field %q. Did you mean: %s?", name, match)
	}

	return ""
}

// SuggestFunction suggests the closest known function name.
func SuggestFunction(name string) string {
	if match := ClosestMatch(name, knownFunctions, 3); match != "" {
		return fmt.Sprintf("Unknown function %q. Did you mean: %s?", name, match)
	}

	return ""
}

// FormatParseError wraps a parse error with helpful hints.
func FormatParseError(err error, query string) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	hint := SuggestFix(msg, nil)

	// Try to show the position in the query.
	const marker = "at position "
	if idx := strings.Index(msg, marker); idx >= 0 {
		rest := msg[idx+len(marker):]
		var pos int
		for _, c := range rest {
			if c >= '0' && c <= '9' {
				pos = pos*10 + int(c-'0')
			} else {
				break
			}
		}
		if pos > 0 && pos <= len(query) {
			lines := formatCaret(query, pos)
			if hint != "" {
				return fmt.Sprintf("%s\n%s\nHint: %s", msg, lines, hint)
			}

			return fmt.Sprintf("%s\n%s", msg, lines)
		}
	}

	if hint != "" {
		return fmt.Sprintf("%s\nHint: %s", msg, hint)
	}

	return msg
}

// formatCaret shows the query with a caret pointing to the error position.
func formatCaret(query string, pos int) string {
	// Find the line containing the position.
	lineStart := 0
	for i := 0; i < pos-1 && i < len(query); i++ {
		if query[i] == '\n' {
			lineStart = i + 1
		}
	}
	lineEnd := strings.IndexByte(query[lineStart:], '\n')
	if lineEnd < 0 {
		lineEnd = len(query) - lineStart
	}
	line := query[lineStart : lineStart+lineEnd]
	col := pos - 1 - lineStart
	if col < 0 {
		col = 0
	}
	if col > len(line) {
		col = len(line)
	}

	return fmt.Sprintf("  %s\n  %s^", line, strings.Repeat(" ", col))
}

// closestMatch returns the closest string from candidates within maxDist,
// or empty if no close match is found.
// ClosestMatch returns the closest string from candidates within maxDist
// edit distance, or empty if no close match is found. Exported for use
// by the server package (fuzzy source name matching in warnings).
func ClosestMatch(input string, candidates []string, maxDist int) string {
	input = strings.ToLower(input)
	bestDist := maxDist + 1
	bestMatch := ""
	for _, c := range candidates {
		d := levenshtein(input, strings.ToLower(c))
		if d < bestDist {
			bestDist = d
			bestMatch = c
		}
	}
	if bestDist <= maxDist {
		return bestMatch
	}

	return ""
}

// levenshtein computes the edit distance between two strings.
func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	// Use single-row DP.
	prev := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr := make([]int, lb+1)
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min3(curr[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev = curr
	}

	return prev[lb]
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}

		return c
	}
	if b < c {
		return b
	}

	return c
}

// extractQuoted extracts the first double-quoted string from s.
func extractQuoted(s string) string {
	start := strings.IndexByte(s, '"')
	if start < 0 {
		return ""
	}
	end := strings.IndexByte(s[start+1:], '"')
	if end < 0 {
		return ""
	}

	return s[start+1 : start+1+end]
}

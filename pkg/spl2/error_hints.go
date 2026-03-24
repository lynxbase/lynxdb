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
	"limit", "from", "index", "materialize",
	"unpack_json", "unpack_logfmt", "unpack_syslog", "unpack_combined",
	"unpack_clf", "unpack_nginx_error", "unpack_cef", "unpack_kv",
	"unpack_docker", "unpack_redis", "unpack_apache_error",
	"unpack_postgres", "unpack_mysql_slow", "unpack_haproxy",
	"unpack_leef", "unpack_w3c", "unpack_pattern",
	"json", "unroll", "pack_json", "tee",
	// Lynx Flow commands.
	"let", "keep", "omit", "select", "group", "every", "bucket",
	"order", "take", "rank", "topby", "bottomby", "bottom",
	"running", "enrich", "parse", "explode", "pack", "lookup",
	"latency", "errors", "rate", "percentiles", "slowest",
	"views", "dropview", "glimpse", "describe",
	"use", "outliers", "compare", "patterns", "trace", "rollup", "correlate", "sessionize", "topology",
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
	"perc25", "perc50", "perc75", "perc90", "perc95", "perc99",
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
	// Check for unresolved ${param} references.
	if hint := suggestUnresolvedParam(errMsg); hint != "" {
		return hint
	}
	// Try each hint generator in priority order.
	if hint := suggestClauseAsCommand(errMsg); hint != "" {
		return hint
	}
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
	if hint := suggestParseFormat(errMsg); hint != "" {
		return hint
	}
	if hint := suggestMissingCompute(errMsg); hint != "" {
		return hint
	}
	if hint := suggestEmptyPipeline(errMsg); hint != "" {
		return hint
	}
	if hint := suggestUnknownFieldFromError(errMsg, knownFields); hint != "" {
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

// FormatParseError wraps a parse error with helpful hints and an error code.
func FormatParseError(err error, query string) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	hint := SuggestFix(msg, nil)
	code := ClassifyError(msg)

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
			tokenLen := extractTokenLength(msg)
			lines := formatCaretSpan(query, pos, tokenLen)
			if hint != "" {
				return fmt.Sprintf("%s %s\n%s\nHint: %s\nRun: lynxdb explain-error %s", code, msg, lines, hint, code)
			}

			return fmt.Sprintf("%s %s\n%s\nRun: lynxdb explain-error %s", code, msg, lines, code)
		}
	}

	if hint != "" {
		return fmt.Sprintf("%s %s\nHint: %s\nRun: lynxdb explain-error %s", code, msg, hint, code)
	}

	return fmt.Sprintf("%s %s\nRun: lynxdb explain-error %s", code, msg, code)
}

// extractTokenLength tries to extract the token literal length from a
// quoted token in the error message. Falls back to 1.
func extractTokenLength(msg string) int {
	if qIdx := strings.LastIndex(msg, "\""); qIdx > 0 {
		sub := msg[:qIdx]
		if oIdx := strings.LastIndex(sub, "\""); oIdx >= 0 {
			tokenLit := msg[oIdx+1 : qIdx]
			if tokenLit != "" && len(tokenLit) < 100 {
				return len(tokenLit)
			}
		}
	}

	return 1
}

// formatCaretSpan shows the query with a caret span pointing to the error.
func formatCaretSpan(query string, pos, length int) string {
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

	if length <= 0 {
		length = 1
	}

	return fmt.Sprintf("  %s\n  %s%s", line, strings.Repeat(" ", col), strings.Repeat("^", length))
}

// formatCaret shows the query with a caret pointing to the error position.
// Deprecated: use formatCaretSpan for multi-caret rendering.
func formatCaret(query string, pos int) string {
	return formatCaretSpan(query, pos, 1)
}

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

// suggestClauseAsCommand detects when a Lynx Flow clause keyword (like
// "compute", "using", "into") is mistakenly used as a standalone command.
// These keywords only make sense inside their parent commands.
func suggestClauseAsCommand(errMsg string) string {
	const prefix = "unexpected command"
	idx := strings.Index(errMsg, prefix)
	if idx < 0 {
		return ""
	}

	rest := errMsg[idx+len(prefix):]
	name := extractQuoted(rest)
	if name == "" {
		return ""
	}

	switch strings.ToLower(name) {
	case "compute":
		return `"compute" is a clause, not a command. Use: | group [by field] compute count()`
	case "using":
		return `"using" is a clause for topby/bottomby. Use: | topby 10 uri using avg(duration_ms)`
	case "asc", "desc":
		return fmt.Sprintf(`"%s" is a direction modifier. Use: | order by field %s`, name, strings.ToLower(name))
	case "if_missing":
		return `"if_missing" is a modifier for parse. Use: | parse json(_raw) if_missing`
	case "extract":
		return `"extract" is a modifier for parse. Use: | parse json(_raw) extract (field1, field2)`
	case "into":
		return `"into" is a clause for pack. Use: | pack f1, f2 into target`
	case "per":
		return `"per" is a clause for rate. Use: | rate per 5m by service`
	}

	return ""
}

// suggestParseFormat detects errors from the Lynx Flow "parse" command when
// the user provides a format name without parentheses, or when the format is
// unrecognizable. It suggests known parse formats.
func suggestParseFormat(errMsg string) string {
	lower := strings.ToLower(errMsg)

	// "parse: expected '(' after format name" — user may have written: | parse json _raw
	if strings.Contains(lower, "parse") && strings.Contains(lower, "expected '(' after format") {
		return "parse requires parentheses around arguments. Example: | parse json(_raw) or | parse regex(_raw, \"pattern\")"
	}

	// "parse: expected format name" — user may have written: | parse (_raw)
	if strings.Contains(lower, "parse") && strings.Contains(lower, "expected format name") {
		return "parse requires a format name. Known formats: json, logfmt, syslog, combined, clf, regex, pattern, " +
			"nginx_error, cef, kv, docker, redis, apache_error, postgres, mysql_slow, haproxy, leef, w3c"
	}

	return ""
}

// suggestMissingCompute detects the "group: expected 'compute'" or "every: expected 'compute'"
// error and provides a clear suggestion with an example.
func suggestMissingCompute(errMsg string) string {
	lower := strings.ToLower(errMsg)

	if strings.Contains(lower, "group") && strings.Contains(lower, "expected 'compute'") {
		return "group requires a 'compute' clause with aggregations. Example: | group by host compute count() as n"
	}

	if strings.Contains(lower, "every") && strings.Contains(lower, "expected 'compute'") {
		return "every requires a 'compute' clause with aggregations. Example: | every 5m compute count() as n"
	}

	return ""
}

// suggestUnknownFieldFromError extracts a field name from an unknown field error
// and suggests the closest match from knownFields. Only fires when knownFields
// is non-empty (requires caller to provide field catalog).
func suggestUnknownFieldFromError(errMsg string, knownFields []string) string {
	if len(knownFields) == 0 {
		return ""
	}

	lower := strings.ToLower(errMsg)

	// Patterns: "unknown field 'X'", "no field 'X'", "field 'X' not found".
	var fieldName string
	for _, pat := range []string{"unknown field '", "no field '", "field '", "no such field '"} {
		idx := strings.Index(lower, pat)
		if idx < 0 {
			continue
		}
		start := idx + len(pat)
		end := strings.IndexByte(errMsg[start:], '\'')
		if end > 0 {
			fieldName = errMsg[start : start+end]
			break
		}
	}

	if fieldName == "" {
		return ""
	}

	if match := SuggestField(fieldName, knownFields); match != "" {
		return match
	}

	return ""
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

// suggestUnresolvedParam detects parse errors caused by unresolved ${name}
// parameter references and reminds the user to pass --param name=value.
func suggestUnresolvedParam(errMsg string) string {
	if strings.Contains(errMsg, "${") {
		return "Unresolved parameter. Use --param name=value to set it."
	}
	return ""
}

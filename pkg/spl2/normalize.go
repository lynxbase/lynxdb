package spl2

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// NormalizeQuery produces a fully-qualified SPL2 query from user input.
// It ensures every query has an explicit FROM clause so the pipeline builder
// always has a source to scan. Without this, server-mode queries that lack
// FROM (e.g. "| stats count", "search error", "level=error") would parse
// with Source=nil and the scan iterator would yield zero rows.
//
// Transformation rules:
//   - "FROM ..."       → unchanged (already has a source)
//   - "$var = ..."     → unchanged (CTE variable definition)
//   - "| cmd ..."      → "FROM main | cmd ..."
//   - "search ..."     → "FROM main | search ..."  (known command)
//   - "stats count"    → "FROM main | stats count"  (known command)
//   - "index=foo ..."  → "FROM foo | ..."           (Splunk-style index)
//   - "level=error"    → "FROM main | search level=error" (implicit search)
func NormalizeQuery(q string) string {
	return NormalizeQueryWithNow(q, time.Now())
}

// NormalizeQueryWithNow is like NormalizeQuery but accepts an explicit "now"
// time for deterministic testing of time range resolution.
func NormalizeQueryWithNow(q string, now time.Time) string {
	// Pre-process: resolve relative time literals like _time > -1h.
	q = resolveTimeLiterals(q, now)

	trimmed := strings.TrimSpace(q)
	if trimmed == "" {
		return trimmed
	}

	upper := strings.ToUpper(trimmed)

	// Explicit FROM clause — no change.
	if strings.HasPrefix(upper, "FROM ") {
		return trimmed
	}

	// CTE variable reference ($threats = ...) — no change.
	if strings.HasPrefix(trimmed, "$") {
		return trimmed
	}

	// Pipe-prefixed — prepend FROM main as the default source.
	if strings.HasPrefix(trimmed, "|") {
		return "FROM main " + trimmed
	}

	// index IN (...) / index NOT IN (...) — rewrite to FROM list or negation filter.
	// Must come before known-command and extractIndexPrefix checks.
	if names, negated, rest, ok := extractIndexInPrefix(trimmed); ok {
		return buildFromIN(names, negated, rest)
	}
	if names, negated, rest, ok := extractSourceInPrefix(trimmed); ok {
		if negated {
			return buildFromIN(names, negated, rest) // Already correct: FROM * | where _source NOT IN (...)
		}
		// Non-negated: field filter, not index selector.
		return buildSourceInFilter(names, rest)
	}

	// index!=<value> / source!=<value> — rewrite to FROM * | where _source!="<value>".
	// Must come before known-command and extractIndexPrefix checks.
	if name, rest, ok := extractIndexNegationPrefix(trimmed); ok {
		return buildFromNegation(name, rest)
	}
	if name, rest, ok := extractSourceNegationPrefix(trimmed); ok {
		return buildFromNegation(name, rest)
	}

	// Splunk-style index selection: index=<name> or index <name>.
	// Rewrites to FROM <name> so the parser handles glob/multi-source.
	// Must come before known-command check since "index" is a known command
	// but "index=foo" and "index foo" are source-selection patterns.
	if indexName, rest, ok := extractIndexPrefix(trimmed); ok {
		return buildFromWithRest(indexName, rest)
	}

	// Splunk-style source selection: source=<name>.
	// Unlike index=, source is a field filter — scan all indexes and filter by _source.
	if sourceName, rest, ok := extractSourcePrefix(trimmed); ok {
		return buildSourceFilter(sourceName, rest)
	}

	// Known command (search, stats, where, etc.) — prepend FROM main.
	// "index" is excluded here because all valid index-as-source patterns
	// (index=foo, index foo, index IN (...), index!=foo) are already handled
	// above. If we reach here with firstWord == "index", it means something
	// like "index stats" where the extraction rejected it — treat as search.
	firstWord := firstToken(trimmed)
	lowerFirst := strings.ToLower(firstWord)
	if lowerFirst != "index" && isKnownCommand(lowerFirst) {
		return "FROM main | " + trimmed
	}

	// Implicit search: prepend FROM main and "search" keyword.
	return "FROM main | search " + trimmed
}

// extractIndexInPrefix detects "index IN (...)" or "index NOT IN (...)" at the
// start of a query. Returns the list of names, whether it's negated, and the
// remaining query text.
//
// Supported forms:
//
//	index IN ("nginx", "postgres")
//	index IN (nginx, postgres)
//	index NOT IN ("internal", "audit")
//	INDEX IN ("a","b")   (case-insensitive)
func extractIndexInPrefix(q string) (names []string, negated bool, rest string, ok bool) {
	return extractFieldInPrefix(q, "index")
}

// extractSourceInPrefix detects "source IN (...)" or "source NOT IN (...)"
// at the start of a query.
func extractSourceInPrefix(q string) (names []string, negated bool, rest string, ok bool) {
	return extractFieldInPrefix(q, "source")
}

// extractFieldInPrefix is the shared implementation for extractIndexInPrefix
// and extractSourceInPrefix.
func extractFieldInPrefix(q, field string) (names []string, negated bool, rest string, ok bool) {
	lower := strings.ToLower(q)

	// Try "<field> NOT IN (" first (longer prefix).
	notInPrefix := field + " not in ("
	inPrefix := field + " in ("

	var after string
	if strings.HasPrefix(lower, notInPrefix) {
		negated = true
		after = q[len(notInPrefix):]
	} else if strings.HasPrefix(lower, inPrefix) {
		after = q[len(inPrefix):]
	} else {
		return nil, false, "", false
	}

	// Find closing paren.
	closeIdx := strings.IndexByte(after, ')')
	if closeIdx < 0 {
		return nil, false, "", false
	}

	inner := after[:closeIdx]
	rest = strings.TrimSpace(after[closeIdx+1:])

	// Parse comma-separated values (bare or quoted).
	parts := strings.Split(inner, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Strip surrounding quotes.
		if len(p) >= 2 && p[0] == '"' && p[len(p)-1] == '"' {
			p = p[1 : len(p)-1]
		}
		if p != "" {
			names = append(names, p)
		}
	}

	if len(names) == 0 {
		return nil, false, "", false
	}

	return names, negated, rest, true
}

// buildFromIN constructs a normalized query from an IN list.
// Non-negated: FROM a, b, c [| rest].
// Negated: FROM * | where _source NOT IN ("a", "b") [| rest].
func buildFromIN(names []string, negated bool, rest string) string {
	if negated {
		quoted := make([]string, len(names))
		for i, n := range names {
			quoted[i] = fmt.Sprintf("%q", n)
		}
		base := fmt.Sprintf("FROM * | where _source NOT IN (%s)", strings.Join(quoted, ", "))
		if rest == "" {
			return base
		}
		if strings.HasPrefix(rest, "|") {
			return base + " " + rest
		}
		word := firstToken(rest)
		if isKnownCommand(strings.ToLower(word)) {
			return base + " | " + rest
		}

		return base + " | search " + rest
	}

	from := "FROM " + strings.Join(names, ", ")
	if rest == "" {
		return from
	}
	if strings.HasPrefix(rest, "|") {
		return from + " " + rest
	}
	word := firstToken(rest)
	if isKnownCommand(strings.ToLower(word)) {
		return from + " | " + rest
	}

	return from + " | search " + rest
}

// extractIndexNegationPrefix detects "index!=<value>" at the start of a query.
func extractIndexNegationPrefix(q string) (name, rest string, ok bool) {
	return extractFieldNegationPrefix(q, "index")
}

// extractSourceNegationPrefix detects "source!=<value>" at the start of a query.
func extractSourceNegationPrefix(q string) (name, rest string, ok bool) {
	return extractFieldNegationPrefix(q, "source")
}

// extractFieldNegationPrefix is the shared implementation for negation detection.
func extractFieldNegationPrefix(q, field string) (name, rest string, ok bool) {
	lower := strings.ToLower(q)
	prefix := field + "!="
	if !strings.HasPrefix(lower, prefix) {
		return "", "", false
	}

	after := q[len(prefix):]
	name, remainder := extractValue(after)
	if name == "" {
		return "", "", false
	}

	return name, strings.TrimSpace(remainder), true
}

// buildFromNegation constructs a FROM * | where _source!="<name>" query.
func buildFromNegation(name, rest string) string {
	base := fmt.Sprintf("FROM * | where _source!=%q", name)
	if rest == "" {
		return base
	}
	if strings.HasPrefix(rest, "|") {
		return base + " " + rest
	}
	word := firstToken(rest)
	if isKnownCommand(strings.ToLower(word)) {
		return base + " | " + rest
	}

	return base + " | search " + rest
}

// buildSourceInFilter constructs a normalized query for non-negated source IN (...).
// Unlike index IN (...) which selects physical indexes, source IN (...) is a
// field-level filter: FROM main | where _source IN ("a", "b") [| rest].
func buildSourceInFilter(names []string, rest string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = fmt.Sprintf("%q", n)
	}
	base := fmt.Sprintf("FROM main | where _source IN (%s)", strings.Join(quoted, ", "))
	if rest == "" {
		return base
	}
	if strings.HasPrefix(rest, "|") {
		return base + " " + rest
	}
	word := firstToken(rest)
	if isKnownCommand(strings.ToLower(word)) {
		return base + " | " + rest
	}
	return base + " | search " + rest
}

// extractSourcePrefix detects "source=<value>" at the start of a query.
// Returns the source name and the remaining query text.
//
// Supported forms:
//
//	source=<name>          source=nginx | stats count
//	source="<name>"        source="my-app" | stats count
//	SOURCE=<name>          SOURCE=nginx (case-insensitive)
func extractSourcePrefix(q string) (sourceName, rest string, ok bool) {
	lower := strings.ToLower(q)
	if !strings.HasPrefix(lower, "source=") {
		return "", "", false
	}

	after := q[len("source="):]
	name, remainder := extractValue(after)
	if name == "" {
		return "", "", false
	}

	return name, strings.TrimSpace(remainder), true
}

// buildSourceFilter constructs a FROM * | where _source="<name>" query.
// This scans all indexes and filters by the _source field, since source=
// is a logical tag, not a physical index selector.
func buildSourceFilter(name, rest string) string {
	base := fmt.Sprintf(`FROM * | where _source=%q`, name)
	if rest == "" {
		return base
	}
	if strings.HasPrefix(rest, "|") {
		return base + " " + rest
	}
	word := firstToken(rest)
	if isKnownCommand(strings.ToLower(word)) {
		return base + " | " + rest
	}

	return base + " | search " + rest
}

// extractIndexPrefix detects Splunk-style index selection at the start of a
// query and returns the index name plus the remaining query text.
//
// Supported forms:
//
//	index=<name>         index=2xlog | stats count
//	index=<"name">       index="my-logs" | stats count
//	index <name>         index 2xlog | stats count
//	INDEX=<name>         INDEX=foo (case-insensitive)
//
// The space-delimited form (index <name>) is only matched when <name> is NOT a
// known SPL2 command, to avoid ambiguity with hypothetical uses of "index" as a
// bare word followed by a command.
func extractIndexPrefix(q string) (indexName, rest string, ok bool) {
	lower := strings.ToLower(q)

	// Form 1: index=<value> (with or without quotes)
	if strings.HasPrefix(lower, "index=") {
		after := q[len("index="):]
		name, remainder := extractValue(after)
		if name == "" {
			return "", "", false
		}

		return name, strings.TrimSpace(remainder), true
	}

	// Form 2: index <value> (space-separated)
	if strings.HasPrefix(lower, "index ") || strings.HasPrefix(lower, "index\t") {
		after := strings.TrimLeft(q[len("index"):], " \t")
		name, remainder := extractValue(after)
		if name == "" {
			return "", "", false
		}

		// Reject if the "name" is actually a known command. This avoids
		// misinterpreting queries like "index stats ..." where "index" might
		// be a bare search term and "stats" is the next command.
		if isKnownCommand(strings.ToLower(name)) {
			return "", "", false
		}

		return name, strings.TrimSpace(remainder), true
	}

	return "", "", false
}

// extractValue extracts a bare or double-quoted value from the start of s.
// It returns the value and the remaining text after it.
func extractValue(s string) (value, rest string) {
	if s == "" {
		return "", ""
	}

	// Quoted value: index="my-logs"
	if s[0] == '"' {
		end := strings.IndexByte(s[1:], '"')
		if end < 0 {
			return "", "" // unclosed quote
		}

		return s[1 : 1+end], s[1+end+1:]
	}

	// Bare value: index=2xlog — terminates at whitespace, pipe, or EOF.
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '|' {
			return s[:i], s[i:]
		}
	}

	return s, ""
}

// buildFromWithRest constructs a normalized query from an extracted index name
// and the remaining query text.
func buildFromWithRest(indexName, rest string) string {
	if rest == "" {
		return "FROM " + indexName
	}

	// Already a pipe — attach directly: FROM idx | stats ...
	if strings.HasPrefix(rest, "|") {
		return "FROM " + indexName + " " + rest
	}

	// Remaining text starts with a known command — insert pipe.
	word := firstToken(rest)
	if isKnownCommand(strings.ToLower(word)) {
		return "FROM " + indexName + " | " + rest
	}

	// Otherwise treat remainder as implicit search terms.
	return "FROM " + indexName + " | search " + rest
}

// isKnownCommand reports whether name (lowercase) is a recognized SPL2 command.
func isKnownCommand(name string) bool {
	for _, cmd := range knownCommands {
		if cmd == name {
			return true
		}
	}

	return false
}

// firstToken returns the first whitespace-delimited token from s.
// It handles the common case where s starts with an identifier.
func firstToken(s string) string {
	for i, c := range s {
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' ||
			c == '|' || c == '=' || c == '>' || c == '<' || c == '!' ||
			c == '(' || c == ')' {
			return s[:i]
		}
	}

	return s
}

// resolveTimeLiterals replaces relative time expressions like _time > -1h
// with absolute timestamp comparisons. This allows the parser and VM to
// handle time filtering as regular numeric comparisons.
//
// Patterns handled:
//
//	_time > -1h          → _time > "2025-03-23T13:00:00Z"
//	_time >= -7d         → _time >= "2025-03-16T14:00:00Z"
//	_time < -1h          → _time < "2025-03-23T13:00:00Z"
//	_time <= -30m        → _time <= "2025-03-23T13:30:00Z"
//	_time between -7d and -1d → _time between "..." and "..."
func resolveTimeLiterals(q string, now time.Time) string {
	// Quick check: only process if the query contains duration patterns.
	if !strings.Contains(q, "-") {
		return q
	}

	// First pass: resolve @date literals in expression context.
	q = resolveAtDateLiterals(q)

	var result strings.Builder
	result.Grow(len(q))

	i := 0
	for i < len(q) {
		// Look for _time followed by a comparison operator followed by a duration.
		if idx := findTimeField(q, i); idx >= 0 {
			// Write everything before _time.
			result.WriteString(q[i:idx])

			// Find the operator and duration after _time.
			afterTime := idx + 5 // len("_time")
			opStart := afterTime
			for opStart < len(q) && (q[opStart] == ' ' || q[opStart] == '\t') {
				opStart++
			}

			// Check for "between <dur> and <dur>" pattern before trying comparison ops.
			if rest := q[opStart:]; strings.HasPrefix(strings.TrimLeft(rest, " \t"), "between ") {
				newStr, endPos := resolveBetweenTimeLiteral(q, idx, afterTime, now)
				if newStr != "" {
					result.WriteString(newStr)
					i = endPos
					continue
				}
			}

			op, opEnd := readCompareOp(q, opStart)
			if op == "" {
				// No valid comparison operator — pass through.
				result.WriteString(q[idx:afterTime])
				i = afterTime
				continue
			}

			// Skip whitespace after operator.
			valStart := opEnd
			for valStart < len(q) && (q[valStart] == ' ' || q[valStart] == '\t') {
				valStart++
			}

			durLit, durEnd := readDurationLiteral(q, valStart)
			if durLit == "" {
				// No duration literal — pass through.
				result.WriteString(q[idx:valStart])
				i = valStart
				continue
			}

			// Resolve the duration to an absolute timestamp.
			ts := resolveDurationToTime(durLit, now)

			// Write the replacement.
			result.WriteString("_time")
			result.WriteString(q[afterTime:opStart])
			result.WriteString(op)
			result.WriteString(q[opEnd:valStart])
			result.WriteString("\"")
			result.WriteString(ts)
			result.WriteString("\"")

			i = durEnd
			continue
		}

		// No more time fields found — write the rest.
		result.WriteString(q[i:])
		break
	}

	return result.String()
}

// findTimeField finds "_time" as a standalone word starting from pos.
func findTimeField(q string, pos int) int {
	for i := pos; i <= len(q)-5; i++ {
		if q[i:i+5] == "_time" {
			// Ensure it's a standalone identifier (not part of a larger word).
			if i > 0 && (isIdentChar(q[i-1]) || q[i-1] == '.') {
				continue
			}
			if i+5 < len(q) && isIdentChar(q[i+5]) {
				continue
			}

			return i
		}
	}

	return -1
}

func isIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_'
}

// readCompareOp reads a comparison operator starting at pos.
// Returns the operator string and the position after it.
func readCompareOp(q string, pos int) (string, int) {
	if pos >= len(q) {
		return "", pos
	}

	switch q[pos] {
	case '=':
		return "=", pos + 1
	case '!':
		if pos+1 < len(q) && q[pos+1] == '=' {
			return "!=", pos + 2
		}
		return "", pos
	case '<':
		if pos+1 < len(q) && q[pos+1] == '=' {
			return "<=", pos + 2
		}
		return "<", pos + 1
	case '>':
		if pos+1 < len(q) && q[pos+1] == '=' {
			return ">=", pos + 2
		}
		return ">", pos + 1
	default:
		return "", pos
	}
}

// readDurationLiteral reads a duration like -1h, -7d, -30m starting at pos.
// Returns the literal and the position after it.
func readDurationLiteral(q string, pos int) (string, int) {
	if pos >= len(q) || q[pos] != '-' {
		return "", pos
	}

	i := pos + 1
	digitStart := i
	for i < len(q) && q[i] >= '0' && q[i] <= '9' {
		i++
	}
	if i == digitStart || i >= len(q) {
		return "", pos
	}

	unit := q[i]
	if unit != 's' && unit != 'm' && unit != 'h' && unit != 'd' && unit != 'w' {
		return "", pos
	}
	i++

	// Optional snap: -1h@h
	if i < len(q) && q[i] == '@' {
		i++
		if i < len(q) {
			snap := q[i]
			if snap == 's' || snap == 'm' || snap == 'h' || snap == 'd' || snap == 'w' {
				i++
			}
		}
	}

	return q[pos:i], i
}

// resolveDurationToTime converts a duration literal like "-1h" to an RFC3339
// timestamp string relative to "now".
func resolveDurationToTime(lit string, now time.Time) string {
	// Strip leading minus.
	s := lit
	if s[0] == '-' {
		s = s[1:]
	}

	// Handle snap suffix.
	snapTo := ""
	if idx := strings.IndexByte(s, '@'); idx > 0 {
		snapTo = s[idx+1:]
		s = s[:idx]
	}

	if len(s) < 2 {
		return now.Format(time.RFC3339)
	}

	unit := s[len(s)-1]
	numStr := s[:len(s)-1]
	n := 0
	for _, c := range numStr {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}

	var dur time.Duration
	switch unit {
	case 's':
		dur = time.Duration(n) * time.Second
	case 'm':
		dur = time.Duration(n) * time.Minute
	case 'h':
		dur = time.Duration(n) * time.Hour
	case 'd':
		dur = time.Duration(n) * 24 * time.Hour
	case 'w':
		dur = time.Duration(n) * 7 * 24 * time.Hour
	default:
		return now.Format(time.RFC3339)
	}

	t := now.Add(-dur)

	// Apply snap.
	if snapTo != "" {
		switch snapTo {
		case "s":
			t = t.Truncate(time.Second)
		case "m":
			t = t.Truncate(time.Minute)
		case "h":
			t = t.Truncate(time.Hour)
		case "d":
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		case "w":
			weekday := int(t.Weekday())
			if weekday == 0 {
				weekday = 7
			}
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).
				AddDate(0, 0, -(weekday - 1))
		}
	}

	return t.Format(time.RFC3339)
}

// resolveBetweenTimeLiteral handles _time between <dur> and <dur> patterns.
// Returns the resolved string and the position to continue scanning from.
func resolveBetweenTimeLiteral(q string, fieldStart, afterTime int, now time.Time) (string, int) {
	// Skip whitespace after _time.
	pos := afterTime
	for pos < len(q) && (q[pos] == ' ' || q[pos] == '\t') {
		pos++
	}

	// Expect "between".
	const between = "between "
	if pos+len(between) > len(q) || q[pos:pos+len(between)] != between {
		return "", 0
	}
	pos += len(between)

	// Skip whitespace.
	for pos < len(q) && (q[pos] == ' ' || q[pos] == '\t') {
		pos++
	}

	// Read first duration.
	dur1, durEnd1 := readDurationLiteral(q, pos)
	if dur1 == "" {
		return "", 0
	}

	// Skip whitespace then "and".
	pos = durEnd1
	for pos < len(q) && (q[pos] == ' ' || q[pos] == '\t') {
		pos++
	}
	const and = "and "
	if pos+len(and) > len(q) || q[pos:pos+len(and)] != and {
		return "", 0
	}
	pos += len(and)

	// Skip whitespace.
	for pos < len(q) && (q[pos] == ' ' || q[pos] == '\t') {
		pos++
	}

	// Read second duration.
	dur2, durEnd2 := readDurationLiteral(q, pos)
	if dur2 == "" {
		return "", 0
	}

	ts1 := resolveDurationToTime(dur1, now)
	ts2 := resolveDurationToTime(dur2, now)

	result := "between \"" + ts1 + "\" and \"" + ts2 + "\""

	return result, durEnd2
}

// resolveAtDateLiterals replaces @YYYY-MM-DD and @YYYY-MM-DDTHH:MM:SS date
// literals with RFC3339 timestamp strings. This allows writing queries like
// where _time > @2025-01-15 without quoting.
func resolveAtDateLiterals(q string) string {
	var result strings.Builder
	result.Grow(len(q))

	i := 0
	for i < len(q) {
		if q[i] == '@' && i+1 < len(q) && q[i+1] >= '0' && q[i+1] <= '9' {
			if dateStr, end := tryParseAtDate(q[i:]); dateStr != "" {
				result.WriteString(dateStr)
				i += end
				continue
			}
		}
		result.WriteByte(q[i])
		i++
	}

	return result.String()
}

// tryParseAtDate attempts to parse a date literal starting with @.
// Returns (rfc3339string, bytesConsumed) or ("", 0) if not a valid date.
func tryParseAtDate(s string) (string, int) {
	if len(s) < 11 || s[0] != '@' {
		return "", 0
	}

	// Match @YYYY-MM-DD minimum.
	date := s[1:]
	if len(date) < 10 || date[4] != '-' || date[7] != '-' {
		return "", 0
	}

	// Validate digits.
	for _, idx := range []int{0, 1, 2, 3, 5, 6, 8, 9} {
		if date[idx] < '0' || date[idx] > '9' {
			return "", 0
		}
	}

	year, _ := strconv.Atoi(date[0:4])
	month, _ := strconv.Atoi(date[5:7])
	day, _ := strconv.Atoi(date[8:10])

	if month < 1 || month > 12 || day < 1 || day > 31 {
		return "", 0
	}

	consumed := 11 // @ + 10 chars

	// Check for optional time component: T or space followed by HH:MM:SS.
	hour, min, sec := 0, 0, 0
	if len(date) > 10 && (date[10] == 'T' || date[10] == ' ' || date[10] == 't') {
		if len(date) >= 19 && date[13] == ':' && date[16] == ':' {
			for _, idx := range []int{11, 12, 14, 15, 17, 18} {
				if date[idx] < '0' || date[idx] > '9' {
					return "", 0
				}
			}
			hour, _ = strconv.Atoi(date[11:13])
			min, _ = strconv.Atoi(date[14:16])
			sec, _ = strconv.Atoi(date[17:19])
			consumed = 20 // @ + 19 chars
		}
	}

	t := time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC)

	return "\"" + t.Format(time.RFC3339) + "\"", consumed
}

// SubstituteParams replaces ${name} references with values from the params map.
// Uses ${...} (braces) to avoid confusion with CTE $name references.
func SubstituteParams(q string, params map[string]string) string {
	if len(params) == 0 {
		return q
	}
	var result strings.Builder
	result.Grow(len(q))
	i := 0
	for i < len(q) {
		if q[i] == '$' && i+1 < len(q) && q[i+1] == '{' {
			// ${name} — parameter reference
			end := i + 2
			for end < len(q) && q[end] != '}' {
				end++
			}
			if end < len(q) {
				name := q[i+2 : end]
				if val, ok := params[name]; ok {
					if isNumericParam(val) {
						result.WriteString(val)
					} else {
						result.WriteString(`"` + val + `"`)
					}
					i = end + 1
					continue
				}
				// Unknown param — leave as-is (will be a parse error)
			}
		}
		result.WriteByte(q[i])
		i++
	}
	return result.String()
}

func isNumericParam(val string) bool {
	if len(val) == 0 {
		return false
	}
	start := 0
	if val[0] == '-' {
		start = 1
	}
	hasDot := false
	for i := start; i < len(val); i++ {
		if val[i] == '.' {
			if hasDot {
				return false
			}
			hasDot = true
			continue
		}
		if val[i] < '0' || val[i] > '9' {
			return false
		}
	}
	return start < len(val)
}

// ParseParamFlags parses --param key=value flags into a map.
func ParseParamFlags(flags []string) map[string]string {
	params := make(map[string]string, len(flags))
	for _, flag := range flags {
		k, v, ok := strings.Cut(flag, "=")
		if ok {
			params[k] = v
		}
	}
	return params
}

package spl2

import (
	"fmt"
	"strings"
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

	// Known command (search, stats, where, etc.) — prepend FROM main.
	firstWord := firstToken(trimmed)
	if isKnownCommand(strings.ToLower(firstWord)) {
		return "FROM main | " + trimmed
	}

	// index IN (...) / index NOT IN (...) — rewrite to FROM list or negation filter.
	// Must come before extractIndexPrefix since "index IN ..." doesn't start with "index=".
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
	// Must come before extractIndexPrefix since "index!=" doesn't start with "index=".
	if name, rest, ok := extractIndexNegationPrefix(trimmed); ok {
		return buildFromNegation(name, rest)
	}
	if name, rest, ok := extractSourceNegationPrefix(trimmed); ok {
		return buildFromNegation(name, rest)
	}

	// Splunk-style index selection: index=<name> or index <name>.
	// Rewrites to FROM <name> so the parser handles glob/multi-source.
	if indexName, rest, ok := extractIndexPrefix(trimmed); ok {
		return buildFromWithRest(indexName, rest)
	}

	// Splunk-style source selection: source=<name>.
	// Unlike index=, source is a field filter — scan all indexes and filter by _source.
	if sourceName, rest, ok := extractSourcePrefix(trimmed); ok {
		return buildSourceFilter(sourceName, rest)
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

package unpack

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// PostgresParser extracts fields from PostgreSQL stderr log format.
// Format: YYYY-MM-DD HH:MM:SS.mmm TZ [pid] user@database SEVERITY:  message
//
// The parser also auto-extracts duration_ms from "duration: N ms" patterns
// commonly found in slow query and auto_explain log messages.
//
// Example:
//
//	2026-02-14 14:52:01.234 UTC [12345] postgres@mydb LOG:  connection authorized: user=postgres database=mydb
type PostgresParser struct{}

// Name returns the parser format name.
func (p *PostgresParser) Name() string { return "postgres" }

// Parse extracts fields from a PostgreSQL stderr log line.
func (p *PostgresParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	i := 0

	// Parse timestamp: "YYYY-MM-DD HH:MM:SS.mmm TZ"
	// Minimum: "2026-01-01 00:00:00" = 19 chars, optionally with ".mmm" and " TZ"
	if len(s) < 19 {
		return nil
	}

	// Verify basic timestamp structure: digit-digit-digit-digit-dash
	if s[4] != '-' || s[7] != '-' || s[10] != ' ' || s[13] != ':' {
		return nil
	}

	// Find the end of timestamp by looking for " [" which starts the PID bracket.
	bracketIdx := strings.Index(s, " [")
	if bracketIdx < 19 {
		return nil
	}

	timestamp := s[:bracketIdx]
	if !emit("timestamp", event.StringValue(timestamp)) {
		return nil
	}
	i = bracketIdx + 2 // skip " ["

	// Parse [pid]
	closeBracket := strings.IndexByte(s[i:], ']')
	if closeBracket < 0 {
		return nil
	}
	pidStr := s[i : i+closeBracket]
	if !emit("pid", InferValue(pidStr)) {
		return nil
	}
	i += closeBracket + 1

	// Skip space after ']'
	i = skipSpaces(s, i)
	if i >= len(s) {
		return nil
	}

	// Parse optional user@database — present in most log lines but not all.
	// Look for "SEVERITY:" pattern. Known severities: LOG, ERROR, WARNING, FATAL,
	// PANIC, DEBUG1-5, INFO, NOTICE, DETAIL, HINT, STATEMENT.
	severityStart := i

	// Check if this position has user@database or directly the severity.
	// If there's an '@' before the next ':', it's user@database.
	nextColon := strings.IndexByte(s[i:], ':')
	if nextColon < 0 {
		return nil
	}

	beforeColon := s[i : i+nextColon]
	atIdx := strings.IndexByte(beforeColon, '@')
	if atIdx >= 0 {
		user := beforeColon[:atIdx]
		database := beforeColon[atIdx+1:]

		// But we need to check if what follows the database is " SEVERITY:"
		// The database ends at a space.
		spaceIdx := strings.IndexByte(database, ' ')
		if spaceIdx >= 0 {
			if !emit("user", event.StringValue(user)) {
				return nil
			}
			if !emit("database", event.StringValue(database[:spaceIdx])) {
				return nil
			}
			severityStart = i + atIdx + 1 + spaceIdx + 1
		} else {
			// Database is the rest before colon — severity not found properly.
			if !emit("user", event.StringValue(user)) {
				return nil
			}
			if !emit("database", event.StringValue(database)) {
				return nil
			}
			// Rest after colon is the message.
			i = i + nextColon + 1
			i = skipSpaces(s, i)
			msg := s[i:]
			if msg != "" {
				if !emit("message", event.StringValue(msg)) {
					return nil
				}
				emitDuration(msg, emit)
			}

			return nil
		}
	}

	// Parse SEVERITY: message
	// Severity is all-caps word before ":"
	nextColon = strings.IndexByte(s[severityStart:], ':')
	if nextColon < 0 {
		return nil
	}

	severity := s[severityStart : severityStart+nextColon]
	if !emit("severity", event.StringValue(severity)) {
		return nil
	}
	i = severityStart + nextColon + 1

	// PostgreSQL uses "SEVERITY:  " (two spaces) before message.
	i = skipSpaces(s, i)
	if i >= len(s) {
		return nil
	}

	message := s[i:]
	if !emit("message", event.StringValue(message)) {
		return nil
	}

	// Auto-extract duration_ms from "duration: N.NNN ms" pattern.
	emitDuration(message, emit)

	return nil
}

// emitDuration looks for "duration: N ms" or "duration: N.NNN ms" in the
// message and emits duration_ms as a float value if found. This is common
// in PostgreSQL slow query and auto_explain output.
func emitDuration(message string, emit func(key string, val event.Value) bool) {
	const durPrefix = "duration: "
	idx := strings.Index(message, durPrefix)
	if idx < 0 {
		return
	}
	rest := message[idx+len(durPrefix):]

	// Find the end of the number.
	numEnd := 0
	for numEnd < len(rest) && (rest[numEnd] >= '0' && rest[numEnd] <= '9' || rest[numEnd] == '.') {
		numEnd++
	}
	if numEnd == 0 {
		return
	}

	numStr := rest[:numEnd]
	// Check for " ms" suffix.
	after := rest[numEnd:]
	if !strings.HasPrefix(after, " ms") {
		return
	}

	if f, err := strconv.ParseFloat(numStr, 64); err == nil {
		emit("duration_ms", event.FloatValue(f))
	}

	// Extract SQL statement if present: "duration: 1.234 ms  statement: SELECT ..."
	const stmtPrefix = " ms  statement: "
	if strings.HasPrefix(after, stmtPrefix) {
		stmt := strings.TrimSpace(after[len(stmtPrefix):])
		if stmt != "" {
			emit("statement", event.StringValue(stmt))
		}
	}
}

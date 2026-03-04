package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// CombinedParser extracts fields from NCSA Combined Log Format lines.
// Format: host ident user [date] "request" status bytes "referer" "user_agent"
// The request field is further parsed into method, uri, and protocol.
type CombinedParser struct{}

// Name returns the parser format name.
func (p *CombinedParser) Name() string { return "combined" }

// Parse extracts fields from an NCSA Combined Log Format line.
func (p *CombinedParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	i := 0

	// Field 1: host (client IP)
	host, next := scanToken(s, i)
	if host == "" {
		return nil
	}
	if !emit("client_ip", emitDash(host)) {
		return nil
	}
	i = next

	// Field 2: ident
	ident, next := scanToken(s, i)
	if !emit("ident", emitDash(ident)) {
		return nil
	}
	i = next

	// Field 3: user
	user, next := scanToken(s, i)
	if !emit("user", emitDash(user)) {
		return nil
	}
	i = next

	// Field 4: date in brackets [dd/Mon/yyyy:HH:MM:SS +zone]
	i = skipSpaces(s, i)
	if i >= len(s) || s[i] != '[' {
		return nil
	}
	i++ // skip '['
	bracketEnd := strings.IndexByte(s[i:], ']')
	if bracketEnd < 0 {
		return nil
	}
	timestamp := s[i : i+bracketEnd]
	if !emit("timestamp", event.StringValue(timestamp)) {
		return nil
	}
	i += bracketEnd + 1 // skip past ']'

	// Field 5: "request" (quoted)
	request, next := scanQuoted(s, i)
	if request == "" {
		return nil
	}
	if !emit("request", event.StringValue(request)) {
		return nil
	}
	i = next

	// Parse request into method, uri, protocol.
	parts := strings.SplitN(request, " ", 3)
	if len(parts) >= 1 {
		if !emit("method", event.StringValue(parts[0])) {
			return nil
		}
	}
	if len(parts) >= 2 {
		if !emit("uri", event.StringValue(parts[1])) {
			return nil
		}
	}
	if len(parts) >= 3 {
		if !emit("protocol", event.StringValue(parts[2])) {
			return nil
		}
	}

	// Field 6: status (number)
	statusStr, next := scanToken(s, i)
	if !emit("status", InferValue(statusStr)) {
		return nil
	}
	i = next

	// Field 7: bytes (number or "-")
	bytesStr, next := scanToken(s, i)
	if !emit("bytes", emitDash(bytesStr)) {
		return nil
	}
	i = next

	// Field 8: "referer" (quoted)
	referer, next := scanQuoted(s, i)
	if !emit("referer", emitDash(referer)) {
		return nil
	}
	i = next

	// Field 9: "user_agent" (quoted)
	userAgent, _ := scanQuoted(s, i)
	if !emit("user_agent", emitDash(userAgent)) {
		return nil
	}

	return nil
}

// scanToken reads a whitespace-delimited token starting at position i.
// Returns the token and the position after the token (past trailing spaces).
func scanToken(s string, i int) (string, int) {
	i = skipSpaces(s, i)
	if i >= len(s) {
		return "", i
	}
	start := i
	for i < len(s) && s[i] != ' ' && s[i] != '\t' {
		i++
	}

	return s[start:i], i
}

// scanQuoted reads a double-quoted string starting at position i.
// Expects leading whitespace then a '"'. Returns the inner content
// (without quotes) and position after the closing quote.
func scanQuoted(s string, i int) (string, int) {
	i = skipSpaces(s, i)
	if i >= len(s) || s[i] != '"' {
		// Not quoted — try to read as plain token.
		return scanToken(s, i)
	}
	i++ // skip opening quote
	start := i
	for i < len(s) && s[i] != '"' {
		if s[i] == '\\' && i+1 < len(s) {
			i++ // skip escaped char
		}
		i++
	}
	val := s[start:i]
	if i < len(s) {
		i++ // skip closing quote
	}

	return val, i
}

// skipSpaces advances past whitespace.
func skipSpaces(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}

	return i
}

// emitDash converts "-" (NCSA "missing" marker) to null, otherwise returns InferValue.
func emitDash(s string) event.Value {
	if s == "-" {
		return event.NullValue()
	}

	return InferValue(s)
}

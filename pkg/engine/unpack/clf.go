package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// CLFParser extracts fields from NCSA Common Log Format lines.
// Format: host ident user [date] "request" status bytes
// This is Combined minus the referer and user_agent fields.
// Reuses scanToken, scanQuoted, skipSpaces, emitDash from combined.go.
type CLFParser struct{}

// Name returns the parser format name.
func (p *CLFParser) Name() string { return "clf" }

// DeclareFields declares the fields produced by the CLF parser.
func (p *CLFParser) DeclareFields() FieldDeclaration {
	return FieldDeclaration{
		Known: []string{"client_ip", "ident", "user", "timestamp", "request", "method", "uri", "protocol", "status", "bytes"},
	}
}

// Parse extracts fields from an NCSA Common Log Format line.
func (p *CLFParser) Parse(input string, emit func(key string, val event.Value) bool) error {
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

	// Field 7: bytes (number or "-")
	bytesStr, _ := scanToken(s, next)
	if !emit("bytes", emitDash(bytesStr)) {
		return nil
	}

	return nil
}

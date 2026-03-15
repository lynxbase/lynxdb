package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// ApacheErrorParser extracts fields from Apache 2.4+ error log lines.
// Format: [timestamp] [module:severity] [pid N:tid N] [client ip:port] error_code: message
//
// Example:
//
//	[Fri Feb 14 14:52:01.234567 2026] [ssl:error] [pid 12345:tid 67890] [client 192.168.1.100:52436] AH02032: Hostname provided via SNI not found
type ApacheErrorParser struct{}

// Name returns the parser format name.
func (p *ApacheErrorParser) Name() string { return "apache_error" }

// DeclareFields declares the fields produced by the Apache error parser.
func (p *ApacheErrorParser) DeclareFields() FieldDeclaration {
	return FieldDeclaration{
		Known:    []string{"timestamp", "severity", "message"},
		Optional: []string{"module", "pid", "tid", "client_ip", "client_port", "error_code"},
	}
}

// Parse extracts fields from an Apache 2.4+ error log line.
func (p *ApacheErrorParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	i := 0

	// Parse [timestamp]
	timestamp, next := scanBracketField(s, i)
	if timestamp == "" {
		return nil
	}
	if !emit("timestamp", event.StringValue(timestamp)) {
		return nil
	}
	i = next

	// Parse [module:severity]
	modSev, next := scanBracketField(s, i)
	if modSev == "" {
		return nil
	}
	i = next

	// Split module:severity
	colonIdx := strings.IndexByte(modSev, ':')
	if colonIdx >= 0 {
		if !emit("module", event.StringValue(modSev[:colonIdx])) {
			return nil
		}
		if !emit("severity", event.StringValue(modSev[colonIdx+1:])) {
			return nil
		}
	} else {
		// Sometimes only severity is present (e.g., [error])
		if !emit("severity", event.StringValue(modSev)) {
			return nil
		}
	}

	// Parse [pid N:tid N] or [pid N]
	pidTid, next := scanBracketField(s, i)
	if pidTid == "" {
		// No pid field — rest is message.
		goto parseMessage
	}
	i = next

	// Parse "pid N:tid N" or "pid N"
	if strings.HasPrefix(pidTid, "pid ") {
		rest := pidTid[4:]
		colonIdx = strings.IndexByte(rest, ':')
		if colonIdx >= 0 {
			if !emit("pid", InferValue(strings.TrimSpace(rest[:colonIdx]))) {
				return nil
			}
			tidPart := rest[colonIdx+1:]
			if strings.HasPrefix(tidPart, "tid ") {
				tidPart = tidPart[4:]
			}
			if !emit("tid", InferValue(strings.TrimSpace(tidPart))) {
				return nil
			}
		} else {
			if !emit("pid", InferValue(strings.TrimSpace(rest))) {
				return nil
			}
		}
	}

	// Parse optional [client ip:port]
	{
		i = skipSpaces(s, i)
		if i < len(s) && s[i] == '[' {
			clientField, next2 := scanBracketField(s, i)
			if clientField != "" && strings.HasPrefix(clientField, "client ") {
				i = next2
				addrPort := clientField[7:]
				// Split ip:port — last colon separates port from IP (handles IPv6).
				lastColon := strings.LastIndexByte(addrPort, ':')
				if lastColon >= 0 {
					if !emit("client_ip", event.StringValue(addrPort[:lastColon])) {
						return nil
					}
					if !emit("client_port", InferValue(addrPort[lastColon+1:])) {
						return nil
					}
				} else {
					if !emit("client_ip", event.StringValue(addrPort)) {
						return nil
					}
				}
			}
		}
	}

parseMessage:
	// Rest is the message, which may start with an error code like "AH02032:"
	i = skipSpaces(s, i)
	if i >= len(s) {
		return nil
	}

	message := s[i:]

	// Try to extract error code (pattern: "AHnnnnn:" at start of message).
	if len(message) > 3 && message[0] == 'A' && message[1] == 'H' {
		codeEnd := strings.IndexByte(message, ':')
		if codeEnd > 0 && codeEnd < 12 {
			code := message[:codeEnd]
			// Verify it looks like an Apache error code (AH followed by digits).
			isCode := true
			for _, c := range code[2:] {
				if c < '0' || c > '9' {
					isCode = false
					break
				}
			}
			if isCode {
				if !emit("error_code", event.StringValue(code)) {
					return nil
				}
				message = strings.TrimSpace(message[codeEnd+1:])
			}
		}
	}

	if message != "" {
		if !emit("message", event.StringValue(message)) {
			return nil
		}
	}

	return nil
}

// scanBracketField reads a bracket-delimited field: "[content]".
// Returns the content (without brackets) and the index after the closing bracket.
// Returns empty string if no bracket field found at position i.
func scanBracketField(s string, i int) (string, int) {
	i = skipSpaces(s, i)
	if i >= len(s) || s[i] != '[' {
		return "", i
	}
	i++ // skip '['
	start := i
	for i < len(s) && s[i] != ']' {
		i++
	}
	if i >= len(s) {
		return "", i
	}
	content := s[start:i]
	i++ // skip ']'

	return content, i
}

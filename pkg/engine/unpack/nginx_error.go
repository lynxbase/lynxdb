package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// NginxErrorParser extracts fields from nginx error log format lines.
// Format: YYYY/MM/DD HH:MM:SS [severity] pid#tid: *cid message, key: value, ...
//
// Example:
//
//	2026/02/14 14:52:01 [error] 12345#67: *890 upstream timed out, client: 10.0.1.5, server: api.example.com, request: "GET /api HTTP/1.1"
type NginxErrorParser struct{}

// Name returns the parser format name.
func (p *NginxErrorParser) Name() string { return "nginx_error" }

// Parse extracts fields from an nginx error log line.
func (p *NginxErrorParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	i := 0

	// Parse timestamp: "YYYY/MM/DD HH:MM:SS"
	// Minimum: "YYYY/MM/DD HH:MM:SS" = 19 chars
	if len(s) < 19 {
		return nil
	}
	// Find the space that separates date and time, then the space after time
	spaceAfterDate := strings.IndexByte(s, ' ')
	if spaceAfterDate < 0 {
		return nil
	}
	spaceAfterTime := strings.IndexByte(s[spaceAfterDate+1:], ' ')
	if spaceAfterTime < 0 {
		return nil
	}
	spaceAfterTime += spaceAfterDate + 1

	timestamp := s[:spaceAfterTime]
	if !emit("timestamp", event.StringValue(timestamp)) {
		return nil
	}
	i = spaceAfterTime

	// Parse severity: [error], [warn], [info], etc.
	i = skipSpaces(s, i)
	if i >= len(s) || s[i] != '[' {
		return nil
	}
	i++ // skip '['
	bracketEnd := strings.IndexByte(s[i:], ']')
	if bracketEnd < 0 {
		return nil
	}
	severity := s[i : i+bracketEnd]
	if !emit("severity", event.StringValue(severity)) {
		return nil
	}
	i += bracketEnd + 1

	// Parse pid#tid:
	i = skipSpaces(s, i)
	pidTidEnd := strings.IndexByte(s[i:], ':')
	if pidTidEnd < 0 {
		return nil
	}
	pidTid := s[i : i+pidTidEnd]
	i += pidTidEnd + 1

	// Split pid#tid
	hashIdx := strings.IndexByte(pidTid, '#')
	if hashIdx >= 0 {
		if !emit("pid", InferValue(pidTid[:hashIdx])) {
			return nil
		}
		if !emit("tid", InferValue(pidTid[hashIdx+1:])) {
			return nil
		}
	}

	// Parse optional *cid
	i = skipSpaces(s, i)
	if i < len(s) && s[i] == '*' {
		i++ // skip '*'
		cidStart := i
		for i < len(s) && s[i] != ' ' && s[i] != ',' {
			i++
		}
		if !emit("cid", InferValue(s[cidStart:i])) {
			return nil
		}
		i = skipSpaces(s, i)
	}

	// The rest is: message, key: value, key: value, ...
	// Find the first ", key: " pattern (comma + space + word + colon + space).
	rest := s[i:]

	// Find the boundary between message and key-value pairs.
	// nginx uses ", key: value" format for structured fields.
	kvStart := findFirstKVPair(rest)

	var message string
	if kvStart >= 0 {
		message = rest[:kvStart]
	} else {
		message = rest
	}

	if message != "" {
		if !emit("message", event.StringValue(strings.TrimSpace(message))) {
			return nil
		}
	}

	// Parse key: value pairs from the rest.
	if kvStart >= 0 {
		kvSection := rest[kvStart:]
		parseNginxKVPairs(kvSection, emit)
	}

	return nil
}

// findFirstKVPair finds the position of the first ", key: value" pattern.
// Returns -1 if not found.
func findFirstKVPair(s string) int {
	// Known nginx error log keys
	knownKeys := []string{
		", client:", ", server:", ", request:", ", upstream:",
		", host:", ", referrer:", ", port:",
	}
	bestIdx := -1
	for _, prefix := range knownKeys {
		idx := strings.Index(strings.ToLower(s), prefix)
		if idx >= 0 && (bestIdx < 0 || idx < bestIdx) {
			bestIdx = idx
		}
	}

	return bestIdx
}

// parseNginxKVPairs parses ", key: value" pairs from an nginx error log tail.
func parseNginxKVPairs(s string, emit func(key string, val event.Value) bool) {
	// Split on ", " and parse key: value
	for len(s) > 0 {
		// Skip leading ", "
		if strings.HasPrefix(s, ", ") {
			s = s[2:]
		}

		// Find "key: value" — colon separates key and value
		colonIdx := strings.Index(s, ": ")
		if colonIdx < 0 {
			break
		}

		key := strings.TrimSpace(s[:colonIdx])
		s = s[colonIdx+2:]

		// Value extends until the next ", " or end of string
		var val string
		nextComma := strings.Index(s, ", ")
		if nextComma >= 0 {
			val = s[:nextComma]
			s = s[nextComma:]
		} else {
			val = s
			s = ""
		}

		// Strip surrounding quotes from request values
		val = strings.TrimSpace(val)
		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}

		if !emit(key, event.StringValue(val)) {
			return
		}

		// If key is "request", also extract method and uri
		if key == "request" {
			parts := strings.SplitN(val, " ", 3)
			if len(parts) >= 1 {
				if !emit("method", event.StringValue(parts[0])) {
					return
				}
			}
			if len(parts) >= 2 {
				if !emit("uri", event.StringValue(parts[1])) {
					return
				}
			}
		}
	}
}

package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// HAProxyParser extracts fields from HAProxy HTTP log format.
// Format (from the HAProxy docs, "option httplog"):
//
//	client_ip:client_port [accept_date] frontend_name backend_name/server_name
//	Tq/Tw/Tc/Tr/Tt status_code bytes_read termination_state
//	actconn/feconn/beconn/srv_conn/retries srv_queue/backend_queue
//	{request_headers} {response_headers} "method uri protocol"
//
// Example:
//
//	10.0.0.1:56000 [14/Feb/2026:14:52:01.234] web~ app/srv1 10/0/30/69/109 200 1234 - - ---- 1/1/0/0/0 0/0 "GET /api/health HTTP/1.1"
type HAProxyParser struct{}

// Name returns the parser format name.
func (p *HAProxyParser) Name() string { return "haproxy" }

// Parse extracts fields from an HAProxy HTTP log line.
func (p *HAProxyParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	i := 0

	// Optional syslog prefix: "Feb 14 14:52:01 hostname haproxy[pid]: "
	// Skip to the actual HAProxy log content by detecting client_ip:port pattern.
	// If the line starts with a letter (syslog month), skip to first IP:port.
	if len(s) > 0 && s[0] >= 'A' && s[0] <= 'Z' {
		haproxyStart := findHAProxyStart(s)
		if haproxyStart > 0 {
			i = haproxyStart
		}
	}

	// Parse client_ip:client_port
	colonIdx := strings.IndexByte(s[i:], ':')
	if colonIdx < 0 {
		return nil
	}
	colonIdx += i

	clientIP := s[i:colonIdx]
	i = colonIdx + 1

	// client_port: digits until space
	portStart := i
	for i < len(s) && s[i] != ' ' {
		i++
	}
	if i == portStart {
		return nil
	}
	clientPort := s[portStart:i]

	if !emit("client_ip", event.StringValue(clientIP)) {
		return nil
	}
	if !emit("client_port", InferValue(clientPort)) {
		return nil
	}

	// Parse [accept_date]
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
	i += bracketEnd + 1

	// Parse frontend_name (may have trailing '~' for SSL)
	i = skipSpaces(s, i)
	frontendEnd := i
	for frontendEnd < len(s) && s[frontendEnd] != ' ' {
		frontendEnd++
	}
	frontend := s[i:frontendEnd]
	frontend = strings.TrimRight(frontend, "~") // strip SSL indicator
	if !emit("frontend", event.StringValue(frontend)) {
		return nil
	}
	i = frontendEnd

	// Parse backend_name/server_name
	i = skipSpaces(s, i)
	bsEnd := i
	for bsEnd < len(s) && s[bsEnd] != ' ' {
		bsEnd++
	}
	backendServer := s[i:bsEnd]
	i = bsEnd

	slashIdx := strings.IndexByte(backendServer, '/')
	if slashIdx >= 0 {
		if !emit("backend", event.StringValue(backendServer[:slashIdx])) {
			return nil
		}
		if !emit("server", event.StringValue(backendServer[slashIdx+1:])) {
			return nil
		}
	} else {
		if !emit("backend", event.StringValue(backendServer)) {
			return nil
		}
	}

	// Parse Tq/Tw/Tc/Tr/Tt (5 timing values separated by /)
	i = skipSpaces(s, i)
	timingEnd := i
	for timingEnd < len(s) && s[timingEnd] != ' ' {
		timingEnd++
	}
	timings := s[i:timingEnd]
	i = timingEnd

	timingNames := []string{"tq", "tw", "tc", "tr", "tt"}
	timingParts := strings.Split(timings, "/")
	for idx, name := range timingNames {
		if idx < len(timingParts) {
			if !emit(name, InferValue(timingParts[idx])) {
				return nil
			}
		}
	}

	// Parse status_code
	i = skipSpaces(s, i)
	statusEnd := i
	for statusEnd < len(s) && s[statusEnd] != ' ' {
		statusEnd++
	}
	if i < statusEnd {
		if !emit("status", InferValue(s[i:statusEnd])) {
			return nil
		}
	}
	i = statusEnd

	// Parse bytes_read
	i = skipSpaces(s, i)
	bytesEnd := i
	for bytesEnd < len(s) && s[bytesEnd] != ' ' {
		bytesEnd++
	}
	if i < bytesEnd {
		if !emit("bytes", InferValue(s[i:bytesEnd])) {
			return nil
		}
	}
	i = bytesEnd

	// Parse captured_request_cookie (or -)
	i = skipSpaces(s, i)
	fieldEnd := i
	for fieldEnd < len(s) && s[fieldEnd] != ' ' {
		fieldEnd++
	}
	i = fieldEnd

	// Parse captured_response_cookie (or -)
	i = skipSpaces(s, i)
	fieldEnd = i
	for fieldEnd < len(s) && s[fieldEnd] != ' ' {
		fieldEnd++
	}
	i = fieldEnd

	// Parse termination_state (4 chars like "----" or "LR--")
	i = skipSpaces(s, i)
	termEnd := i
	for termEnd < len(s) && s[termEnd] != ' ' {
		termEnd++
	}
	if i < termEnd {
		if !emit("term_state", event.StringValue(s[i:termEnd])) {
			return nil
		}
	}
	i = termEnd

	// Parse actconn/feconn/beconn/srv_conn/retries
	i = skipSpaces(s, i)
	connEnd := i
	for connEnd < len(s) && s[connEnd] != ' ' {
		connEnd++
	}
	if i < connEnd {
		connStr := s[i:connEnd]
		connNames := []string{"actconn", "feconn", "beconn", "srv_conn", "retries"}
		connParts := strings.Split(connStr, "/")
		for idx, name := range connNames {
			if idx < len(connParts) {
				if !emit(name, InferValue(connParts[idx])) {
					return nil
				}
			}
		}
	}
	i = connEnd

	// Parse srv_queue/backend_queue
	i = skipSpaces(s, i)
	qEnd := i
	for qEnd < len(s) && s[qEnd] != ' ' {
		qEnd++
	}
	i = qEnd

	// Skip optional {captured_request_headers} and {captured_response_headers}
	i = skipSpaces(s, i)
	for i < len(s) && s[i] == '{' {
		braceEnd := strings.IndexByte(s[i:], '}')
		if braceEnd < 0 {
			break
		}
		i += braceEnd + 1
		i = skipSpaces(s, i)
	}

	// Parse "method uri protocol" (HTTP request in quotes)
	if i < len(s) && s[i] == '"' {
		i++ // skip opening quote
		closeQuote := strings.IndexByte(s[i:], '"')
		if closeQuote >= 0 {
			request := s[i : i+closeQuote]
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
		}
	}

	return nil
}

// findHAProxyStart finds the position of the HAProxy log content after
// an optional syslog prefix. Returns 0 if no syslog prefix detected.
// Looks for the pattern ": " followed by IP:port.
func findHAProxyStart(s string) int {
	idx := strings.Index(s, ": ")
	if idx < 0 {
		return 0
	}

	return idx + 2
}

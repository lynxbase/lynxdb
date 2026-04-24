package unpack

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// facilityNames maps syslog facility codes (0-23) to human-readable names
// per RFC 5424 Section 6.2.1.
var facilityNames = [24]string{
	"kern", "user", "mail", "daemon", "auth", "syslog",
	"lpr", "news", "uucp", "cron", "authpriv", "ftp",
	"ntp", "audit", "alert", "clock",
	"local0", "local1", "local2", "local3",
	"local4", "local5", "local6", "local7",
}

// severityNames maps syslog severity codes (0-7) to human-readable names
// per RFC 5424 Section 6.2.1.
var severityNames = [8]string{
	"emergency", "alert", "critical", "error",
	"warning", "notice", "info", "debug",
}

// SyslogParser extracts fields from syslog messages, auto-detecting RFC 3164
// (BSD) vs RFC 5424 format by checking for a version digit after the PRI header.
//
// RFC 3164: <PRI>TIMESTAMP HOSTNAME APP[PID]: MESSAGE
// RFC 5424: <PRI>VERSION TIMESTAMP HOSTNAME APPNAME PROCID MSGID STRUCTURED-DATA MSG
type SyslogParser struct{}

// Name returns the parser format name.
func (p *SyslogParser) Name() string { return "syslog" }

// DeclareFields declares the fields produced by the syslog parser.
func (p *SyslogParser) DeclareFields() FieldDeclaration {
	return FieldDeclaration{
		Known:    []string{"priority", "facility", "severity", "facility_name", "severity_name", "format", "timestamp", "hostname", "appname", "message"},
		Optional: []string{"procid", "version", "msgid", "structured_data"},
	}
}

// Parse extracts syslog fields from input and calls emit for each.
func (p *SyslogParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	i := 0

	var priority int
	if s[i] == '<' {
		i++
		priStart := i
		for i < len(s) && s[i] != '>' {
			i++
		}
		if i >= len(s) {
			return nil // malformed — no closing >
		}
		pri, err := strconv.Atoi(s[priStart:i])
		if err != nil {
			return nil // malformed PRI
		}
		priority = pri
		i++ // skip '>'
	} else {
		// No PRI header — common when syslog messages come through log shippers
		// (filebeat, fluentd, vector) that strip the PRI. Try parsing as RFC 3164 body.
		return p.parseRFC3164(s, emit)
	}

	// Emit priority, facility, severity.
	facility := priority / 8
	severity := priority % 8

	if !emit("priority", event.IntValue(int64(priority))) {
		return nil
	}
	if !emit("facility", event.IntValue(int64(facility))) {
		return nil
	}
	if !emit("severity", event.IntValue(int64(severity))) {
		return nil
	}

	// Emit human-readable facility and severity names.
	if facility < len(facilityNames) {
		if !emit("facility_name", event.StringValue(facilityNames[facility])) {
			return nil
		}
	}
	if severity < len(severityNames) {
		if !emit("severity_name", event.StringValue(severityNames[severity])) {
			return nil
		}
	}

	// Detect RFC 5424 vs RFC 3164.
	// RFC 5424 has a version digit immediately after PRI: <PRI>1 ...
	if i < len(s) && s[i] >= '1' && s[i] <= '9' && (i+1 >= len(s) || s[i+1] == ' ') {
		if !emit("format", event.StringValue("rfc5424")) {
			return nil
		}
		return p.parseRFC5424(s[i:], emit)
	}

	if !emit("format", event.StringValue("rfc3164")) {
		return nil
	}
	return p.parseRFC3164(s[i:], emit)
}

// parseRFC3164 parses: TIMESTAMP HOSTNAME APP[PID]: MESSAGE
// Timestamp format: "Feb 14 14:52:01" (Mmm dd HH:MM:SS)
func (p *SyslogParser) parseRFC3164(s string, emit func(string, event.Value) bool) error {
	// BSD syslog timestamp is always exactly 15 characters:
	// "Mmm dd HH:MM:SS" or "Mmm  d HH:MM:SS" (double space for single-digit day).
	// Examples: "Feb 14 14:52:01", "Mar  5 09:00:00"
	if len(s) < 16 { // 15 for timestamp + at least 1 space
		if !emit("message", event.StringValue(s)) {
			return nil
		}

		return nil
	}

	timestamp := s[:15]
	rest := s[16:] // skip timestamp + space

	if !emit("timestamp", event.StringValue(timestamp)) {
		return nil
	}

	// Hostname: next token until space.
	spaceIdx := strings.IndexByte(rest, ' ')
	if spaceIdx < 0 {
		if !emit("hostname", event.StringValue(rest)) {
			return nil
		}

		return nil
	}
	hostname := rest[:spaceIdx]
	rest = rest[spaceIdx+1:]

	if !emit("hostname", event.StringValue(hostname)) {
		return nil
	}

	// App[PID]: or App:
	colonIdx := strings.IndexByte(rest, ':')
	if colonIdx < 0 {
		if !emit("message", event.StringValue(rest)) {
			return nil
		}

		return nil
	}
	appPart := rest[:colonIdx]
	msg := ""
	if colonIdx+1 < len(rest) {
		msg = strings.TrimLeft(rest[colonIdx+1:], " ")
	}

	// Extract app and optional PID.
	if bracketIdx := strings.IndexByte(appPart, '['); bracketIdx >= 0 {
		appname := appPart[:bracketIdx]
		pid := strings.TrimRight(appPart[bracketIdx+1:], "]")
		if !emit("appname", event.StringValue(appname)) {
			return nil
		}
		if pid != "" {
			if !emit("procid", event.StringValue(pid)) {
				return nil
			}
		}
	} else {
		if !emit("appname", event.StringValue(appPart)) {
			return nil
		}
	}

	if !emit("message", event.StringValue(msg)) {
		return nil
	}

	return nil
}

// parseRFC5424 parses: VERSION TIMESTAMP HOSTNAME APPNAME PROCID MSGID SD MSG
func (p *SyslogParser) parseRFC5424(s string, emit func(string, event.Value) bool) error {
	// Tokenize by spaces. RFC 5424 has 6 space-delimited header fields,
	// then STRUCTURED-DATA (which may contain spaces inside brackets), then MSG.
	// VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP SD SP MSG
	fields := make([]string, 0, 7)
	rest := s

	for i := 0; i < 6; i++ {
		idx := strings.IndexByte(rest, ' ')
		if idx < 0 {
			fields = append(fields, rest)
			rest = ""

			break
		}
		fields = append(fields, rest[:idx])
		rest = rest[idx+1:]
	}

	// Parse STRUCTURED-DATA: either "-" or "[...]" (may contain spaces).
	if rest != "" {
		if rest[0] == '[' {
			// Find the matching closing bracket, accounting for nested brackets.
			depth := 0
			sdEnd := 0
			for j := 0; j < len(rest); j++ {
				if rest[j] == '[' {
					depth++
				} else if rest[j] == ']' {
					depth--
					if depth == 0 {
						sdEnd = j + 1

						break
					}
				}
			}
			if sdEnd > 0 {
				fields = append(fields, rest[:sdEnd])
				if sdEnd < len(rest) && rest[sdEnd] == ' ' {
					rest = rest[sdEnd+1:]
				} else {
					rest = rest[sdEnd:]
				}
			} else {
				fields = append(fields, rest)
				rest = ""
			}
		} else {
			// "-" or other token.
			idx := strings.IndexByte(rest, ' ')
			if idx < 0 {
				fields = append(fields, rest)
				rest = ""
			} else {
				fields = append(fields, rest[:idx])
				rest = rest[idx+1:]
			}
		}
	}

	nilval := func(s string) event.Value {
		if s == "-" {
			return event.NullValue()
		}

		return event.StringValue(s)
	}

	if len(fields) >= 1 {
		if !emit("version", InferValue(fields[0])) {
			return nil
		}
	}
	if len(fields) >= 2 {
		if !emit("timestamp", nilval(fields[1])) {
			return nil
		}
	}
	if len(fields) >= 3 {
		if !emit("hostname", nilval(fields[2])) {
			return nil
		}
	}
	if len(fields) >= 4 {
		if !emit("appname", nilval(fields[3])) {
			return nil
		}
	}
	if len(fields) >= 5 {
		if !emit("procid", nilval(fields[4])) {
			return nil
		}
	}
	if len(fields) >= 6 {
		if !emit("msgid", nilval(fields[5])) {
			return nil
		}
	}
	if len(fields) >= 7 {
		if !emit("structured_data", nilval(fields[6])) {
			return nil
		}
	}

	if rest != "" {
		if !emit("message", event.StringValue(rest)) {
			return nil
		}
	}

	return nil
}

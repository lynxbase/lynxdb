package unpack

import (
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// MySQLSlowParser extracts fields from MySQL slow query log entries.
// Each entry is multi-line but assumed to be concatenated into a single _raw
// string (lines joined with \n). The parser handles the standard slow query
// log format produced by MySQL 5.7+ and MariaDB.
//
// Format:
//
//	# Time: 2026-02-14T14:52:01.234567Z
//	# User@Host: root[root] @ localhost [127.0.0.1]  Id:   42
//	# Query_time: 3.456789  Lock_time: 0.000123  Rows_sent: 10  Rows_examined: 50000
//	SELECT * FROM users WHERE status = 'active';
type MySQLSlowParser struct{}

// Name returns the parser format name.
func (p *MySQLSlowParser) Name() string { return "mysql_slow" }

// Parse extracts fields from a MySQL slow query log entry.
func (p *MySQLSlowParser) Parse(input string, emit func(key string, val event.Value) bool) error {
	s := strings.TrimSpace(input)
	if len(s) == 0 {
		return nil
	}

	lines := strings.Split(s, "\n")
	var stmtLines []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "# Time:") {
			ts := strings.TrimSpace(line[7:])
			if !emit("timestamp", event.StringValue(ts)) {
				return nil
			}

			continue
		}

		if strings.HasPrefix(line, "# User@Host:") {
			if !parseUserHost(line[12:], emit) {
				return nil
			}

			continue
		}

		if strings.HasPrefix(line, "# Query_time:") {
			if !parseQueryMetrics(line[2:], emit) {
				return nil
			}

			continue
		}

		// Skip other comment lines (e.g., "# Bytes_sent:", "# Schema:", etc.)
		// but try to extract known fields from them.
		if strings.HasPrefix(line, "# Schema:") {
			schema := strings.TrimSpace(line[9:])
			if schema != "" {
				if !emit("schema", event.StringValue(schema)) {
					return nil
				}
			}

			continue
		}

		if strings.HasPrefix(line, "#") {
			continue
		}

		// Non-comment line is part of the SQL statement.
		stmtLines = append(stmtLines, line)
	}

	if len(stmtLines) > 0 {
		stmt := strings.Join(stmtLines, " ")
		// Remove trailing semicolons for cleaner output.
		stmt = strings.TrimRight(stmt, ";")
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			if !emit("statement", event.StringValue(stmt)) {
				return nil
			}
		}
	}

	return nil
}

// parseUserHost parses the "# User@Host:" line.
// Format variants:
//
//	root[root] @ localhost [127.0.0.1]  Id:   42
//	root[root] @  [10.0.0.1]  Id:   42
func parseUserHost(s string, emit func(key string, val event.Value) bool) bool {
	s = strings.TrimSpace(s)

	// Extract user — everything before the first '['.
	bracketIdx := strings.IndexByte(s, '[')
	if bracketIdx < 0 {
		return true
	}
	user := strings.TrimSpace(s[:bracketIdx])
	if user != "" {
		if !emit("user", event.StringValue(user)) {
			return false
		}
	}

	// Find the '@' separator.
	atIdx := strings.IndexByte(s, '@')
	if atIdx < 0 {
		return true
	}
	rest := strings.TrimSpace(s[atIdx+1:])

	// Parse host — may be "hostname [ip]" or just "[ip]".
	bracketIdx = strings.IndexByte(rest, '[')
	if bracketIdx >= 0 {
		host := strings.TrimSpace(rest[:bracketIdx])
		if host != "" {
			if !emit("host", event.StringValue(host)) {
				return false
			}
		}

		// Extract IP from brackets.
		closeBracket := strings.IndexByte(rest[bracketIdx+1:], ']')
		if closeBracket >= 0 {
			ip := rest[bracketIdx+1 : bracketIdx+1+closeBracket]
			if ip != "" {
				if !emit("client_ip", event.StringValue(ip)) {
					return false
				}
			}
			rest = rest[bracketIdx+1+closeBracket+1:]
		}
	} else {
		// No brackets — host is until space or end.
		spaceIdx := strings.IndexByte(rest, ' ')
		if spaceIdx >= 0 {
			host := rest[:spaceIdx]
			if host != "" {
				if !emit("host", event.StringValue(host)) {
					return false
				}
			}
			rest = rest[spaceIdx:]
		}
	}

	// Parse optional "Id: N" (connection ID).
	if idIdx := strings.Index(rest, "Id:"); idIdx >= 0 {
		idStr := strings.TrimSpace(rest[idIdx+3:])
		if idStr != "" {
			if !emit("connection_id", InferValue(idStr)) {
				return false
			}
		}
	}

	return true
}

// parseQueryMetrics parses the metrics line:
// "Query_time: 3.456  Lock_time: 0.001  Rows_sent: 10  Rows_examined: 50000"
func parseQueryMetrics(s string, emit func(key string, val event.Value) bool) bool {
	// Split by double-space to separate key-value pairs.
	s = strings.TrimSpace(s)

	// Mapping from MySQL field names to our output field names.
	fieldMap := map[string]string{
		"Query_time":    "query_time",
		"Lock_time":     "lock_time",
		"Rows_sent":     "rows_sent",
		"Rows_examined": "rows_examined",
	}

	// Parse "Key: value" pairs separated by whitespace.
	for len(s) > 0 {
		s = strings.TrimSpace(s)
		if s == "" {
			break
		}

		// Find "Key:"
		colonIdx := strings.IndexByte(s, ':')
		if colonIdx < 0 {
			break
		}
		key := strings.TrimSpace(s[:colonIdx])
		s = strings.TrimSpace(s[colonIdx+1:])

		// Find the value (until next whitespace or end).
		valEnd := 0
		for valEnd < len(s) && s[valEnd] != ' ' && s[valEnd] != '\t' {
			valEnd++
		}
		if valEnd == 0 {
			break
		}
		val := s[:valEnd]
		s = s[valEnd:]

		// Emit with mapped name if known, otherwise raw name.
		outKey := key
		if mapped, ok := fieldMap[key]; ok {
			outKey = mapped
		}
		if !emit(outKey, InferValue(val)) {
			return false
		}
	}

	return true
}

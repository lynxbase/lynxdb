// Package unpack provides format-specific parsers for extracting structured
// fields from log lines. Each parser implements the FormatParser interface
// and uses a callback-based design to write fields directly into batch columns,
// avoiding intermediate map[string]Value allocations per row.
package unpack

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// FormatParser parses a log line and emits extracted key-value pairs.
// The emit callback writes directly into batch columns. Returning false
// from emit short-circuits parsing (used for fields() filtering).
type FormatParser interface {
	// Parse extracts fields from input and calls emit for each key-value pair.
	// If emit returns false, parsing stops early (short-circuit for fields filtering).
	Parse(input string, emit func(key string, val event.Value) bool) error

	// Name returns the parser format name (e.g., "json", "logfmt", "syslog", "combined").
	Name() string
}

// NewParser creates a FormatParser for the given format name.
// Supported formats: json, logfmt, syslog, combined, clf, nginx_error, cef, kv,
// docker, redis, apache_error, postgres, mysql_slow, haproxy, leef, w3c.
// "pattern" and "w3c" (with custom header) require constructor arguments
// and should be instantiated directly via NewPatternParser / NewW3CParser.
func NewParser(format string) (FormatParser, error) {
	switch strings.ToLower(format) {
	case "json":
		return &JSONParser{}, nil
	case "logfmt":
		return &LogfmtParser{}, nil
	case "syslog":
		return &SyslogParser{}, nil
	case "combined":
		return &CombinedParser{}, nil
	case "clf":
		return &CLFParser{}, nil
	case "nginx_error":
		return &NginxErrorParser{}, nil
	case "cef":
		return &CEFParser{}, nil
	case "kv":
		return &KVParser{}, nil
	case "docker":
		return &DockerParser{}, nil
	case "redis":
		return &RedisParser{}, nil
	case "apache_error":
		return &ApacheErrorParser{}, nil
	case "postgres":
		return &PostgresParser{}, nil
	case "mysql_slow":
		return &MySQLSlowParser{}, nil
	case "haproxy":
		return &HAProxyParser{}, nil
	case "leef":
		return &LEEFParser{}, nil
	case "w3c":
		return &W3CParser{}, nil
	default:
		return nil, fmt.Errorf("unpack: unknown format %q (supported: json, logfmt, syslog, combined, clf, nginx_error, cef, kv, docker, redis, apache_error, postgres, mysql_slow, haproxy, leef, w3c)", format)
	}
}

// InferValue auto-detects the type of a string value and returns a typed event.Value.
// Detection order: bool → int → float → string.
func InferValue(s string) event.Value {
	// Boolean
	if strings.EqualFold(s, "true") {
		return event.BoolValue(true)
	}
	if strings.EqualFold(s, "false") {
		return event.BoolValue(false)
	}

	// Integer (avoid matching floats)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return event.IntValue(n)
	}

	// Float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return event.FloatValue(f)
	}

	// Default: string
	return event.StringValue(s)
}

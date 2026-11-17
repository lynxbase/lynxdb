package ndjson

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// FormatParser parses a log line and emits the key-value pairs it extracts.
type FormatParser interface {
	// Parse calls emit for each field of input; emit returning false stops it.
	Parse(input string, emit func(key string, val event.Value) bool) error

	// Name returns the parser format name: "syslog" or "combined".
	Name() string
}

// NewParser creates a FormatParser for the given format name: syslog or
// combined.
func NewParser(format string) (FormatParser, error) {
	switch strings.ToLower(format) {
	case "syslog":
		return &SyslogParser{}, nil
	case "combined":
		return &CombinedParser{}, nil
	default:
		return nil, fmt.Errorf("ndjson: unknown format %q (supported: syslog, combined)", format)
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

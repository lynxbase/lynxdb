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
		return nil, fmt.Errorf("unpack: unknown format %q (supported: syslog, combined)", format)
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

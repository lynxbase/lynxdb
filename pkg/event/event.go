package event

import (
	"fmt"
	"time"
)

// Event represents an atomic log entry in LynxDB.
type Event struct {
	// Time is the event timestamp.
	Time time.Time
	// Raw is the original log line.
	Raw string
	// Source identifies where the event came from (e.g., file path, network address).
	Source string
	// SourceType classifies the event format (e.g., "syslog", "json", "csv").
	SourceType string
	// Host is the hostname that generated the event.
	Host string
	// Index is the logical namespace this event belongs to.
	Index string
	// Fields holds extracted key-value fields.
	Fields map[string]Value
	// ParseError is true when JSON/KV parsing failed for this event.
	// The event still carries _raw and metadata; only field extraction is missing.
	ParseError bool
}

// NewEvent creates a new Event with the given timestamp and raw text.
func NewEvent(t time.Time, raw string) *Event {
	return &Event{
		Time:   t,
		Raw:    raw,
		Fields: make(map[string]Value),
	}
}

// SetField sets a named field on the event.
func (e *Event) SetField(name string, v Value) {
	if e.Fields == nil {
		e.Fields = make(map[string]Value)
	}
	e.Fields[name] = v
}

// GetField returns the named field value, or NullValue if not present.
func (e *Event) GetField(name string) Value {
	switch name {
	case "_time":
		return TimestampValue(e.Time)
	case "_raw":
		return StringValue(e.Raw)
	case "_source", "source":
		return StringValue(e.Source)
	case "_sourcetype", "sourcetype":
		return StringValue(e.SourceType)
	case "host":
		return StringValue(e.Host)
	case "index":
		// "index" is the physical partition key (event.Index). Segments
		// are stored under segments/hot/<INDEX_NAME>/, and index is a
		// real column in the .lsg format. Default to "main" if unset.
		idx := e.Index
		if idx == "" {
			idx = "main"
		}

		return StringValue(idx)
	}
	if e.Fields == nil {
		return NullValue()
	}
	v, ok := e.Fields[name]
	if !ok {
		return NullValue()
	}

	return v
}

// FieldNames returns all user-defined field names (excluding built-in fields).
func (e *Event) FieldNames() []string {
	names := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		names = append(names, k)
	}

	return names
}

// EstimateEventSize returns a conservative byte estimate for the in-memory
// representation of the event. Used by the scan operator for memory budget
// tracking. The estimate includes struct overhead, string data, and field
// map entries. It is intentionally an over-estimate to avoid under-counting.
func EstimateEventSize(e *Event) int64 {
	// Base: struct header + map header + pointer overhead.
	const baseBytes int64 = 128

	size := baseBytes
	size += int64(len(e.Raw))
	size += int64(len(e.Source))
	size += int64(len(e.SourceType))
	size += int64(len(e.Host))
	size += int64(len(e.Index))

	for k, v := range e.Fields {
		// Map entry: key string header + key data + Value struct.
		size += int64(len(k)) + 16
		if s, ok := v.TryAsString(); ok {
			size += int64(len(s))
		} else {
			size += 8 // numeric types
		}
	}

	return size
}

// String returns a human-readable representation of the event.
func (e *Event) String() string {
	return fmt.Sprintf("Event{time=%s, host=%s, index=%s, raw=%q}",
		e.Time.UTC().Format(time.RFC3339), e.Host, e.Index, truncate(e.Raw, 80))
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	return s[:maxLen-3] + "..."
}

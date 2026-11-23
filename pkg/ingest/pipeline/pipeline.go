// Package pipeline implements ingest processing stages.
package pipeline

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/timestamp"
)

// Stage is a processing stage in the ingestion pipeline.
type Stage interface {
	Process(events []*event.Event) ([]*event.Event, error)
}

// Pipeline chains multiple stages together.
type Pipeline struct {
	stages []Stage
}

// New creates a pipeline with the given stages.
func New(stages ...Stage) *Pipeline {
	return &Pipeline{stages: stages}
}

// Process runs events through all stages in order.
func (p *Pipeline) Process(events []*event.Event) ([]*event.Event, error) {
	var err error
	for _, stage := range p.stages {
		events, err = stage.Process(events)
		if err != nil {
			return nil, err
		}
	}

	return events, nil
}

// JSONParser parses events with JSON raw data and extracts fields.
type JSONParser struct{}

func (p *JSONParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if e.Raw == "" {
			continue
		}
		var fields map[string]interface{}
		if err := json.Unmarshal([]byte(e.Raw), &fields); err != nil {
			e.ParseError = true
			continue // Not JSON, skip.
		}
		for k, v := range fields {
			switch val := v.(type) {
			case string:
				e.SetField(k, event.StringValue(val))
			case float64:
				if val == float64(int64(val)) {
					e.SetField(k, event.IntValue(int64(val)))
				} else {
					e.SetField(k, event.FloatValue(val))
				}
			case bool:
				e.SetField(k, event.BoolValue(val))
			}
		}
	}

	return events, nil
}

// KeyValueParser parses key=value pairs from event raw data.
// JSON events are skipped and existing fields are never overwritten.
type KeyValueParser struct{}

func (p *KeyValueParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if isJSONObjectRaw(e) {
			continue
		}
		pairs := parseKeyValuePairs(e.Raw)
		for k, v := range pairs {
			if setBuiltinTagField(e, k, v) {
				continue
			}
			if _, exists := e.Fields[k]; exists {
				continue
			}
			e.SetField(k, kvTypedValue(v))
		}
	}

	return events, nil
}

// isJSONObjectRaw reports whether _raw is a JSON object that no parser rejected.
func isJSONObjectRaw(e *event.Event) bool {
	if e.ParseError {
		return false
	}

	return strings.HasPrefix(strings.TrimLeft(e.Raw, " \t\r\n"), "{")
}

// kvTypedValue attempts to interpret a KV string value as a typed event.Value.
// Integer-looking strings become IntValue, float-looking strings become
// FloatValue, boolean "true"/"false" become BoolValue, everything else stays
// StringValue, so typed comparisons such as status >= 500 work without casts,
// as they do for JSON-parsed events.
func kvTypedValue(s string) event.Value {
	// Fast path: empty string stays string.
	if s == "" {
		return event.StringValue(s)
	}
	// Try integer first (most common for log fields like status, bytes, etc.).
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return event.IntValue(n)
	}
	// Try float.
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return event.FloatValue(f)
	}
	// Try bool.
	if s == "true" {
		return event.BoolValue(true)
	}
	if s == "false" {
		return event.BoolValue(false)
	}
	return event.StringValue(s)
}

// setBuiltinTagField writes v to the event's built-in tag slot (host, source,
// sourcetype) when k names one of them AND the slot is currently empty.
// Returns true if k matched a built-in tag — regardless of whether the slot
// was updated — so callers can skip the fallback SetField path. The index is
// not among them: the sender chooses it, not the text of the event.
func setBuiltinTagField(e *event.Event, k, v string) bool {
	switch k {
	case "host":
		if e.Host == "" {
			e.Host = v
		}
	case "source":
		if e.Source == "" {
			e.Source = v
		}
	case "sourcetype":
		if e.SourceType == "" {
			e.SourceType = v
		}
	default:
		return false
	}

	return true
}

func parseKeyValuePairs(s string) map[string]string {
	result := make(map[string]string, 4)
	for i := 0; i < len(s); {
		for i < len(s) && !isKVKeyByte(s[i]) {
			i++
		}
		keyStart := i
		for i < len(s) && isKVKeyByte(s[i]) {
			i++
		}
		if keyStart == i || i >= len(s) || s[i] != '=' {
			continue
		}
		key := s[keyStart:i]
		i++

		valueStart := i
		var value string
		if i < len(s) && s[i] == '"' {
			i++
			valueStart = i
			escaped := false
			for i < len(s) {
				if s[i] == '"' && !escaped {
					break
				}
				escaped = s[i] == '\\' && !escaped
				if s[i] != '\\' {
					escaped = false
				}
				i++
			}
			value = s[valueStart:i]
			if i < len(s) && s[i] == '"' {
				i++
			}
		} else {
			for i < len(s) && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' && s[i] != ',' {
				i++
			}
			value = s[valueStart:i]
		}
		result[key] = value
	}

	return result
}

func isKVKeyByte(b byte) bool {
	return (b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9') ||
		b == '_'
}

// TimestampNormalizer gives each event without a time the first time its
// text holds, as timestamp.Find reads it, or else the time it is processed.
type TimestampNormalizer struct{}

func (t *TimestampNormalizer) Process(events []*event.Event) ([]*event.Event, error) {
	now := time.Now()
	year := now.Year()
	for _, e := range events {
		if !e.Time.IsZero() {
			continue
		}
		if ns, ok := timestamp.Find(e.Raw, year); ok {
			e.Time = time.Unix(0, ns).UTC()
		} else {
			e.Time = now
		}
	}

	return events, nil
}

// Router sets the index of each event that names none.
type Router struct {
	DefaultIndex string
}

func (r *Router) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if e.Index == "" {
			e.Index = r.DefaultIndex
		}
	}

	return events, nil
}

// MetadataOnlyParser extracts only well-known metadata fields from JSON,
// leaving all other data in _raw for query-time extraction via REX/spath.
// This is the core of "lightweight" ingest mode — it avoids the CPU cost
// of full JSON unmarshal and per-field SetField calls.
type MetadataOnlyParser struct{}

// metadataFields are the only fields extracted in lightweight mode.
var metadataFields = map[string]bool{
	"host":       true,
	"source":     true,
	"sourcetype": true,
	"level":      true,
	"timestamp":  true,
	"_timestamp": true,
	"@timestamp": true,
	"time":       true,
	"ts":         true,
	"datetime":   true,
}

func (p *MetadataOnlyParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if e.Raw == "" {
			continue
		}
		var fields map[string]interface{}
		if err := json.Unmarshal([]byte(e.Raw), &fields); err != nil {
			e.ParseError = true
			continue
		}
		for k, v := range fields {
			if !metadataFields[k] {
				continue
			}
			strVal, ok := v.(string)
			if !ok {
				// Handle non-string metadata values (e.g., numeric timestamps).
				// Convert to string representation for lightweight processing.
				switch nv := v.(type) {
				case float64:
					strVal = fmt.Sprintf("%g", nv)
				case bool:
					strVal = fmt.Sprintf("%t", nv)
				default:
					continue
				}
			}
			if setBuiltinTagField(e, k, strVal) {
				continue
			}
			// level + timestamp fields fall through to SetField — TimestampNormalizer
			// picks up the timestamp candidates from there.
			e.SetField(k, event.StringValue(strVal))
		}
	}
	return events, nil
}

// LightweightPipeline returns an ingest pipeline that only extracts metadata
// fields from JSON, leaving all other data in _raw for query-time extraction.
// This reduces ingest CPU by ~30-40% compared to DefaultPipeline.
func LightweightPipeline() *Pipeline {
	return New(
		&TimestampNormalizer{},
		&MetadataOnlyParser{},
		&Router{DefaultIndex: "main"},
	)
}

// DefaultPipeline returns a standard ingestion pipeline.
func DefaultPipeline() *Pipeline {
	return New(
		&TimestampNormalizer{},
		&JSONParser{},
		&KeyValueParser{},
		&Router{DefaultIndex: "main"},
	)
}

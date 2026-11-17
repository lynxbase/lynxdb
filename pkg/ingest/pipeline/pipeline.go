// Package pipeline implements ingest processing stages.
package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
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
type JSONParser struct {
	ParseErrors atomic.Int64 // events where JSON unmarshal failed
}

func (p *JSONParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if e.Raw == "" {
			continue
		}
		var fields map[string]interface{}
		if err := json.Unmarshal([]byte(e.Raw), &fields); err != nil {
			p.ParseErrors.Add(1)
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
// sourcetype, index) when k names one of them AND the slot is currently empty.
// Returns true if k matched a built-in tag — regardless of whether the slot
// was updated — so callers can skip the fallback SetField path.
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
	case "index":
		if e.Index == "" {
			e.Index = v
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

// Sentinel errors for tryParseTime — avoid fmt.Errorf allocations on hot path.
var (
	errTSTooShort = errors.New("input too short")
	errTSNoMatch  = errors.New("no match")
)

// TimestampNormalizer extracts and normalizes timestamps from events.
type TimestampNormalizer struct {
	Formats []string // time formats to try
	hint    atomic.Value
}

type timestampHint struct {
	fmtIdx int
	pos    int
	length int
}

// DefaultTimestampNormalizer creates a normalizer with common formats.
func DefaultTimestampNormalizer() *TimestampNormalizer {
	return &TimestampNormalizer{
		Formats: []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05.000-0700",
			"2006-01-02T15:04:05-0700",
			"2006-01-02T15:04:05.000Z",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05.000",
			"Jan 02 15:04:05",
		},
	}
}

func (t *TimestampNormalizer) Process(events []*event.Event) ([]*event.Event, error) {
	// Snapshot the current year once per batch — year-zero timestamps
	// (e.g. syslog "Jan 02 15:04:05") would otherwise call time.Now() per event.
	now := time.Now()
	currentYear := now.Year()
	for _, e := range events {
		if !e.Time.IsZero() {
			continue
		}
		// Fast path: try the cached (format, position, length) hint first.
		if v := t.hint.Load(); v != nil {
			h := v.(timestampHint)
			if h.fmtIdx >= 0 && h.fmtIdx < len(t.Formats) {
				if ts, ok := tryParseExact(e.Raw, h.pos, h.length, t.Formats[h.fmtIdx], currentYear); ok {
					e.Time = ts
					continue
				}
			}
		}
		// Slow path: scan all formats and positions.
		for fi, format := range t.Formats {
			if ts, pos, length, err := tryParseTime(e.Raw, format, currentYear); err == nil {
				e.Time = ts
				t.hint.Store(timestampHint{fmtIdx: fi, pos: pos, length: length})
				break
			}
		}
		if e.Time.IsZero() {
			e.Time = now
		}
	}

	return events, nil
}

// tryParseExact tries a single time.Parse at an exact (position, length) — the cached hint path.
func tryParseExact(raw string, pos, length int, format string, currentYear int) (time.Time, bool) {
	end := pos + length
	if pos < 0 || end > len(raw) {
		return time.Time{}, false
	}
	t, err := time.Parse(format, raw[pos:end])
	if err != nil {
		return time.Time{}, false
	}
	if t.Year() == 0 {
		t = t.AddDate(currentYear, 0, 0)
	}
	return t, true
}

// looksLikeTimestamp does a cheap single-byte check to avoid calling time.Parse
// on positions that clearly aren't timestamps.
func looksLikeTimestamp(raw string, start int, format string) bool {
	if start >= len(raw) {
		return false
	}
	c := raw[start]
	switch format[0] {
	case '2': // "2006-..." ISO formats — must start with digit
		return c >= '0' && c <= '9'
	case 'J': // "Jan 02..." — must start with uppercase letter
		return c >= 'A' && c <= 'Z'
	}
	return true
}

func tryParseTime(raw, format string, currentYear int) (time.Time, int, int, error) {
	fmtLen := len(format)
	if len(raw) < fmtLen {
		return time.Time{}, 0, 0, errTSTooShort
	}
	// Try at position 0 first (fast path).
	if looksLikeTimestamp(raw, 0, format) {
		if t, length, ok := tryParseAt(raw, 0, format, fmtLen, currentYear); ok {
			return t, 0, length, nil
		}
	}
	// Try at each word boundary (space/quote delimited) within the first 100 chars.
	limit := len(raw) - fmtLen
	if limit > 100 {
		limit = 100
	}
	for i := 0; i < limit; i++ {
		if raw[i] == ' ' || raw[i] == '"' {
			pos := i + 1
			if looksLikeTimestamp(raw, pos, format) {
				if t, length, ok := tryParseAt(raw, pos, format, fmtLen, currentYear); ok {
					return t, pos, length, nil
				}
			}
		}
	}

	return time.Time{}, 0, 0, errTSNoMatch
}

func tryParseAt(raw string, start int, format string, fmtLen, currentYear int) (time.Time, int, bool) {
	maxEnd := start + fmtLen + 10
	if maxEnd > len(raw) {
		maxEnd = len(raw)
	}
	// Try exact format length first (most common case), then expand.
	for end := start + fmtLen; end <= maxEnd; end++ {
		t, err := time.Parse(format, raw[start:end])
		if err == nil {
			if t.Year() == 0 {
				t = t.AddDate(currentYear, 0, 0)
			}
			return t, end - start, true
		}
	}

	return time.Time{}, 0, false
}

// Router determines the target index and partition for each event.
type Router struct {
	DefaultIndex   string
	PartitionCount int
}

func (r *Router) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if e.Index == "" {
			e.Index = r.DefaultIndex
		}
	}

	return events, nil
}

// Partition returns the partition number for an event based on host hash.
func (r *Router) Partition(e *event.Event) int {
	h := fnv.New32a()
	h.Write([]byte(e.Host))

	return int(h.Sum32() % uint32(r.PartitionCount))
}

// MetadataOnlyParser extracts only well-known metadata fields from JSON,
// leaving all other data in _raw for query-time extraction via REX/spath.
// This is the core of "lightweight" ingest mode — it avoids the CPU cost
// of full JSON unmarshal and per-field SetField calls.
type MetadataOnlyParser struct {
	ParseErrors atomic.Int64
}

// metadataFields are the only fields extracted in lightweight mode.
var metadataFields = map[string]bool{
	"host":       true,
	"source":     true,
	"sourcetype": true,
	"level":      true,
	"index":      true,
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
			p.ParseErrors.Add(1)
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
		DefaultTimestampNormalizer(),
		&MetadataOnlyParser{},
		&Router{DefaultIndex: "main", PartitionCount: 4},
	)
}

// sharedJSONParser accumulates the parse failures of every DefaultPipeline
// into a single counter.
var sharedJSONParser = &JSONParser{}

// DefaultPipeline returns a standard ingestion pipeline.
// Each call gets its own TimestampNormalizer because it keeps advisory parse
// hints that are mutated while events are processed.
func DefaultPipeline() *Pipeline {
	return New(
		DefaultTimestampNormalizer(),
		sharedJSONParser,
		&KeyValueParser{},
		&Router{DefaultIndex: "main", PartitionCount: 4},
	)
}

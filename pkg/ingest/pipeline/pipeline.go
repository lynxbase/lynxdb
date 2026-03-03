package pipeline

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
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

// ParseStage: JSON parser

// JSONParser parses events with JSON raw data and extracts fields.
type JSONParser struct{}

func (p *JSONParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if e.Raw == "" {
			continue
		}
		// Try to parse as JSON.
		var fields map[string]interface{}
		if err := json.Unmarshal([]byte(e.Raw), &fields); err != nil {
			continue // Not JSON, skip.
		}
		for k, v := range fields {
			switch val := v.(type) {
			case string:
				e.SetField(k, event.StringValue(val))
			case float64:
				// Check if it's an integer.
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
type KeyValueParser struct{}

func (p *KeyValueParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		pairs := parseKeyValuePairs(e.Raw)
		for k, v := range pairs {
			// Set built-in fields directly on the struct so
			// GetField returns them correctly.
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
				e.SetField(k, event.StringValue(v))
			}
		}
	}

	return events, nil
}

var kvPattern = regexp.MustCompile(`(\w+)=("(?:[^"\\]|\\.)*"|[^\s,]+)`)

func parseKeyValuePairs(s string) map[string]string {
	result := make(map[string]string)
	matches := kvPattern.FindAllStringSubmatch(s, -1)
	for _, m := range matches {
		key := m[1]
		value := m[2]
		// Strip quotes if present.
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}
		result[key] = value
	}

	return result
}

// TransformStage

// TimestampNormalizer extracts and normalizes timestamps from events.
type TimestampNormalizer struct {
	Formats []string // time formats to try
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
	for _, e := range events {
		if e.Time.IsZero() {
			// Try to extract timestamp from raw.
			for _, format := range t.Formats {
				if ts, err := tryParseTime(e.Raw, format); err == nil {
					e.Time = ts

					break
				}
			}
			// Fall back to current time.
			if e.Time.IsZero() {
				e.Time = time.Now()
			}
		}
	}

	return events, nil
}

func tryParseTime(raw, format string) (time.Time, error) {
	// Trim leading quote and whitespace (common in CSV fields).
	raw = strings.TrimLeft(raw, "\" ")
	fmtLen := len(format)
	if len(raw) < fmtLen {
		return time.Time{}, fmt.Errorf("too short")
	}
	// Try at position 0 first (fast path).
	if t, ok := tryParseAt(raw, 0, format, fmtLen); ok {
		return t, nil
	}
	// Try at each word boundary (space-delimited) within the first 100 chars.
	limit := len(raw) - fmtLen
	if limit > 100 {
		limit = 100
	}
	for i := 0; i < limit; i++ {
		if raw[i] == ' ' {
			if t, ok := tryParseAt(raw, i+1, format, fmtLen); ok {
				return t, nil
			}
		}
	}

	return time.Time{}, fmt.Errorf("no match")
}

func tryParseAt(raw string, start int, format string, fmtLen int) (time.Time, bool) {
	maxEnd := start + fmtLen + 10
	if maxEnd > len(raw) {
		maxEnd = len(raw)
	}
	for end := maxEnd; end >= start+fmtLen; end-- {
		t, err := time.Parse(format, raw[start:end])
		if err == nil {
			// If the format has no year component, Go defaults to year 0.
			// Set to the current year instead.
			if t.Year() == 0 {
				t = t.AddDate(time.Now().Year(), 0, 0)
			}

			return t, true
		}
	}

	return time.Time{}, false
}

// RouteStage

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

// BatchStage

// Batcher collects events into batches.
type Batcher struct {
	BatchSize int
}

func NewBatcher(batchSize int) *Batcher {
	return &Batcher{BatchSize: batchSize}
}

// Batch splits events into batches of the configured size.
func (b *Batcher) Batch(events []*event.Event) [][]*event.Event {
	var batches [][]*event.Event
	for i := 0; i < len(events); i += b.BatchSize {
		end := i + b.BatchSize
		if end > len(events) {
			end = len(events)
		}
		batches = append(batches, events[i:end])
	}

	return batches
}

// SyslogParser

// SyslogParser extracts fields from syslog-formatted messages.
type SyslogParser struct{}

var syslogPattern = regexp.MustCompile(`^<(\d+)>(\w{3}\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+(\S+?)(?:\[(\d+)\])?:\s*(.*)$`)

func (p *SyslogParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		matches := syslogPattern.FindStringSubmatch(e.Raw)
		if matches == nil {
			continue
		}
		e.SetField("priority", event.StringValue(matches[1]))
		e.Host = matches[3]
		e.SetField("program", event.StringValue(matches[4]))
		if matches[5] != "" {
			e.SetField("pid", event.StringValue(matches[5]))
		}
		e.SetField("message", event.StringValue(matches[6]))
	}

	return events, nil
}

// defaultPipeline is a shared instance returned by DefaultPipeline().
// All stages are stateless (they only read their own config and mutate
// the events passed in, not internal state), so sharing is safe.
var defaultPipeline = New(
	DefaultTimestampNormalizer(),
	&JSONParser{},
	&KeyValueParser{},
	&Router{DefaultIndex: "main", PartitionCount: 4},
)

// DefaultPipeline returns a standard ingestion pipeline.
// The returned instance is shared; callers must not modify it.
func DefaultPipeline() *Pipeline {
	return defaultPipeline
}

// isInternalField returns true for fields that are always available without parsing.
func isInternalField(name string) bool {
	switch name {
	case "_raw", "_time", "_source", "_sourcetype", "host", "index", "source", "sourcetype":
		return true
	}

	return false
}

// FastTimestampAssigner assigns time.Now() to events with zero timestamps
// without attempting any format-based parsing. Used when the full
// TimestampNormalizer would waste CPU trying formats that won't match
// (e.g., JSON lines where JSON parser is skipped).
type FastTimestampAssigner struct{}

func (f *FastTimestampAssigner) Process(events []*event.Event) ([]*event.Event, error) {
	now := time.Now()
	for _, e := range events {
		if e.Time.IsZero() {
			e.Time = now
		}
	}

	return events, nil
}

// SelectivePipeline builds an ingest pipeline that only runs the stages
// needed to produce the required fields. If requiredFields is nil, all stages
// run (same as DefaultPipeline). If requiredFields contains only internal
// fields (like _raw, _time), JSON and KV parsing are skipped entirely,
// and timestamp normalization uses a fast path (no format parsing).
func SelectivePipeline(requiredFields map[string]bool) *Pipeline {
	// Determine if we need JSON/KV parsing.
	needParsing := requiredFields == nil // nil means "all fields needed"
	if !needParsing {
		for field := range requiredFields {
			if !isInternalField(field) {
				needParsing = true

				break
			}
		}
	}

	var stages []Stage
	if needParsing {
		// Full timestamp normalization needed since JSON parser may extract timestamps.
		stages = append(stages, DefaultTimestampNormalizer(), &JSONParser{}, &KeyValueParser{})
	} else {
		// Fast path: no JSON/KV parsing, so timestamp normalization from raw text
		// will fail on JSON lines anyway. Use fast assigner to avoid wasted work.
		stages = append(stages, &FastTimestampAssigner{})
	}

	stages = append(stages, &Router{DefaultIndex: "main", PartitionCount: 4})

	return New(stages...)
}

// SplitRawLines splits raw text into individual events by newline.
func SplitRawLines(raw, source, sourceType string) []*event.Event {
	lines := strings.Split(raw, "\n")
	var events []*event.Event
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		e := event.NewEvent(time.Time{}, line)
		e.Source = source
		e.SourceType = sourceType
		events = append(events, e)
	}

	return events
}

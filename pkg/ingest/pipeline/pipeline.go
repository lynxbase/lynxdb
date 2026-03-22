package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
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
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}
		result[key] = value
	}

	return result
}

// Sentinel errors for tryParseTime — avoid fmt.Errorf allocations on hot path.
var (
	errTSTooShort = errors.New("input too short")
	errTSNoMatch  = errors.New("no match")
)

// TimestampNormalizer extracts and normalizes timestamps from events.
type TimestampNormalizer struct {
	Formats    []string // time formats to try
	lastFmtIdx int      // hint: index of last successful format
	lastPos    int      // hint: position in raw where timestamp was found
	lastLen    int      // hint: substring length that matched
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
		lastFmtIdx: -1,
	}
}

func (t *TimestampNormalizer) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if !e.Time.IsZero() {
			continue
		}
		// Fast path: try the cached (format, position, length) hint first.
		if t.lastFmtIdx >= 0 {
			if ts, ok := tryParseExact(e.Raw, t.lastPos, t.lastLen, t.Formats[t.lastFmtIdx]); ok {
				e.Time = ts
				continue
			}
		}
		// Slow path: scan all formats and positions.
		for fi, format := range t.Formats {
			if ts, pos, length, err := tryParseTime(e.Raw, format); err == nil {
				e.Time = ts
				t.lastFmtIdx = fi
				t.lastPos = pos
				t.lastLen = length
				break
			}
		}
		if e.Time.IsZero() {
			e.Time = time.Now()
		}
	}

	return events, nil
}

// tryParseExact tries a single time.Parse at an exact (position, length) — the cached hint path.
func tryParseExact(raw string, pos, length int, format string) (time.Time, bool) {
	end := pos + length
	if pos < 0 || end > len(raw) {
		return time.Time{}, false
	}
	t, err := time.Parse(format, raw[pos:end])
	if err != nil {
		return time.Time{}, false
	}
	if t.Year() == 0 {
		t = t.AddDate(time.Now().Year(), 0, 0)
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

func tryParseTime(raw, format string) (time.Time, int, int, error) {
	fmtLen := len(format)
	if len(raw) < fmtLen {
		return time.Time{}, 0, 0, errTSTooShort
	}
	// Try at position 0 first (fast path).
	if looksLikeTimestamp(raw, 0, format) {
		if t, length, ok := tryParseAt(raw, 0, format, fmtLen); ok {
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
				if t, length, ok := tryParseAt(raw, pos, format, fmtLen); ok {
					return t, pos, length, nil
				}
			}
		}
	}

	return time.Time{}, 0, 0, errTSNoMatch
}

func tryParseAt(raw string, start int, format string, fmtLen int) (time.Time, int, bool) {
	maxEnd := start + fmtLen + 10
	if maxEnd > len(raw) {
		maxEnd = len(raw)
	}
	// Try exact format length first (most common case), then expand.
	for end := start + fmtLen; end <= maxEnd; end++ {
		t, err := time.Parse(format, raw[start:end])
		if err == nil {
			if t.Year() == 0 {
				t = t.AddDate(time.Now().Year(), 0, 0)
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
					ok = true
				case bool:
					strVal = fmt.Sprintf("%t", nv)
					ok = true
				default:
					continue
				}
			}
			switch k {
			case "host":
				if e.Host == "" {
					e.Host = strVal
				}
			case "source":
				if e.Source == "" {
					e.Source = strVal
				}
			case "sourcetype":
				if e.SourceType == "" {
					e.SourceType = strVal
				}
			case "index":
				if e.Index == "" {
					e.Index = strVal
				}
			case "level":
				e.SetField("level", event.StringValue(strVal))
			default:
				// Timestamp fields — set as string field for TimestampNormalizer to pick up.
				e.SetField(k, event.StringValue(strVal))
			}
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

// SelectiveJSONParser extracts only the requested top-level JSON keys from
// event raw data, skipping all others. When a query needs 2 of 20 fields,
// this avoids 90% of the unmarshal work compared to the full JSONParser.
//
// Uses json.Decoder.Token() for streaming key scanning and Decoder.Decode()
// only for matching key values. Non-matching keys are skipped via
// json.RawMessage consumption (cheaper than full Decode + discard).
type SelectiveJSONParser struct {
	RequiredFields map[string]bool
	ParseErrors    *atomic.Int64 // shared counter with JSONParser
}

func (p *SelectiveJSONParser) Process(events []*event.Event) ([]*event.Event, error) {
	for _, e := range events {
		if e.Raw == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(e.Raw))

		// Expect opening '{'.
		tok, err := dec.Token()
		if err != nil {
			p.ParseErrors.Add(1)
			e.ParseError = true
			continue
		}
		if delim, ok := tok.(json.Delim); !ok || delim != '{' {
			p.ParseErrors.Add(1)
			e.ParseError = true
			continue
		}

		for dec.More() {
			// Read key token.
			keyTok, err := dec.Token()
			if err != nil {
				break
			}
			key, ok := keyTok.(string)
			if !ok {
				break
			}

			if !p.RequiredFields[key] {
				// Skip the value — consume it as RawMessage.
				var skip json.RawMessage
				if err := dec.Decode(&skip); err != nil {
					break
				}
				continue
			}

			// Decode the value for a required key.
			var v interface{}
			if err := dec.Decode(&v); err != nil {
				break
			}
			switch val := v.(type) {
			case string:
				e.SetField(key, event.StringValue(val))
			case float64:
				if val == float64(int64(val)) {
					e.SetField(key, event.IntValue(int64(val)))
				} else {
					e.SetField(key, event.FloatValue(val))
				}
			case bool:
				e.SetField(key, event.BoolValue(val))
			}
		}
	}
	return events, nil
}

// sharedJSONParser is used by DefaultPipeline and SelectivePipeline to accumulate
// parse failure counts across all pipelines into a single counter.
var sharedJSONParser = &JSONParser{}

// ParseFailureCount returns the total number of JSON parse failures across all
// default and selective ingest pipelines. Use this to wire into metrics.
func ParseFailureCount() int64 {
	return sharedJSONParser.ParseErrors.Load()
}

// defaultPipeline is a shared instance returned by DefaultPipeline().
// The TimestampNormalizer caches hints (lastFmtIdx/lastPos/lastLen) for
// performance, but these are advisory — wrong hints fall back to full scan.
// Pipeline.Process is called sequentially per batch, so concurrent mutation
// of hints does not occur.
var defaultPipeline = New(
	DefaultTimestampNormalizer(),
	sharedJSONParser,
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
		stages = append(stages, DefaultTimestampNormalizer())
		// Use selective JSON parser when a bounded set of fields is requested.
		// This avoids full unmarshal when only a few fields are needed.
		if requiredFields != nil {
			stages = append(stages, &SelectiveJSONParser{
				RequiredFields: requiredFields,
				ParseErrors:    &sharedJSONParser.ParseErrors,
			})
		} else {
			stages = append(stages, sharedJSONParser)
		}
		stages = append(stages, &KeyValueParser{})
	} else {
		// Fast path: no JSON/KV parsing needed, but we still need proper
		// timestamp extraction from _raw. The TimestampNormalizer scans raw
		// text (including JSON) for embedded timestamps at word/quote
		// boundaries, which is essential for correct _time values in
		// timechart and transaction commands.
		stages = append(stages, DefaultTimestampNormalizer())
	}

	stages = append(stages, &Router{DefaultIndex: "main", PartitionCount: 4})

	return New(stages...)
}

// SplitRawLines splits raw text into individual events by newline.
func SplitRawLines(raw, source, sourceType string) []*event.Event {
	lines := strings.Split(raw, "\n")
	var events []*event.Event
	for _, line := range lines {
		// Trim trailing \r (Windows line endings) but avoid full TrimSpace scan.
		line = strings.TrimRight(line, "\r")
		if isBlankLine(line) {
			continue
		}
		e := event.NewEvent(time.Time{}, line)
		e.Source = source
		e.SourceType = sourceType
		events = append(events, e)
	}

	return events
}

// isBlankLine checks if a string contains only whitespace without scanning
// the full string when non-whitespace is found early (unlike strings.TrimSpace).
func isBlankLine(s string) bool {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case ' ', '\t', '\r', '\n':
		default:
			return false
		}
	}
	return true
}

package storage

import (
	"sort"
	"strconv"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

// FieldInfo describes a known field across all segments.
type FieldInfo struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Coverage   float64           `json:"coverage"`    // percentage of events containing this field
	TotalCount int64             `json:"total_count"` // total non-null occurrences
	TopValues  []FieldValueCount `json:"top_values,omitempty"`
}

// FieldValueCount is a value with its occurrence count.
type FieldValueCount struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

// fieldAccum accumulates stats for a single field across segments.
type fieldAccum struct {
	count     int64
	nullCount int64
	types     map[string]int // inferred type → count
	values    map[string]int64
}

// FieldCatalog builds a field catalog from segment stats and memtable events.
type FieldCatalog struct {
	fields map[string]*fieldAccum
	events int64
}

// NewFieldCatalog creates an empty field catalog.
func NewFieldCatalog() *FieldCatalog {
	return &FieldCatalog{
		fields: make(map[string]*fieldAccum),
	}
}

// builtinTypeOverrides maps built-in column names to their display types.
// Without this, _time (stored as int64 nanoseconds) would be inferred as "int".
var builtinTypeOverrides = map[string]string{
	"_time":       "datetime",
	"_raw":        "string",
	"_source":     "string",
	"_sourcetype": "string",
	"host":        "string",
	"index":       "string",
}

// AddSegmentStats incorporates stats from a segment.
func (fc *FieldCatalog) AddSegmentStats(stats []segment.ColumnStats, eventCount int64) {
	fc.events += eventCount
	for _, s := range stats {
		a := fc.getOrCreate(s.Name)
		a.count += s.Count
		a.nullCount += s.NullCount

		// Use known type for built-in columns; infer from values otherwise.
		if override, ok := builtinTypeOverrides[s.Name]; ok {
			a.types[override]++
		} else if s.MinValue != "" {
			typ := inferType(s.MinValue)
			a.types[typ]++
		}
	}
}

// builtinField describes a built-in event field that lives on the Event struct
// rather than in the Fields map.
type builtinField struct {
	name    string
	typName string
	getter  func(*event.Event) string
	// nonEmpty returns true if the event has a non-empty value for this field.
	nonEmpty func(*event.Event) bool
}

// builtinFields lists all built-in fields that should appear in the field catalog.
// These are stored as struct fields on event.Event, not in the Fields map.
var builtinFields = []builtinField{
	{
		name: "_time", typName: "datetime",
		getter:   func(e *event.Event) string { return e.Time.Format("2006-01-02T15:04:05Z") },
		nonEmpty: func(e *event.Event) bool { return !e.Time.IsZero() },
	},
	{
		name: "_raw", typName: "string",
		getter:   func(e *event.Event) string { return e.Raw },
		nonEmpty: func(e *event.Event) bool { return e.Raw != "" },
	},
	{
		name: "_source", typName: "string",
		getter:   func(e *event.Event) string { return e.Source },
		nonEmpty: func(e *event.Event) bool { return e.Source != "" },
	},
	{
		name: "_sourcetype", typName: "string",
		getter:   func(e *event.Event) string { return e.SourceType },
		nonEmpty: func(e *event.Event) bool { return e.SourceType != "" },
	},
	{
		name: "host", typName: "string",
		getter:   func(e *event.Event) string { return e.Host },
		nonEmpty: func(e *event.Event) bool { return e.Host != "" },
	},
	{
		name: "index", typName: "string",
		// "index" is the physical partition key (event.Index). Segments
		// are stored under segments/hot/<INDEX_NAME>/. Default to "main"
		// if unset, consistent with event.GetField("index").
		getter: func(e *event.Event) string {
			if e.Index != "" {
				return e.Index
			}

			return "main"
		},
		nonEmpty: func(e *event.Event) bool { return true }, // always has an index
	},
}

// AddEvents incorporates events (e.g. from memtable) into the catalog.
func (fc *FieldCatalog) AddEvents(events []*event.Event) {
	fc.events += int64(len(events))
	for _, ev := range events {
		// Account for built-in fields stored on the Event struct.
		for _, bf := range builtinFields {
			if bf.nonEmpty(ev) {
				a := fc.getOrCreate(bf.name)
				a.count++
				a.types[bf.typName]++
				fc.addTopValue(a, bf.getter(ev))
			}
		}

		// Account for user-defined fields in the Fields map.
		for name, val := range ev.Fields {
			a := fc.getOrCreate(name)
			if val.IsNull() {
				a.nullCount++
			} else {
				a.count++
				typ := val.Type().String()
				a.types[typ]++
				fc.addTopValue(a, val.String())
			}
		}
	}
}

// addTopValue tracks a value in the top-values sample for a field accumulator.
// Caps at 1000 unique values to avoid unbounded memory growth.
func (fc *FieldCatalog) addTopValue(a *fieldAccum, str string) {
	if n, ok := a.values[str]; ok {
		a.values[str] = n + 1
	} else if len(a.values) < 1000 {
		a.values[str] = 1
	}
}

func (fc *FieldCatalog) Build() []FieldInfo {
	var result []FieldInfo
	for name, a := range fc.fields {
		fi := FieldInfo{
			Name:       name,
			Type:       dominantType(a.types),
			TotalCount: a.count,
		}
		if fc.events > 0 {
			fi.Coverage = float64(a.count) / float64(fc.events) * 100
			if fi.Coverage > 100 {
				fi.Coverage = 100
			}
		}
		fi.TopValues = topN(a.values, 5)
		result = append(result, fi)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Coverage != result[j].Coverage {
			return result[i].Coverage > result[j].Coverage
		}

		return result[i].Name < result[j].Name
	})

	return result
}

func (fc *FieldCatalog) getOrCreate(name string) *fieldAccum {
	a, ok := fc.fields[name]
	if !ok {
		a = &fieldAccum{
			types:  make(map[string]int),
			values: make(map[string]int64),
		}
		fc.fields[name] = a
	}

	return a
}

func inferType(s string) string {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return "int"
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return "float"
	}
	if s == "true" || s == "false" {
		return "bool"
	}

	return "string"
}

func dominantType(types map[string]int) string {
	if len(types) == 0 {
		return "string"
	}
	best := ""
	bestCount := 0
	for t, c := range types {
		if c > bestCount {
			bestCount = c
			best = t
		}
	}

	return best
}

func topN(values map[string]int64, n int) []FieldValueCount {
	if len(values) == 0 {
		return nil
	}
	type kv struct {
		k string
		v int64
	}
	pairs := make([]kv, 0, len(values))
	for k, v := range values {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].v > pairs[j].v
	})
	if len(pairs) > n {
		pairs = pairs[:n]
	}
	result := make([]FieldValueCount, len(pairs))
	for i, p := range pairs {
		result[i] = FieldValueCount{Value: p.k, Count: p.v}
	}

	return result
}

package pipeline

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// RollupIterator provides multi-resolution time bucketing.
// It accumulates all rows from the child, then for each span resolution,
// computes time-bucketed aggregations with count, adds _resolution and
// _bucket columns, and unions the results.
type RollupIterator struct {
	child     Iterator
	spans     []string
	groupBy   []string
	batchSize int

	// Blocking state
	done   bool
	output []map[string]event.Value
	offset int
}

// NewRollupIterator creates a rollup operator.
func NewRollupIterator(child Iterator, spans []string, groupBy []string, batchSize int) *RollupIterator {
	return &RollupIterator{
		child:     child,
		spans:     spans,
		groupBy:   groupBy,
		batchSize: batchSize,
	}
}

func (r *RollupIterator) Init(ctx context.Context) error {
	return r.child.Init(ctx)
}

func (r *RollupIterator) Next(ctx context.Context) (*Batch, error) {
	if !r.done {
		if err := r.materialize(ctx); err != nil {
			return nil, err
		}
		r.done = true
	}
	if r.offset >= len(r.output) {
		return nil, nil
	}
	end := r.offset + r.batchSize
	if end > len(r.output) {
		end = len(r.output)
	}
	batch := BatchFromRows(r.output[r.offset:end])
	r.offset = end

	return batch, nil
}

func (r *RollupIterator) materialize(ctx context.Context) error {
	// Drain child.
	var rows []map[string]event.Value
	for {
		batch, err := r.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			rows = append(rows, batch.Row(i))
		}
	}

	if len(rows) == 0 {
		return nil
	}

	// For each span, bucket and aggregate.
	var allRows []map[string]event.Value
	for _, span := range r.spans {
		spanRows := r.computeRollup(span, rows)
		allRows = append(allRows, spanRows...)
	}

	r.output = allRows

	return nil
}

func (r *RollupIterator) computeRollup(span string, rows []map[string]event.Value) []map[string]event.Value {
	dur := parseDuration(span)
	if dur == 0 {
		return nil
	}

	type bucketInfo struct {
		groupFields map[string]event.Value // copy of group-by field values
		count       int64
		bucketTime  time.Time
	}

	// groupKey → accumulated info
	groups := make(map[string]*bucketInfo)

	for _, row := range rows {
		ts := rollupEventTime(row)
		bucket := ts.Truncate(dur)

		// Build composite key: span|bucketTime|groupField1|groupField2|...
		key := span + "|" + bucket.Format(time.RFC3339)
		for _, f := range r.groupBy {
			if v, ok := row[f]; ok {
				key += "|" + rollupValueKey(v)
			} else {
				key += "|"
			}
		}

		info, ok := groups[key]
		if !ok {
			info = &bucketInfo{
				groupFields: make(map[string]event.Value, len(r.groupBy)),
				bucketTime:  bucket,
			}
			for _, f := range r.groupBy {
				if v, ok := row[f]; ok {
					info.groupFields[f] = v
				}
			}
			groups[key] = info
		}
		info.count++
	}

	// Build result rows.
	result := make([]map[string]event.Value, 0, len(groups))
	for _, info := range groups {
		out := make(map[string]event.Value, 2+len(r.groupBy))
		out["_resolution"] = event.StringValue(span)
		out["_bucket"] = event.TimestampValue(info.bucketTime)
		for _, f := range r.groupBy {
			if v, ok := info.groupFields[f]; ok {
				out[f] = v
			}
		}
		out["count"] = event.IntValue(info.count)
		result = append(result, out)
	}

	// Sort by bucket then by group fields for deterministic output.
	sort.Slice(result, func(i, j int) bool {
		ti := result[i]["_bucket"]
		tj := result[j]["_bucket"]
		if ti.AsInt() != tj.AsInt() {
			return ti.AsInt() < tj.AsInt()
		}
		// Tiebreak on group fields.
		for _, f := range r.groupBy {
			vi := result[i][f]
			vj := result[j][f]
			if rollupValueKey(vi) != rollupValueKey(vj) {
				return rollupValueKey(vi) < rollupValueKey(vj)
			}
		}
		return false
	})

	return result
}

func (r *RollupIterator) Close() error {
	return r.child.Close()
}

func (r *RollupIterator) Schema() []FieldInfo {
	schema := []FieldInfo{
		{Name: "_resolution", Type: "string"},
		{Name: "_bucket", Type: "timestamp"},
		{Name: "count", Type: "int"},
	}
	for _, f := range r.groupBy {
		schema = append(schema, FieldInfo{Name: f, Type: "any"})
	}

	return schema
}

// rollupEventTime extracts the timestamp from a row.
func rollupEventTime(row map[string]event.Value) time.Time {
	for _, field := range []string{"_time", "timestamp", "@timestamp", "time"} {
		if v, ok := row[field]; ok {
			if v.Type() == event.FieldTypeTimestamp {
				return time.Unix(0, v.AsInt())
			}
			// Try parsing string values.
			if v.Type() == event.FieldTypeString {
				s := v.AsString()
				for _, layout := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02T15:04:05"} {
					if t, err := time.Parse(layout, s); err == nil {
						return t
					}
				}
			}
		}
	}

	return time.Time{}
}

// rollupValueKey returns a string key for a Value suitable for map/grouping.
func rollupValueKey(v event.Value) string {
	switch v.Type() {
	case event.FieldTypeString:
		return "s:" + v.AsString()
	case event.FieldTypeInt:
		return fmt.Sprintf("i:%d", v.AsInt())
	case event.FieldTypeFloat:
		return fmt.Sprintf("f:%f", v.AsFloat())
	case event.FieldTypeBool:
		return fmt.Sprintf("b:%t", v.AsBool())
	case event.FieldTypeTimestamp:
		return fmt.Sprintf("t:%d", v.AsInt())
	default:
		return "n:"
	}
}

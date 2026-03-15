package pipeline

import (
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// builtinFieldOrder defines the canonical display order for LynxDB internal
// fields. Kept in sync with internal/output and pkg/api/rest.
var builtinFieldOrder = [...]string{
	"_time", "_raw", "index", "source", "_source", "sourcetype", "_sourcetype", "host",
}

var builtinFieldRank = func() map[string]int {
	m := make(map[string]int, len(builtinFieldOrder))
	for i, name := range builtinFieldOrder {
		m[name] = i
	}

	return m
}()

// DefaultBatchSize is the default number of rows per batch.
const DefaultBatchSize = 1024

// Batch is the unit of data transfer between operators.
// Columnar layout for CPU cache efficiency.
type Batch struct {
	Columns map[string][]event.Value
	Len     int
}

// NewBatch creates an empty batch with pre-allocated column maps.
func NewBatch(capacity int) *Batch {
	return &Batch{
		Columns: make(map[string][]event.Value),
		Len:     0,
	}
}

// AddRow adds a row (as a field map) to the batch.
// Columns not present in the current row are padded with null values to keep
// all columns at the same length as Len, preventing sparse-column panics in Slice.
func (b *Batch) AddRow(fields map[string]event.Value) {
	// Pad existing columns not present in this row with a null value.
	for k, col := range b.Columns {
		if _, ok := fields[k]; !ok {
			b.Columns[k] = append(col, event.Value{})
		}
	}
	for k, v := range fields {
		col := b.Columns[k]
		// Pad new columns that didn't exist in previous rows.
		for len(col) < b.Len {
			col = append(col, event.Value{})
		}
		b.Columns[k] = append(col, v)
	}
	b.Len++
}

// Value returns the value at (column, row) without materializing a full row map.
// Returns a null Value if the column does not exist or the row index is out of range.
func (b *Batch) Value(column string, row int) event.Value {
	col, ok := b.Columns[column]
	if !ok || row < 0 || row >= len(col) {
		return event.Value{}
	}

	return col[row]
}

// Row returns the fields for a given row index.
func (b *Batch) Row(i int) map[string]event.Value {
	row := make(map[string]event.Value, len(b.Columns))
	for k, col := range b.Columns {
		if i < len(col) {
			row[k] = col[i]
		}
	}

	return row
}

// ColumnNames returns all column names in the batch in deterministic order:
// builtin fields in canonical order, then user-defined fields alphabetically.
func (b *Batch) ColumnNames() []string {
	names := make([]string, 0, len(b.Columns))
	for k := range b.Columns {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool {
		ri, oki := builtinFieldRank[names[i]]
		rj, okj := builtinFieldRank[names[j]]
		switch {
		case oki && okj:
			return ri < rj
		case oki:
			return true // builtin before user
		case okj:
			return false // user after builtin
		default:
			return names[i] < names[j]
		}
	})

	return names
}

// Slice returns a new batch containing rows [start:end).
// Safe with sparse columns: per-column bounds are clamped to actual column length.
func (b *Batch) Slice(start, end int) *Batch {
	if start < 0 {
		start = 0
	}
	if start >= b.Len {
		return &Batch{Columns: make(map[string][]event.Value), Len: 0}
	}
	if end > b.Len {
		end = b.Len
	}
	nb := &Batch{
		Columns: make(map[string][]event.Value, len(b.Columns)),
		Len:     end - start,
	}
	for k, col := range b.Columns {
		colStart := start
		colEnd := end
		if colStart >= len(col) {
			continue // column has no values in this range
		}
		if colEnd > len(col) {
			colEnd = len(col)
		}
		nb.Columns[k] = col[colStart:colEnd]
	}

	return nb
}

// BatchFromEvents converts a slice of events into a Batch.
// Optimized to build columns directly with exact-size pre-allocation,
// avoiding per-event map allocation and append-doubling overhead.
func BatchFromEvents(events []*event.Event) *Batch {
	n := len(events)
	if n == 0 {
		return &Batch{Columns: make(map[string][]event.Value), Len: 0}
	}

	// Discover all user-defined field names and check builtin presence.
	fieldSet := make(map[string]bool, 16)
	hasSource, hasSourceType, hasHost, hasIndex := false, false, false, false
	for _, ev := range events {
		if ev.Source != "" {
			hasSource = true
		}
		if ev.SourceType != "" {
			hasSourceType = true
		}
		if ev.Host != "" {
			hasHost = true
		}
		if ev.Index != "" {
			hasIndex = true
		}
		for k := range ev.Fields {
			fieldSet[k] = true
		}
	}

	// Estimate column count for map pre-allocation.
	colCount := 2 + len(fieldSet) // _time + _raw + user fields
	if hasSource {
		colCount += 2 // _source + source alias
	}
	if hasSourceType {
		colCount += 2 // _sourcetype + sourcetype alias
	}
	if hasHost {
		colCount++
	}
	if hasIndex {
		colCount++
	}

	b := &Batch{Columns: make(map[string][]event.Value, colCount), Len: n}

	// Pre-allocate builtin columns with exact size.
	// Skip _time when all events have zero time (e.g. aggregation results)
	// to avoid int64 overflow from time.Time{}.UnixNano() (year 0001 < 1678 minimum).
	// Skip _raw when all events have empty raw (meaningless for aggregation output).
	hasTime, hasRaw := false, false
	for _, ev := range events {
		if !ev.Time.IsZero() {
			hasTime = true
		}
		if ev.Raw != "" {
			hasRaw = true
		}
		if hasTime && hasRaw {
			break
		}
	}
	if hasTime {
		times := make([]event.Value, n)
		for i, ev := range events {
			times[i] = event.TimestampValue(ev.Time)
		}
		b.Columns["_time"] = times
	}
	if hasRaw {
		raws := make([]event.Value, n)
		for i, ev := range events {
			raws[i] = event.StringValue(ev.Raw)
		}
		b.Columns["_raw"] = raws
	}

	if hasSource {
		col := make([]event.Value, n)
		for i, ev := range events {
			col[i] = event.StringValue(ev.Source)
		}
		b.Columns["_source"] = col
		b.Columns["source"] = col // alias: SPL2 queries use "source" without underscore
		b.Columns["index"] = col  // Splunk compat: "index" is a virtual alias for _source
	}
	if hasSourceType {
		col := make([]event.Value, n)
		for i, ev := range events {
			col[i] = event.StringValue(ev.SourceType)
		}
		b.Columns["_sourcetype"] = col
		b.Columns["sourcetype"] = col // alias
	}
	if hasHost {
		col := make([]event.Value, n)
		for i, ev := range events {
			col[i] = event.StringValue(ev.Host)
		}
		b.Columns["host"] = col
	}
	// Event.Index (logical namespace like "main") is intentionally NOT
	// exposed as a batch column. The "index" column is a Splunk-compatible
	// alias for _source. The physical index name is available via query hints.

	// User-defined fields: one pre-allocated column per field.
	for field := range fieldSet {
		col := make([]event.Value, n)
		for i, ev := range events {
			if v, ok := ev.Fields[field]; ok {
				col[i] = v
			}
		}
		b.Columns[field] = col
	}

	return b
}

// AppendBatch appends all rows from other into b, merging columns.
// Columns present in other but not in b are created with leading nulls.
// Columns present in b but not in other are extended with trailing nulls.
func (b *Batch) AppendBatch(other *Batch) {
	if other == nil || other.Len == 0 {
		return
	}

	newLen := b.Len + other.Len

	// Extend existing columns that are NOT in other with trailing nulls.
	for k, col := range b.Columns {
		if _, exists := other.Columns[k]; !exists {
			extended := make([]event.Value, newLen)
			copy(extended, col)
			b.Columns[k] = extended
		}
	}

	// For each column in other: append (or create with leading nulls).
	for k, otherCol := range other.Columns {
		if col, exists := b.Columns[k]; exists {
			extended := make([]event.Value, newLen)
			copy(extended, col)
			copy(extended[b.Len:], otherCol)
			b.Columns[k] = extended
		} else {
			newCol := make([]event.Value, newLen)
			copy(newCol[b.Len:], otherCol)
			b.Columns[k] = newCol
		}
	}

	b.Len = newLen
}

// PermuteSlice creates a new batch from a subset of rows reordered by indices.
// Each element in indices refers to a row index in the original batch.
func (b *Batch) PermuteSlice(indices []int) *Batch {
	result := &Batch{
		Columns: make(map[string][]event.Value, len(b.Columns)),
		Len:     len(indices),
	}
	for k, col := range b.Columns {
		newCol := make([]event.Value, len(indices))
		for i, idx := range indices {
			if idx < len(col) {
				newCol[i] = col[idx]
			}
		}
		result.Columns[k] = newCol
	}

	return result
}

// BatchFromRows converts a slice of field maps into a Batch.
func BatchFromRows(rows []map[string]event.Value) *Batch {
	b := NewBatch(len(rows))
	for _, row := range rows {
		b.AddRow(row)
	}

	return b
}

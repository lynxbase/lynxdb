package pipeline

import (
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// builtinFieldOrder defines the canonical display order for LynxDB internal
// fields. Kept in sync with internal/output and pkg/api/rest.
var builtinFieldOrder = [...]string{
	"_time", "_raw", "index", "_source", "_sourcetype", "source", "sourcetype", "host",
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
	// Present optionally tracks per-row field presence for sparse columns.
	// Missing metadata for a column means every in-range cell is present.
	Present map[string][]bool
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
			pres := b.ensurePresence(k)
			b.Present[k] = append(pres, false)
		}
	}
	for k, v := range fields {
		col := b.Columns[k]
		_, exists := b.Columns[k]
		// Pad new columns that didn't exist in previous rows.
		for len(col) < b.Len {
			col = append(col, event.Value{})
		}
		b.Columns[k] = append(col, v)
		if exists {
			pres := b.ensurePresence(k)
			b.Present[k] = append(pres, true)
		} else {
			b.markSparsePrefixPresent(k)
		}
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
		if i < len(col) && b.IsPresent(k, i) {
			row[k] = col[i]
		}
	}

	return row
}

// RowInto fills dst with the fields for row i, reusing the map allocation.
// Callers can pass the same dst across iterations to avoid per-row allocations.
// The map is cleared before filling.
func (b *Batch) RowInto(i int, dst map[string]event.Value) {
	for k := range dst {
		delete(dst, k)
	}
	for k, col := range b.Columns {
		if i < len(col) && b.IsPresent(k, i) {
			dst[k] = col[i]
		}
	}
}

// IsPresent reports whether column has a value in row. Columns without
// explicit presence metadata are dense, so every in-range cell is present.
func (b *Batch) IsPresent(column string, row int) bool {
	if row < 0 || row >= b.Len {
		return false
	}
	pres, ok := b.Present[column]
	if !ok {
		return true
	}
	return row < len(pres) && pres[row]
}

// MarkPresent marks a sparse cell as present. It is a no-op for dense columns.
func (b *Batch) MarkPresent(column string, row int) {
	if row < 0 || row >= b.Len {
		return
	}
	pres, ok := b.Present[column]
	if !ok {
		return
	}
	for len(pres) < b.Len {
		pres = append(pres, true)
	}
	pres[row] = true
	b.Present[column] = pres
}

func (b *Batch) ensurePresence(column string) []bool {
	if b.Present == nil {
		b.Present = make(map[string][]bool)
	}
	if pres, ok := b.Present[column]; ok {
		for len(pres) < b.Len {
			pres = append(pres, true)
		}
		return pres
	}
	pres := make([]bool, b.Len)
	for i := range pres {
		pres[i] = true
	}
	return pres
}

func (b *Batch) markSparsePrefixPresent(column string) {
	if b.Len == 0 {
		return
	}
	if b.Present == nil {
		b.Present = make(map[string][]bool)
	}
	pres := make([]bool, b.Len+1)
	pres[b.Len] = true
	b.Present[column] = pres
}

func setPresenceIfSparse(b *Batch, column string, pres []bool) {
	if allPresent(pres) {
		return
	}
	if b.Present == nil {
		b.Present = make(map[string][]bool)
	}
	b.Present[column] = pres
}

func allPresent(pres []bool) bool {
	for _, ok := range pres {
		if !ok {
			return false
		}
	}
	return true
}

func densePresence(n int) []bool {
	pres := make([]bool, n)
	for i := range pres {
		pres[i] = true
	}
	return pres
}

func presenceFor(b *Batch, column string, n int) []bool {
	if b == nil {
		return nil
	}
	if pres, ok := b.Present[column]; ok {
		out := make([]bool, n)
		copy(out, pres)
		return out
	}
	return densePresence(n)
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
	copyPresenceSlice(nb, b, start, end)

	return nb
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
			pres := presenceFor(b, k, b.Len)
			pres = append(pres, make([]bool, other.Len)...)
			setPresenceIfSparse(b, k, pres)
		}
	}

	// For each column in other: append (or create with leading nulls).
	for k, otherCol := range other.Columns {
		if col, exists := b.Columns[k]; exists {
			extended := make([]event.Value, newLen)
			copy(extended, col)
			copy(extended[b.Len:], otherCol)
			b.Columns[k] = extended
			pres := presenceFor(b, k, b.Len)
			pres = append(pres, presenceFor(other, k, other.Len)...)
			setPresenceIfSparse(b, k, pres)
		} else {
			newCol := make([]event.Value, newLen)
			copy(newCol[b.Len:], otherCol)
			b.Columns[k] = newCol
			pres := make([]bool, b.Len, newLen)
			pres = append(pres, presenceFor(other, k, other.Len)...)
			setPresenceIfSparse(b, k, pres)
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
	copyPresencePermutation(result, b, indices)

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

func copyPresenceSlice(dst, src *Batch, start, end int) {
	if src == nil || dst == nil || len(src.Present) == 0 || start >= end {
		return
	}
	dst.Present = make(map[string][]bool, len(src.Present))
	for field, pres := range src.Present {
		if start >= len(pres) {
			continue
		}
		presEnd := end
		if presEnd > len(pres) {
			presEnd = len(pres)
		}
		out := make([]bool, end-start)
		copy(out, pres[start:presEnd])
		if !allPresent(out) {
			dst.Present[field] = out
		}
	}
	if len(dst.Present) == 0 {
		dst.Present = nil
	}
}

func copyPresencePermutation(dst, src *Batch, indices []int) {
	if src == nil || dst == nil || len(src.Present) == 0 || len(indices) == 0 {
		return
	}
	dst.Present = make(map[string][]bool, len(src.Present))
	for field, pres := range src.Present {
		out := make([]bool, len(indices))
		for newRow, oldRow := range indices {
			if oldRow >= 0 && oldRow < len(pres) {
				out[newRow] = pres[oldRow]
			}
		}
		if !allPresent(out) {
			dst.Present[field] = out
		}
	}
	if len(dst.Present) == 0 {
		dst.Present = nil
	}
}

func copyPresenceColumn(dst, src *Batch, column string) {
	if src == nil || dst == nil {
		return
	}
	pres, ok := src.Present[column]
	if !ok {
		return
	}
	out := make([]bool, len(pres))
	copy(out, pres)
	setPresenceIfSparse(dst, column, out)
}

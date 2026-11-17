package pipeline

import (
	"sort"

	"github.com/RoaringBitmap/roaring"

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

	// BSIHandledRows marks batch rows that survived a range-BSI row mask.
	// BSIHandledFields narrows that proof to a specific field when available.
	BSIHandledRows   *roaring.Bitmap
	BSIHandledFields map[string]*roaring.Bitmap
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

// MarkAbsent marks a cell as absent while leaving its padded column value null.
func (b *Batch) MarkAbsent(column string, row int) {
	if row < 0 || row >= b.Len {
		return
	}
	pres := b.ensurePresence(column)
	for len(pres) < b.Len {
		pres = append(pres, true)
	}
	pres[row] = false
	b.Present[column] = pres
}

func (b *Batch) MarkColumnAbsent(column string) {
	if b.Len == 0 {
		return
	}
	if b.Present == nil {
		b.Present = make(map[string][]bool)
	}
	b.Present[column] = make([]bool, b.Len)
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
	copyBSIMetadataSlice(nb, b, start, end)

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
		pres := make([]bool, n)
		for i, ev := range events {
			if !ev.Time.IsZero() {
				times[i] = event.TimestampValue(ev.Time)
				pres[i] = true
			}
		}
		b.Columns["_time"] = times
		setPresenceIfSparse(b, "_time", pres)
	}
	if hasRaw {
		raws := make([]event.Value, n)
		pres := make([]bool, n)
		for i, ev := range events {
			if ev.Raw != "" {
				raws[i] = event.StringValue(ev.Raw)
				pres[i] = true
			}
		}
		b.Columns["_raw"] = raws
		setPresenceIfSparse(b, "_raw", pres)
	}

	if hasSource {
		col := make([]event.Value, n)
		pres := make([]bool, n)
		for i, ev := range events {
			if ev.Source != "" {
				col[i] = event.StringValue(ev.Source)
				pres[i] = true
			}
		}
		b.Columns["_source"] = col
		b.Columns["source"] = col // alias without the underscore
		setPresenceIfSparse(b, "_source", pres)
		setPresenceIfSparse(b, "source", pres)
	}
	if hasSourceType {
		col := make([]event.Value, n)
		pres := make([]bool, n)
		for i, ev := range events {
			if ev.SourceType != "" {
				col[i] = event.StringValue(ev.SourceType)
				pres[i] = true
			}
		}
		b.Columns["_sourcetype"] = col
		b.Columns["sourcetype"] = col // alias
		setPresenceIfSparse(b, "_sourcetype", pres)
		setPresenceIfSparse(b, "sourcetype", pres)
	}
	if hasHost {
		col := make([]event.Value, n)
		pres := make([]bool, n)
		for i, ev := range events {
			if ev.Host != "" {
				col[i] = event.StringValue(ev.Host)
				pres[i] = true
			}
		}
		b.Columns["host"] = col
		setPresenceIfSparse(b, "host", pres)
	}
	if hasIndex {
		col := make([]event.Value, n)
		pres := make([]bool, n)
		for i, ev := range events {
			if ev.Index != "" {
				col[i] = event.StringValue(ev.Index)
				pres[i] = true
			}
		}
		b.Columns["index"] = col
		setPresenceIfSparse(b, "index", pres)
	}

	// User-defined fields: one pre-allocated column per field.
	for field := range fieldSet {
		col := make([]event.Value, n)
		pres := make([]bool, n)
		for i, ev := range events {
			if v, ok := ev.Fields[field]; ok {
				col[i] = v
				pres[i] = true
			}
		}
		b.Columns[field] = col
		setPresenceIfSparse(b, field, pres)
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

	offset := b.Len
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
	appendBSIMetadata(b, other, offset)
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
	copyBSIMetadataPermutation(result, b, indices)

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

func copyBSIMetadataSlice(dst, src *Batch, start, end int) {
	if src == nil || dst == nil || start >= end {
		return
	}
	dst.BSIHandledRows = sliceBitmap(src.BSIHandledRows, start, end)
	if len(src.BSIHandledFields) == 0 {
		return
	}
	dst.BSIHandledFields = make(map[string]*roaring.Bitmap, len(src.BSIHandledFields))
	for field, bm := range src.BSIHandledFields {
		if sliced := sliceBitmap(bm, start, end); sliced != nil && !sliced.IsEmpty() {
			dst.BSIHandledFields[field] = sliced
		}
	}
}

func appendBSIMetadata(dst, src *Batch, offset int) {
	if dst == nil || src == nil {
		return
	}
	if src.BSIHandledRows != nil && !src.BSIHandledRows.IsEmpty() {
		if dst.BSIHandledRows == nil {
			dst.BSIHandledRows = roaring.New()
		}
		shifted := translateBatchBitmap(src.BSIHandledRows, uint32(offset))
		dst.BSIHandledRows.Or(shifted)
	}
	if len(src.BSIHandledFields) == 0 {
		return
	}
	if dst.BSIHandledFields == nil {
		dst.BSIHandledFields = make(map[string]*roaring.Bitmap, len(src.BSIHandledFields))
	}
	for field, bm := range src.BSIHandledFields {
		if bm == nil || bm.IsEmpty() {
			continue
		}
		shifted := translateBatchBitmap(bm, uint32(offset))
		if dst.BSIHandledFields[field] == nil {
			dst.BSIHandledFields[field] = shifted
			continue
		}
		dst.BSIHandledFields[field].Or(shifted)
	}
}

func copyBSIMetadataPermutation(dst, src *Batch, indices []int) {
	if src == nil || dst == nil || len(indices) == 0 {
		return
	}
	dst.BSIHandledRows = permuteBitmap(src.BSIHandledRows, indices)
	if len(src.BSIHandledFields) == 0 {
		return
	}
	dst.BSIHandledFields = make(map[string]*roaring.Bitmap, len(src.BSIHandledFields))
	for field, bm := range src.BSIHandledFields {
		if permuted := permuteBitmap(bm, indices); permuted != nil && !permuted.IsEmpty() {
			dst.BSIHandledFields[field] = permuted
		}
	}
}

func sliceBitmap(src *roaring.Bitmap, start, end int) *roaring.Bitmap {
	if src == nil || src.IsEmpty() || start >= end {
		return nil
	}
	out := roaring.New()
	for it := src.Iterator(); it.HasNext(); {
		row := int(it.Next())
		if row < start {
			continue
		}
		if row >= end {
			break
		}
		out.Add(uint32(row - start))
	}
	if out.IsEmpty() {
		return nil
	}

	return out
}

func permuteBitmap(src *roaring.Bitmap, indices []int) *roaring.Bitmap {
	if src == nil || src.IsEmpty() {
		return nil
	}
	out := roaring.New()
	for newRow, oldRow := range indices {
		if oldRow >= 0 && src.Contains(uint32(oldRow)) {
			out.Add(uint32(newRow))
		}
	}
	if out.IsEmpty() {
		return nil
	}

	return out
}

func translateBatchBitmap(src *roaring.Bitmap, offset uint32) *roaring.Bitmap {
	if src == nil || src.IsEmpty() {
		return nil
	}
	out := roaring.New()
	for it := src.Iterator(); it.HasNext(); {
		out.Add(it.Next() + offset)
	}

	return out
}

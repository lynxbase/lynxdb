package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/column"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// QueryHints provides optional hints for row group pruning.
type QueryHints struct {
	MinTime     *time.Time // prune row groups entirely before this time
	MaxTime     *time.Time // prune row groups entirely after this time
	Columns     []string   // only read these columns (projection)
	Predicates  []Predicate
	SearchTerms []string // bloom filter terms for row group pruning
}

// Reader reads .lsg V4 segment files.
type Reader struct {
	data         []byte
	footer       *Footer
	columnIndex  map[string]int                        // catalog name → index (built once on open)
	perColBlooms map[int]map[string]*index.BloomFilter // lazily populated: rgIdx → colName → bloom
}

// OpenSegment opens a segment from raw bytes.
func OpenSegment(data []byte) (*Reader, error) {
	if len(data) < HeaderSize+8 {
		return nil, fmt.Errorf("%w: file too small", ErrCorruptSegment)
	}

	// Verify header magic.
	if string(data[0:4]) != MagicBytes {
		return nil, ErrInvalidMagic
	}

	version := binary.LittleEndian.Uint16(data[4:6])
	if version != FormatV4 {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrCorruptSegment, version)
	}

	footer, err := decodeFooter(data)
	if err != nil {
		return nil, fmt.Errorf("segment: parse footer: %w", err)
	}

	// Build column index from catalog for O(1) presence bitmap lookups.
	colIdx := make(map[string]int, len(footer.Catalog))
	for i, cat := range footer.Catalog {
		colIdx[cat.Name] = i
	}

	return &Reader{
		data:         data,
		footer:       footer,
		columnIndex:  colIdx,
		perColBlooms: make(map[int]map[string]*index.BloomFilter),
	}, nil
}

// EventCount returns the number of events in the segment.
func (r *Reader) EventCount() int64 {
	return r.footer.EventCount
}

// BytesRead returns the on-disk size of the segment data in bytes.
// This is an approximation of the total I/O performed when reading the segment
// (exact byte tracking would require instrumenting every io.Read call).
func (r *Reader) BytesRead() int64 {
	return int64(len(r.data))
}

// MaxTimestamp returns the maximum _time value in the segment from zone maps.
// Returns zero time if _time column is not found.
func (r *Reader) MaxTimestamp() time.Time {
	stats := r.StatsByName("_time")
	if stats == nil || stats.MaxValue == "" {
		return time.Time{}
	}
	ns, err := strconv.ParseInt(stats.MaxValue, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(0, ns)
}

// MinTimestamp returns the minimum _time value in the segment from zone maps.
// Returns zero time if _time column is not found.
func (r *Reader) MinTimestamp() time.Time {
	stats := r.StatsByName("_time")
	if stats == nil || stats.MinValue == "" {
		return time.Time{}
	}
	ns, err := strconv.ParseInt(stats.MinValue, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(0, ns)
}

// Stats returns aggregated column statistics across all row groups.
func (r *Reader) Stats() []ColumnStats {
	return r.footer.Stats()
}

// ColumnNames returns all column names in the segment.
// Includes both chunk columns and const columns.
func (r *Reader) ColumnNames() []string {
	if len(r.footer.RowGroups) == 0 {
		return nil
	}
	rg := &r.footer.RowGroups[0]
	seen := make(map[string]bool, len(rg.Columns)+len(rg.ConstColumns))
	names := make([]string, 0, len(rg.Columns)+len(rg.ConstColumns))
	for _, c := range rg.Columns {
		if !seen[c.Name] {
			names = append(names, c.Name)
			seen[c.Name] = true
		}
	}
	for _, cc := range rg.ConstColumns {
		if !seen[cc.Name] {
			names = append(names, cc.Name)
			seen[cc.Name] = true
		}
	}

	return names
}

// HasColumn returns true if the segment contains the named column
// (either as a chunk or a const column).
func (r *Reader) HasColumn(name string) bool {
	if len(r.footer.RowGroups) == 0 {
		return false
	}
	rg := &r.footer.RowGroups[0]
	for _, c := range rg.Columns {
		if c.Name == name {
			return true
		}
	}
	for _, cc := range rg.ConstColumns {
		if cc.Name == name {
			return true
		}
	}

	return false
}

// HasColumnInRowGroup returns true if the named column is present in the given
// row group, using the presence bitmap for O(1) check when the column index < 64.
func (r *Reader) HasColumnInRowGroup(rgIdx int, columnName string) bool {
	if rgIdx < 0 || rgIdx >= len(r.footer.RowGroups) {
		return false
	}
	idx, ok := r.columnIndex[columnName]
	if ok && idx < 64 {
		return r.footer.RowGroups[rgIdx].ColumnPresenceBits&(1<<uint(idx)) != 0
	}
	// Fallback for columns at index >= 64 or unknown columns: scan arrays.
	rg := &r.footer.RowGroups[rgIdx]
	for _, c := range rg.Columns {
		if c.Name == columnName {
			return true
		}
	}
	for _, cc := range rg.ConstColumns {
		if cc.Name == columnName {
			return true
		}
	}

	return false
}

// IsConstColumn returns true if the named column is a const column in the given row group.
func (r *Reader) IsConstColumn(rgIdx int, columnName string) bool {
	if rgIdx < 0 || rgIdx >= len(r.footer.RowGroups) {
		return false
	}
	for _, cc := range r.footer.RowGroups[rgIdx].ConstColumns {
		if cc.Name == columnName {
			return true
		}
	}

	return false
}

// GetConstValue returns the constant value for a column in a row group, if it is a const column.
// Returns ("", false) if the column is not const.
func (r *Reader) GetConstValue(rgIdx int, columnName string) (string, bool) {
	if rgIdx < 0 || rgIdx >= len(r.footer.RowGroups) {
		return "", false
	}
	for _, cc := range r.footer.RowGroups[rgIdx].ConstColumns {
		if cc.Name == columnName {
			return cc.Value, true
		}
	}

	return "", false
}

// ColumnChunkInRowGroup returns the ColumnChunkMeta for a named column in a row group.
// Returns nil if the column doesn't exist as a chunk (may be const or absent).
func (r *Reader) ColumnChunkInRowGroup(rgIdx int, name string) *ColumnChunkMeta {
	if rgIdx < 0 || rgIdx >= len(r.footer.RowGroups) {
		return nil
	}
	for i := range r.footer.RowGroups[rgIdx].Columns {
		if r.footer.RowGroups[rgIdx].Columns[i].Name == name {
			return &r.footer.RowGroups[rgIdx].Columns[i]
		}
	}

	return nil
}

// StatsByName returns column statistics for the named column, or nil if not found.
func (r *Reader) StatsByName(name string) *ColumnStats {
	stats := r.footer.Stats()
	for i := range stats {
		if stats[i].Name == name {
			return &stats[i]
		}
	}

	return nil
}

// readChunk reads a column chunk from a specific row group, handling layer 2 decompression.
func (r *Reader) readChunk(cc *ColumnChunkMeta) ([]byte, error) {
	start := cc.Offset
	end := cc.Offset + cc.Length
	if start < 0 || end > int64(len(r.data)) {
		return nil, fmt.Errorf("%w: column %q offset out of range", ErrCorruptSegment, cc.Name)
	}
	chunkData := r.data[start:end]

	// Verify CRC.
	if crc32.ChecksumIEEE(chunkData) != cc.CRC32 {
		return nil, fmt.Errorf("%w: column %q", ErrChecksumMismatch, cc.Name)
	}

	// Layer 2 decompression.
	switch cc.Compression {
	case CompressionNone:
		return chunkData, nil
	case CompressionLZ4:
		return decompressLZ4Block(chunkData)
	case CompressionZSTD:
		return decompressZSTDBlock(chunkData)
	default:
		return nil, fmt.Errorf("%w: unsupported compression %d", ErrCorruptSegment, cc.Compression)
	}
}

// findChunk finds a column chunk by name in a row group.
func findChunk(rg *RowGroupMeta, name string) *ColumnChunkMeta {
	for i := range rg.Columns {
		if rg.Columns[i].Name == name {
			return &rg.Columns[i]
		}
	}

	return nil
}

// findConstColumn finds a const column entry by name in a row group.
func findConstColumn(rg *RowGroupMeta, name string) *ConstColumnEntry {
	for i := range rg.ConstColumns {
		if rg.ConstColumns[i].Name == name {
			return &rg.ConstColumns[i]
		}
	}

	return nil
}

// readStringsFromChunk decodes a string column chunk.
func (r *Reader) readStringsFromChunk(cc *ColumnChunkMeta) ([]string, error) {
	data, err := r.readChunk(cc)
	if err != nil {
		return nil, err
	}
	encType := column.EncodingType(cc.EncodingType)
	switch encType {
	case column.EncodingDict8, column.EncodingDict16:
		return column.NewDictEncoder().DecodeStrings(data)
	case column.EncodingLZ4:
		return column.NewLZ4Encoder().DecodeStrings(data)
	default:
		return nil, fmt.Errorf("%w: column %q has encoding %v, expected string", ErrCorruptSegment, cc.Name, encType)
	}
}

// readInt64sFromChunk decodes an int64 column chunk.
func (r *Reader) readInt64sFromChunk(cc *ColumnChunkMeta) ([]int64, error) {
	data, err := r.readChunk(cc)
	if err != nil {
		return nil, err
	}
	if column.EncodingType(cc.EncodingType) != column.EncodingDelta {
		return nil, fmt.Errorf("%w: column %q has encoding %v, expected delta", ErrCorruptSegment, cc.Name, cc.EncodingType)
	}

	return column.NewDeltaEncoder().DecodeInt64s(data)
}

// readFloat64sFromChunk decodes a float64 column chunk.
func (r *Reader) readFloat64sFromChunk(cc *ColumnChunkMeta) ([]float64, error) {
	data, err := r.readChunk(cc)
	if err != nil {
		return nil, err
	}
	if column.EncodingType(cc.EncodingType) != column.EncodingGorilla {
		return nil, fmt.Errorf("%w: column %q has encoding %v, expected gorilla", ErrCorruptSegment, cc.Name, cc.EncodingType)
	}

	return column.NewGorillaEncoder().DecodeFloat64s(data)
}

// ReadStrings decodes a string column by name (all row groups concatenated).
// Handles both chunk columns and const columns.
func (r *Reader) ReadStrings(name string) ([]string, error) {
	var result []string
	for rgi := range r.footer.RowGroups {
		rg := &r.footer.RowGroups[rgi]
		// Check const columns first.
		if cc := findConstColumn(rg, name); cc != nil {
			vals := make([]string, rg.RowCount)
			for i := range vals {
				vals[i] = cc.Value
			}
			result = append(result, vals...)

			continue
		}
		cc := findChunk(rg, name)
		if cc == nil {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, name)
		}
		vals, err := r.readStringsFromChunk(cc)
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
	}

	return result, nil
}

// ReadInt64s decodes an int64 column by name (all row groups concatenated).
func (r *Reader) ReadInt64s(name string) ([]int64, error) {
	var result []int64
	for rgi := range r.footer.RowGroups {
		cc := findChunk(&r.footer.RowGroups[rgi], name)
		if cc == nil {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, name)
		}
		vals, err := r.readInt64sFromChunk(cc)
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
	}

	return result, nil
}

// ReadFloat64s decodes a float64 column by name (all row groups concatenated).
func (r *Reader) ReadFloat64s(name string) ([]float64, error) {
	var result []float64
	for rgi := range r.footer.RowGroups {
		cc := findChunk(&r.footer.RowGroups[rgi], name)
		if cc == nil {
			return nil, fmt.Errorf("%w: %q", ErrColumnNotFound, name)
		}
		vals, err := r.readFloat64sFromChunk(cc)
		if err != nil {
			return nil, err
		}
		result = append(result, vals...)
	}

	return result, nil
}

// ReadTimestamps reads the _time column as time.Time values.
func (r *Reader) ReadTimestamps() ([]time.Time, error) {
	nanos, err := r.ReadInt64s("_time")
	if err != nil {
		return nil, err
	}
	times := make([]time.Time, len(nanos))
	for i, ns := range nanos {
		times[i] = time.Unix(0, ns)
	}

	return times, nil
}

// ReadEvents reconstructs full events from the segment (all row groups).
func (r *Reader) ReadEvents() ([]*event.Event, error) {
	count := int(r.footer.EventCount)
	events := make([]*event.Event, 0, count)

	for rgi := range r.footer.RowGroups {
		rg := &r.footer.RowGroups[rgi]
		rgEvents, err := r.readRowGroupEvents(rg)
		if err != nil {
			return nil, err
		}
		events = append(events, rgEvents...)
	}

	return events, nil
}

// ReadEventsWithColumns reconstructs events reading only the requested columns.
func (r *Reader) ReadEventsWithColumns(columns []string) ([]*event.Event, error) {
	need := make(map[string]bool, len(columns))
	for _, c := range columns {
		need[c] = true
	}

	count := int(r.footer.EventCount)
	events := make([]*event.Event, 0, count)

	for rgi := range r.footer.RowGroups {
		rg := &r.footer.RowGroups[rgi]
		rgEvents, err := r.readRowGroupEventsProjected(rg, need)
		if err != nil {
			return nil, err
		}
		events = append(events, rgEvents...)
	}

	return events, nil
}

// ReadEventsWithHints reads events using query hints for row group pruning.
func (r *Reader) ReadEventsWithHints(hints QueryHints) ([]*event.Event, error) {
	need := make(map[string]bool)
	for _, c := range hints.Columns {
		need[c] = true
	}

	// Pre-compute bloom-eligible row groups if search terms provided.
	var bloomEligible map[int]bool
	if len(hints.SearchTerms) > 0 {
		eligible, err := r.CheckBloomAllTermsForRowGroups(hints.SearchTerms)
		if err == nil {
			bloomEligible = make(map[int]bool, len(eligible))
			for _, idx := range eligible {
				bloomEligible[idx] = true
			}
		}
	}

	var events []*event.Event
	rowOffset := 0

	for rgi := range r.footer.RowGroups {
		rg := &r.footer.RowGroups[rgi]

		// Zone map pruning: check _time range.
		if hints.MinTime != nil || hints.MaxTime != nil {
			if r.canPruneRowGroup(rg, hints.MinTime, hints.MaxTime) {
				rowOffset += rg.RowCount

				continue
			}
		}

		// Bloom filter pruning: skip row groups where search terms are absent.
		if bloomEligible != nil && !bloomEligible[rgi] {
			rowOffset += rg.RowCount

			continue
		}

		var rgEvents []*event.Event
		var err error
		if len(need) > 0 {
			rgEvents, err = r.readRowGroupEventsProjected(rg, need)
		} else {
			rgEvents, err = r.readRowGroupEvents(rg)
		}
		if err != nil {
			return nil, err
		}
		events = append(events, rgEvents...)
		rowOffset += rg.RowCount
	}

	return events, nil
}

// RowGroupCount returns the number of row groups in this segment.
func (r *Reader) RowGroupCount() int {
	return len(r.footer.RowGroups)
}

// ReadRowGroup reads all events from row group at the given index.
func (r *Reader) ReadRowGroup(idx int) ([]*event.Event, error) {
	if idx < 0 || idx >= len(r.footer.RowGroups) {
		return nil, fmt.Errorf("segment: row group index %d out of range [0, %d)", idx, len(r.footer.RowGroups))
	}

	return r.readRowGroupEvents(&r.footer.RowGroups[idx])
}

// CanPruneRowGroupByIndex checks if row group at rgIdx can be skipped by time range.
// Public, index-based wrapper around canPruneRowGroup for streaming iterators
// that address row groups by index rather than pointer.
func (r *Reader) CanPruneRowGroupByIndex(rgIdx int, minTime, maxTime *time.Time) bool {
	if rgIdx < 0 || rgIdx >= len(r.footer.RowGroups) {
		return false
	}

	return r.canPruneRowGroup(&r.footer.RowGroups[rgIdx], minTime, maxTime)
}

// canPruneRowGroup checks if a row group can be entirely skipped based on time range.
func (r *Reader) canPruneRowGroup(rg *RowGroupMeta, minTime, maxTime *time.Time) bool {
	timeChunk := findChunk(rg, "_time")
	if timeChunk == nil {
		return false
	}

	rgMinNano, err := strconv.ParseInt(timeChunk.MinValue, 10, 64)
	if err != nil {
		return false
	}
	rgMaxNano, err := strconv.ParseInt(timeChunk.MaxValue, 10, 64)
	if err != nil {
		return false
	}

	rgMin := time.Unix(0, rgMinNano)
	rgMax := time.Unix(0, rgMaxNano)

	if minTime != nil && rgMax.Before(*minTime) {
		return true // entire row group is before the query range
	}
	if maxTime != nil && rgMin.After(*maxTime) {
		return true // entire row group is after the query range
	}

	return false
}

// readRowGroupEvents reads all events from a single row group.
// Handles both chunk columns and const columns.
func (r *Reader) readRowGroupEvents(rg *RowGroupMeta) ([]*event.Event, error) {
	count := rg.RowCount

	// Read timestamps.
	timeChunk := findChunk(rg, "_time")
	if timeChunk == nil {
		return nil, fmt.Errorf("segment: row group missing _time column")
	}
	timestamps, err := r.readInt64sFromChunk(timeChunk)
	if err != nil {
		return nil, fmt.Errorf("segment: read _time: %w", err)
	}

	// Read built-in string columns (from chunks or const).
	type builtinCol struct {
		name   string
		setter func(e *event.Event, v string)
	}
	builtins := []builtinCol{
		{"_raw", func(e *event.Event, v string) { e.Raw = v }},
		{"_source", func(e *event.Event, v string) { e.Source = v }},
		{"_sourcetype", func(e *event.Event, v string) { e.SourceType = v }},
		{"host", func(e *event.Event, v string) { e.Host = v }},
		{"index", func(e *event.Event, v string) { e.Index = v }},
	}

	// Collect values from chunks or const entries.
	builtinValues := make(map[string][]string, len(builtins))
	builtinConst := make(map[string]string, len(rg.ConstColumns))
	for _, b := range builtins {
		if cc := findConstColumn(rg, b.name); cc != nil {
			builtinConst[b.name] = cc.Value

			continue
		}
		cc := findChunk(rg, b.name)
		if cc == nil {
			continue
		}
		vals, err := r.readStringsFromChunk(cc)
		if err != nil {
			return nil, fmt.Errorf("segment: read %s: %w", b.name, err)
		}
		builtinValues[b.name] = vals
	}

	// Arena allocation: one contiguous slab for all Event structs.
	arena := make([]event.Event, count)
	events := make([]*event.Event, count)
	for i := 0; i < count; i++ {
		arena[i].Time = time.Unix(0, timestamps[i])
		events[i] = &arena[i]
	}

	for _, b := range builtins {
		if constVal, ok := builtinConst[b.name]; ok {
			for i := range events {
				b.setter(events[i], constVal)
			}

			continue
		}
		vals, ok := builtinValues[b.name]
		if !ok {
			continue
		}
		for i, v := range vals {
			b.setter(events[i], v)
		}
	}

	// Read user-defined field columns (chunks).
	builtinCols := map[string]bool{
		"_time": true, "_raw": true, "_source": true,
		"_sourcetype": true, "host": true, "index": true,
	}

	for ci := range rg.Columns {
		cc := &rg.Columns[ci]
		if builtinCols[cc.Name] {
			continue
		}
		if err := r.readFieldColumn(cc, events); err != nil {
			return nil, err
		}
	}

	// Read user-defined const columns.
	for _, cc := range rg.ConstColumns {
		if builtinCols[cc.Name] {
			continue
		}
		for _, ev := range events {
			ev.SetField(cc.Name, event.StringValue(cc.Value))
		}
	}

	return events, nil
}

// readRowGroupEventsProjected reads events from a row group with column projection.
// Handles both chunk columns and const columns.
func (r *Reader) readRowGroupEventsProjected(rg *RowGroupMeta, need map[string]bool) ([]*event.Event, error) {
	count := rg.RowCount

	// Always read _time.
	timeChunk := findChunk(rg, "_time")
	if timeChunk == nil {
		return nil, fmt.Errorf("segment: row group missing _time column")
	}
	timestamps, err := r.readInt64sFromChunk(timeChunk)
	if err != nil {
		return nil, fmt.Errorf("segment: read _time: %w", err)
	}

	// Arena allocation.
	arena := make([]event.Event, count)
	events := make([]*event.Event, count)
	for i := 0; i < count; i++ {
		arena[i].Time = time.Unix(0, timestamps[i])
		events[i] = &arena[i]
	}

	// Read requested built-in string columns.
	builtinStrings := []struct {
		name   string
		setter func(e *event.Event, v string)
	}{
		{"_raw", func(e *event.Event, v string) { e.Raw = v }},
		{"_source", func(e *event.Event, v string) { e.Source = v }},
		{"_sourcetype", func(e *event.Event, v string) { e.SourceType = v }},
		{"host", func(e *event.Event, v string) { e.Host = v }},
		{"index", func(e *event.Event, v string) { e.Index = v }},
	}

	for _, bs := range builtinStrings {
		if !need[bs.name] {
			continue
		}
		// Check const first.
		if cc := findConstColumn(rg, bs.name); cc != nil {
			for i := range events {
				bs.setter(events[i], cc.Value)
			}

			continue
		}
		cc := findChunk(rg, bs.name)
		if cc == nil {
			continue
		}
		values, err := r.readStringsFromChunk(cc)
		if err != nil {
			return nil, fmt.Errorf("segment: read %s: %w", bs.name, err)
		}
		for i, v := range values {
			bs.setter(events[i], v)
		}
	}

	// Read requested user-defined columns (chunks).
	builtinCols := map[string]bool{
		"_time": true, "_raw": true, "_source": true,
		"_sourcetype": true, "host": true, "index": true,
	}

	for ci := range rg.Columns {
		cc := &rg.Columns[ci]
		if builtinCols[cc.Name] || !need[cc.Name] {
			continue
		}
		if err := r.readFieldColumn(cc, events); err != nil {
			return nil, err
		}
	}

	// Read requested user-defined const columns.
	for _, cc := range rg.ConstColumns {
		if builtinCols[cc.Name] || !need[cc.Name] {
			continue
		}
		for _, ev := range events {
			ev.SetField(cc.Name, event.StringValue(cc.Value))
		}
	}

	return events, nil
}

// readFieldColumn reads a user-defined field column and populates events.
func (r *Reader) readFieldColumn(cc *ColumnChunkMeta, events []*event.Event) error {
	encType := column.EncodingType(cc.EncodingType)
	switch encType {
	case column.EncodingDict8, column.EncodingDict16, column.EncodingLZ4:
		values, err := r.readStringsFromChunk(cc)
		if err != nil {
			return fmt.Errorf("segment: read field %q: %w", cc.Name, err)
		}
		for i, v := range values {
			if v != "" {
				events[i].SetField(cc.Name, event.StringValue(v))
			}
		}
	case column.EncodingDelta:
		values, err := r.readInt64sFromChunk(cc)
		if err != nil {
			return fmt.Errorf("segment: read field %q: %w", cc.Name, err)
		}
		for i, v := range values {
			events[i].SetField(cc.Name, event.IntValue(v))
		}
	case column.EncodingGorilla:
		values, err := r.readFloat64sFromChunk(cc)
		if err != nil {
			return fmt.Errorf("segment: read field %q: %w", cc.Name, err)
		}
		for i, v := range values {
			events[i].SetField(cc.Name, event.FloatValue(v))
		}
	}

	return nil
}

// ReadRowGroupFiltered reads events from a single row group that match
// the bitmap AND field predicates. This is the per-row-group equivalent
// of ReadEventsByBitmap — extracted for streaming scan where one row group
// is read at a time.
//
// Parameters:
//   - rgIdx: row group index (0-based)
//   - bm: roaring bitmap filter (nil = no bitmap filter, read all rows)
//   - preds: field predicates to evaluate (nil = no field filter)
//   - columns: projected columns (nil = all columns)
//
// When bm is nil and preds is nil, this is equivalent to ReadRowGroup(rgIdx).
// When only bm is set (preds nil), this filters by bitmap only.
// When both are set, bitmap is applied first (cheap), then preds on survivors.
func (r *Reader) ReadRowGroupFiltered(
	rgIdx int,
	bm *roaring.Bitmap,
	preds []Predicate,
	columns []string,
) ([]*event.Event, error) {
	if rgIdx < 0 || rgIdx >= len(r.footer.RowGroups) {
		return nil, fmt.Errorf("segment: row group index %d out of range [0, %d)", rgIdx, len(r.footer.RowGroups))
	}

	rg := &r.footer.RowGroups[rgIdx]

	// No filters at all — read the row group directly.
	if bm == nil && len(preds) == 0 {
		if len(columns) > 0 {
			need := make(map[string]bool, len(columns))
			for _, c := range columns {
				need[c] = true
			}

			return r.readRowGroupEventsProjected(rg, need)
		}

		return r.readRowGroupEvents(rg)
	}

	// Compute the absolute row offset of this row group.
	rowOffset := uint32(0)
	for i := 0; i < rgIdx; i++ {
		rowOffset += uint32(r.footer.RowGroups[i].RowCount)
	}
	rgStart := rowOffset
	rgEnd := rowOffset + uint32(rg.RowCount)

	// Read the row group (with optional column projection).
	need := make(map[string]bool, len(columns))
	for _, c := range columns {
		need[c] = true
	}

	var rgEvents []*event.Event
	var err error
	if len(need) > 0 {
		rgEvents, err = r.readRowGroupEventsProjected(rg, need)
	} else {
		rgEvents, err = r.readRowGroupEvents(rg)
	}
	if err != nil {
		return nil, err
	}

	// Apply bitmap filter: intersect with the row group's range.
	var localBitmap *roaring.Bitmap
	if bm != nil {
		rgRange := roaring.New()
		rgRange.AddRange(uint64(rgStart), uint64(rgEnd))
		localBitmap = roaring.And(bm, rgRange)
		if localBitmap.GetCardinality() == 0 {
			return nil, nil
		}
	}

	// Apply field predicate filter.
	if len(preds) > 0 {
		// Build a bitmap of rows passing all predicates.
		predBitmap := roaring.New()
		if localBitmap != nil {
			// Only evaluate predicates on bitmap-selected rows.
			iter := localBitmap.Iterator()
			for iter.HasNext() {
				pos := iter.Next()
				localIdx := int(pos - rgStart)
				if localIdx >= 0 && localIdx < len(rgEvents) {
					if r.evalPredicatesOnEvent(rgEvents[localIdx], preds) {
						predBitmap.Add(pos)
					}
				}
			}
		} else {
			// No bitmap — evaluate all rows.
			for i, ev := range rgEvents {
				if r.evalPredicatesOnEvent(ev, preds) {
					predBitmap.Add(rgStart + uint32(i))
				}
			}
		}
		localBitmap = predBitmap
		if localBitmap.GetCardinality() == 0 {
			return nil, nil
		}
	}

	// Extract matching events.
	if localBitmap == nil {
		return rgEvents, nil
	}

	result := make([]*event.Event, 0, localBitmap.GetCardinality())
	iter := localBitmap.Iterator()
	for iter.HasNext() {
		pos := iter.Next()
		localIdx := int(pos - rgStart)
		if localIdx >= 0 && localIdx < len(rgEvents) {
			result = append(result, rgEvents[localIdx])
		}
	}

	return result, nil
}

// evalPredicatesOnEvent checks if an event passes all given predicates.
func (r *Reader) evalPredicatesOnEvent(ev *event.Event, preds []Predicate) bool {
	for _, pred := range preds {
		val := ev.GetField(pred.Field)
		if val.IsNull() {
			return false
		}
		switch val.Type() {
		case event.FieldTypeInt:
			predValF, err := strconv.ParseFloat(pred.Value, 64)
			if err != nil {
				return false
			}
			if !evalInt64Predicate(val.AsInt(), pred.Op, int64(predValF)) {
				return false
			}
		case event.FieldTypeFloat:
			predVal, err := strconv.ParseFloat(pred.Value, 64)
			if err != nil {
				return false
			}
			if !evalFloat64Predicate(val.AsFloat(), pred.Op, predVal) {
				return false
			}
		default:
			if !evalStringPredicate(val.String(), pred.Op, pred.Value) {
				return false
			}
		}
	}

	return true
}

// ReadEventsByBitmap reads only the events at positions specified in the bitmap.
// Only row groups containing matching positions are read (positional reads).
func (r *Reader) ReadEventsByBitmap(bm *roaring.Bitmap, columns []string) ([]*event.Event, error) {
	if bm.GetCardinality() == 0 {
		return nil, nil
	}

	need := make(map[string]bool, len(columns))
	for _, c := range columns {
		need[c] = true
	}

	result := make([]*event.Event, 0, bm.GetCardinality())
	rowOffset := uint32(0)

	for rgi := range r.footer.RowGroups {
		rg := &r.footer.RowGroups[rgi]
		rgStart := rowOffset
		rgEnd := rowOffset + uint32(rg.RowCount)

		// Check if any bitmap positions fall in this row group.
		rgRange := roaring.New()
		rgRange.AddRange(uint64(rgStart), uint64(rgEnd))
		rgRange.And(bm)

		if rgRange.GetCardinality() == 0 {
			rowOffset = rgEnd

			continue
		}

		// Read this row group's columns.
		var rgEvents []*event.Event
		var err error
		if len(need) > 0 {
			rgEvents, err = r.readRowGroupEventsProjected(rg, need)
		} else {
			rgEvents, err = r.readRowGroupEvents(rg)
		}
		if err != nil {
			return nil, err
		}

		// Extract only matching rows (converted to local offsets).
		iter := rgRange.Iterator()
		for iter.HasNext() {
			pos := iter.Next()
			localIdx := int(pos - rgStart)
			if localIdx < len(rgEvents) {
				result = append(result, rgEvents[localIdx])
			}
		}

		rowOffset = rgEnd
	}

	return result, nil
}

// Predicate represents a simple filter to push down to segment read level.
type Predicate struct {
	Field string
	Op    string // "=", "!=", "<", "<=", ">", ">="
	Value string
}

// ReadEventsFiltered reads events with predicate pushdown and optional search bitmap.
func (r *Reader) ReadEventsFiltered(preds []Predicate, searchBitmap *roaring.Bitmap, columns []string) ([]*event.Event, error) {
	count := int(r.footer.EventCount)
	if count == 0 {
		return nil, nil
	}

	// Start with search bitmap or full bitmap.
	var matchBitmap *roaring.Bitmap
	if searchBitmap != nil {
		matchBitmap = searchBitmap.Clone()
	} else {
		matchBitmap = roaring.New()
		matchBitmap.AddRange(0, uint64(count))
	}

	// Evaluate each predicate.
	for _, pred := range preds {
		if matchBitmap.GetCardinality() == 0 {
			return nil, nil
		}
		if !r.HasColumn(pred.Field) {
			continue
		}

		predBitmap := roaring.New()

		// Read the full column for predicate evaluation.
		cc := r.findColumnInAllRowGroups(pred.Field)
		if cc == nil {
			// Column exists (HasColumn returned true) but has no chunk — const column.
			// Fall back to ReadStrings which handles const columns correctly.
			values, err := r.ReadStrings(pred.Field)
			if err != nil {
				continue
			}
			for _, pos := range matchBitmap.ToArray() {
				if int(pos) < len(values) && evalStringPredicate(values[pos], pred.Op, pred.Value) {
					predBitmap.Add(pos)
				}
			}
			matchBitmap.And(predBitmap)

			continue
		}

		encType := column.EncodingType(cc.EncodingType)
		switch encType {
		case column.EncodingDict8, column.EncodingDict16:
			// Dict-encoded: try fast DictFilter path for equality.
			if pred.Op == "=" || pred.Op == "==" || pred.Op == "!=" {
				rawData, err := r.readChunk(cc)
				if err == nil {
					df, dfErr := column.NewDictFilterFromEncoded(rawData)
					if dfErr == nil {
						if pred.Op == "!=" {
							predBitmap = df.FilterNotEquality(pred.Value)
						} else {
							predBitmap = df.FilterEquality(pred.Value)
						}
						matchBitmap.And(predBitmap)

						continue
					}
				}
			}
			// Fallback: full decode.
			values, err := r.ReadStrings(pred.Field)
			if err != nil {
				continue
			}
			for _, pos := range matchBitmap.ToArray() {
				if int(pos) < len(values) && evalStringPredicate(values[pos], pred.Op, pred.Value) {
					predBitmap.Add(pos)
				}
			}
		case column.EncodingLZ4:
			values, err := r.ReadStrings(pred.Field)
			if err != nil {
				continue
			}
			for _, pos := range matchBitmap.ToArray() {
				if int(pos) < len(values) && evalStringPredicate(values[pos], pred.Op, pred.Value) {
					predBitmap.Add(pos)
				}
			}
		case column.EncodingDelta:
			values, err := r.ReadInt64s(pred.Field)
			if err != nil {
				continue
			}
			predValF, err := strconv.ParseFloat(pred.Value, 64)
			if err != nil {
				continue
			}
			predValI := int64(predValF)
			for _, pos := range matchBitmap.ToArray() {
				if int(pos) < len(values) && evalInt64Predicate(values[pos], pred.Op, predValI) {
					predBitmap.Add(pos)
				}
			}
		case column.EncodingGorilla:
			values, err := r.ReadFloat64s(pred.Field)
			if err != nil {
				continue
			}
			predVal, err := strconv.ParseFloat(pred.Value, 64)
			if err != nil {
				continue
			}
			for _, pos := range matchBitmap.ToArray() {
				if int(pos) < len(values) && evalFloat64Predicate(values[pos], pred.Op, predVal) {
					predBitmap.Add(pos)
				}
			}
		default:
			continue
		}

		matchBitmap.And(predBitmap)
	}

	if matchBitmap.GetCardinality() == 0 {
		return nil, nil
	}

	return r.ReadEventsByBitmap(matchBitmap, columns)
}

// findColumnInAllRowGroups finds a column chunk from the first row group (for type info).
func (r *Reader) findColumnInAllRowGroups(name string) *ColumnChunkMeta {
	if len(r.footer.RowGroups) == 0 {
		return nil
	}

	return findChunk(&r.footer.RowGroups[0], name)
}

// Per-column bloom filter access.

// loadPerColumnBlooms parses the per-column bloom section for a row group
// and caches the result. Returns the cached map on subsequent calls.
func (r *Reader) loadPerColumnBlooms(rgIdx int) (map[string]*index.BloomFilter, error) {
	if cached, ok := r.perColBlooms[rgIdx]; ok {
		return cached, nil
	}

	if rgIdx < 0 || rgIdx >= len(r.footer.RowGroups) {
		return nil, fmt.Errorf("segment: row group index %d out of range", rgIdx)
	}
	rg := &r.footer.RowGroups[rgIdx]
	if rg.PerColumnBloomLength == 0 {
		r.perColBlooms[rgIdx] = nil

		return nil, nil
	}
	start := rg.PerColumnBloomOffset
	end := start + rg.PerColumnBloomLength
	if start < 0 || end > int64(len(r.data)) {
		return nil, fmt.Errorf("%w: per-column bloom offset out of range for rg %d", ErrCorruptSegment, rgIdx)
	}

	section := r.data[start:end]
	pos := 0

	if pos+2 > len(section) {
		return nil, fmt.Errorf("%w: per-column bloom section truncated", ErrCorruptSegment)
	}
	bloomCount := binary.LittleEndian.Uint16(section[pos : pos+2])
	pos += 2

	blooms := make(map[string]*index.BloomFilter, bloomCount)
	for i := uint16(0); i < bloomCount; i++ {
		// Column name.
		if pos+2 > len(section) {
			return nil, fmt.Errorf("%w: per-column bloom name truncated", ErrCorruptSegment)
		}
		nameLen := binary.LittleEndian.Uint16(section[pos : pos+2])
		pos += 2
		if pos+int(nameLen) > len(section) {
			return nil, fmt.Errorf("%w: per-column bloom name data truncated", ErrCorruptSegment)
		}
		name := string(section[pos : pos+int(nameLen)])
		pos += int(nameLen)

		// Bloom data.
		if pos+4 > len(section) {
			return nil, fmt.Errorf("%w: per-column bloom data len truncated", ErrCorruptSegment)
		}
		dataLen := binary.LittleEndian.Uint32(section[pos : pos+4])
		pos += 4
		if pos+int(dataLen) > len(section) {
			return nil, fmt.Errorf("%w: per-column bloom data truncated", ErrCorruptSegment)
		}
		bf, err := index.DecodeBloomFilter(section[pos : pos+int(dataLen)])
		if err != nil {
			return nil, fmt.Errorf("segment: decode per-column bloom %q rg%d: %w", name, rgIdx, err)
		}
		blooms[name] = bf
		pos += int(dataLen)
	}

	r.perColBlooms[rgIdx] = blooms

	return blooms, nil
}

// CheckColumnBloom checks if a term may exist in a specific column of a specific row group.
// Returns true if the bloom says "maybe" (or if no bloom exists for that column).
func (r *Reader) CheckColumnBloom(rgIdx int, columnName, term string) (bool, error) {
	blooms, err := r.loadPerColumnBlooms(rgIdx)
	if err != nil {
		return true, err // conservative: assume may contain on error
	}
	if blooms == nil {
		return true, nil // no bloom section — conservative
	}
	bf, ok := blooms[columnName]
	if !ok {
		return true, nil // no bloom for this column (e.g., const column) — conservative
	}

	return bf.MayContain(term), nil
}

// CheckColumnBloomAllTerms checks if all terms may exist in a specific column of a row group.
func (r *Reader) CheckColumnBloomAllTerms(rgIdx int, columnName string, terms []string) (bool, error) {
	blooms, err := r.loadPerColumnBlooms(rgIdx)
	if err != nil {
		return true, err
	}
	if blooms == nil {
		return true, nil
	}
	bf, ok := blooms[columnName]
	if !ok {
		return true, nil
	}

	return bf.MayContainAll(terms), nil
}

// BloomFilterForRowGroup returns the _raw bloom filter for a specific row group.
// Backward-compat method — returns the _raw per-column bloom.
func (r *Reader) BloomFilterForRowGroup(rgIndex int) (*index.BloomFilter, error) {
	if rgIndex < 0 || rgIndex >= len(r.footer.RowGroups) {
		return nil, fmt.Errorf("segment: row group index %d out of range [0, %d)", rgIndex, len(r.footer.RowGroups))
	}
	blooms, err := r.loadPerColumnBlooms(rgIndex)
	if err != nil {
		return nil, err
	}
	if blooms == nil {
		return nil, nil
	}

	return blooms["_raw"], nil
}

// CheckBloomForRowGroups returns the indices of row groups whose _raw bloom filter
// may contain the given term. A nil return means all row groups should be checked.
func (r *Reader) CheckBloomForRowGroups(term string) ([]int, error) {
	var matching []int
	for rgi := range r.footer.RowGroups {
		bf, err := r.BloomFilterForRowGroup(rgi)
		if err != nil {
			return nil, err
		}
		if bf == nil || bf.MayContain(term) {
			matching = append(matching, rgi)
		}
	}

	return matching, nil
}

// CheckBloomAllTermsForRowGroups returns the indices of row groups whose _raw bloom
// filter may contain all given terms.
func (r *Reader) CheckBloomAllTermsForRowGroups(terms []string) ([]int, error) {
	if len(terms) == 0 {
		// No terms — all row groups match.
		matching := make([]int, len(r.footer.RowGroups))
		for i := range matching {
			matching[i] = i
		}

		return matching, nil
	}
	var matching []int
	for rgi := range r.footer.RowGroups {
		bf, err := r.BloomFilterForRowGroup(rgi)
		if err != nil {
			return nil, err
		}
		if bf == nil || bf.MayContainAll(terms) {
			matching = append(matching, rgi)
		}
	}

	return matching, nil
}

// BloomFilter returns a bloom filter that represents the union of all row group
// _raw bloom filters (backward compat). For multi-RG segments, returns a bitwise-OR
// union so that MayContain returns true if ANY row group may contain the term.
func (r *Reader) BloomFilter() (*index.BloomFilter, error) {
	if len(r.footer.RowGroups) == 0 {
		return nil, nil
	}
	// For a single row group, just return its bloom directly.
	if len(r.footer.RowGroups) == 1 {
		return r.BloomFilterForRowGroup(0)
	}
	// Multiple row groups: union all _raw bloom filters (bitwise OR).
	// All _raw blooms are uniformly sized (same m/k) by the writer,
	// so Merge succeeds. If sizes mismatch, return nil (conservative).
	var merged *index.BloomFilter
	for rgi := range r.footer.RowGroups {
		bf, err := r.BloomFilterForRowGroup(rgi)
		if err != nil {
			return nil, err
		}
		if bf == nil {
			continue
		}
		if merged == nil {
			merged = bf
		} else {
			if mergeErr := merged.Union(bf); mergeErr != nil {
				return nil, nil //nolint:nilerr // intentional: mismatched bloom sizes
			}
		}
	}

	return merged, nil
}

// Primary index, inverted index, sort key support.

// PrimaryIndex returns the sparse primary index, or nil if no index is present.
func (r *Reader) PrimaryIndex() (*PrimaryIndex, error) {
	if r.footer.PrimaryIndexLength == 0 {
		return nil, nil
	}
	start := r.footer.PrimaryIndexOffset
	end := start + r.footer.PrimaryIndexLength
	if start < 0 || end > int64(len(r.data)) {
		return nil, fmt.Errorf("%w: primary index offset out of range", ErrCorruptSegment)
	}

	return DecodePrimaryIndex(r.data[start:end])
}

// SeekBySortKey performs a binary search on the sparse primary index to find
// the row range that may contain events matching the given sort key values.
// Returns (startRow, endRow) where events in [startRow, endRow) should be scanned.
// If no primary index exists, returns the full range [0, EventCount).
func (r *Reader) SeekBySortKey(values []string) (int, int, error) {
	idx, err := r.PrimaryIndex()
	if err != nil {
		return 0, 0, err
	}
	if idx == nil || len(idx.Entries) == 0 {
		return 0, int(r.footer.EventCount), nil
	}

	totalRows := int(r.footer.EventCount)

	// Binary search: find the first entry whose sort key values are > target.
	// The matching range starts one entry before that.
	lo, hi := 0, len(idx.Entries)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if compareSortKeys(idx.Entries[mid].SortKeyValues, values) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// lo is the index of the first entry strictly greater than values.
	// The range of interest is [entry[lo-1].RowOffset, entry[lo].RowOffset).
	startRow := 0
	if lo > 0 {
		startRow = int(idx.Entries[lo-1].RowOffset)
	}
	endRow := totalRows
	if lo < len(idx.Entries) {
		endRow = int(idx.Entries[lo].RowOffset)
	}

	return startRow, endRow, nil
}

// compareSortKeys lexicographically compares two sort key value slices.
// Returns -1 if a < b, 0 if equal, +1 if a > b.
func compareSortKeys(a, b []string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}

	return 0
}

// InvertedIndex returns the inverted index embedded in the segment.
func (r *Reader) InvertedIndex() (*index.SerializedIndex, error) {
	if r.footer.InvertedLength == 0 {
		return nil, nil
	}
	start := r.footer.InvertedOffset
	end := start + r.footer.InvertedLength
	if start < 0 || end > int64(len(r.data)) {
		return nil, fmt.Errorf("%w: inverted index offset out of range", ErrCorruptSegment)
	}

	return index.DecodeInvertedIndex(r.data[start:end])
}

func evalStringPredicate(val, op, target string) bool {
	switch op {
	case "=", "==":
		return val == target
	case "!=":
		return val != target
	case "<":
		return val < target
	case "<=":
		return val <= target
	case ">":
		return val > target
	case ">=":
		return val >= target
	}

	return false
}

func evalInt64Predicate(val int64, op string, target int64) bool {
	switch op {
	case "=", "==":
		return val == target
	case "!=":
		return val != target
	case "<":
		return val < target
	case "<=":
		return val <= target
	case ">":
		return val > target
	case ">=":
		return val >= target
	}

	return false
}

func evalFloat64Predicate(val float64, op string, target float64) bool {
	switch op {
	case "=", "==":
		return val == target
	case "!=":
		return val != target
	case "<":
		return val < target
	case "<=":
		return val <= target
	case ">":
		return val > target
	case ">=":
		return val >= target
	}

	return false
}

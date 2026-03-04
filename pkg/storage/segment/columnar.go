package segment

import (
	"fmt"
	"strconv"
	"time"

	"github.com/RoaringBitmap/roaring"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/column"
)

// ColumnarResult holds decoded column data in typed arrays. Data stays columnar
// from disk to pipeline. Typed arrays for builtins and timestamps avoid boxing
// overhead on the hot path. Timestamps are []int64 (nanos) rather than
// []time.Time to avoid 24-byte struct allocation per timestamp.
type ColumnarResult struct {
	Timestamps []int64             // nanoseconds since epoch
	Raws       []string            // _raw values (LZ4-decompressed)
	Builtins   map[string][]string // _source, _sourcetype, host, index
	Fields     map[string][]event.Value
	Count      int
}

// builtinColumnSet is a set of all builtin column names for quick lookup.
// Used to distinguish builtin columns from user-defined fields during columnar reads.
var builtinColumnSet = map[string]bool{
	"_time": true, "_raw": true, "_source": true,
	"_sourcetype": true, "host": true, "index": true,
}

// ReadColumnar reads requested columns directly into typed arrays, bypassing
// the per-event *event.Event intermediate representation.
// The columns parameter specifies which columns to decode; nil or empty means all.
// The bitmap parameter, if non-nil, filters rows by global position across row groups.
//
// Primary entry point for the direct columnar read path.
// Data flows: compressed .lsg segments → typed arrays → pipeline.Batch,
// eliminating the row-oriented Event intermediate that dominated query cost.
func (r *Reader) ReadColumnar(columns []string, bitmap *roaring.Bitmap) (*ColumnarResult, error) {
	need := make(map[string]bool, len(columns))
	for _, c := range columns {
		need[c] = true
	}

	result := &ColumnarResult{
		Builtins: make(map[string][]string),
		Fields:   make(map[string][]event.Value),
	}

	rowOffset := uint32(0)

	for rgi := range r.footer.RowGroups {
		rg := &r.footer.RowGroups[rgi]
		rgStart := rowOffset
		rgEnd := rowOffset + uint32(rg.RowCount)

		// Compute local bitmap for this row group from the global bitmap.
		var localBitmap *roaring.Bitmap
		if bitmap != nil {
			rgRange := roaring.New()
			rgRange.AddRange(uint64(rgStart), uint64(rgEnd))
			rgRange.And(bitmap)

			if rgRange.GetCardinality() == 0 {
				rowOffset = rgEnd

				continue
			}

			// Convert global positions to row-group-local offsets.
			localBitmap = roaring.New()
			iter := rgRange.Iterator()
			for iter.HasNext() {
				pos := iter.Next()
				localBitmap.Add(pos - rgStart)
			}
		}

		rgResult, err := r.readRowGroupColumnar(rg, need, localBitmap)
		if err != nil {
			return nil, fmt.Errorf("segment.Reader.ReadColumnar: row group %d: %w", rgi, err)
		}

		result.Timestamps = append(result.Timestamps, rgResult.Timestamps...)
		result.Raws = append(result.Raws, rgResult.Raws...)
		for k, v := range rgResult.Builtins {
			result.Builtins[k] = append(result.Builtins[k], v...)
		}
		for k, v := range rgResult.Fields {
			result.Fields[k] = append(result.Fields[k], v...)
		}
		result.Count += rgResult.Count

		rowOffset = rgEnd
	}

	return result, nil
}

// ReadColumnarWithHints reads columns with row group pruning based on time range
// and bloom filter hints. This is the columnar equivalent of ReadEventsWithHints.
func (r *Reader) ReadColumnarWithHints(hints QueryHints) (*ColumnarResult, error) {
	need := make(map[string]bool, len(hints.Columns))
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

	result := &ColumnarResult{
		Builtins: make(map[string][]string),
		Fields:   make(map[string][]event.Value),
	}

	for rgi := range r.footer.RowGroups {
		rg := &r.footer.RowGroups[rgi]

		// Zone map pruning: check _time range.
		if hints.MinTime != nil || hints.MaxTime != nil {
			if r.canPruneRowGroup(rg, hints.MinTime, hints.MaxTime) {
				continue
			}
		}

		// Bloom filter pruning: skip row groups where search terms are absent.
		if bloomEligible != nil && !bloomEligible[rgi] {
			continue
		}

		rgResult, err := r.readRowGroupColumnar(rg, need, nil)
		if err != nil {
			return nil, fmt.Errorf("segment.Reader.ReadColumnarWithHints: row group %d: %w", rgi, err)
		}

		result.Timestamps = append(result.Timestamps, rgResult.Timestamps...)
		result.Raws = append(result.Raws, rgResult.Raws...)
		for k, v := range rgResult.Builtins {
			result.Builtins[k] = append(result.Builtins[k], v...)
		}
		for k, v := range rgResult.Fields {
			result.Fields[k] = append(result.Fields[k], v...)
		}
		result.Count += rgResult.Count
	}

	return result, nil
}

// ReadColumnarFiltered reads columns with predicate pushdown and optional search bitmap.
func (r *Reader) ReadColumnarFiltered(preds []Predicate, searchBitmap *roaring.Bitmap, columns []string) (*ColumnarResult, error) {
	count := int(r.footer.EventCount)
	if count == 0 {
		return nil, nil
	}

	matchBitmap, err := r.evaluatePredicateBitmap(preds, searchBitmap)
	if err != nil {
		return nil, err
	}
	if matchBitmap != nil && matchBitmap.GetCardinality() == 0 {
		return nil, nil
	}

	return r.ReadColumnar(columns, matchBitmap)
}

// evaluatePredicateBitmap computes a bitmap of matching rows from predicates.
// Shared predicate evaluation logic used by ReadColumnarFiltered.
func (r *Reader) evaluatePredicateBitmap(preds []Predicate, searchBitmap *roaring.Bitmap) (*roaring.Bitmap, error) {
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

	for _, pred := range preds {
		if matchBitmap.GetCardinality() == 0 {
			return matchBitmap, nil
		}
		if !r.HasColumn(pred.Field) {
			continue
		}

		predBitmap := roaring.New()

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

	return matchBitmap, nil
}

// readRowGroupColumnar reads columns from a single row group into a ColumnarResult.
// The need map specifies which columns to read; nil or empty means all columns.
// The localBitmap, when non-nil, filters rows within this row group using local offsets.
func (r *Reader) readRowGroupColumnar(rg *RowGroupMeta, need map[string]bool, localBitmap *roaring.Bitmap) (*ColumnarResult, error) {
	res := &ColumnarResult{
		Builtins: make(map[string][]string),
		Fields:   make(map[string][]event.Value),
	}

	// Decode timestamps (required for time range filtering and _time column).
	timeChunk := findChunk(rg, "_time")
	if timeChunk == nil {
		return nil, fmt.Errorf("segment: row group missing _time column")
	}
	timestamps, err := r.readInt64sFromChunk(timeChunk)
	if err != nil {
		return nil, fmt.Errorf("decode _time: %w", err)
	}

	// Apply bitmap filter if present.
	if localBitmap != nil {
		timestamps = filterInt64sByBitmap(timestamps, localBitmap)
	}
	res.Timestamps = timestamps
	res.Count = len(timestamps)

	// Decode _raw only if requested.
	if len(need) == 0 || need["_raw"] {
		rawChunk := findChunk(rg, "_raw")
		if rawChunk != nil {
			raws, err := r.readStringsFromChunk(rawChunk)
			if err != nil {
				return nil, fmt.Errorf("decode _raw: %w", err)
			}
			if localBitmap != nil {
				raws = filterStringsByBitmap(raws, localBitmap)
			}
			res.Raws = raws
		}
	}

	// Decode requested builtin string columns (_source, _sourcetype, host, index).
	// Const columns are checked first, then fall back to chunk data.
	for _, name := range [...]string{"_source", "_sourcetype", "host", "index"} {
		if len(need) > 0 && !need[name] {
			continue
		}
		// Check const column first.
		if cc := findConstColumn(rg, name); cc != nil {
			vals := make([]string, rg.RowCount)
			for i := range vals {
				vals[i] = cc.Value
			}
			if localBitmap != nil {
				vals = filterStringsByBitmap(vals, localBitmap)
			}
			res.Builtins[name] = vals

			continue
		}
		cc := findChunk(rg, name)
		if cc == nil {
			continue
		}
		vals, err := r.readStringsFromChunk(cc)
		if err != nil {
			return nil, fmt.Errorf("decode builtin %s: %w", name, err)
		}
		if localBitmap != nil {
			vals = filterStringsByBitmap(vals, localBitmap)
		}
		res.Builtins[name] = vals
	}

	// Decode requested user field columns (chunks).
	for ci := range rg.Columns {
		cc := &rg.Columns[ci]
		if builtinColumnSet[cc.Name] {
			continue
		}
		if len(need) > 0 && !need[cc.Name] {
			continue
		}
		vals, err := r.readFieldColumnValues(cc)
		if err != nil {
			return nil, fmt.Errorf("decode field %s: %w", cc.Name, err)
		}
		if localBitmap != nil {
			vals = filterValuesByBitmap(vals, localBitmap)
		}
		res.Fields[cc.Name] = vals
	}

	// Decode requested user field const columns.
	for _, cc := range rg.ConstColumns {
		if builtinColumnSet[cc.Name] {
			continue
		}
		if len(need) > 0 && !need[cc.Name] {
			continue
		}
		vals := make([]event.Value, rg.RowCount)
		for i := range vals {
			vals[i] = event.StringValue(cc.Value)
		}
		if localBitmap != nil {
			vals = filterValuesByBitmap(vals, localBitmap)
		}
		res.Fields[cc.Name] = vals
	}

	return res, nil
}

// readFieldColumnValues reads a user-defined field column into an []event.Value,
// returning typed values instead of setting them on Event structs.
//
// Encoding to Value type mapping:
//   - Dict8/Dict16/LZ4 (string) to StringValue (empty string becomes NullValue, matching readFieldColumn).
//   - Delta (int64) to IntValue.
//   - Gorilla (float64) to FloatValue.
func (r *Reader) readFieldColumnValues(cc *ColumnChunkMeta) ([]event.Value, error) {
	encType := column.EncodingType(cc.EncodingType)
	switch encType {
	case column.EncodingDict8, column.EncodingDict16, column.EncodingLZ4:
		values, err := r.readStringsFromChunk(cc)
		if err != nil {
			return nil, fmt.Errorf("segment: read field %q: %w", cc.Name, err)
		}
		result := make([]event.Value, len(values))
		for i, v := range values {
			if v != "" {
				result[i] = event.StringValue(v)
			}
			// else: result[i] is zero Value (null) — matches readFieldColumn behavior
			// where empty strings are not set on the event.
		}

		return result, nil
	case column.EncodingDelta:
		values, err := r.readInt64sFromChunk(cc)
		if err != nil {
			return nil, fmt.Errorf("segment: read field %q: %w", cc.Name, err)
		}
		result := make([]event.Value, len(values))
		for i, v := range values {
			result[i] = event.IntValue(v)
		}

		return result, nil
	case column.EncodingGorilla:
		values, err := r.readFloat64sFromChunk(cc)
		if err != nil {
			return nil, fmt.Errorf("segment: read field %q: %w", cc.Name, err)
		}
		result := make([]event.Value, len(values))
		for i, v := range values {
			result[i] = event.FloatValue(v)
		}

		return result, nil
	default:
		return nil, fmt.Errorf("segment: field %q unsupported encoding %d", cc.Name, cc.EncodingType)
	}
}

// FilterByTimeRange removes rows from the ColumnarResult whose timestamps
// fall outside the given [earliest, latest] range. Zero time means no bound.
func (cr *ColumnarResult) FilterByTimeRange(earliest, latest time.Time) {
	if (earliest.IsZero() && latest.IsZero()) || cr.Count == 0 {
		return
	}

	earliestNano := earliest.UnixNano()
	latestNano := latest.UnixNano()

	// Build list of row indices to keep.
	keep := make([]int, 0, cr.Count)
	for i, ts := range cr.Timestamps {
		if !earliest.IsZero() && ts < earliestNano {
			continue
		}
		if !latest.IsZero() && ts > latestNano {
			continue
		}
		keep = append(keep, i)
	}

	if len(keep) == cr.Count {
		return // No filtering needed — all rows pass.
	}

	// Apply filter using indices.
	cr.Timestamps = gatherInt64s(cr.Timestamps, keep)
	if cr.Raws != nil {
		cr.Raws = gatherStrings(cr.Raws, keep)
	}
	for k, v := range cr.Builtins {
		cr.Builtins[k] = gatherStrings(v, keep)
	}
	for k, v := range cr.Fields {
		cr.Fields[k] = gatherValues(v, keep)
	}
	cr.Count = len(keep)
}

// filterInt64sByBitmap retains only elements at positions present in the bitmap.
func filterInt64sByBitmap(data []int64, bitmap *roaring.Bitmap) []int64 {
	ids := bitmap.ToArray()
	result := make([]int64, 0, len(ids))
	for _, id := range ids {
		if int(id) < len(data) {
			result = append(result, data[id])
		}
	}

	return result
}

// filterStringsByBitmap retains only elements at positions present in the bitmap.
func filterStringsByBitmap(data []string, bitmap *roaring.Bitmap) []string {
	ids := bitmap.ToArray()
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		if int(id) < len(data) {
			result = append(result, data[id])
		}
	}

	return result
}

// filterValuesByBitmap retains only elements at positions present in the bitmap.
func filterValuesByBitmap(data []event.Value, bitmap *roaring.Bitmap) []event.Value {
	ids := bitmap.ToArray()
	result := make([]event.Value, 0, len(ids))
	for _, id := range ids {
		if int(id) < len(data) {
			result = append(result, data[id])
		}
	}

	return result
}

// gatherInt64s collects elements at the given indices into a new slice.
func gatherInt64s(data []int64, indices []int) []int64 {
	result := make([]int64, len(indices))
	for i, idx := range indices {
		result[i] = data[idx]
	}

	return result
}

// gatherStrings collects elements at the given indices into a new slice.
func gatherStrings(data []string, indices []int) []string {
	result := make([]string, len(indices))
	for i, idx := range indices {
		result[i] = data[idx]
	}

	return result
}

// gatherValues collects elements at the given indices into a new slice.
func gatherValues(data []event.Value, indices []int) []event.Value {
	result := make([]event.Value, len(indices))
	for i, idx := range indices {
		result[i] = data[idx]
	}

	return result
}

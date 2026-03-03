package server

import (
	"fmt"

	"github.com/OrlovEvgeny/Lynxdb/pkg/cache"
	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// pipelineRowsToResultRows converts pipeline engine output to spl2.ResultRow.
// Only runs on the final result set (post-aggregation, typically small).
func pipelineRowsToResultRows(rows []map[string]event.Value) []spl2.ResultRow {
	result := make([]spl2.ResultRow, len(rows))
	for i, row := range rows {
		fields := make(map[string]interface{}, len(row))
		for k, v := range row {
			fields[k] = v.Interface()
		}
		result[i] = spl2.ResultRow{Fields: fields}
	}

	return result
}

// resultRowsToCachedResult converts executor result rows to a CachedResult.
func resultRowsToCachedResult(rows []spl2.ResultRow) *cache.CachedResult {
	if len(rows) == 0 {
		return &cache.CachedResult{}
	}
	// Collect all column names.
	colSet := make(map[string]struct{})
	for _, row := range rows {
		for k := range row.Fields {
			colSet[k] = struct{}{}
		}
	}
	cols := make(map[string][]cache.CachedValue, len(colSet))
	for col := range colSet {
		vals := make([]cache.CachedValue, len(rows))
		for i, row := range rows {
			vals[i] = interfaceToCachedValue(row.Fields[col])
		}
		cols[col] = vals
	}

	return &cache.CachedResult{
		Batches: []cache.CachedBatch{{Columns: cols, Len: len(rows)}},
	}
}

// cachedResultToResultRows converts a CachedResult back to executor result rows.
func cachedResultToResultRows(cr *cache.CachedResult) []spl2.ResultRow {
	if cr == nil || len(cr.Batches) == 0 {
		return nil
	}
	var rows []spl2.ResultRow
	for _, batch := range cr.Batches {
		for i := 0; i < batch.Len; i++ {
			fields := make(map[string]interface{}, len(batch.Columns))
			for col, vals := range batch.Columns {
				if i < len(vals) {
					fields[col] = cachedValueToInterface(vals[i])
				}
			}
			rows = append(rows, spl2.ResultRow{Fields: fields})
		}
	}

	return rows
}

// interfaceToCachedValue converts an interface{} to a CachedValue.
func interfaceToCachedValue(v interface{}) cache.CachedValue {
	switch val := v.(type) {
	case string:
		return cache.CachedValue{Type: 1, Str: val}
	case int64:
		return cache.CachedValue{Type: 2, Num: val}
	case float64:
		return cache.CachedValue{Type: 3, Flt: val}
	case bool:
		cv := cache.CachedValue{Type: 4}
		if val {
			cv.Num = 1
		}

		return cv
	case int:
		return cache.CachedValue{Type: 2, Num: int64(val)}
	default:
		if v == nil {
			return cache.CachedValue{}
		}

		return cache.CachedValue{Type: 1, Str: fmt.Sprint(v)}
	}
}

// cachedValueToInterface converts a CachedValue back to an interface{}.
func cachedValueToInterface(cv cache.CachedValue) interface{} {
	switch cv.Type {
	case 1:
		return cv.Str
	case 2:
		return cv.Num
	case 3:
		return cv.Flt
	case 4:
		return cv.Num != 0
	default:
		return nil
	}
}

package pipeline

import (
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

// BatchFromColumnar converts a ColumnarResult into a pipeline Batch.
// Only materialization point — data stays columnar from segment decode
// through to pipeline execution. No per-event map allocation, no
// field-discovery pass, no row-oriented intermediate.
//
// Aliases: _source is aliased as "source", _sourcetype as "sourcetype"
// to match BatchFromEvents behavior (SPL2 queries use the short names).
func BatchFromColumnar(cr *segment.ColumnarResult) *Batch {
	if cr == nil || cr.Count == 0 {
		return &Batch{Columns: make(map[string][]event.Value), Len: 0}
	}

	// Estimate column count: _time + _raw + builtins (with aliases) + fields.
	colCount := 2 + len(cr.Builtins)*2 + len(cr.Fields)
	batch := &Batch{
		Len:     cr.Count,
		Columns: make(map[string][]event.Value, colCount),
	}

	// Timestamps: convert []int64 -> []event.Value.
	times := make([]event.Value, cr.Count)
	for i, ts := range cr.Timestamps {
		times[i] = event.TimestampValue(time.Unix(0, ts))
	}
	batch.Columns["_time"] = times

	// _raw.
	if cr.Raws != nil {
		raws := make([]event.Value, cr.Count)
		for i, raw := range cr.Raws {
			raws[i] = event.StringValue(raw)
		}
		batch.Columns["_raw"] = raws
	}

	// Builtins: _source, _sourcetype, host, index.
	for name, vals := range cr.Builtins {
		col := make([]event.Value, cr.Count)
		for i, v := range vals {
			col[i] = event.StringValue(v)
		}
		batch.Columns[name] = col

		// Aliases: SPL2 queries use "source" and "sourcetype" without underscore.
		switch name {
		case "_source":
			batch.Columns["source"] = col
		case "_sourcetype":
			batch.Columns["sourcetype"] = col
		}
	}

	// User fields (already []event.Value — zero-copy assignment).
	for name, vals := range cr.Fields {
		batch.Columns[name] = vals
	}

	return batch
}

// SplitColumnarBatches splits a ColumnarResult into pipeline-sized Batch slices.
// Each batch contains at most batchSize rows. Sub-slicing is zero-copy for the
// underlying arrays — only slice headers are created, not data copies.
func SplitColumnarBatches(cr *segment.ColumnarResult, batchSize int) []*Batch {
	if cr == nil || cr.Count == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	numBatches := (cr.Count + batchSize - 1) / batchSize
	batches := make([]*Batch, 0, numBatches)

	for start := 0; start < cr.Count; start += batchSize {
		end := start + batchSize
		if end > cr.Count {
			end = cr.Count
		}

		// Create a sub-slice of the ColumnarResult (zero-copy: shares backing arrays).
		slice := &segment.ColumnarResult{
			Timestamps: cr.Timestamps[start:end],
			Count:      end - start,
			Builtins:   make(map[string][]string, len(cr.Builtins)),
			Fields:     make(map[string][]event.Value, len(cr.Fields)),
		}
		if cr.Raws != nil {
			slice.Raws = cr.Raws[start:end]
		}
		for k, v := range cr.Builtins {
			slice.Builtins[k] = v[start:end]
		}
		for k, v := range cr.Fields {
			slice.Fields[k] = v[start:end]
		}

		batches = append(batches, BatchFromColumnar(slice))
	}

	return batches
}

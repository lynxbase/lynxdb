package pipeline

import (
	"context"
	"fmt"

	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
)

// ColumnarScanIterator serves pre-built Batch objects from the direct columnar
// read path. This replaces ScanIterator + BatchFromEvents for segment-sourced
// queries, eliminating the row-oriented Event intermediate representation.
//
// Data arrives already in columnar Batch format via segment.ReadColumnar ->
// SplitColumnarBatches. This iterator simply yields the pre-built batches
// in sequence.
//
// NOT thread-safe. Designed for single-goroutine Volcano pipeline execution.
type ColumnarScanIterator struct {
	batches           []*Batch
	idx               int
	acct              stats.MemoryAccount
	lastBatchEstimate int64 // tracks previous batch size for Shrink
}

// NewColumnarScanIterator creates a scan iterator over pre-built columnar batches.
func NewColumnarScanIterator(batches []*Batch) *ColumnarScanIterator {
	return &ColumnarScanIterator{batches: batches, acct: stats.NopAccount()}
}

// NewColumnarScanIteratorWithBudget creates a columnar scan iterator with memory
// budget tracking. When the budget is genuinely exceeded (real pressure from
// downstream operators), the scan returns an explicit error.
func NewColumnarScanIteratorWithBudget(batches []*Batch, acct stats.MemoryAccount) *ColumnarScanIterator {
	return &ColumnarScanIterator{batches: batches, acct: stats.EnsureAccount(acct)}
}

// Init prepares the iterator. No-op for ColumnarScanIterator.
func (c *ColumnarScanIterator) Init(ctx context.Context) error { return nil }

// Next returns the next pre-built batch, or (nil, nil) at EOF.
func (c *ColumnarScanIterator) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.idx >= len(c.batches) {
		return nil, nil
	}

	// Shrink previous batch — it has been consumed by downstream operator.
	if c.acct != nil && c.lastBatchEstimate > 0 {
		c.acct.Shrink(c.lastBatchEstimate)
		c.lastBatchEstimate = 0
	}

	b := c.batches[c.idx]

	// Budget tracking with hard error on genuine budget pressure.
	if c.acct != nil {
		estimate := estimateBatchMemory(b)
		if err := c.acct.Grow(estimate); err != nil {
			return nil, fmt.Errorf("query memory limit exceeded at batch %d: %w", c.idx, err)
		}
		c.lastBatchEstimate = estimate
	}

	c.idx++

	return b, nil
}

// Close releases resources held by this iterator.
func (c *ColumnarScanIterator) Close() error {
	if c.acct != nil && c.lastBatchEstimate > 0 {
		c.acct.Shrink(c.lastBatchEstimate)
		c.lastBatchEstimate = 0
	}
	c.acct.Close()
	c.batches = nil

	return nil
}

// Schema returns nil — schema is inferred from batch columns.
func (c *ColumnarScanIterator) Schema() []FieldInfo { return nil }

// estimateBatchMemory estimates the heap size of a Batch for budget tracking.
// Approximation: map overhead + per-column slice headers + per-value size.
// Value structs are ~32 bytes each (type tag + string header or numeric payload).
func estimateBatchMemory(b *Batch) int64 {
	if b == nil || b.Len == 0 {
		return 0
	}

	// Map header (~8 bytes) + per-column: bucket pointer (8) + slice header (24).
	const perColumnOverhead int64 = 32
	const perValueBytes int64 = 32

	total := int64(8) + int64(len(b.Columns))*perColumnOverhead
	for _, col := range b.Columns {
		total += int64(len(col)) * perValueBytes
	}

	return total
}

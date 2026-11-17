package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// ReverseIterator materializes input rows and emits them in reverse order.
//
// With a SpillManager configured, budget pressure flushes the in-memory
// buffer to a columnar spill chunk. Emission walks chunks newest-to-oldest,
// loading and reversing one chunk at a time, so peak memory stays bounded by
// the budget rather than the input size.
type ReverseIterator struct {
	child     Iterator
	rows      []map[string]event.Value
	rowsBytes int64
	emitted   bool
	offset    int
	batchSize int
	acct      memgov.MemoryAccount

	// Spill state.
	spillMgr        *SpillManager
	spillWriter     *ColumnarSpillWriter
	chunks          []string // spill chunk paths in accumulation order
	chunkIdx        int      // next chunk to emit (walks chunks backwards)
	spilledRows     int64
	spillBytesTotal int64
}

func NewReverseIteratorWithBudget(child Iterator, batchSize int, acct memgov.MemoryAccount) *ReverseIterator {
	return NewReverseIteratorWithSpill(child, batchSize, acct, nil)
}

// NewReverseIteratorWithSpill creates a reverse operator with budget tracking
// and optional chunked spill support.
func NewReverseIteratorWithSpill(child Iterator, batchSize int, acct memgov.MemoryAccount, mgr *SpillManager) *ReverseIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	return &ReverseIterator{
		child:     child,
		batchSize: batchSize,
		acct:      memgov.EnsureAccount(acct),
		spillMgr:  mgr,
	}
}

func (r *ReverseIterator) Init(ctx context.Context) error { return r.child.Init(ctx) }

func (r *ReverseIterator) Next(ctx context.Context) (*Batch, error) {
	if !r.emitted {
		if err := r.materialize(ctx); err != nil {
			return nil, err
		}
		r.emitted = true
	}

	for r.offset >= len(r.rows) {
		if r.chunkIdx < 0 {
			return nil, nil
		}
		if err := r.loadNextChunk(); err != nil {
			return nil, err
		}
	}

	end := r.offset + r.batchSize
	if end > len(r.rows) {
		end = len(r.rows)
	}
	batch := BatchFromRows(r.rows[r.offset:end])
	r.offset = end

	return batch, nil
}

func (r *ReverseIterator) materialize(ctx context.Context) error {
	for {
		batch, err := r.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			rowBytes := EstimateRowBytes(row)
			if growErr := r.acct.Grow(rowBytes); growErr != nil {
				if r.spillMgr == nil || len(r.rows) == 0 {
					return fmt.Errorf("reverse: memory budget exceeded: %w", growErr)
				}
				if spillErr := r.spillBufferedRows(); spillErr != nil {
					return spillErr
				}
				// Retry after freeing the buffer.
				if growErr := r.acct.Grow(rowBytes); growErr != nil {
					return fmt.Errorf("reverse: memory budget exceeded: %w", growErr)
				}
			}
			r.rows = append(r.rows, row)
			r.rowsBytes += rowBytes
		}
	}

	// Tail buffer (newest rows) emits first: reverse it in place.
	reverseRows(r.rows)
	// Chunks are emitted newest-to-oldest after the tail buffer.
	r.chunkIdx = len(r.chunks) - 1

	return nil
}

// spillBufferedRows flushes the in-memory buffer to a new spill chunk.
func (r *ReverseIterator) spillBufferedRows() error {
	sw, err := NewColumnarSpillWriter(r.spillMgr, "reverse")
	if err != nil {
		return fmt.Errorf("reverse: create spill file: %w", err)
	}
	for _, row := range r.rows {
		if err := sw.WriteRow(row); err != nil {
			sw.Close()

			return fmt.Errorf("reverse: write spill: %w", err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		return fmt.Errorf("reverse: close spill: %w", err)
	}
	r.chunks = append(r.chunks, sw.Path())
	r.spilledRows += int64(len(r.rows))
	r.spillBytesTotal += sw.BytesWritten()

	r.acct.Shrink(r.rowsBytes)
	r.rows = nil
	r.rowsBytes = 0
	if sn, ok := r.acct.(SpillNotifier); ok {
		sn.NotifySpilled()
	}

	return nil
}

// loadNextChunk replaces the emission buffer with the next chunk (walking
// backwards), reversed. Each chunk fit within budget when it was written, so
// loading one at a time keeps peak memory bounded.
func (r *ReverseIterator) loadNextChunk() error {
	r.acct.Shrink(r.rowsBytes)
	r.rows = nil
	r.rowsBytes = 0
	r.offset = 0

	path := r.chunks[r.chunkIdx]
	r.chunkIdx--

	reader, err := NewColumnarSpillReader(path)
	if err != nil {
		return fmt.Errorf("reverse: open spill chunk: %w", err)
	}
	defer reader.Close()

	for {
		row, err := reader.ReadRow()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reverse: read spill chunk: %w", err)
		}
		rowBytes := EstimateRowBytes(row)
		if growErr := r.acct.Grow(rowBytes); growErr != nil {
			return fmt.Errorf("reverse: memory budget exceeded reloading chunk: %w", growErr)
		}
		r.rows = append(r.rows, row)
		r.rowsBytes += rowBytes
	}
	reverseRows(r.rows)
	r.spillMgr.Release(path)

	return nil
}

func reverseRows(rows []map[string]event.Value) {
	for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
		rows[i], rows[j] = rows[j], rows[i]
	}
}

func (r *ReverseIterator) Close() error {
	// Release any chunks not yet consumed.
	for i := r.chunkIdx; i >= 0 && i < len(r.chunks); i-- {
		r.spillMgr.Release(r.chunks[i])
	}
	r.acct.Close()

	return r.child.Close()
}

func (r *ReverseIterator) Schema() []FieldInfo { return r.child.Schema() }

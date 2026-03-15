package pipeline

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/buffer"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// BufferedSortIterator is the buffer-pool-aware sort operator. Accumulation
// uses the Go heap (for sort.SliceStable), but sorted runs are serialized
// into pool pages. Pages are unpinned after writing, making them eviction
// candidates. When evicted, page data is saved to disk via write-back.
// During merge, pages are read from pool (if cached) or disk (if evicted).
//
// Activation: only when qc.bufferPool != nil && qc.spillMgr != nil.
// Otherwise SortIteratorWithSpill is used (threshold-based spill, unchanged).
type BufferedSortIterator struct {
	child       Iterator
	fields      []SortField
	rows        []map[string]event.Value // Go heap accumulation buffer
	sorted      bool
	offset      int // for single-run in-memory fast path
	batchSize   int
	maxSortRows int
	acct        stats.MemoryAccount // tracks heap memory only

	// Buffer pool integration.
	pool       *buffer.Pool
	queryID    string
	runCounter int // monotonic counter for unique ownerTag per run

	// Buffered runs (sorted data in pool pages).
	runs     []*bufferedRun
	spillCtx *spillFileContext // shared spill file for evicted pages

	// Disk-only fallback runs (when pool can't allocate pages).
	diskRuns []string // ColumnarSpillWriter file paths
	spillMgr *SpillManager

	// Output.
	merger      SpillMergerI
	spilledRows int64

	// Columnar fast path state (used when no spill occurs).
	mergedBatch   *Batch // accumulated columnar data from child batches
	sortedIndices []int  // permutation index after sort
	useColumnar   bool   // true when columnar path is active
}

// dataPageRef tracks a pool page holding a serialized columnar batch.
// Write-back sets evicted/diskPath/diskOffset during eviction.
type dataPageRef struct {
	pageID   buffer.PageID
	dataLen  int // valid bytes in page (< pageSize)
	rowCount int

	// Written by write-back callback (under pool.mu), read by merge reader
	// after PinPageIfOwned fails. No additional locking needed because
	// the write happens-before the read (pool.mu serializes eviction and
	// PinPageIfOwned).
	evicted    bool
	diskPath   string
	diskOffset int64
}

// bufferedRun is a sorted run stored as a sequence of pool pages.
type bufferedRun struct {
	pages     []*dataPageRef
	totalRows int
	ownerTag  string // "bsort-<queryID>-<runIdx>" for PinPageIfOwned
}

// sortPageWriteBack implements buffer.PageWriteBack. Stored in
// page.OwnerData. Saves page data to the shared spill file on eviction.
type sortPageWriteBack struct {
	ref      *dataPageRef
	spillCtx *spillFileContext
}

// WriteBackPage persists page data to the shared spill file on eviction.
// Called under pool.mu, so writes are serialized.
func (wb *sortPageWriteBack) WriteBackPage(p *buffer.Page) error {
	data := p.DataSlice()[:wb.ref.dataLen]
	offset, err := wb.spillCtx.writeData(data)
	if err != nil {
		return fmt.Errorf("sort.writeBack: %w", err)
	}
	wb.ref.evicted = true
	wb.ref.diskPath = wb.spillCtx.path
	wb.ref.diskOffset = offset

	return nil
}

// spillFileContext manages a shared append-only spill file for evicted
// pages. Created lazily on first eviction. Thread-safe (writes serialized
// by pool.mu during eviction).
type spillFileContext struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	offset   int64
	spillMgr *SpillManager
	created  bool
}

// ensureFile lazily creates the spill file via SpillManager.
func (sc *spillFileContext) ensureFile() error {
	if sc.created {
		return nil
	}
	f, err := sc.spillMgr.NewSpillFile("sort-buf")
	if err != nil {
		return fmt.Errorf("spillFileContext.ensureFile: %w", err)
	}
	sc.file = f
	sc.path = f.Name()
	sc.created = true

	return nil
}

// writeData appends data to the spill file and returns the write offset.
func (sc *spillFileContext) writeData(data []byte) (int64, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if err := sc.ensureFile(); err != nil {
		return 0, err
	}

	writeOffset := sc.offset
	n, err := sc.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("spillFileContext.writeData: %w", err)
	}
	sc.offset += int64(n)
	sc.spillMgr.TrackBytes(int64(n))

	return writeOffset, nil
}

// readData reads length bytes from the spill file at the given offset.
func (sc *spillFileContext) readData(offset int64, length int) ([]byte, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.file == nil {
		return nil, fmt.Errorf("spillFileContext.readData: file not created")
	}

	buf := make([]byte, length)
	n, err := sc.file.ReadAt(buf, offset)
	if err != nil && n != length {
		return nil, fmt.Errorf("spillFileContext.readData: %w", err)
	}

	return buf, nil
}

// close closes the file handle. SpillManager handles deletion.
func (sc *spillFileContext) close() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.file != nil {
		sc.file.Close()
		sc.file = nil
	}
}

// release closes and releases the spill file via SpillManager.
func (sc *spillFileContext) release() {
	sc.close()
	if sc.created && sc.spillMgr != nil {
		sc.spillMgr.Release(sc.path)
	}
}

// NewBufferedSortIterator creates a buffer-pool-aware sort operator.
// The pool and spillMgr must be non-nil; callers should fall back to
// NewSortIteratorWithSpill if either is nil.
func NewBufferedSortIterator(
	child Iterator,
	fields []SortField,
	batchSize int,
	acct stats.MemoryAccount,
	pool *buffer.Pool,
	queryID string,
	spillMgr *SpillManager,
) *BufferedSortIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &BufferedSortIterator{
		child:       child,
		fields:      fields,
		batchSize:   batchSize,
		maxSortRows: DefaultMaxSortRows,
		acct:        stats.EnsureAccount(acct),
		pool:        pool,
		queryID:     queryID,
		spillMgr:    spillMgr,
		spillCtx:    &spillFileContext{spillMgr: spillMgr},
	}
}

func (bs *BufferedSortIterator) Init(ctx context.Context) error {
	return bs.child.Init(ctx)
}

func (bs *BufferedSortIterator) Next(ctx context.Context) (*Batch, error) {
	if !bs.sorted {
		if err := bs.materialize(ctx); err != nil {
			return nil, err
		}
	}

	// External merge path: read from the k-way merger.
	if bs.merger != nil {
		return bs.merger.NextBatch(bs.batchSize)
	}

	// Columnar fast path: emit slices of the sorted permutation.
	if bs.useColumnar {
		if bs.offset >= len(bs.sortedIndices) {
			return nil, nil
		}
		end := bs.offset + bs.batchSize
		if end > len(bs.sortedIndices) {
			end = len(bs.sortedIndices)
		}
		batch := bs.mergedBatch.PermuteSlice(bs.sortedIndices[bs.offset:end])
		bs.offset = end

		return batch, nil
	}

	// Row-based in-memory path (fallback after columnar→row degradation).
	if bs.offset >= len(bs.rows) {
		return nil, nil
	}
	end := bs.offset + bs.batchSize
	if end > len(bs.rows) {
		end = len(bs.rows)
	}
	batch := BatchFromRows(bs.rows[bs.offset:end])
	bs.offset = end

	return batch, nil
}

func (bs *BufferedSortIterator) Close() error {
	// Transition to complete phase — memory can be reclaimed by coordinator.
	if pn, ok := bs.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseComplete)
	}

	// Close the merger first (closes all run readers).
	if bs.merger != nil {
		bs.merger.Close()
		bs.merger = nil
	}

	// Release all pool pages from buffered runs.
	bs.releaseAllPages()

	// Release disk-only fallback runs.
	for _, path := range bs.diskRuns {
		bs.spillMgr.Release(path)
	}
	bs.diskRuns = nil

	// Release the shared spill file.
	bs.spillCtx.release()

	bs.acct.Close()

	return bs.child.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (bs *BufferedSortIterator) MemoryUsed() int64 {
	return bs.acct.Used()
}

func (bs *BufferedSortIterator) Schema() []FieldInfo { return bs.child.Schema() }

// ResourceStats implements ResourceReporter for per-operator spill metrics.
func (bs *BufferedSortIterator) ResourceStats() OperatorResourceStats {
	return OperatorResourceStats{
		PeakBytes:   bs.acct.MaxUsed(),
		SpilledRows: bs.spilledRows,
	}
}

// materialize reads all child batches, sorts, and prepares for output.
func (bs *BufferedSortIterator) materialize(ctx context.Context) error {
	// Transition to building phase — accumulating rows for sort.
	if pn, ok := bs.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseBuilding)
	}

	// Start in columnar mode. Accumulate child batches directly into a
	// merged columnar buffer. If a spill is triggered, degrade to row path.
	columnarMode := true
	bs.mergedBatch = NewBatch(0)
	var columnarRows int

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := bs.child.Next(ctx)
		if err != nil {
			// Bug fix: when child fails because the shared budget is exhausted,
			// sort may hold a large spillable buffer. Spill that buffer to free
			// shared budget capacity, then retry.
			if stats.IsMemoryExhausted(err) {
				if columnarMode && columnarRows > 0 {
					bs.degradeToRowPath()
					columnarMode = false
				}
				if len(bs.rows) > 0 {
					if spillErr := bs.spillCurrentBuffer(); spillErr != nil {
						return fmt.Errorf("buffered_sort.materialize: spill on child budget pressure: %w", spillErr)
					}
				}
				batch, err = bs.child.Next(ctx)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		if batch == nil {
			break
		}

		if columnarMode {
			batchBytes := estimateColumnarBatchBytes(batch)
			growErr := bs.acct.Grow(batchBytes)

			if growErr != nil {
				// Budget exceeded — degrade to row path and try spilling.
				bs.degradeToRowPath()
				columnarMode = false
				// Fall through to row-based accumulation for this batch.
			} else {
				// Check row count safety valve.
				if columnarRows+batch.Len > bs.maxSortRows {
					// Degrade to row path and spill.
					bs.degradeToRowPath()
					columnarMode = false
					// Fall through to row-based accumulation for this batch.
				} else {
					bs.mergedBatch.AppendBatch(batch)
					columnarRows += batch.Len
					continue
				}
			}
		}

		// Row-based accumulation path (after degradation or for spill support).
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			rowBytes := estimateRowMapBytes(row)

			if err := bs.growOrSpill(rowBytes); err != nil {
				return err
			}

			// Row count safety valve.
			if len(bs.rows)+1 > bs.maxSortRows {
				if len(bs.rows) > 0 {
					if err := bs.spillCurrentBuffer(); err != nil {
						return fmt.Errorf("buffered_sort.materialize: row limit spill: %w", err)
					}
				}
			}

			bs.rows = append(bs.rows, row)
		}
	}

	// All input consumed. Choose sort strategy.
	hasSpilledRuns := len(bs.runs) > 0 || len(bs.diskRuns) > 0

	if columnarMode && columnarRows > 0 && !hasSpilledRuns {
		// Columnar fast path: sort via permutation index.
		if err := bs.sortColumnar(ctx); err != nil {
			return err
		}
		if pn, ok := bs.acct.(PhaseNotifier); ok {
			pn.SetPhase(PhaseProbing)
		}
		bs.sorted = true

		return nil
	}

	// Clean up unused columnar state.
	bs.mergedBatch = nil

	if !hasSpilledRuns {
		// Row-based in-memory fast path: sort in-place, no spill occurred.
		if err := bs.sortInPlaceCtx(ctx); err != nil {
			return err
		}
		if pn, ok := bs.acct.(PhaseNotifier); ok {
			pn.SetPhase(PhaseProbing)
		}
		bs.sorted = true

		return nil
	}

	// External merge path: spill any remaining in-memory rows as the final run.
	if len(bs.rows) > 0 {
		if err := bs.spillCurrentBuffer(); err != nil {
			return fmt.Errorf("buffered_sort.materialize: final spill failed: %w", err)
		}
	}

	// Create the k-way merger over all runs.
	merger, err := bs.buildMerger()
	if err != nil {
		return fmt.Errorf("buffered_sort.materialize: create merger: %w", err)
	}
	bs.merger = merger
	// Transition to probing phase — producing merged output.
	if pn, ok := bs.acct.(PhaseNotifier); ok {
		pn.SetPhase(PhaseProbing)
	}
	bs.sorted = true

	return nil
}

// degradeToRowPath converts accumulated columnar data to row maps and
// resets the columnar state. Called when a spill is needed.
func (bs *BufferedSortIterator) degradeToRowPath() {
	if bs.mergedBatch == nil || bs.mergedBatch.Len == 0 {
		bs.mergedBatch = nil
		return
	}
	bs.rows = make([]map[string]event.Value, 0, bs.mergedBatch.Len)
	for i := 0; i < bs.mergedBatch.Len; i++ {
		bs.rows = append(bs.rows, bs.mergedBatch.Row(i))
	}
	bs.mergedBatch = nil
}

// sortColumnar sorts the accumulated columnar data using a permutation index.
func (bs *BufferedSortIterator) sortColumnar(ctx context.Context) error {
	n := bs.mergedBatch.Len
	bs.sortedIndices = make([]int, n)
	for i := range bs.sortedIndices {
		bs.sortedIndices[i] = i
	}

	var canceled bool
	var comparisons int64
	sort.SliceStable(bs.sortedIndices, func(i, j int) bool {
		if canceled {
			return false
		}
		comparisons++
		if comparisons&0x3FF == 0 {
			select {
			case <-ctx.Done():
				canceled = true

				return false
			default:
			}
		}
		for _, sf := range bs.fields {
			a := bs.mergedBatch.Value(sf.Name, bs.sortedIndices[i])
			b := bs.mergedBatch.Value(sf.Name, bs.sortedIndices[j])
			cmp := vm.CompareValues(a, b)
			if cmp == 0 {
				continue
			}
			if sf.Desc {
				return cmp > 0
			}

			return cmp < 0
		}

		return false
	})
	if canceled {
		return ctx.Err()
	}

	bs.useColumnar = true
	bs.sorted = true

	return nil
}

// spillCurrentBuffer sorts the current in-memory rows and stores them as a
// buffered run in pool pages. Falls back to disk if the pool can't allocate.
func (bs *BufferedSortIterator) spillCurrentBuffer() error {
	bs.spilledRows += int64(len(bs.rows))

	// Sort in-place using the same comparator as the final sort.
	bs.sortInPlace()

	// Try buffer-pool path first.
	if err := bs.createBufferedRun(); err != nil {
		// Pool exhaustion: fall back to disk.
		if err := bs.createDiskRun(); err != nil {
			return err
		}
	}

	// Release all tracked memory and reset the buffer.
	bs.acct.Shrink(bs.acct.Used())
	bs.rows = bs.rows[:0]

	// Notify coordinator that this operator has spilled, allowing rebalancing.
	if sn, ok := bs.acct.(SpillNotifier); ok {
		sn.NotifySpilled()
	}

	return nil
}

// growOrSpill attempts to reserve rowBytes in the memory budget. If the budget
// is exceeded, it spills accumulated rows to free capacity and retries. If the
// row still cannot fit after spilling (e.g., a single row exceeds the entire
// reservation), the underlying budget error is propagated (preserving the
// *stats.BudgetExceededError type for callers that inspect it).
func (bs *BufferedSortIterator) growOrSpill(rowBytes int64) error {
	growErr := bs.acct.Grow(rowBytes)
	if growErr == nil {
		return nil // fast path
	}

	// Try spilling accumulated rows to free capacity.
	if len(bs.rows) > 0 {
		spilledCount := len(bs.rows)
		if spillErr := bs.spillCurrentBuffer(); spillErr != nil {
			return fmt.Errorf("buffered_sort.materialize: spill failed: %w", spillErr)
		}

		if err := bs.acct.Grow(rowBytes); err == nil {
			return nil
		}

		// Spill freed memory but still not enough for this row.
		available := bs.acct.MaxUsed() - bs.acct.Used()
		if available < 0 {
			available = 0
		}
		suggestedMin := rowBytes * 2

		return fmt.Errorf("sort operator cannot make progress: row size (%d bytes) exceeds "+
			"available memory (%d bytes) after spilling %d rows to disk; "+
			"try increasing --memory to at least %d bytes: %w",
			rowBytes, available, spilledCount, suggestedMin, growErr)
	}

	// No rows to spill — propagate original error.
	return fmt.Errorf("buffered_sort.materialize: %w", growErr)
}

// createBufferedRun serializes sorted rows into pool pages.
func (bs *BufferedSortIterator) createBufferedRun() error {
	if len(bs.rows) == 0 {
		return nil
	}

	bs.runCounter++
	ownerTag := fmt.Sprintf("bsort-%s-%d", bs.queryID, bs.runCounter)

	pageSize := bs.pool.PageSize()

	// Estimate rows per page using average row size.
	var totalBytes int64
	for _, row := range bs.rows {
		totalBytes += estimateRowMapBytes(row)
	}
	avgRowBytes := totalBytes / int64(len(bs.rows))
	if avgRowBytes < 1 {
		avgRowBytes = 1
	}

	// Conservative: assume 2x overhead from columnar encoding + headers.
	// Start with a guess, adjust downward if serialized data exceeds page size.
	rowsPerPage := int(int64(pageSize) / (avgRowBytes * 2))
	if rowsPerPage < 1 {
		rowsPerPage = 1
	}
	if rowsPerPage > len(bs.rows) {
		rowsPerPage = len(bs.rows)
	}

	run := &bufferedRun{
		ownerTag: ownerTag,
	}

	var allocatedPages []*buffer.Page // for rollback on failure

	i := 0
	for i < len(bs.rows) {
		end := i + rowsPerPage
		if end > len(bs.rows) {
			end = len(bs.rows)
		}

		chunk := bs.rows[i:end]
		data, err := serializeBatchToBytes(chunk)
		if err != nil {
			bs.releasePages(allocatedPages)

			return fmt.Errorf("buffered_sort.createBufferedRun: serialize: %w", err)
		}

		// If serialized data exceeds page size, halve rowsPerPage and retry.
		if len(data) > pageSize {
			if rowsPerPage <= 1 {
				// Single row too large for a page — fall back to disk.
				bs.releasePages(allocatedPages)

				return fmt.Errorf("buffered_sort.createBufferedRun: row too large for page (%d > %d)", len(data), pageSize)
			}
			rowsPerPage /= 2
			if rowsPerPage < 1 {
				rowsPerPage = 1
			}

			continue // retry with smaller chunk (don't advance i)
		}

		// Allocate a pool page.
		page, allocErr := bs.pool.AllocPageForOwner(buffer.OwnerQueryOperator, ownerTag, buffer.OwnerSegmentCache)
		if allocErr != nil {
			// Pool exhausted — release partial pages and fall back to disk.
			bs.releasePages(allocatedPages)

			return fmt.Errorf("buffered_sort.createBufferedRun: alloc: %w", allocErr)
		}
		allocatedPages = append(allocatedPages, page)

		// Write serialized data into the page.
		if err := page.WriteAt(data, 0); err != nil {
			bs.releasePages(allocatedPages)

			return fmt.Errorf("buffered_sort.createBufferedRun: write: %w", err)
		}

		ref := &dataPageRef{
			pageID:   page.ID(),
			dataLen:  len(data),
			rowCount: len(chunk),
		}

		// Set owner data for per-page write-back.
		page.SetOwnerData(&sortPageWriteBack{
			ref:      ref,
			spillCtx: bs.spillCtx,
		})

		// Unpin — makes the page an eviction candidate.
		page.Unpin()

		run.pages = append(run.pages, ref)
		run.totalRows += len(chunk)
		i = end
	}

	bs.runs = append(bs.runs, run)

	return nil
}

// createDiskRun writes sorted rows to a disk file as a fallback when the pool
// can't allocate pages.
func (bs *BufferedSortIterator) createDiskRun() error {
	sw, err := NewColumnarSpillWriter(bs.spillMgr, "sort")
	if err != nil {
		return fmt.Errorf("buffered_sort.createDiskRun: %w", err)
	}

	for _, row := range bs.rows {
		if err := sw.WriteRow(row); err != nil {
			_ = sw.CloseFile()

			return fmt.Errorf("buffered_sort.createDiskRun: write: %w", err)
		}
	}

	if err := sw.CloseFile(); err != nil {
		return fmt.Errorf("buffered_sort.createDiskRun: close: %w", err)
	}

	bs.diskRuns = append(bs.diskRuns, sw.Path())

	return nil
}

// releasePages frees a slice of pool pages (used for partial rollback).
func (bs *BufferedSortIterator) releasePages(pages []*buffer.Page) {
	for _, p := range pages {
		bs.pool.FreePage(p)
	}
}

// releaseAllPages frees all pool pages from all buffered runs.
func (bs *BufferedSortIterator) releaseAllPages() {
	for _, run := range bs.runs {
		for _, ref := range run.pages {
			if ref.evicted {
				continue // already evicted, nothing to free
			}
			// Try to pin and free. If the page was reassigned, skip it.
			p, ok := bs.pool.PinPageIfOwned(ref.pageID, run.ownerTag)
			if ok {
				p.Unpin()
				bs.pool.FreePage(p)
			}
		}
	}
	bs.runs = nil
}

// sortInPlace sorts bs.rows using the configured sort fields.
func (bs *BufferedSortIterator) sortInPlace() {
	sort.SliceStable(bs.rows, func(i, j int) bool {
		for _, sf := range bs.fields {
			a := bs.rows[i][sf.Name]
			b := bs.rows[j][sf.Name]
			cmp := vm.CompareValues(a, b)
			if cmp == 0 {
				continue
			}
			if sf.Desc {
				return cmp > 0
			}

			return cmp < 0
		}

		return false
	})
}

// sortInPlaceCtx sorts bs.rows with periodic context cancellation checks.
func (bs *BufferedSortIterator) sortInPlaceCtx(ctx context.Context) error {
	var canceled bool
	var comparisons int64
	sort.SliceStable(bs.rows, func(i, j int) bool {
		if canceled {
			return false
		}
		comparisons++
		if comparisons&0x3FF == 0 { // check every 1024 comparisons
			select {
			case <-ctx.Done():
				canceled = true

				return false
			default:
			}
		}
		for _, sf := range bs.fields {
			a := bs.rows[i][sf.Name]
			b := bs.rows[j][sf.Name]
			cmp := vm.CompareValues(a, b)
			if cmp == 0 {
				continue
			}
			if sf.Desc {
				return cmp > 0
			}

			return cmp < 0
		}

		return false
	})
	if canceled {
		return ctx.Err()
	}

	return nil
}

// serializeBatchToBytes encodes rows as a columnar batch into a byte buffer.
// Uses the same binary format as ColumnarSpillWriter.flushBatch() so that
// deserializeBatchFromBytes can read it identically to ColumnarSpillReader.
func serializeBatchToBytes(rows []map[string]event.Value) ([]byte, error) {
	rowCount := len(rows)
	if rowCount == 0 {
		return nil, nil
	}

	// Discover all column names (deterministic order).
	colSet := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			colSet[k] = struct{}{}
		}
	}
	colNames := make([]string, 0, len(colSet))
	for k := range colSet {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)

	// Encode each column.
	type encodedCol struct {
		name       string
		fieldType  uint8
		encoding   uint8
		nullBitmap []byte
		data       []byte
	}
	encoded := make([]encodedCol, len(colNames))

	for ci, name := range colNames {
		values := make([]event.Value, rowCount)
		for ri, row := range rows {
			if v, ok := row[name]; ok {
				values[ri] = v
			}
		}

		nullBitmap, hasNulls, allNull := buildNullBitmap(values)

		var fieldType, encoding uint8
		var data []byte

		if allNull {
			fieldType = uint8(event.FieldTypeNull)
			encoding = encodingNone
		} else {
			var err error
			fieldType, encoding, data, err = encodeColumnValues(values, hasNulls)
			if err != nil {
				return nil, fmt.Errorf("serializeBatch: encode column %q: %w", name, err)
			}
		}

		ec := encodedCol{
			name:      name,
			fieldType: fieldType,
			encoding:  encoding,
			data:      data,
		}
		if hasNulls {
			ec.nullBitmap = nullBitmap
		}
		encoded[ci] = ec
	}

	// Write into a bytes.Buffer.
	var buf bytes.Buffer

	// Batch header: magic (4) + row_count (4) + num_columns (2).
	buf.Write(columnarSpillMagic[:])
	_ = binary.Write(&buf, binary.LittleEndian, uint32(rowCount))
	_ = binary.Write(&buf, binary.LittleEndian, uint16(len(colNames)))

	// Column headers + data.
	for _, ec := range encoded {
		nameBytes := []byte(ec.name)
		nullBitmapLen := uint32(len(ec.nullBitmap))
		dataLen := uint32(len(ec.data))

		h := crc32.NewIEEE()
		if len(ec.nullBitmap) > 0 {
			h.Write(ec.nullBitmap)
		}
		if len(ec.data) > 0 {
			h.Write(ec.data)
		}
		checksum := h.Sum32()

		_ = binary.Write(&buf, binary.LittleEndian, uint16(len(nameBytes)))
		buf.Write(nameBytes)
		buf.Write([]byte{ec.fieldType, ec.encoding})
		_ = binary.Write(&buf, binary.LittleEndian, nullBitmapLen)
		_ = binary.Write(&buf, binary.LittleEndian, dataLen)
		_ = binary.Write(&buf, binary.LittleEndian, checksum)

		if len(ec.nullBitmap) > 0 {
			buf.Write(ec.nullBitmap)
		}
		if len(ec.data) > 0 {
			buf.Write(ec.data)
		}
	}

	return buf.Bytes(), nil
}

// deserializeBatchFromBytes decodes a columnar batch from a byte buffer.
// Returns the batch and the number of bytes consumed.
func deserializeBatchFromBytes(data []byte) (*Batch, error) {
	r := bufio.NewReader(bytes.NewReader(data))

	// Read magic.
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, fmt.Errorf("deserializeBatch: read magic: %w", err)
	}
	if magic != columnarSpillMagic {
		return nil, fmt.Errorf("deserializeBatch: invalid magic: %x", magic)
	}

	var rowCount uint32
	var numCols uint16
	if err := binary.Read(r, binary.LittleEndian, &rowCount); err != nil {
		return nil, fmt.Errorf("deserializeBatch: read row count: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &numCols); err != nil {
		return nil, fmt.Errorf("deserializeBatch: read col count: %w", err)
	}

	batch := &Batch{
		Columns: make(map[string][]event.Value, numCols),
		Len:     int(rowCount),
	}

	for i := uint16(0); i < numCols; i++ {
		var nameLen uint16
		if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return nil, fmt.Errorf("deserializeBatch: read col name len: %w", err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, fmt.Errorf("deserializeBatch: read col name: %w", err)
		}
		var typeEnc [2]byte
		if _, err := io.ReadFull(r, typeEnc[:]); err != nil {
			return nil, fmt.Errorf("deserializeBatch: read type/encoding: %w", err)
		}
		var nullBitmapLen, dataLen, checksum uint32
		if err := binary.Read(r, binary.LittleEndian, &nullBitmapLen); err != nil {
			return nil, fmt.Errorf("deserializeBatch: read null bitmap len: %w", err)
		}
		if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
			return nil, fmt.Errorf("deserializeBatch: read data len: %w", err)
		}
		if err := binary.Read(r, binary.LittleEndian, &checksum); err != nil {
			return nil, fmt.Errorf("deserializeBatch: read crc32: %w", err)
		}

		colName := string(nameBytes)

		var nullBitmap []byte
		if nullBitmapLen > 0 {
			nullBitmap = make([]byte, nullBitmapLen)
			if _, err := io.ReadFull(r, nullBitmap); err != nil {
				return nil, fmt.Errorf("deserializeBatch: read null bitmap for %q: %w", colName, err)
			}
		}

		var colData []byte
		if dataLen > 0 {
			colData = make([]byte, dataLen)
			if _, err := io.ReadFull(r, colData); err != nil {
				return nil, fmt.Errorf("deserializeBatch: read data for %q: %w", colName, err)
			}
		}

		// Verify CRC32.
		h := crc32.NewIEEE()
		if len(nullBitmap) > 0 {
			h.Write(nullBitmap)
		}
		if len(colData) > 0 {
			h.Write(colData)
		}
		if h.Sum32() != checksum {
			return nil, fmt.Errorf("deserializeBatch: checksum mismatch for column %q", colName)
		}

		values, err := decodeColumnValues(typeEnc[0], typeEnc[1], colData, nullBitmap, int(rowCount))
		if err != nil {
			return nil, fmt.Errorf("deserializeBatch: decode column %q: %w", colName, err)
		}

		batch.Columns[colName] = values
	}

	return batch, nil
}

// bufferedRunReader reads rows from a single buffered run, page by page.
// Tries pool first (cache hit), falls back to spill file (evicted).
type bufferedRunReader struct {
	run      *bufferedRun
	pool     *buffer.Pool
	spillCtx *spillFileContext
	pageIdx  int    // current page index in run.pages
	rowIdx   int    // current row within current batch
	batch    *Batch // decoded batch from current page
}

func newBufferedRunReader(run *bufferedRun, pool *buffer.Pool, spillCtx *spillFileContext) *bufferedRunReader {
	return &bufferedRunReader{
		run:      run,
		pool:     pool,
		spillCtx: spillCtx,
	}
}

// ReadRow returns the next row from this run, or (nil, io.EOF) when exhausted.
func (br *bufferedRunReader) ReadRow() (map[string]event.Value, error) {
	for {
		// Try current batch.
		if br.batch != nil && br.rowIdx < br.batch.Len {
			row := br.batch.Row(br.rowIdx)
			br.rowIdx++

			return row, nil
		}

		// Load next page.
		if br.pageIdx >= len(br.run.pages) {
			return nil, io.EOF
		}

		ref := br.run.pages[br.pageIdx]
		br.pageIdx++

		batch, err := br.loadPage(ref)
		if err != nil {
			return nil, fmt.Errorf("bufferedRunReader.ReadRow: %w", err)
		}
		br.batch = batch
		br.rowIdx = 0
	}
}

// loadPage loads a page's data from pool (if still cached) or disk (if evicted).
func (br *bufferedRunReader) loadPage(ref *dataPageRef) (*Batch, error) {
	// Try pool first.
	p, ok := br.pool.PinPageIfOwned(ref.pageID, br.run.ownerTag)
	if ok {
		data := make([]byte, ref.dataLen)
		copy(data, p.DataSlice()[:ref.dataLen])
		p.Unpin()

		return deserializeBatchFromBytes(data)
	}

	// Page was evicted — read from spill file.
	if !ref.evicted {
		return nil, fmt.Errorf("page %d not in pool and not evicted", ref.pageID)
	}

	data, err := br.spillCtx.readData(ref.diskOffset, ref.dataLen)
	if err != nil {
		return nil, fmt.Errorf("read evicted page %d: %w", ref.pageID, err)
	}

	return deserializeBatchFromBytes(data)
}

// Close is a no-op (pages are freed by BufferedSortIterator.Close).
func (br *bufferedRunReader) Close() error {
	return nil
}

// bufferedRunMerger performs k-way merge over heterogeneous sources:
// pool-backed runs (bufferedRunReader) and disk-only runs (ColumnarSpillReader).
type bufferedRunMerger struct {
	readers    []runReader // all readers (pool-backed + disk)
	sortFields []SortField
	h          *mergeHeap
}

// runReader is the common interface for run readers in the merger.
type runReader interface {
	ReadRow() (map[string]event.Value, error)
	Close() error
}

// Compile-time interface checks.
var (
	_ runReader = (*bufferedRunReader)(nil)
	_ runReader = (*ColumnarSpillReader)(nil)
)

func newBufferedRunMerger(readers []runReader, fields []SortField) (*bufferedRunMerger, error) {
	h := &mergeHeap{
		sortFields: fields,
	}

	// Prime the heap with the first row from each reader.
	for i, r := range readers {
		row, err := r.ReadRow()
		if err != nil {
			continue // exhausted or error — skip
		}
		h.entries = append(h.entries, mergeEntry{row: row, index: i})
	}
	if len(h.entries) > 0 {
		bsortHeapInit(h)
	}

	return &bufferedRunMerger{
		readers:    readers,
		sortFields: fields,
		h:          h,
	}, nil
}

// Next returns the next row in sorted order across all runs.
func (m *bufferedRunMerger) Next() (map[string]event.Value, error) {
	if m.h.Len() == 0 {
		return nil, nil
	}

	entry := bsortHeapPop(m.h).(mergeEntry)
	result := entry.row

	// Refill from the same reader.
	nextRow, err := m.readers[entry.index].ReadRow()
	if err == nil {
		bsortHeapPush(m.h, mergeEntry{row: nextRow, index: entry.index})
	}

	return result, nil
}

// NextBatch returns up to batchSize rows in sorted order.
func (m *bufferedRunMerger) NextBatch(batchSize int) (*Batch, error) {
	if m.h.Len() == 0 {
		return nil, nil
	}

	batch := NewBatch(batchSize)
	for i := 0; i < batchSize; i++ {
		if m.h.Len() == 0 {
			break
		}
		entry := bsortHeapPop(m.h).(mergeEntry)
		batch.AddRow(entry.row)

		nextRow, err := m.readers[entry.index].ReadRow()
		if err == nil {
			bsortHeapPush(m.h, mergeEntry{row: nextRow, index: entry.index})
		}
	}

	if batch.Len == 0 {
		return nil, nil
	}

	return batch, nil
}

// Close closes all underlying readers.
func (m *bufferedRunMerger) Close() error {
	for _, r := range m.readers {
		r.Close()
	}

	return nil
}

// Compile-time interface check.
var _ SpillMergerI = (*bufferedRunMerger)(nil)

// buildMerger creates a k-way merger over all buffered runs and disk runs.
func (bs *BufferedSortIterator) buildMerger() (SpillMergerI, error) {
	totalReaders := len(bs.runs) + len(bs.diskRuns)
	readers := make([]runReader, 0, totalReaders)

	// Add buffered run readers.
	for _, run := range bs.runs {
		readers = append(readers, newBufferedRunReader(run, bs.pool, bs.spillCtx))
	}

	// Add disk run readers.
	for _, path := range bs.diskRuns {
		r, err := NewColumnarSpillReader(path)
		if err != nil {
			// Close already opened readers.
			for _, opened := range readers {
				opened.Close()
			}

			return nil, fmt.Errorf("buffered_sort.buildMerger: open disk run %s: %w", path, err)
		}
		readers = append(readers, r)
	}

	return newBufferedRunMerger(readers, bs.fields)
}

func bsortHeapInit(h *mergeHeap) {
	heap.Init(h)
}

func bsortHeapPush(h *mergeHeap, x interface{}) {
	heap.Push(h, x)
}

func bsortHeapPop(h *mergeHeap) interface{} {
	return heap.Pop(h)
}

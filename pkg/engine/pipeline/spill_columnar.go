package pipeline

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/column"
)

// columnarBatchSize is the number of rows buffered before flushing a columnar
// batch to disk. Chosen to balance compression ratio (more rows = better
// dictionary/delta) against memory overhead per flush.
const columnarBatchSize = 2048

// columnarSpillMagic identifies the start of a columnar spill batch.
var columnarSpillMagic = [4]byte{'C', 'S', 'P', 'B'}

// encodingNone indicates an all-null column with no encoded data.
const encodingNone uint8 = 0

// ColumnarSpillWriter

// ColumnarSpillWriter buffers rows and writes them as columnar batches to a
// temporary file. Each batch transposes rows into typed columns and applies
// production-tested encoders (delta-varint, Gorilla XOR, dictionary, LZ4)
// for significantly better compression than per-row msgpack serialization.
//
// Thread-unsafe — intended for single-goroutine use within one operator.
type ColumnarSpillWriter struct {
	file    *os.File
	mgr     *SpillManager
	buf     *bufio.Writer
	rows    []map[string]event.Value // buffered rows (up to columnarBatchSize)
	written int                      // total rows written (across all batches)
	bytes   int64                    // total bytes written to file
}

// NewColumnarSpillWriter creates a new columnar spill writer. The file is
// tracked by the given SpillManager for lifecycle management. If mgr is nil,
// a temp file is created in os.TempDir() without tracking.
func NewColumnarSpillWriter(mgr *SpillManager, prefix string) (*ColumnarSpillWriter, error) {
	var f *os.File
	var err error

	if mgr != nil {
		f, err = mgr.NewSpillFile(prefix)
	} else {
		f, err = os.CreateTemp("", "lynxdb-spill-"+prefix+"-*.tmp")
	}
	if err != nil {
		return nil, fmt.Errorf("columnar_spill: create file: %w", err)
	}

	return &ColumnarSpillWriter{
		file: f,
		mgr:  mgr,
		buf:  bufio.NewWriterSize(f, 64*1024), // 64KB write buffer
		rows: make([]map[string]event.Value, 0, columnarBatchSize),
	}, nil
}

// WriteRow appends a single row to the internal buffer. When the buffer
// reaches columnarBatchSize, it is automatically flushed as a columnar batch.
func (w *ColumnarSpillWriter) WriteRow(row map[string]event.Value) error {
	w.rows = append(w.rows, row)
	if len(w.rows) >= columnarBatchSize {
		return w.flushBatch()
	}

	return nil
}

// WriteBatch appends all rows from a Batch. Flushes when the buffer is full.
func (w *ColumnarSpillWriter) WriteBatch(batch *Batch) error {
	for i := 0; i < batch.Len; i++ {
		if err := w.WriteRow(batch.Row(i)); err != nil {
			return err
		}
	}

	return nil
}

// Flush writes any buffered rows as a partial batch.
func (w *ColumnarSpillWriter) Flush() error {
	if len(w.rows) == 0 {
		return nil
	}

	return w.flushBatch()
}

// CloseFile flushes remaining rows and closes the file handle without
// removing the file. Use this when the spill file will be read back later.
func (w *ColumnarSpillWriter) CloseFile() error {
	if err := w.Flush(); err != nil {
		w.file.Close()

		return err
	}
	if err := w.buf.Flush(); err != nil {
		w.file.Close()

		return fmt.Errorf("columnar_spill: flush bufio: %w", err)
	}

	return w.file.Close()
}

// Close flushes, closes the file, and deletes it if unmanaged (no SpillManager).
func (w *ColumnarSpillWriter) Close() error {
	name := w.file.Name()
	if err := w.CloseFile(); err != nil {
		return err
	}
	if w.mgr == nil {
		return os.Remove(name)
	}

	return nil
}

// Path returns the path to the underlying spill file.
func (w *ColumnarSpillWriter) Path() string {
	return w.file.Name()
}

// Rows returns the total number of rows written (flushed + buffered).
func (w *ColumnarSpillWriter) Rows() int {
	return w.written + len(w.rows)
}

// BytesWritten returns the total bytes written to the file. This is accurate
// (not estimated), enabling precise quota enforcement in SpillManager.
func (w *ColumnarSpillWriter) BytesWritten() int64 {
	return w.bytes
}

// flushBatch transposes the buffered rows into columnar layout, encodes each
// column, and writes the batch to the file.
func (w *ColumnarSpillWriter) flushBatch() error {
	rows := w.rows
	rowCount := len(rows)
	if rowCount == 0 {
		return nil
	}

	// Discover all column names (deterministic order for reproducibility).
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
	type encodedColumn struct {
		name       string
		fieldType  uint8
		encoding   uint8
		nullBitmap []byte // nil if no nulls
		data       []byte // nil for all-null columns
	}
	encoded := make([]encodedColumn, len(colNames))

	for ci, name := range colNames {
		// Extract values for this column.
		values := make([]event.Value, rowCount)
		for ri, row := range rows {
			if v, ok := row[name]; ok {
				values[ri] = v
			}
			// else: zero Value = null
		}

		nullBitmap, hasNulls, allNull := buildNullBitmap(values)

		var fieldType uint8
		var encoding uint8
		var data []byte
		var err error

		if allNull {
			fieldType = uint8(event.FieldTypeNull)
			encoding = encodingNone
		} else {
			fieldType, encoding, data, err = encodeColumnValues(values, hasNulls)
			if err != nil {
				return fmt.Errorf("columnar_spill: encode column %q: %w", name, err)
			}
		}

		ec := encodedColumn{
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

	// Write the batch to the buffered writer.
	startBytes := w.bytes
	bw := &countWriter{w: w.buf, n: &w.bytes}

	// Batch header: magic (4) + row_count (4) + num_columns (2).
	if _, err := bw.Write(columnarSpillMagic[:]); err != nil {
		return fmt.Errorf("columnar_spill: write magic: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(rowCount)); err != nil {
		return fmt.Errorf("columnar_spill: write row count: %w", err)
	}
	if err := binary.Write(bw, binary.LittleEndian, uint16(len(colNames))); err != nil {
		return fmt.Errorf("columnar_spill: write col count: %w", err)
	}

	// Column headers, then column data.
	for _, ec := range encoded {
		nameBytes := []byte(ec.name)
		nullBitmapLen := uint32(len(ec.nullBitmap))
		dataLen := uint32(len(ec.data))

		// Compute CRC32 over (nullBitmap + data).
		h := crc32.NewIEEE()
		if len(ec.nullBitmap) > 0 {
			h.Write(ec.nullBitmap)
		}
		if len(ec.data) > 0 {
			h.Write(ec.data)
		}
		checksum := h.Sum32()

		// name_length (2) + name + field_type (1) + encoding (1) +
		// null_bitmap_length (4) + data_length (4) + crc32 (4)
		if err := binary.Write(bw, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
			return fmt.Errorf("columnar_spill: write col header: %w", err)
		}
		if _, err := bw.Write(nameBytes); err != nil {
			return fmt.Errorf("columnar_spill: write col name: %w", err)
		}
		if _, err := bw.Write([]byte{ec.fieldType, ec.encoding}); err != nil {
			return fmt.Errorf("columnar_spill: write col type/encoding: %w", err)
		}
		if err := binary.Write(bw, binary.LittleEndian, nullBitmapLen); err != nil {
			return fmt.Errorf("columnar_spill: write null bitmap len: %w", err)
		}
		if err := binary.Write(bw, binary.LittleEndian, dataLen); err != nil {
			return fmt.Errorf("columnar_spill: write data len: %w", err)
		}
		if err := binary.Write(bw, binary.LittleEndian, checksum); err != nil {
			return fmt.Errorf("columnar_spill: write crc32: %w", err)
		}

		// Column data: null bitmap + encoded data.
		if len(ec.nullBitmap) > 0 {
			if _, err := bw.Write(ec.nullBitmap); err != nil {
				return fmt.Errorf("columnar_spill: write null bitmap: %w", err)
			}
		}
		if len(ec.data) > 0 {
			if _, err := bw.Write(ec.data); err != nil {
				return fmt.Errorf("columnar_spill: write col data: %w", err)
			}
		}
	}

	// Track actual bytes written.
	actualBytes := w.bytes - startBytes
	w.mgr.TrackBytes(actualBytes)

	w.written += rowCount
	w.rows = w.rows[:0]

	return nil
}

// ColumnarSpillReader

// ColumnarSpillReader reads columnar batches from a spill file produced by
// ColumnarSpillWriter.
type ColumnarSpillReader struct {
	file *os.File
	buf  *bufio.Reader
	done bool

	// Row-at-a-time compatibility buffer.
	currentBatch *Batch
	currentRow   int
}

// NewColumnarSpillReader opens a columnar spill file for reading.
func NewColumnarSpillReader(path string) (*ColumnarSpillReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("columnar_spill: open %s: %w", path, err)
	}

	return &ColumnarSpillReader{
		file: f,
		buf:  bufio.NewReaderSize(f, 64*1024),
	}, nil
}

// ReadBatch reads the next columnar batch from the file. Returns (nil, io.EOF)
// when no more batches are available.
func (r *ColumnarSpillReader) ReadBatch() (*Batch, error) {
	if r.done {
		return nil, io.EOF
	}

	// Read magic.
	var magic [4]byte
	if _, err := io.ReadFull(r.buf, magic[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			r.done = true

			return nil, io.EOF
		}

		return nil, fmt.Errorf("columnar_spill: read magic: %w", err)
	}
	if magic != columnarSpillMagic {
		return nil, fmt.Errorf("columnar_spill: invalid batch magic: %x", magic)
	}

	// Row count and column count.
	var rowCount uint32
	var numCols uint16
	if err := binary.Read(r.buf, binary.LittleEndian, &rowCount); err != nil {
		return nil, fmt.Errorf("columnar_spill: read row count: %w", err)
	}
	if err := binary.Read(r.buf, binary.LittleEndian, &numCols); err != nil {
		return nil, fmt.Errorf("columnar_spill: read col count: %w", err)
	}

	// Read columns. The writer interleaves (header + data) per column,
	// so we read each column's header and data together in one pass.
	batch := &Batch{
		Columns: make(map[string][]event.Value, numCols),
		Len:     int(rowCount),
	}

	for i := uint16(0); i < numCols; i++ {
		// Column header.
		var nameLen uint16
		if err := binary.Read(r.buf, binary.LittleEndian, &nameLen); err != nil {
			return nil, fmt.Errorf("columnar_spill: read col name len: %w", err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r.buf, nameBytes); err != nil {
			return nil, fmt.Errorf("columnar_spill: read col name: %w", err)
		}
		var typeEnc [2]byte
		if _, err := io.ReadFull(r.buf, typeEnc[:]); err != nil {
			return nil, fmt.Errorf("columnar_spill: read type/encoding: %w", err)
		}
		var nullBitmapLen, dataLen, checksum uint32
		if err := binary.Read(r.buf, binary.LittleEndian, &nullBitmapLen); err != nil {
			return nil, fmt.Errorf("columnar_spill: read null bitmap len: %w", err)
		}
		if err := binary.Read(r.buf, binary.LittleEndian, &dataLen); err != nil {
			return nil, fmt.Errorf("columnar_spill: read data len: %w", err)
		}
		if err := binary.Read(r.buf, binary.LittleEndian, &checksum); err != nil {
			return nil, fmt.Errorf("columnar_spill: read crc32: %w", err)
		}

		colName := string(nameBytes)

		// Column data: null bitmap + encoded data (immediately after header).
		var nullBitmap []byte
		if nullBitmapLen > 0 {
			nullBitmap = make([]byte, nullBitmapLen)
			if _, err := io.ReadFull(r.buf, nullBitmap); err != nil {
				return nil, fmt.Errorf("columnar_spill: read null bitmap for %q: %w", colName, err)
			}
		}

		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			if _, err := io.ReadFull(r.buf, data); err != nil {
				return nil, fmt.Errorf("columnar_spill: read data for %q: %w", colName, err)
			}
		}

		// Verify CRC32.
		h := crc32.NewIEEE()
		if len(nullBitmap) > 0 {
			h.Write(nullBitmap)
		}
		if len(data) > 0 {
			h.Write(data)
		}
		if h.Sum32() != checksum {
			return nil, fmt.Errorf("columnar_spill: checksum mismatch for column %q", colName)
		}

		// Decode column values.
		values, err := decodeColumnValues(typeEnc[0], typeEnc[1], data, nullBitmap, int(rowCount))
		if err != nil {
			return nil, fmt.Errorf("columnar_spill: decode column %q: %w", colName, err)
		}

		batch.Columns[colName] = values
	}

	return batch, nil
}

// ReadRow reads a single row at a time, internally buffering batches.
// Returns (nil, io.EOF) when exhausted.
func (r *ColumnarSpillReader) ReadRow() (map[string]event.Value, error) {
	for {
		if r.currentBatch != nil && r.currentRow < r.currentBatch.Len {
			row := r.currentBatch.Row(r.currentRow)
			r.currentRow++

			return row, nil
		}

		// Need next batch.
		batch, err := r.ReadBatch()
		if err != nil {
			return nil, err
		}
		r.currentBatch = batch
		r.currentRow = 0
	}
}

// Close closes the underlying file.
func (r *ColumnarSpillReader) Close() error {
	return r.file.Close()
}

// ColumnarSpillMerger

// ColumnarSpillMerger performs k-way merge of sorted columnar spill files.
// Uses the same heap-based merge algorithm as SpillMerger but reads from
// ColumnarSpillReader instances.
type ColumnarSpillMerger struct {
	readers    []*ColumnarSpillReader
	sortFields []SortField
	h          *mergeHeap
}

// NewColumnarSpillMerger creates a k-way merge reader over sorted columnar
// spill files. Each file must contain rows sorted according to sortFields.
// If len(paths) > maxMergeFanIn, a multi-pass cascade merge is performed.
func NewColumnarSpillMerger(paths []string, sortFields []SortField) (*ColumnarSpillMerger, error) {
	return newColumnarSpillMergerWithMgr(paths, sortFields, nil)
}

// NewColumnarSpillMergerManaged creates a k-way merge reader with SpillManager
// support for intermediate files in multi-pass merges.
func NewColumnarSpillMergerManaged(paths []string, sortFields []SortField, mgr *SpillManager) (*ColumnarSpillMerger, error) {
	return newColumnarSpillMergerWithMgr(paths, sortFields, mgr)
}

func newColumnarSpillMergerWithMgr(paths []string, sortFields []SortField, mgr *SpillManager) (*ColumnarSpillMerger, error) {
	// Multi-pass cascade when fan-in exceeds the limit.
	if len(paths) > maxMergeFanIn {
		reduced, err := columnarCascadeMerge(paths, sortFields, mgr)
		if err != nil {
			return nil, fmt.Errorf("columnar_spill: cascade merge: %w", err)
		}
		paths = reduced
	}

	readers := make([]*ColumnarSpillReader, len(paths))
	for i, path := range paths {
		r, err := NewColumnarSpillReader(path)
		if err != nil {
			for j := 0; j < i; j++ {
				readers[j].Close()
			}

			return nil, err
		}
		readers[i] = r
	}

	h := &mergeHeap{
		sortFields: sortFields,
	}

	// Prime the heap with the first row from each reader.
	for i, r := range readers {
		row, err := r.ReadRow()
		if err != nil {
			continue // exhausted or error — skip
		}
		h.entries = append(h.entries, mergeEntry{row: row, index: i})
	}
	heap.Init(h)

	return &ColumnarSpillMerger{
		readers:    readers,
		sortFields: sortFields,
		h:          h,
	}, nil
}

// columnarCascadeMerge reduces len(paths) to <= maxMergeFanIn by merging
// chunks into intermediate columnar files.
func columnarCascadeMerge(paths []string, sortFields []SortField, mgr *SpillManager) ([]string, error) {
	for len(paths) > maxMergeFanIn {
		var nextPaths []string
		for i := 0; i < len(paths); i += maxMergeFanIn {
			end := i + maxMergeFanIn
			if end > len(paths) {
				end = len(paths)
			}
			chunk := paths[i:end]

			if len(chunk) <= 1 {
				nextPaths = append(nextPaths, chunk...)

				continue
			}

			// Merge chunk into an intermediate file.
			sm, err := newColumnarSpillMergerWithMgr(chunk, sortFields, nil)
			if err != nil {
				return nil, err
			}

			sw, swErr := NewColumnarSpillWriter(mgr, "merge-intermediate")
			if swErr != nil {
				sm.Close()

				return nil, swErr
			}

			for {
				row, mergeErr := sm.Next()
				if mergeErr != nil {
					sm.Close()
					sw.Close()

					return nil, mergeErr
				}
				if row == nil {
					break
				}
				if writeErr := sw.WriteRow(row); writeErr != nil {
					sm.Close()
					sw.Close()

					return nil, writeErr
				}
			}
			sm.Close()

			if closeErr := sw.CloseFile(); closeErr != nil {
				return nil, closeErr
			}
			nextPaths = append(nextPaths, sw.Path())
		}
		paths = nextPaths
	}

	return paths, nil
}

// Next returns the next row in sorted order across all spill files.
// Returns (nil, nil) when all files are exhausted.
func (m *ColumnarSpillMerger) Next() (map[string]event.Value, error) {
	if m.h.Len() == 0 {
		return nil, nil
	}

	entry := heap.Pop(m.h).(mergeEntry)
	result := entry.row

	// Refill from the same reader.
	nextRow, err := m.readers[entry.index].ReadRow()
	if err == nil {
		heap.Push(m.h, mergeEntry{row: nextRow, index: entry.index})
	}

	return result, nil
}

// NextBatch returns up to batchSize rows in sorted order.
// Returns (nil, nil) when all files are exhausted (EOF).
func (m *ColumnarSpillMerger) NextBatch(batchSize int) (*Batch, error) {
	if m.h.Len() == 0 {
		return nil, nil
	}

	batch := NewBatch(batchSize)
	for i := 0; i < batchSize; i++ {
		if m.h.Len() == 0 {
			break
		}
		entry := heap.Pop(m.h).(mergeEntry)
		batch.AddRow(entry.row)

		// Refill from the same reader.
		nextRow, err := m.readers[entry.index].ReadRow()
		if err == nil {
			heap.Push(m.h, mergeEntry{row: nextRow, index: entry.index})
		}
	}

	if batch.Len == 0 {
		return nil, nil
	}

	return batch, nil
}

// Close closes all underlying readers.
func (m *ColumnarSpillMerger) Close() error {
	for _, r := range m.readers {
		r.Close()
	}

	return nil
}

// SpillMergerI — common interface for both merger implementations

// SpillMergerI is the interface for k-way merge over spill files.
// Both SpillMerger and ColumnarSpillMerger implement this interface.
type SpillMergerI interface {
	Next() (map[string]event.Value, error)
	NextBatch(batchSize int) (*Batch, error)
	Close() error
}

// Compile-time interface checks.
var (
	_ SpillMergerI = (*SpillMerger)(nil)
	_ SpillMergerI = (*ColumnarSpillMerger)(nil)
)

// Encoding helpers

// buildNullBitmap creates a null bitmap for the given values.
// Returns the bitmap, whether any nulls exist, and whether all values are null.
func buildNullBitmap(values []event.Value) (bitmap []byte, hasNulls, allNull bool) {
	n := (len(values) + 7) / 8
	bitmap = make([]byte, n)
	nullCount := 0
	for i, v := range values {
		if v.IsNull() {
			bitmap[i/8] |= 1 << uint(i%8)
			nullCount++
		}
	}
	hasNulls = nullCount > 0
	allNull = nullCount == len(values)

	return bitmap, hasNulls, allNull
}

// isNullBit checks the null bitmap at position i.
func isNullBit(bitmap []byte, i int) bool {
	if len(bitmap) == 0 {
		return false
	}

	return bitmap[i/8]&(1<<uint(i%8)) != 0
}

// detectDominantType determines the most common non-null type in a slice of values.
// Returns the dominant FieldType. If mixed types are present, returns FieldTypeString
// (all values will be string-converted).
func detectDominantType(values []event.Value) event.FieldType {
	var counts [6]int // indexed by FieldType

	for _, v := range values {
		if v.IsNull() {
			continue
		}
		t := v.Type()
		if int(t) < len(counts) {
			counts[t]++
		}
	}

	// Find the type with the most values.
	bestType := event.FieldTypeNull
	bestCount := 0
	for t := event.FieldTypeString; t <= event.FieldTypeTimestamp; t++ {
		if counts[t] > bestCount {
			bestCount = counts[t]
			bestType = t
		}
	}

	if bestCount == 0 {
		return event.FieldTypeNull // all null
	}

	// Check if there are mixed types (more than one non-null type).
	nonNullTypes := 0
	for t := event.FieldTypeString; t <= event.FieldTypeTimestamp; t++ {
		if counts[t] > 0 {
			nonNullTypes++
		}
	}
	if nonNullTypes > 1 {
		// Mixed types: fall back to string encoding.
		return event.FieldTypeString
	}

	return bestType
}

// encodeColumnValues encodes non-null values using the appropriate column encoder.
// Null positions get sentinel/zero values; the null bitmap handles reconstruction.
func encodeColumnValues(values []event.Value, _ bool) (fieldType, encoding uint8, data []byte, err error) {
	domType := detectDominantType(values)

	switch domType {
	case event.FieldTypeNull:
		return uint8(event.FieldTypeNull), encodingNone, nil, nil

	case event.FieldTypeInt, event.FieldTypeTimestamp, event.FieldTypeBool:
		ints := make([]int64, len(values))
		for i, v := range values {
			if v.IsNull() {
				ints[i] = 0 // sentinel for null positions

				continue
			}
			switch v.Type() {
			case event.FieldTypeInt:
				ints[i] = v.AsInt()
			case event.FieldTypeTimestamp:
				ints[i] = v.AsTimestamp().UnixNano()
			case event.FieldTypeBool:
				if v.AsBool() {
					ints[i] = 1
				}
			default:
				ints[i] = 0
			}
		}
		data, err = column.NewDeltaEncoder().EncodeInt64s(ints)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("delta encode: %w", err)
		}

		return uint8(domType), uint8(column.EncodingDelta), data, nil

	case event.FieldTypeFloat:
		floats := make([]float64, len(values))
		for i, v := range values {
			if v.IsNull() {
				floats[i] = 0 // sentinel

				continue
			}
			if v.Type() == event.FieldTypeFloat {
				floats[i] = v.AsFloat()
			} else if v.Type() == event.FieldTypeInt {
				floats[i] = float64(v.AsInt())
			}
		}
		data, err = column.NewGorillaEncoder().EncodeFloat64s(floats)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("gorilla encode: %w", err)
		}

		return uint8(event.FieldTypeFloat), uint8(column.EncodingGorilla), data, nil

	case event.FieldTypeString:
		strs := make([]string, len(values))
		for i, v := range values {
			if v.IsNull() {
				strs[i] = "" // sentinel

				continue
			}
			if v.Type() == event.FieldTypeString {
				strs[i] = v.AsString()
			} else {
				// Mixed type: convert to string.
				strs[i] = v.String()
			}
		}

		// Try dictionary first.
		data, err = column.NewDictEncoder().EncodeStrings(strs)
		if err != nil {
			// ErrTooManyUnique -> fall back to LZ4.
			if errors.Is(err, column.ErrTooManyUnique) {
				data, err = column.NewLZ4Encoder().EncodeStrings(strs)
				if err != nil {
					return 0, 0, nil, fmt.Errorf("lz4 encode: %w", err)
				}

				return uint8(event.FieldTypeString), uint8(column.EncodingLZ4), data, nil
			}

			return 0, 0, nil, fmt.Errorf("dict encode: %w", err)
		}
		// Determine which dict encoding was used by checking the first byte.
		enc := column.EncodingType(data[0])

		return uint8(event.FieldTypeString), uint8(enc), data, nil

	default:
		return uint8(event.FieldTypeNull), encodingNone, nil, nil
	}
}

// decodeColumnValues decodes a column from its encoded representation back
// to a slice of event.Value, applying the null bitmap.
func decodeColumnValues(fieldType, encoding uint8, data, nullBitmap []byte, rowCount int) ([]event.Value, error) {
	values := make([]event.Value, rowCount)

	// All-null column.
	if encoding == encodingNone || data == nil {
		// All values are null (default zero value of event.Value is null).
		return values, nil
	}

	ft := event.FieldType(fieldType)
	enc := column.EncodingType(encoding)

	switch enc {
	case column.EncodingDelta:
		ints, err := column.NewDeltaEncoder().DecodeInt64s(data)
		if err != nil {
			return nil, fmt.Errorf("delta decode: %w", err)
		}
		for i := 0; i < rowCount && i < len(ints); i++ {
			if isNullBit(nullBitmap, i) {
				continue // leave as null
			}
			switch ft {
			case event.FieldTypeInt:
				values[i] = event.IntValue(ints[i])
			case event.FieldTypeTimestamp:
				values[i] = event.TimestampValue(time.Unix(0, ints[i]))
			case event.FieldTypeBool:
				values[i] = event.BoolValue(ints[i] != 0)
			default:
				values[i] = event.IntValue(ints[i])
			}
		}

	case column.EncodingGorilla:
		floats, err := column.NewGorillaEncoder().DecodeFloat64s(data)
		if err != nil {
			return nil, fmt.Errorf("gorilla decode: %w", err)
		}
		for i := 0; i < rowCount && i < len(floats); i++ {
			if isNullBit(nullBitmap, i) {
				continue
			}
			values[i] = event.FloatValue(floats[i])
		}

	case column.EncodingDict8, column.EncodingDict16:
		strs, err := column.NewDictEncoder().DecodeStrings(data)
		if err != nil {
			return nil, fmt.Errorf("dict decode: %w", err)
		}
		for i := 0; i < rowCount && i < len(strs); i++ {
			if isNullBit(nullBitmap, i) {
				continue
			}
			values[i] = event.StringValue(strs[i])
		}

	case column.EncodingLZ4:
		strs, err := column.NewLZ4Encoder().DecodeStrings(data)
		if err != nil {
			return nil, fmt.Errorf("lz4 decode: %w", err)
		}
		for i := 0; i < rowCount && i < len(strs); i++ {
			if isNullBit(nullBitmap, i) {
				continue
			}
			values[i] = event.StringValue(strs[i])
		}

	default:
		return nil, fmt.Errorf("unknown encoding type: %d", encoding)
	}

	return values, nil
}

// countWriter wraps an io.Writer and counts bytes written.

type countWriter struct {
	w io.Writer
	n *int64
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	*cw.n += int64(n)

	return n, err
}

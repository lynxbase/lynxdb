package pipeline

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// Round-trip tests — basic types

func TestColumnarSpillBasicRoundtrip(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "test")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"key":  event.IntValue(int64(i)),
			"data": event.StringValue("hello"),
		}); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("rows buffered: %d, bytes written before close: %d", sw.Rows(), sw.BytesWritten())

	if err := sw.CloseFile(); err != nil {
		t.Fatal("CloseFile:", err)
	}

	t.Logf("after close: bytes written: %d, path: %s", sw.BytesWritten(), sw.Path())

	// Check file size.
	info, statErr := os.Stat(sw.Path())
	if statErr != nil {
		t.Fatal("stat:", statErr)
	}
	t.Logf("file size: %d bytes", info.Size())

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal("open reader:", err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal("ReadBatch:", err)
	}
	t.Logf("batch.Len=%d, columns=%v", batch.Len, batch.ColumnNames())

	// Try second batch (should be EOF).
	_, err = sr.ReadBatch()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	// Verify data.
	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		key := row["key"]
		if key.AsInt() != int64(i) {
			t.Fatalf("row %d: expected key=%d, got %v", i, i, key)
		}
		data := row["data"]
		if data.AsString() != "hello" {
			t.Fatalf("row %d: expected data=hello, got %v", i, data)
		}
	}
}

func TestColumnarSpillAllTypes(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	now := time.Now().Truncate(time.Nanosecond)

	sw, err := NewColumnarSpillWriter(mgr, "all-types")
	if err != nil {
		t.Fatal(err)
	}

	const numRows = 100
	for i := 0; i < numRows; i++ {
		row := map[string]event.Value{
			"int_col":   event.IntValue(int64(i * 100)),
			"float_col": event.FloatValue(float64(i) * 1.5),
			"str_col":   event.StringValue(fmt.Sprintf("val_%d", i)),
			"bool_col":  event.BoolValue(i%2 == 0),
			"ts_col":    event.TimestampValue(now.Add(time.Duration(i) * time.Second)),
		}
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}

	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != numRows {
		t.Fatalf("expected %d rows, got %d", numRows, batch.Len)
	}

	for i := 0; i < numRows; i++ {
		row := batch.Row(i)

		// int
		if got := row["int_col"].AsInt(); got != int64(i*100) {
			t.Fatalf("row %d: int_col: expected %d, got %d", i, i*100, got)
		}

		// float
		if got := row["float_col"].AsFloat(); math.Abs(got-float64(i)*1.5) > 1e-9 {
			t.Fatalf("row %d: float_col: expected %.2f, got %.2f", i, float64(i)*1.5, got)
		}

		// string
		expected := fmt.Sprintf("val_%d", i)
		if got := row["str_col"].AsString(); got != expected {
			t.Fatalf("row %d: str_col: expected %s, got %s", i, expected, got)
		}

		// bool
		expectedBool := i%2 == 0
		if got := row["bool_col"].AsBool(); got != expectedBool {
			t.Fatalf("row %d: bool_col: expected %v, got %v", i, expectedBool, got)
		}

		// timestamp
		expectedTS := now.Add(time.Duration(i) * time.Second)
		if got := row["ts_col"].AsTimestamp(); !got.Equal(expectedTS) {
			t.Fatalf("row %d: ts_col: expected %v, got %v", i, expectedTS, got)
		}
	}
}

// Row counts and batch boundaries

func TestColumnarSpillSingleRow(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "single")
	if err != nil {
		t.Fatal(err)
	}

	if err := sw.WriteRow(map[string]event.Value{
		"x": event.IntValue(42),
	}); err != nil {
		t.Fatal(err)
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != 1 {
		t.Fatalf("expected 1 row, got %d", batch.Len)
	}
	if batch.Row(0)["x"].AsInt() != 42 {
		t.Fatalf("expected x=42, got %v", batch.Row(0)["x"])
	}
}

func TestColumnarSpillExactBatchSize(t *testing.T) {
	// Write exactly columnarBatchSize rows — should produce one batch with no partial flush.
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "exact")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < columnarBatchSize; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"i": event.IntValue(int64(i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != columnarBatchSize {
		t.Fatalf("expected %d rows, got %d", columnarBatchSize, batch.Len)
	}

	// Second batch should be EOF.
	_, err = sr.ReadBatch()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF for second batch, got %v", err)
	}
}

func TestColumnarSpillMultipleBatches(t *testing.T) {
	// Write columnarBatchSize+1 rows — should produce two batches.
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "multi")
	if err != nil {
		t.Fatal(err)
	}

	total := columnarBatchSize + 1
	for i := 0; i < total; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"i": event.IntValue(int64(i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	// Batch 1: full batch.
	b1, err := sr.ReadBatch()
	if err != nil {
		t.Fatal("batch 1:", err)
	}
	if b1.Len != columnarBatchSize {
		t.Fatalf("batch 1: expected %d rows, got %d", columnarBatchSize, b1.Len)
	}

	// Batch 2: 1 row.
	b2, err := sr.ReadBatch()
	if err != nil {
		t.Fatal("batch 2:", err)
	}
	if b2.Len != 1 {
		t.Fatalf("batch 2: expected 1 row, got %d", b2.Len)
	}

	// Verify all values are correct across batches.
	for i := 0; i < b1.Len; i++ {
		if b1.Row(i)["i"].AsInt() != int64(i) {
			t.Fatalf("batch 1, row %d: expected %d, got %d", i, i, b1.Row(i)["i"].AsInt())
		}
	}
	if b2.Row(0)["i"].AsInt() != int64(columnarBatchSize) {
		t.Fatalf("batch 2, row 0: expected %d, got %d", columnarBatchSize, b2.Row(0)["i"].AsInt())
	}

	// Third read should be EOF.
	_, err = sr.ReadBatch()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF for third batch, got %v", err)
	}
}

func TestColumnarSpillManyRows(t *testing.T) {
	// Write 10000 rows (multiple full batches + remainder).
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "many")
	if err != nil {
		t.Fatal(err)
	}

	const total = 10000
	for i := 0; i < total; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"key":   event.IntValue(int64(i)),
			"level": event.StringValue([]string{"INFO", "WARN", "ERROR"}[i%3]),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	t.Logf("10K rows written: %d bytes (%.1f bytes/row)", sw.BytesWritten(), float64(sw.BytesWritten())/total)

	// Read all rows back via ReadRow (the compat path).
	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	count := 0
	for {
		row, readErr := sr.ReadRow()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatal("ReadRow:", readErr)
		}
		if row["key"].AsInt() != int64(count) {
			t.Fatalf("row %d: expected key=%d, got %d", count, count, row["key"].AsInt())
		}
		count++
	}

	if count != total {
		t.Fatalf("expected %d rows, got %d", total, count)
	}
}

// Null handling

func TestColumnarSpillAllNullColumn(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "null")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		row := map[string]event.Value{
			"real": event.IntValue(int64(i)),
		}
		// "maybe" column is absent = null for all rows.
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		if row["real"].AsInt() != int64(i) {
			t.Fatalf("row %d: real: expected %d, got %d", i, i, row["real"].AsInt())
		}
	}
}

func TestColumnarSpillMixedNulls(t *testing.T) {
	// Some rows have "optional" field, others don't (null).
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "mixed-nulls")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		row := map[string]event.Value{
			"key": event.IntValue(int64(i)),
		}
		if i%3 == 0 {
			row["optional"] = event.StringValue(fmt.Sprintf("present_%d", i))
		}
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		if row["key"].AsInt() != int64(i) {
			t.Fatalf("row %d: key mismatch", i)
		}
		opt := row["optional"]
		if i%3 == 0 {
			expected := fmt.Sprintf("present_%d", i)
			if opt.AsString() != expected {
				t.Fatalf("row %d: expected optional=%q, got %q", i, expected, opt.AsString())
			}
		} else if !opt.IsNull() {
			t.Fatalf("row %d: expected null optional, got %v", i, opt)
		}
	}
}

func TestColumnarSpillNoNulls(t *testing.T) {
	// All rows have all fields — no null bitmap should be needed.
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "no-nulls")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"a": event.IntValue(int64(i)),
			"b": event.StringValue("x"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != 30 {
		t.Fatalf("expected 30 rows, got %d", batch.Len)
	}

	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		if row["a"].AsInt() != int64(i) {
			t.Fatalf("row %d: a mismatch", i)
		}
		if row["b"].AsString() != "x" {
			t.Fatalf("row %d: b mismatch", i)
		}
	}
}

// CRC32 validation

func TestColumnarSpillCRC32Corruption(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "corrupt")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"x": event.IntValue(int64(i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	path := sw.Path()
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte in the middle of the file.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 30 {
		t.Fatalf("file too small: %d bytes", len(data))
	}
	// Flip a byte near the end (in the encoded column data).
	data[len(data)-5] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	_, readErr := sr.ReadBatch()
	if readErr == nil {
		t.Fatal("expected CRC32 error on corrupted data")
	}
	t.Logf("got expected error: %v", readErr)
}

// Empty file

func TestColumnarSpillEmptyFile(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "empty")
	if err != nil {
		t.Fatal(err)
	}

	// Write 0 rows, then close.
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	_, readErr := sr.ReadBatch()
	if !errors.Is(readErr, io.EOF) {
		t.Fatalf("expected io.EOF for empty file, got %v", readErr)
	}
}

// WriteBatch bulk path

func TestColumnarSpillWriteBatch(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Build a batch.
	srcBatch := NewBatch(20)
	for i := 0; i < 20; i++ {
		srcBatch.AddRow(map[string]event.Value{
			"idx":  event.IntValue(int64(i)),
			"name": event.StringValue(fmt.Sprintf("item_%d", i)),
		})
	}

	sw, err := NewColumnarSpillWriter(mgr, "write-batch")
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.WriteBatch(srcBatch); err != nil {
		t.Fatal(err)
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, readErr := sr.ReadBatch()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if batch.Len != 20 {
		t.Fatalf("expected 20 rows, got %d", batch.Len)
	}

	for i := 0; i < 20; i++ {
		row := batch.Row(i)
		if row["idx"].AsInt() != int64(i) {
			t.Fatalf("row %d: idx mismatch", i)
		}
		expected := fmt.Sprintf("item_%d", i)
		if row["name"].AsString() != expected {
			t.Fatalf("row %d: name mismatch: got %q, want %q", i, row["name"].AsString(), expected)
		}
	}
}

// ReadRow compatibility

func TestColumnarSpillReadRowCompat(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "readrow")
	if err != nil {
		t.Fatal(err)
	}

	const total = 50
	for i := 0; i < total; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"v": event.IntValue(int64(i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	count := 0
	for {
		row, readErr := sr.ReadRow()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		if row["v"].AsInt() != int64(count) {
			t.Fatalf("row %d: expected v=%d, got %d", count, count, row["v"].AsInt())
		}
		count++
	}

	if count != total {
		t.Fatalf("expected %d rows via ReadRow, got %d", total, count)
	}
}

// ColumnarSpillMerger tests

func TestColumnarSpillMergerTwoRuns(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Write two sorted runs.
	paths := make([]string, 2)
	for run := 0; run < 2; run++ {
		sw, swErr := NewColumnarSpillWriter(mgr, "merge")
		if swErr != nil {
			t.Fatal(swErr)
		}
		start := run * 50
		for i := start; i < start+50; i++ {
			if writeErr := sw.WriteRow(map[string]event.Value{
				"key": event.IntValue(int64(i)),
			}); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		if closeErr := sw.CloseFile(); closeErr != nil {
			t.Fatal(closeErr)
		}
		paths[run] = sw.Path()
	}

	merger, err := NewColumnarSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var result []int64
	for {
		row, mergeErr := merger.Next()
		if mergeErr != nil {
			t.Fatal(mergeErr)
		}
		if row == nil {
			break
		}
		result = append(result, row["key"].AsInt())
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result))
	}
	for i, v := range result {
		if v != int64(i) {
			t.Fatalf("row %d: expected %d, got %d", i, i, v)
		}
	}
}

func TestColumnarSpillMergerManyRuns(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	const numRuns = 16
	const rowsPerRun = 100
	paths := make([]string, numRuns)

	for run := 0; run < numRuns; run++ {
		sw, swErr := NewColumnarSpillWriter(mgr, "merge")
		if swErr != nil {
			t.Fatal(swErr)
		}
		for i := 0; i < rowsPerRun; i++ {
			// Interleaved: each run has run, run+16, run+32, ...
			val := int64(run + i*numRuns)
			if writeErr := sw.WriteRow(map[string]event.Value{
				"key": event.IntValue(val),
			}); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		if closeErr := sw.CloseFile(); closeErr != nil {
			t.Fatal(closeErr)
		}
		paths[run] = sw.Path()
	}

	merger, err := NewColumnarSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var result []int64
	for {
		row, mergeErr := merger.Next()
		if mergeErr != nil {
			t.Fatal(mergeErr)
		}
		if row == nil {
			break
		}
		result = append(result, row["key"].AsInt())
	}

	if len(result) != numRuns*rowsPerRun {
		t.Fatalf("expected %d rows, got %d", numRuns*rowsPerRun, len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i] < result[i-1] {
			t.Fatalf("not sorted at index %d: %d < %d", i, result[i], result[i-1])
		}
	}
}

func TestColumnarSpillMergerNextBatch(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	paths := make([]string, 3)
	for run := 0; run < 3; run++ {
		sw, swErr := NewColumnarSpillWriter(mgr, "merge-batch")
		if swErr != nil {
			t.Fatal(swErr)
		}
		for i := 0; i < 30; i++ {
			val := int64(run + i*3) // interleaved
			if writeErr := sw.WriteRow(map[string]event.Value{
				"key": event.IntValue(val),
			}); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		if closeErr := sw.CloseFile(); closeErr != nil {
			t.Fatal(closeErr)
		}
		paths[run] = sw.Path()
	}

	merger, err := NewColumnarSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var allVals []int64
	for {
		batch, batchErr := merger.NextBatch(16)
		if batchErr != nil {
			t.Fatal(batchErr)
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			allVals = append(allVals, batch.Row(i)["key"].AsInt())
		}
	}

	if len(allVals) != 90 {
		t.Fatalf("expected 90 rows, got %d", len(allVals))
	}
	for i := 1; i < len(allVals); i++ {
		if allVals[i] < allVals[i-1] {
			t.Fatalf("not sorted at index %d: %d < %d", i, allVals[i], allVals[i-1])
		}
	}
}

func TestColumnarSpillMergerDescending(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Two runs, each sorted descending.
	paths := make([]string, 2)
	for run := 0; run < 2; run++ {
		sw, swErr := NewColumnarSpillWriter(mgr, "desc")
		if swErr != nil {
			t.Fatal(swErr)
		}
		// Run 0: 99, 97, 95, ...  Run 1: 98, 96, 94, ...
		for i := 49; i >= 0; i-- {
			val := int64(run + i*2)
			if writeErr := sw.WriteRow(map[string]event.Value{
				"key": event.IntValue(val),
			}); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		if closeErr := sw.CloseFile(); closeErr != nil {
			t.Fatal(closeErr)
		}
		paths[run] = sw.Path()
	}

	merger, err := NewColumnarSpillMerger(paths, []SortField{{Name: "key", Desc: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	var result []int64
	for {
		row, mergeErr := merger.Next()
		if mergeErr != nil {
			t.Fatal(mergeErr)
		}
		if row == nil {
			break
		}
		result = append(result, row["key"].AsInt())
	}

	if len(result) != 100 {
		t.Fatalf("expected 100 rows, got %d", len(result))
	}
	// Should be descending: 99, 98, 97, ..., 0.
	for i := 0; i < len(result); i++ {
		expected := int64(99 - i)
		if result[i] != expected {
			t.Fatalf("row %d: expected %d, got %d", i, expected, result[i])
		}
	}
}

func TestColumnarSpillMergerEmptyRuns(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// 3 files: first empty, second with 5 rows, third empty.
	paths := make([]string, 3)
	for i := 0; i < 3; i++ {
		sw, swErr := NewColumnarSpillWriter(mgr, "empty-merge")
		if swErr != nil {
			t.Fatal(swErr)
		}
		if i == 1 {
			for j := 0; j < 5; j++ {
				if writeErr := sw.WriteRow(map[string]event.Value{
					"key": event.IntValue(int64(j)),
				}); writeErr != nil {
					t.Fatal(writeErr)
				}
			}
		}
		if closeErr := sw.CloseFile(); closeErr != nil {
			t.Fatal(closeErr)
		}
		paths[i] = sw.Path()
	}

	merger, err := NewColumnarSpillMerger(paths, []SortField{{Name: "key", Desc: false}})
	if err != nil {
		t.Fatal(err)
	}
	defer merger.Close()

	count := 0
	for {
		row, mergeErr := merger.Next()
		if mergeErr != nil {
			t.Fatal(mergeErr)
		}
		if row == nil {
			break
		}
		count++
	}
	if count != 5 {
		t.Fatalf("expected 5 rows, got %d", count)
	}
}

// Byte tracking accuracy

func TestColumnarSpillByteTracking(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "bytes")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"key":  event.IntValue(int64(i)),
			"data": event.StringValue(fmt.Sprintf("value_%04d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	// BytesWritten should match actual file size.
	info, statErr := os.Stat(sw.Path())
	if statErr != nil {
		t.Fatal(statErr)
	}

	reported := sw.BytesWritten()
	actual := info.Size()
	if reported != actual {
		t.Fatalf("BytesWritten()=%d but file size=%d", reported, actual)
	}
}

// Writer without SpillManager (nil mgr)

func TestColumnarSpillNilManager(t *testing.T) {
	sw, err := NewColumnarSpillWriter(nil, "nilmgr")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"x": event.IntValue(int64(i)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	path := sw.Path()
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	// File should exist.
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatal("file should exist after CloseFile:", statErr)
	}

	// Read back.
	sr, err := NewColumnarSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, readErr := sr.ReadBatch()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if batch.Len != 5 {
		t.Fatalf("expected 5 rows, got %d", batch.Len)
	}

	// Clean up.
	os.Remove(path)
}

// Rows() accounting

func TestColumnarSpillRowsCount(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "count")
	if err != nil {
		t.Fatal(err)
	}

	// Before any writes.
	if sw.Rows() != 0 {
		t.Fatalf("expected 0 rows initially, got %d", sw.Rows())
	}

	for i := 0; i < 5; i++ {
		if err := sw.WriteRow(map[string]event.Value{"x": event.IntValue(1)}); err != nil {
			t.Fatal(err)
		}
	}
	if sw.Rows() != 5 {
		t.Fatalf("expected 5 rows after write, got %d", sw.Rows())
	}

	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}
	if sw.Rows() != 5 {
		t.Fatalf("expected 5 rows after close, got %d", sw.Rows())
	}
}

// Dictionary encoding with many unique strings → LZ4 fallback

func TestColumnarSpillHighCardinalityStrings(t *testing.T) {
	// If there are too many unique strings (>65536), the dict encoder falls
	// back to LZ4. Verify that round-trip still works.
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "highcard")
	if err != nil {
		t.Fatal(err)
	}

	// columnarBatchSize rows with unique strings ensures the dict encoder
	// may fall back to LZ4 within a single batch.
	const n = columnarBatchSize
	for i := 0; i < n; i++ {
		if err := sw.WriteRow(map[string]event.Value{
			"id": event.StringValue(fmt.Sprintf("unique_value_%06d", i)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, readErr := sr.ReadBatch()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if batch.Len != n {
		t.Fatalf("expected %d rows, got %d", n, batch.Len)
	}

	for i := 0; i < n; i++ {
		expected := fmt.Sprintf("unique_value_%06d", i)
		if got := batch.Row(i)["id"].AsString(); got != expected {
			t.Fatalf("row %d: expected %q, got %q", i, expected, got)
		}
	}
}

// Sparse columns (different rows have different columns)

func TestColumnarSpillSparseColumns(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "sparse")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		row := map[string]event.Value{
			"always": event.IntValue(int64(i)),
		}
		if i%2 == 0 {
			row["even_only"] = event.StringValue("even")
		}
		if i%5 == 0 {
			row["five_only"] = event.FloatValue(float64(i) * 0.1)
		}
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != 30 {
		t.Fatalf("expected 30 rows, got %d", batch.Len)
	}

	for i := 0; i < 30; i++ {
		row := batch.Row(i)

		if row["always"].AsInt() != int64(i) {
			t.Fatalf("row %d: always mismatch", i)
		}

		even := row["even_only"]
		if i%2 == 0 {
			if even.AsString() != "even" {
				t.Fatalf("row %d: expected even_only=even, got %v", i, even)
			}
		} else {
			if !even.IsNull() {
				t.Fatalf("row %d: expected null even_only, got %v", i, even)
			}
		}

		five := row["five_only"]
		if i%5 == 0 {
			expected := float64(i) * 0.1
			if math.Abs(five.AsFloat()-expected) > 1e-9 {
				t.Fatalf("row %d: five_only mismatch: got %f, want %f", i, five.AsFloat(), expected)
			}
		} else if !five.IsNull() {
			t.Fatalf("row %d: expected null five_only, got %v", i, five)
		}
	}
}

// Encoding helper unit tests

func TestBuildNullBitmap(t *testing.T) {
	tests := []struct {
		name     string
		values   []event.Value
		hasNulls bool
		allNull  bool
	}{
		{
			name:     "no nulls",
			values:   []event.Value{event.IntValue(1), event.IntValue(2)},
			hasNulls: false,
			allNull:  false,
		},
		{
			name:     "all nulls",
			values:   []event.Value{{}, {}, {}},
			hasNulls: true,
			allNull:  true,
		},
		{
			name:     "mixed",
			values:   []event.Value{event.IntValue(1), {}, event.IntValue(3)},
			hasNulls: true,
			allNull:  false,
		},
		{
			name:     "single null",
			values:   []event.Value{{}},
			hasNulls: true,
			allNull:  true,
		},
		{
			name:     "single non-null",
			values:   []event.Value{event.StringValue("x")},
			hasNulls: false,
			allNull:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bitmap, hasNulls, allNull := buildNullBitmap(tt.values)
			if hasNulls != tt.hasNulls {
				t.Errorf("hasNulls: got %v, want %v", hasNulls, tt.hasNulls)
			}
			if allNull != tt.allNull {
				t.Errorf("allNull: got %v, want %v", allNull, tt.allNull)
			}

			// Verify each bit.
			for i, v := range tt.values {
				expectedNull := v.IsNull()
				gotNull := isNullBit(bitmap, i)
				if gotNull != expectedNull {
					t.Errorf("position %d: expected null=%v, got %v", i, expectedNull, gotNull)
				}
			}
		})
	}
}

func TestDetectDominantType(t *testing.T) {
	tests := []struct {
		name     string
		values   []event.Value
		expected event.FieldType
	}{
		{
			name:     "all ints",
			values:   []event.Value{event.IntValue(1), event.IntValue(2)},
			expected: event.FieldTypeInt,
		},
		{
			name:     "all floats",
			values:   []event.Value{event.FloatValue(1.0), event.FloatValue(2.0)},
			expected: event.FieldTypeFloat,
		},
		{
			name:     "all strings",
			values:   []event.Value{event.StringValue("a"), event.StringValue("b")},
			expected: event.FieldTypeString,
		},
		{
			name:     "all bools",
			values:   []event.Value{event.BoolValue(true), event.BoolValue(false)},
			expected: event.FieldTypeBool,
		},
		{
			name:     "all nulls",
			values:   []event.Value{{}, {}},
			expected: event.FieldTypeNull,
		},
		{
			name:     "mixed int and string → string",
			values:   []event.Value{event.IntValue(1), event.StringValue("a")},
			expected: event.FieldTypeString,
		},
		{
			name:     "ints with nulls → int",
			values:   []event.Value{event.IntValue(1), {}, event.IntValue(3)},
			expected: event.FieldTypeInt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectDominantType(tt.values)
			if got != tt.expected {
				t.Errorf("got %v, want %v", got, tt.expected)
			}
		})
	}
}

// Benchmarks

func BenchmarkColumnarSpillWrite(b *testing.B) {
	mgr, err := NewSpillManager(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Pre-build rows.
	const numRows = 10000
	rows := make([]map[string]event.Value, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]event.Value{
			"_time":  event.TimestampValue(time.Now().Add(time.Duration(i) * time.Millisecond)),
			"level":  event.StringValue([]string{"INFO", "WARN", "ERROR"}[i%3]),
			"status": event.IntValue(int64(200 + (i%5)*100)),
			"dur":    event.FloatValue(float64(i) * 0.01),
			"host":   event.StringValue(fmt.Sprintf("web-%02d", i%10)),
			"_raw":   event.StringValue(fmt.Sprintf("log line %d with some padding data here for realism", i)),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		sw, _ := NewColumnarSpillWriter(mgr, "bench")
		for _, row := range rows {
			_ = sw.WriteRow(row)
		}
		sw.CloseFile()
		b.SetBytes(sw.BytesWritten())
		mgr.Release(sw.Path())
	}
}

func BenchmarkColumnarSpillRead(b *testing.B) {
	mgr, err := NewSpillManager(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Write a file to read.
	sw, _ := NewColumnarSpillWriter(mgr, "bench-read")
	for i := 0; i < 10000; i++ {
		_ = sw.WriteRow(map[string]event.Value{
			"_time":  event.TimestampValue(time.Now().Add(time.Duration(i) * time.Millisecond)),
			"level":  event.StringValue([]string{"INFO", "WARN", "ERROR"}[i%3]),
			"status": event.IntValue(int64(200 + (i%5)*100)),
			"dur":    event.FloatValue(float64(i) * 0.01),
			"host":   event.StringValue(fmt.Sprintf("web-%02d", i%10)),
			"_raw":   event.StringValue(fmt.Sprintf("log line %d with some padding data here", i)),
		})
	}
	sw.CloseFile()
	path := sw.Path()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		sr, _ := NewColumnarSpillReader(path)
		for {
			_, readErr := sr.ReadBatch()
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
		sr.Close()
	}
}

func BenchmarkColumnarSpillRoundtrip(b *testing.B) {
	mgr, err := NewSpillManager(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.CleanupAll()

	const numRows = 10000
	rows := make([]map[string]event.Value, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]event.Value{
			"key":  event.IntValue(int64(i)),
			"data": event.StringValue(fmt.Sprintf("value_%d", i)),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		sw, _ := NewColumnarSpillWriter(mgr, "bench-rt")
		for _, row := range rows {
			_ = sw.WriteRow(row)
		}
		sw.CloseFile()

		sr, _ := NewColumnarSpillReader(sw.Path())
		for {
			_, readErr := sr.ReadBatch()
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
		sr.Close()
		mgr.Release(sw.Path())
	}
}

func BenchmarkMsgpackSpillWrite(b *testing.B) {
	mgr, err := NewSpillManager(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.CleanupAll()

	const numRows = 10000
	rows := make([]map[string]event.Value, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]event.Value{
			"_time":  event.TimestampValue(time.Now().Add(time.Duration(i) * time.Millisecond)),
			"level":  event.StringValue([]string{"INFO", "WARN", "ERROR"}[i%3]),
			"status": event.IntValue(int64(200 + (i%5)*100)),
			"dur":    event.FloatValue(float64(i) * 0.01),
			"host":   event.StringValue(fmt.Sprintf("web-%02d", i%10)),
			"_raw":   event.StringValue(fmt.Sprintf("log line %d with some padding data here for realism", i)),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		sw, _ := NewManagedSpillWriter(mgr, "bench-msgpack")
		for _, row := range rows {
			_ = sw.WriteRow(row)
		}
		sw.CloseFile()
		mgr.Release(sw.Path())
	}
}

func BenchmarkMsgpackSpillRead(b *testing.B) {
	mgr, err := NewSpillManager(b.TempDir(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, _ := NewManagedSpillWriter(mgr, "bench-read-msgpack")
	for i := 0; i < 10000; i++ {
		_ = sw.WriteRow(map[string]event.Value{
			"_time":  event.TimestampValue(time.Now().Add(time.Duration(i) * time.Millisecond)),
			"level":  event.StringValue([]string{"INFO", "WARN", "ERROR"}[i%3]),
			"status": event.IntValue(int64(200 + (i%5)*100)),
			"dur":    event.FloatValue(float64(i) * 0.01),
			"host":   event.StringValue(fmt.Sprintf("web-%02d", i%10)),
			"_raw":   event.StringValue(fmt.Sprintf("log line %d with some padding data here", i)),
		})
	}
	sw.CloseFile()
	path := sw.Path()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		sr, _ := NewSpillReader(path)
		for {
			_, readErr := sr.ReadRow()
			if readErr != nil {
				break
			}
		}
		sr.Close()
	}
}

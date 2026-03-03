package part

import (
	"context"
	crypto_rand "crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

// Writer writes events as immutable part files with atomic rename.
//
// The write protocol ensures crash safety:
//  1. Write to a temporary file (tmp_<uuid>.lsg) in the partition directory.
//  2. Optionally sync the file to stable storage (controlled by FSync).
//  3. Close the file.
//  4. Rename to the final path (atomic on POSIX filesystems).
//
// If a crash occurs before step 4, the tmp_* file is cleaned up on next
// startup by Registry.ScanDir.
type Writer struct {
	layout       *Layout
	compression  segment.CompressionType
	rowGroupSize int
	fsync        bool // whether to fsync before rename (default: true)
	maxColumns   int  // max user-defined columns per part (0 = unlimited)
}

// WriterOption configures optional Writer behavior.
type WriterOption func(*Writer)

// WithFSync controls whether the writer calls fsync before atomic rename.
// When true (default), data is guaranteed durable after Write returns.
// When false, the OS page cache may buffer writes — faster but data can be
// lost on power failure. Safe for async ingest where HTTP 200 is returned
// before the write completes, and lost data can be re-sent.
func WithFSync(enabled bool) WriterOption {
	return func(w *Writer) {
		w.fsync = enabled
	}
}

// WithMaxColumns limits the number of user-defined columns per part.
// Fields beyond this limit remain searchable via _raw full-text search
// but are not stored as individual columns. This prevents column explosion
// from high-cardinality JSON keys. A value <= 0 disables the cap.
func WithMaxColumns(n int) WriterOption {
	return func(w *Writer) {
		w.maxColumns = n
	}
}

// NewWriter creates a Writer with the given layout and default settings.
func NewWriter(layout *Layout, compression segment.CompressionType, rowGroupSize int, opts ...WriterOption) *Writer {
	if rowGroupSize <= 0 {
		rowGroupSize = DefaultRowGroupSize
	}

	w := &Writer{
		layout:       layout,
		compression:  compression,
		rowGroupSize: rowGroupSize,
		fsync:        true, // safe default: always fsync
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Write writes events to a new part file and returns the metadata.
// Events should be pre-sorted by timestamp within the batch.
// The context is checked for cancellation before the write begins.
func (w *Writer) Write(ctx context.Context, index string, events []*event.Event, level int) (*Meta, error) {
	if len(events) == 0 {
		return nil, fmt.Errorf("part.Writer.Write: no events")
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("part.Writer.Write: %w", err)
	}

	now := time.Now()

	// Compute min/max time from events (don't assume sorted).
	minTime, maxTime := events[0].Time, events[0].Time
	for _, ev := range events[1:] {
		if ev.Time.Before(minTime) {
			minTime = ev.Time
		}
		if ev.Time.After(maxTime) {
			maxTime = ev.Time
		}
	}

	// Determine partition from the minimum timestamp.
	if err := w.layout.EnsurePartitionDir(index, minTime); err != nil {
		return nil, fmt.Errorf("part.Writer.Write: %w", err)
	}

	partDir := w.layout.PartitionDir(index, minTime)
	partitionKey := w.layout.PartitionKey(minTime)

	// Generate filenames.
	finalName := Filename(index, level, now)
	partID := ID(index, level, now)
	finalPath := filepath.Join(partDir, finalName)

	// Create temp file in the same directory (atomic rename requires same filesystem).
	randBytes := make([]byte, 16)
	if _, err := crypto_rand.Read(randBytes); err != nil {
		return nil, fmt.Errorf("part.Writer.Write: generate temp name: %w", err)
	}
	tmpName := "tmp_" + hex.EncodeToString(randBytes) + ".lsg"
	tmpPath := filepath.Join(partDir, tmpName)

	// Write to temp file.
	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("part.Writer.Write: create temp: %w", err)
	}

	sw := segment.NewWriterWithCompression(f, w.compression)
	sw.SetRowGroupSize(w.rowGroupSize)
	if w.maxColumns > 0 {
		sw.SetMaxColumns(w.maxColumns)
	}
	written, err := sw.Write(events)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)

		return nil, fmt.Errorf("part.Writer.Write: encode: %w", err)
	}

	// Sync to stable storage before rename (when fsync is enabled).
	// When fsync is disabled, the OS page cache may buffer writes — faster
	// but data can be lost on power failure. See WithFSync() for rationale.
	if w.fsync {
		if err := f.Sync(); err != nil {
			f.Close()
			os.Remove(tmpPath)

			return nil, fmt.Errorf("part.Writer.Write: sync: %w", err)
		}
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)

		return nil, fmt.Errorf("part.Writer.Write: close: %w", err)
	}

	// Atomic rename: the part becomes visible only after this succeeds.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)

		return nil, fmt.Errorf("part.Writer.Write: rename: %w", err)
	}

	// Extract column names from the written segment.
	columns := extractColumnNames(events)

	meta := &Meta{
		ID:         partID,
		Index:      index,
		MinTime:    minTime,
		MaxTime:    maxTime,
		EventCount: int64(len(events)),
		SizeBytes:  written,
		Level:      level,
		Path:       finalPath,
		CreatedAt:  now,
		Columns:    columns,
		Tier:       "hot",
		Partition:  partitionKey,
	}

	return meta, nil
}

// extractColumnNames collects all unique column names from events.
// This includes built-in columns and user-defined fields.
func extractColumnNames(events []*event.Event) []string {
	// Built-in columns are always present.
	nameSet := map[string]struct{}{
		"_time":       {},
		"_raw":        {},
		"_source":     {},
		"_sourcetype": {},
		"host":        {},
		"index":       {},
	}

	for _, ev := range events {
		for _, name := range ev.FieldNames() {
			nameSet[name] = struct{}{}
		}
	}

	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}

	return names
}

// IsTempFile reports whether the filename is a temporary part file
// (incomplete write from a crash).
func IsTempFile(name string) bool {
	return strings.HasPrefix(name, "tmp_") && strings.HasSuffix(name, ".lsg")
}

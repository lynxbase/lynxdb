package part

import (
	"context"
	crypto_rand "crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

// PartStreamWriter writes events to a part file incrementally, one row group
// at a time, with crash-safe protocol (tmp file → fsync → atomic rename).
//
// This is the streaming counterpart of part.Writer.Write(): instead of
// requiring all events upfront, it accepts bounded batches via WriteRowGroup().
// Memory usage is bounded to O(1 row group) instead of O(all events).
//
// Usage:
//
//	psw, _ := NewPartStreamWriter(layout, "main", 1, opts...)
//	psw.WriteRowGroup(ctx, batch1)
//	psw.WriteRowGroup(ctx, batch2)
//	meta, _ := psw.Finalize(ctx)
//	// On error:
//	psw.Abort()
type PartStreamWriter struct {
	layout       *Layout
	index        string
	level        int
	compression  segment.CompressionType
	rowGroupSize int
	fsync        bool
	maxColumns   int
	logger       *slog.Logger

	// File state.
	tmpPath  string
	file     *os.File
	sw       *segment.StreamWriter
	partDir  string
	partKey  string
	startNow time.Time

	// Accumulated metadata.
	minTime    time.Time
	maxTime    time.Time
	eventCount int64
	columns    map[string]struct{}

	finalized bool
	aborted   bool
}

// NewPartStreamWriter creates a streaming part writer that writes to a temporary
// file in the appropriate partition directory. The partition is determined from
// the first event's timestamp on the first WriteRowGroup call.
//
// The caller must call either Finalize() (on success) or Abort() (on error)
// to clean up resources.
func NewPartStreamWriter(layout *Layout, index string, level int, opts ...WriterOption) (*PartStreamWriter, error) {
	// Apply options via a temporary Writer to reuse option parsing.
	tmp := &Writer{fsync: true}
	for _, opt := range opts {
		opt(tmp)
	}

	return &PartStreamWriter{
		layout:       layout,
		index:        index,
		level:        level,
		compression:  segment.CompressionLZ4,
		rowGroupSize: DefaultRowGroupSize,
		fsync:        tmp.fsync,
		maxColumns:   tmp.maxColumns,
		logger:       tmp.logger,
		columns:      make(map[string]struct{}),
		startNow:     time.Now(),
	}, nil
}

// SetCompression sets the layer-2 compression type.
// Must be called before the first WriteRowGroup().
func (psw *PartStreamWriter) SetCompression(c segment.CompressionType) {
	psw.compression = c
}

// SetRowGroupSize overrides the row group size.
// Must be called before the first WriteRowGroup().
func (psw *PartStreamWriter) SetRowGroupSize(size int) {
	if size > 0 {
		psw.rowGroupSize = size
	}
}

// initFile creates the tmp file and segment stream writer on first use.
// Deferred to the first WriteRowGroup so the partition dir is determined
// from the actual event timestamps.
func (psw *PartStreamWriter) initFile(events []*event.Event) error {
	// Determine partition from the minimum timestamp.
	minTime := events[0].Time
	for _, ev := range events[1:] {
		if ev.Time.Before(minTime) {
			minTime = ev.Time
		}
	}

	if err := psw.layout.EnsurePartitionDir(psw.index, minTime); err != nil {
		return fmt.Errorf("part.PartStreamWriter: %w", err)
	}

	psw.partDir = psw.layout.PartitionDir(psw.index, minTime)
	psw.partKey = psw.layout.PartitionKey(minTime)

	// Create temp file in the same directory (atomic rename requires same filesystem).
	randBytes := make([]byte, 16)
	if _, err := crypto_rand.Read(randBytes); err != nil {
		return fmt.Errorf("part.PartStreamWriter: generate temp name: %w", err)
	}
	tmpName := "tmp_" + hex.EncodeToString(randBytes) + ".lsg"
	psw.tmpPath = filepath.Join(psw.partDir, tmpName)

	f, err := os.Create(psw.tmpPath)
	if err != nil {
		return fmt.Errorf("part.PartStreamWriter: create temp: %w", err)
	}
	psw.file = f

	psw.sw = segment.NewStreamWriter(f, psw.compression)
	psw.sw.SetRowGroupSize(psw.rowGroupSize)
	if psw.maxColumns > 0 {
		psw.sw.SetMaxColumns(psw.maxColumns)
	}

	return nil
}

// WriteRowGroup writes a batch of events as a single row group.
// Events should be sorted by timestamp within the batch.
func (psw *PartStreamWriter) WriteRowGroup(ctx context.Context, events []*event.Event) error {
	if psw.finalized || psw.aborted {
		return fmt.Errorf("part.PartStreamWriter: writer is closed")
	}
	if len(events) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Lazy init on first batch.
	if psw.file == nil {
		if err := psw.initFile(events); err != nil {
			return err
		}
	}

	// Write the row group via the streaming segment writer.
	if err := psw.sw.WriteRowGroup(events); err != nil {
		return fmt.Errorf("part.PartStreamWriter: write row group: %w", err)
	}

	for _, ev := range events {
		if psw.eventCount == 0 || ev.Time.Before(psw.minTime) {
			psw.minTime = ev.Time
		}
		if psw.eventCount == 0 || ev.Time.After(psw.maxTime) {
			psw.maxTime = ev.Time
		}
		psw.eventCount++
	}

	// Track column names.
	for _, b := range []string{"_time", "_raw", "_source", "_sourcetype", "host", "index"} {
		psw.columns[b] = struct{}{}
	}
	for _, ev := range events {
		for _, name := range ev.FieldNames() {
			psw.columns[name] = struct{}{}
		}
	}

	return nil
}

// Finalize completes the segment file (writes inverted index + footer),
// fsyncs, and performs the atomic rename. Returns the part metadata.
func (psw *PartStreamWriter) Finalize(ctx context.Context) (*Meta, error) {
	if psw.finalized || psw.aborted {
		return nil, fmt.Errorf("part.PartStreamWriter: writer is closed")
	}
	psw.finalized = true

	if psw.file == nil || psw.sw == nil {
		return nil, fmt.Errorf("part.PartStreamWriter: no data written")
	}

	if err := ctx.Err(); err != nil {
		psw.cleanup()
		return nil, err
	}

	// Finalize the segment (writes inverted index + footer).
	written, err := psw.sw.Finalize()
	if err != nil {
		psw.cleanup()
		return nil, fmt.Errorf("part.PartStreamWriter: finalize segment: %w", err)
	}

	// Check context after expensive finalize.
	if err := ctx.Err(); err != nil {
		psw.cleanup()
		return nil, err
	}

	// Sync to stable storage before rename.
	if psw.fsync {
		if err := psw.file.Sync(); err != nil {
			psw.cleanup()
			return nil, fmt.Errorf("part.PartStreamWriter: sync: %w", err)
		}
	}

	if err := psw.file.Close(); err != nil {
		os.Remove(psw.tmpPath)
		return nil, fmt.Errorf("part.PartStreamWriter: close: %w", err)
	}
	psw.file = nil // mark as closed

	// Atomic rename.
	now := psw.startNow
	finalName := Filename(psw.index, psw.level, now)
	partID := ID(psw.index, psw.level, now)
	finalPath := filepath.Join(psw.partDir, finalName)

	if err := os.Rename(psw.tmpPath, finalPath); err != nil {
		os.Remove(psw.tmpPath)
		return nil, fmt.Errorf("part.PartStreamWriter: rename: %w", err)
	}
	if err := syncDir(psw.partDir); err != nil {
		os.Remove(finalPath)
		return nil, fmt.Errorf("part.PartStreamWriter: %w", err)
	}

	if psw.logger != nil {
		psw.logger.Debug("streaming part write complete",
			"index", psw.index,
			"level", psw.level,
			"events", psw.eventCount,
			"path", finalPath,
			"size", written,
		)
	}

	columns := make([]string, 0, len(psw.columns))
	for name := range psw.columns {
		columns = append(columns, name)
	}

	return &Meta{
		ID:         partID,
		Index:      psw.index,
		MinTime:    psw.minTime,
		MaxTime:    psw.maxTime,
		EventCount: psw.eventCount,
		SizeBytes:  written,
		Level:      psw.level,
		Path:       finalPath,
		CreatedAt:  now,
		Columns:    columns,
		Tier:       "hot",
		Partition:  psw.partKey,
	}, nil
}

// Abort cleans up the temporary file without completing the write.
// Safe to call multiple times and after Finalize (no-op if already finalized).
func (psw *PartStreamWriter) Abort() error {
	if psw.aborted {
		return nil
	}
	psw.aborted = true

	psw.cleanup()
	return nil
}

// cleanup removes the temp file and closes the file handle.
func (psw *PartStreamWriter) cleanup() {
	if psw.file != nil {
		psw.file.Close()
		psw.file = nil
	}
	if psw.tmpPath != "" {
		os.Remove(psw.tmpPath)
	}
}

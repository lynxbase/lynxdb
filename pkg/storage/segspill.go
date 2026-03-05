package storage

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// SegmentSpiller manages temporary .lsg segment files for large input processing.
// Events are accumulated in an in-memory buffer; when the buffer exceeds a
// byte threshold, it is flushed to a temporary segment file on disk with
// bloom filter and inverted index for query-time skip.
type SegmentSpiller struct {
	tempDir   string
	buffer    []*event.Event
	sizeBytes int64 // estimated byte usage of buffer
	segments  []*SpilledSegment
	threshold int64 // bytes before flush
}

// SpilledSegment holds a mmap'd segment and its cached bloom filter.
type SpilledSegment struct {
	mmap  *segment.MmapSegment
	bloom *index.BloomFilter
	path  string
}

// NewSegmentSpiller creates a spiller that flushes when buffer exceeds threshold bytes.
// The caller MUST call Cleanup() when done (typically via defer) to remove temporary
// files and release mmap'd segments, even if an error occurred during Add/Finalize.
func NewSegmentSpiller(threshold int64) (*SegmentSpiller, error) {
	dir, err := os.MkdirTemp("", "lynxdb-query-")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}

	return &SegmentSpiller{
		tempDir:   dir,
		buffer:    make([]*event.Event, 0, 4096),
		threshold: threshold,
	}, nil
}

// Add adds an event to the spiller. If the buffer exceeds the threshold,
// it flushes to a segment file on disk.
func (s *SegmentSpiller) Add(ev *event.Event) error {
	s.buffer = append(s.buffer, ev)
	s.sizeBytes += estimateEventSize(ev)
	if s.sizeBytes >= s.threshold {
		return s.flush()
	}

	return nil
}

// AddBatch adds multiple events. Flushes if threshold exceeded.
func (s *SegmentSpiller) AddBatch(events []*event.Event) error {
	for _, ev := range events {
		s.buffer = append(s.buffer, ev)
		s.sizeBytes += estimateEventSize(ev)
	}
	if s.sizeBytes >= s.threshold {
		return s.flush()
	}

	return nil
}

func estimateEventSize(ev *event.Event) int64 {
	size := int64(64) // base struct
	size += int64(len(ev.Raw))
	size += int64(len(ev.Source))
	size += int64(len(ev.SourceType))
	size += int64(len(ev.Host))
	size += int64(len(ev.Index))
	for k := range ev.Fields {
		size += int64(len(k)) + 16 // key + value estimate
	}

	return size
}

func (s *SegmentSpiller) flush() error {
	if len(s.buffer) == 0 {
		return nil
	}

	// Sort by timestamp for segment format.
	sort.Slice(s.buffer, func(i, j int) bool {
		return s.buffer[i].Time.Before(s.buffer[j].Time)
	})

	path := filepath.Join(s.tempDir, fmt.Sprintf("seg-%04d.lsg", len(s.segments)))

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create segment: %w", err)
	}

	w := segment.NewWriter(f)
	if _, err := w.Write(s.buffer); err != nil {
		f.Close()

		return fmt.Errorf("write segment: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close segment: %w", err)
	}

	mms, err := segment.OpenSegmentFile(path)
	if err != nil {
		os.Remove(path) // clean up the file we just wrote

		return fmt.Errorf("open segment: %w", err)
	}

	// Extract bloom filter for query-time skip. Bloom is optional —
	// query still works without it, so log a warning on failure.
	bloom, bloomErr := mms.Reader().BloomFilter()
	if bloomErr != nil {
		slog.Warn("segspill: bloom filter extraction failed", "path", path, "error", bloomErr)
	}

	s.segments = append(s.segments, &SpilledSegment{
		mmap:  mms,
		bloom: bloom,
		path:  path,
	})

	s.buffer = s.buffer[:0]
	s.sizeBytes = 0

	return nil
}

// Finalize flushes remaining events to a segment and returns all spilled segments.
// The second return value is always nil (retained for API compatibility).
func (s *SegmentSpiller) Finalize() ([]*SpilledSegment, []*event.Event, error) {
	if len(s.buffer) > 0 {
		if err := s.flush(); err != nil {
			return nil, nil, err
		}
	}

	return s.segments, nil, nil
}

// SegmentCount returns the number of flushed segments.
func (s *SegmentSpiller) SegmentCount() int {
	return len(s.segments)
}

// BufferCount returns events currently buffered (not yet flushed).
func (s *SegmentSpiller) BufferCount() int {
	return len(s.buffer)
}

// Cleanup closes all mmap'd segments and removes the temp directory.
func (s *SegmentSpiller) Cleanup() error {
	var firstErr error
	for _, seg := range s.segments {
		if err := seg.mmap.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.segments = nil
	s.buffer = nil
	if s.tempDir != "" {
		if err := os.RemoveAll(s.tempDir); err != nil {
			if firstErr == nil {
				firstErr = err
			}

			return firstErr
		}
	}

	return firstErr
}

// SpillIterator streams events from spilled segments one segment at a time,
// bounding peak memory to O(one_segment_size) instead of O(total_spilled).
type SpillIterator struct {
	segments    []*SpilledSegment
	searchTerms []string
	idx         int // current segment index
	events      []*event.Event
	pos         int // position within current segment's events
	skipped     int
}

// Iterator returns a streaming iterator over spilled segments. Events are
// read one segment at a time. Segments that cannot contain the search terms
// (per bloom filter) are skipped automatically.
func (s *SegmentSpiller) Iterator(searchTerms []string) *SpillIterator {
	return &SpillIterator{
		segments:    s.segments,
		searchTerms: searchTerms,
	}
}

// Next returns the next event, or nil when all segments are exhausted.
// Returns an error if a segment cannot be read.
func (si *SpillIterator) Next() (*event.Event, error) {
	for {
		// Yield from current segment's events.
		if si.pos < len(si.events) {
			ev := si.events[si.pos]
			si.pos++

			return ev, nil
		}

		// Load next segment.
		if si.idx >= len(si.segments) {
			return nil, nil // exhausted
		}

		seg := si.segments[si.idx]
		si.idx++

		// Bloom filter skip.
		if len(si.searchTerms) > 0 && seg.bloom != nil {
			if !seg.bloom.MayContainAll(si.searchTerms) {
				si.skipped++

				continue
			}
		}

		events, err := seg.mmap.Reader().ReadEvents()
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", seg.path, err)
		}
		si.events = events
		si.pos = 0
	}
}

// Skipped returns the number of segments skipped by bloom filter.
func (si *SpillIterator) Skipped() int {
	return si.skipped
}

// WriteSegmentBytes writes events to segment format in memory and returns the bytes.
func WriteSegmentBytes(events []*event.Event) ([]byte, error) {
	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

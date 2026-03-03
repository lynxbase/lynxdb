package segment

import (
	"fmt"
	"os"
	"sync/atomic"

	mmap "github.com/blevesearch/mmap-go"
)

// MmapSegment wraps a Reader backed by a memory-mapped file.
// The mmap provides the []byte slice that Reader operates on,
// giving zero-copy reads with OS page cache management.
type MmapSegment struct {
	reader *Reader
	mapped mmap.MMap
	file   *os.File
	closed atomic.Bool
}

// OpenSegmentFile opens a .lsg segment file using mmap and returns
// an MmapSegment ready for reading.
func OpenSegmentFile(path string) (*MmapSegment, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment: open file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("segment: stat file: %w", err)
	}

	if fi.Size() == 0 {
		f.Close()

		return nil, fmt.Errorf("segment: file is empty: %s", path)
	}

	mapped, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("segment: mmap: %w", err)
	}

	reader, err := OpenSegment([]byte(mapped))
	if err != nil {
		_ = mapped.Unmap()
		f.Close()

		return nil, fmt.Errorf("segment: open mmap segment: %w", err)
	}

	return &MmapSegment{
		reader: reader,
		mapped: mapped,
		file:   f,
	}, nil
}

// Reader returns the underlying Reader.
func (ms *MmapSegment) Reader() *Reader {
	return ms.reader
}

// Bytes returns the raw mmap'd bytes (for compaction or tiering upload).
func (ms *MmapSegment) Bytes() []byte {
	return []byte(ms.mapped)
}

// Close unmaps the file and closes the file handle. Safe to call multiple times.
func (ms *MmapSegment) Close() error {
	if !ms.closed.CompareAndSwap(false, true) {
		return nil
	}
	var firstErr error
	if ms.mapped != nil {
		if err := ms.mapped.Unmap(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if ms.file != nil {
		if err := ms.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

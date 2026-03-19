package consumers

import (
	"fmt"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/bufmgr"
)

// SegmentCacheKey uniquely identifies a cached segment column chunk.
type SegmentCacheKey struct {
	SegmentID string
	Column    string
	RowGroup  int
}

// segmentCacheEntry tracks the frames holding data for a single cache key.
type segmentCacheEntry struct {
	frames []*bufmgr.Frame
	size   int // total bytes stored across all frames
}

// SegmentCacheConsumer manages segment data frames in the buffer manager.
// It replaces pkg/buffer/SegmentCacheConsumer with the new frame-based API.
//
// Thread-safe. Concurrent queries may access cached segments simultaneously.
type SegmentCacheConsumer struct {
	mu    sync.RWMutex
	mgr   bufmgr.Manager
	index map[SegmentCacheKey]*segmentCacheEntry
}

// NewSegmentCacheConsumer creates a segment cache backed by the buffer manager.
func NewSegmentCacheConsumer(mgr bufmgr.Manager) *SegmentCacheConsumer {
	return &SegmentCacheConsumer{
		mgr:   mgr,
		index: make(map[SegmentCacheKey]*segmentCacheEntry),
	}
}

// Get returns cached segment data for a column/row-group. If the data is cached,
// all frames are pinned before returning. The caller MUST unpin each frame after use.
func (sc *SegmentCacheConsumer) Get(key SegmentCacheKey) ([]*bufmgr.Frame, bool) {
	frames, _, ok := sc.GetWithSize(key)

	return frames, ok
}

// GetWithSize returns cached segment data along with the original data size.
// Frames are pinned before returning.
func (sc *SegmentCacheConsumer) GetWithSize(key SegmentCacheKey) ([]*bufmgr.Frame, int, bool) {
	sc.mu.RLock()
	entry, ok := sc.index[key]
	sc.mu.RUnlock()

	if !ok {
		return nil, 0, false
	}

	// Pin all frames via PinFrameIfOwned to atomically check that the frame
	// is still owned by this cache entry. If any frame was evicted and reused
	// by another consumer, treat the whole entry as a cache miss.
	pinned := make([]*bufmgr.Frame, 0, len(entry.frames))
	for _, f := range entry.frames {
		pf, owned := sc.mgr.PinFrameIfOwned(f.ID, key.SegmentID)
		if !owned {
			// Frame was evicted — unpin any frames we already pinned.
			for _, p := range pinned {
				p.Unpin()
			}
			sc.mu.Lock()
			delete(sc.index, key)
			sc.mu.Unlock()
			return nil, 0, false
		}
		pinned = append(pinned, pf)
	}

	return pinned, entry.size, true
}

// Put stores segment data in the cache via the buffer manager.
func (sc *SegmentCacheConsumer) Put(key SegmentCacheKey, data []byte) error {
	frameSize := sc.mgr.FrameSize()
	numFrames := (len(data) + frameSize - 1) / frameSize

	frames := make([]*bufmgr.Frame, 0, numFrames)
	offset := 0

	for i := 0; i < numFrames; i++ {
		f, err := sc.mgr.AllocFrame(bufmgr.OwnerSegCache, key.SegmentID)
		if err != nil {
			// Free any frames we already allocated.
			for _, allocated := range frames {
				allocated.Unpin()
			}

			return fmt.Errorf("bufmgr.SegmentCacheConsumer.Put: %w", err)
		}

		end := offset + frameSize
		if end > len(data) {
			end = len(data)
		}

		if err := f.WriteAt(data[offset:end], 0); err != nil {
			f.Unpin()
			for _, allocated := range frames {
				allocated.Unpin()
			}

			return fmt.Errorf("bufmgr.SegmentCacheConsumer.Put: write frame: %w", err)
		}

		f.Unpin()
		frames = append(frames, f)
		offset = end
	}

	sc.mu.Lock()
	sc.index[key] = &segmentCacheEntry{
		frames: frames,
		size:   len(data),
	}
	sc.mu.Unlock()

	return nil
}

// Invalidate removes a cache entry.
func (sc *SegmentCacheConsumer) Invalidate(key SegmentCacheKey) {
	sc.mu.Lock()
	_, ok := sc.index[key]
	if ok {
		delete(sc.index, key)
	}
	sc.mu.Unlock()
}

// InvalidateSegment removes all cache entries for a given segment ID.
func (sc *SegmentCacheConsumer) InvalidateSegment(segmentID string) {
	sc.mu.Lock()
	for key := range sc.index {
		if key.SegmentID == segmentID {
			delete(sc.index, key)
		}
	}
	sc.mu.Unlock()
}

// EntryCount returns the number of cached entries.
func (sc *SegmentCacheConsumer) EntryCount() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return len(sc.index)
}

// Clear removes all cache entries.
func (sc *SegmentCacheConsumer) Clear() {
	sc.mu.Lock()
	sc.index = make(map[SegmentCacheKey]*segmentCacheEntry)
	sc.mu.Unlock()
}

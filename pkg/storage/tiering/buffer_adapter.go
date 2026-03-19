package tiering

import (
	"log/slog"

	"github.com/lynxbase/lynxdb/pkg/bufmgr"
	"github.com/lynxbase/lynxdb/pkg/bufmgr/consumers"
)

// Compile-time check: BufferPoolChunkAdapter satisfies ChunkCache.
var _ ChunkCache = (*BufferPoolChunkAdapter)(nil)

// BufferPoolChunkAdapter implements ChunkCache backed by the buffer manager v2.
// On Put, data is stored in frames via consumers.SegmentCacheConsumer. On Get,
// if the frame was evicted, it falls back to the disk-based SegmentCache.
//
// This provides unified eviction: when queries need memory, segment cache frames
// are evicted first (via Clock policy). When queries release memory, segment
// data can reclaim frames.
//
// Thread-safe: both SegmentCacheConsumer and SegmentCache use internal locks.
type BufferPoolChunkAdapter struct {
	segCache  *consumers.SegmentCacheConsumer
	diskCache *SegmentCache // fallback for evicted data
	logger    *slog.Logger
}

// NewBufferPoolChunkAdapter creates an adapter that bridges consumers.SegmentCacheConsumer
// to the ChunkCache interface with disk-based fallback.
func NewBufferPoolChunkAdapter(segCache *consumers.SegmentCacheConsumer, diskCache *SegmentCache, logger *slog.Logger) *BufferPoolChunkAdapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &BufferPoolChunkAdapter{
		segCache:  segCache,
		diskCache: diskCache,
		logger:    logger,
	}
}

// GetChunk returns a cached column chunk. It first checks the frame cache
// (frames may have been evicted), then falls back to the disk cache. On a
// disk hit, the data is promoted back to the frame cache on a best-effort basis.
func (a *BufferPoolChunkAdapter) GetChunk(segmentID string, rgIndex int, column string) ([]byte, bool) {
	key := consumers.SegmentCacheKey{
		SegmentID: segmentID,
		Column:    column,
		RowGroup:  rgIndex,
	}

	// Try frame cache first. GetWithSize returns the original data length
	// so we can truncate the last frame (which may be partially filled).
	frames, dataSize, ok := a.segCache.GetWithSize(key)
	if ok {
		// Reassemble data from frames. Frames are pinned by GetWithSize(); unpin after copy.
		data := assembleFrames(frames, dataSize)
		for _, f := range frames {
			f.Unpin()
		}

		return data, true
	}

	// Fallback: try disk cache.
	data, diskOk := a.diskCache.GetChunk(segmentID, rgIndex, column)
	if !diskOk {
		return nil, false
	}

	// Promote back to frame cache (best-effort — pool may be full).
	if err := a.segCache.Put(key, data); err != nil {
		a.logger.Debug("buffer_adapter: promote to frame cache failed",
			"segment", segmentID, "rg", rgIndex, "column", column, "error", err)
	}

	return data, true
}

// PutChunk stores a column chunk. It writes to the frame cache first, falling
// back to disk-only if the pool is exhausted. The disk cache always receives
// a copy as a durable backup.
func (a *BufferPoolChunkAdapter) PutChunk(segmentID string, rgIndex int, column string, data []byte) {
	key := consumers.SegmentCacheKey{
		SegmentID: segmentID,
		Column:    column,
		RowGroup:  rgIndex,
	}

	// Write to frame cache.
	if err := a.segCache.Put(key, data); err != nil {
		a.logger.Debug("buffer_adapter: frame cache put failed, using disk only",
			"segment", segmentID, "rg", rgIndex, "column", column, "error", err)
	}

	// Always write to disk cache as durable backup.
	a.diskCache.PutChunk(segmentID, rgIndex, column, data)
}

// assembleFrames concatenates data from pinned frames into a single byte slice,
// truncating to totalSize bytes. Frames are fixed-size (e.g. 64KB), so the last
// frame is typically partially filled — totalSize ensures we return only the
// actual data without trailing zeros.
func assembleFrames(frames []*bufmgr.Frame, totalSize int) []byte {
	if len(frames) == 0 || totalSize <= 0 {
		return nil
	}

	data := make([]byte, 0, totalSize)
	remaining := totalSize

	for _, f := range frames {
		src := f.DataSlice()
		if len(src) > remaining {
			src = src[:remaining]
		}
		data = append(data, src...)
		remaining -= len(src)

		if remaining <= 0 {
			break
		}
	}

	return data
}

package tiering

import (
	"log/slog"

	"github.com/OrlovEvgeny/Lynxdb/pkg/buffer"
)

// Compile-time check: BufferPoolChunkAdapter satisfies ChunkCache.
var _ ChunkCache = (*BufferPoolChunkAdapter)(nil)

// BufferPoolChunkAdapter implements ChunkCache backed by the unified buffer pool.
// On Put, data is stored in pool pages via SegmentCacheConsumer. On Get, if the
// page was evicted, it falls back to the disk-based SegmentCache.
//
// This provides unified eviction: when queries need memory, segment cache pages
// are evicted first (via Clock policy). When queries release memory, segment
// data can reclaim pool pages.
//
// Thread-safe: both SegmentCacheConsumer and SegmentCache use internal locks.
type BufferPoolChunkAdapter struct {
	poolCache *buffer.SegmentCacheConsumer
	diskCache *SegmentCache // fallback for evicted data
	logger    *slog.Logger
}

// NewBufferPoolChunkAdapter creates an adapter that bridges buffer.SegmentCacheConsumer
// to the ChunkCache interface with disk-based fallback.
func NewBufferPoolChunkAdapter(poolCache *buffer.SegmentCacheConsumer, diskCache *SegmentCache, logger *slog.Logger) *BufferPoolChunkAdapter {
	if logger == nil {
		logger = slog.Default()
	}

	return &BufferPoolChunkAdapter{
		poolCache: poolCache,
		diskCache: diskCache,
		logger:    logger,
	}
}

// GetChunk returns a cached column chunk. It first checks the pool cache (pages
// may have been evicted), then falls back to the disk cache. On a disk hit, the
// data is promoted back to the pool cache on a best-effort basis.
func (a *BufferPoolChunkAdapter) GetChunk(segmentID string, rgIndex int, column string) ([]byte, bool) {
	key := buffer.SegmentCacheKey{
		SegmentID: segmentID,
		Column:    column,
		RowGroup:  rgIndex,
	}

	// Try pool cache first. GetWithSize returns the original data length
	// so we can truncate the last page (which may be partially filled).
	pages, dataSize, ok := a.poolCache.GetWithSize(key)
	if ok {
		// Reassemble data from pages. Pages are pinned by GetWithSize(); unpin after copy.
		data := assemblePages(pages, dataSize)
		for _, p := range pages {
			p.Unpin()
		}

		return data, true
	}

	// Fallback: try disk cache.
	data, diskOk := a.diskCache.GetChunk(segmentID, rgIndex, column)
	if !diskOk {
		return nil, false
	}

	// Promote back to pool cache (best-effort — pool may be full).
	if err := a.poolCache.Put(key, data); err != nil {
		a.logger.Debug("buffer_adapter: promote to pool failed",
			"segment", segmentID, "rg", rgIndex, "column", column, "error", err)
	}

	return data, true
}

// PutChunk stores a column chunk. It writes to the pool cache first, falling
// back to disk-only if the pool is exhausted. The disk cache always receives
// a copy as a durable backup.
func (a *BufferPoolChunkAdapter) PutChunk(segmentID string, rgIndex int, column string, data []byte) {
	key := buffer.SegmentCacheKey{
		SegmentID: segmentID,
		Column:    column,
		RowGroup:  rgIndex,
	}

	// Write to pool cache.
	if err := a.poolCache.Put(key, data); err != nil {
		a.logger.Debug("buffer_adapter: pool put failed, using disk only",
			"segment", segmentID, "rg", rgIndex, "column", column, "error", err)
	}

	// Always write to disk cache as durable backup.
	a.diskCache.PutChunk(segmentID, rgIndex, column, data)
}

// assemblePages concatenates data from pinned pages into a single byte slice,
// truncating to totalSize bytes. Pages are fixed-size (e.g. 64KB), so the last
// page is typically partially filled — totalSize ensures we return only the
// actual data without trailing zeros.
func assemblePages(pages []*buffer.Page, totalSize int) []byte {
	if len(pages) == 0 || totalSize <= 0 {
		return nil
	}

	data := make([]byte, 0, totalSize)
	remaining := totalSize

	for _, p := range pages {
		src := p.DataSlice()
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

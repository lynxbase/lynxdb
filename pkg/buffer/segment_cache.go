package buffer

import (
	"fmt"
	"sync"
)

// SegmentCacheKey uniquely identifies a cached segment column chunk.
type SegmentCacheKey struct {
	SegmentID string
	Column    string
	RowGroup  int
}

// segmentCacheEntry tracks the pages holding data for a single cache key.
type segmentCacheEntry struct {
	pages []*Page
	size  int // total bytes stored across all pages
}

// SegmentCacheConsumer manages segment data pages in the buffer pool.
// It replaces the separate LRU cache with a pool-integrated approach where
// cached segment data lives in buffer pool pages alongside query and memtable
// data. The cache no longer has a separate size limit — its effective size is
// determined dynamically by the buffer pool eviction policy.
//
// Thread-safe. Concurrent queries may access cached segments simultaneously.
type SegmentCacheConsumer struct {
	mu    sync.RWMutex
	pool  *Pool
	index map[SegmentCacheKey]*segmentCacheEntry
}

// NewSegmentCacheConsumer creates a segment cache backed by the buffer pool.
func NewSegmentCacheConsumer(pool *Pool) *SegmentCacheConsumer {
	return &SegmentCacheConsumer{
		pool:  pool,
		index: make(map[SegmentCacheKey]*segmentCacheEntry),
	}
}

// Get returns cached segment data for a column/row-group. If the data is cached,
// all pages are pinned before returning. The caller MUST unpin each page after use
// (defer page.Unpin()). Returns nil, false on cache miss.
//
// If a page was evicted by the buffer pool since it was cached, the entry is
// treated as a miss and removed from the index.
func (sc *SegmentCacheConsumer) Get(key SegmentCacheKey) ([]*Page, bool) {
	pages, _, ok := sc.GetWithSize(key)

	return pages, ok
}

// GetWithSize returns cached segment data along with the original data size in
// bytes. Pages are pinned before returning; the caller MUST unpin each page
// after use. The size is needed because the last page may be partially filled —
// callers should only read `size` bytes from the assembled page data.
//
// Returns (nil, 0, false) on cache miss.
func (sc *SegmentCacheConsumer) GetWithSize(key SegmentCacheKey) ([]*Page, int, bool) {
	sc.mu.RLock()
	entry, ok := sc.index[key]
	sc.mu.RUnlock()

	if !ok {
		return nil, 0, false
	}

	// Pin all pages. If any page was evicted (data is nil), treat as miss.
	for _, p := range entry.pages {
		if p.data == nil {
			// Page was evicted — invalidate this cache entry.
			sc.mu.Lock()
			delete(sc.index, key)
			sc.mu.Unlock()

			return nil, 0, false
		}
		p.Pin()
	}

	return entry.pages, entry.size, true
}

// Put stores segment data in the cache via the buffer pool. The data is split
// across one or more pages. If the pool is full, cold pages from ANY consumer
// are evicted (including other cached segments).
//
// Returns an error only if the pool cannot allocate pages (all pinned).
func (sc *SegmentCacheConsumer) Put(key SegmentCacheKey, data []byte) error {
	pageSize := sc.pool.PageSize()
	numPages := (len(data) + pageSize - 1) / pageSize

	pages := make([]*Page, 0, numPages)
	offset := 0

	for i := 0; i < numPages; i++ {
		p, err := sc.pool.AllocPage(OwnerSegmentCache, key.SegmentID)
		if err != nil {
			// Failed to allocate — free any pages we already allocated.
			for _, allocated := range pages {
				allocated.Unpin()
				sc.pool.FreePage(allocated)
			}

			return fmt.Errorf("buffer.SegmentCacheConsumer.Put: %w", err)
		}

		end := offset + pageSize
		if end > len(data) {
			end = len(data)
		}

		if err := p.WriteAt(data[offset:end], 0); err != nil {
			// Should not happen since we respect page size, but handle defensively.
			p.Unpin()
			sc.pool.FreePage(p)
			for _, allocated := range pages {
				allocated.Unpin()
				sc.pool.FreePage(allocated)
			}

			return fmt.Errorf("buffer.SegmentCacheConsumer.Put: write page: %w", err)
		}

		// Unpin the page — it's now an eviction candidate. Readers will pin
		// it when they call Get().
		p.Unpin()
		pages = append(pages, p)
		offset = end
	}

	sc.mu.Lock()
	// If there's an existing entry, free its pages first.
	if existing, ok := sc.index[key]; ok {
		for _, p := range existing.pages {
			sc.pool.FreePage(p)
		}
	}
	sc.index[key] = &segmentCacheEntry{
		pages: pages,
		size:  len(data),
	}
	sc.mu.Unlock()

	return nil
}

// Invalidate removes a cache entry and frees its pages.
func (sc *SegmentCacheConsumer) Invalidate(key SegmentCacheKey) {
	sc.mu.Lock()
	entry, ok := sc.index[key]
	if ok {
		delete(sc.index, key)
	}
	sc.mu.Unlock()

	if ok {
		for _, p := range entry.pages {
			sc.pool.FreePage(p)
		}
	}
}

// InvalidateSegment removes all cache entries for a given segment ID.
func (sc *SegmentCacheConsumer) InvalidateSegment(segmentID string) {
	sc.mu.Lock()
	var toFree []*segmentCacheEntry
	for key, entry := range sc.index {
		if key.SegmentID == segmentID {
			toFree = append(toFree, entry)
			delete(sc.index, key)
		}
	}
	sc.mu.Unlock()

	for _, entry := range toFree {
		for _, p := range entry.pages {
			sc.pool.FreePage(p)
		}
	}
}

// EntryCount returns the number of cached entries.
func (sc *SegmentCacheConsumer) EntryCount() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return len(sc.index)
}

// Clear removes all cache entries and frees their pages.
func (sc *SegmentCacheConsumer) Clear() {
	sc.mu.Lock()
	entries := sc.index
	sc.index = make(map[SegmentCacheKey]*segmentCacheEntry)
	sc.mu.Unlock()

	for _, entry := range entries {
		for _, p := range entry.pages {
			sc.pool.FreePage(p)
		}
	}
}

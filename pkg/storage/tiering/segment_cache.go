package tiering

import (
	"container/list"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

// knownDirs tracks directories already created to avoid os.MkdirAll syscalls on every write.
var knownDirs sync.Map

const (
	defaultMaxCacheBytes = 10 << 30 // 10GB local disk cache
	defaultMaxFooters    = 10000    // ~10KB per footer → ~100MB max
)

// ChunkCache abstracts column chunk storage for remote segment data.
// The default implementation (SegmentCache) uses a disk-based LRU cache.
// When the buffer pool is enabled, BufferPoolChunkAdapter provides a
// pool-backed implementation with unified eviction across cache, queries,
// and memtable data.
type ChunkCache interface {
	// GetChunk returns a cached column chunk, or (nil, false) on cache miss.
	GetChunk(segmentID string, rgIndex int, column string) ([]byte, bool)
	// PutChunk stores a column chunk in the cache.
	PutChunk(segmentID string, rgIndex int, column string, data []byte)
}

// Compile-time check: SegmentCache satisfies ChunkCache.
var _ ChunkCache = (*SegmentCache)(nil)

// SegmentCache provides a two-layer cache for remote segment data:
//   - In-memory: parsed Footer + bloom filter per segment (~10KB each)
//   - On-disk: column chunks in segment-cache/{segment-id}/rg{N}.{column}.chunk
//
// Both layers use O(1) LRU eviction via doubly-linked lists.
type SegmentCache struct {
	mu sync.RWMutex

	// In-memory footer cache (LRU).
	footerMap  map[string]*list.Element // key -> list element
	footerLRU  *list.List               // front = most recent, back = oldest
	maxFooters int

	// Disk chunk cache (LRU).
	dir          string
	chunkMap     map[string]*list.Element // key -> list element
	chunkLRU     *list.List               // front = most recent, back = oldest
	diskBytes    int64
	maxDiskBytes int64
}

type footerEntry struct {
	key    string
	footer *segment.Footer
}

type chunkEntry struct {
	key       string // segmentID/rgN/column
	path      string
	sizeBytes int64
}

// SegmentCacheConfig configures the segment cache limits.
type SegmentCacheConfig struct {
	MaxDiskBytes int64 // max disk usage for chunk cache (default 10GB)
	MaxFooters   int   // max in-memory footer entries (default 10000)
}

// NewSegmentCache creates a segment cache with the given disk directory and config.
// Zero-value config fields use production defaults.
func NewSegmentCache(dir string, maxDiskBytes int64) *SegmentCache {
	return NewSegmentCacheWithConfig(dir, SegmentCacheConfig{MaxDiskBytes: maxDiskBytes})
}

// NewSegmentCacheWithConfig creates a segment cache with full configuration control.
func NewSegmentCacheWithConfig(dir string, cfg SegmentCacheConfig) *SegmentCache {
	if cfg.MaxDiskBytes <= 0 {
		cfg.MaxDiskBytes = defaultMaxCacheBytes
	}
	if cfg.MaxFooters <= 0 {
		cfg.MaxFooters = defaultMaxFooters
	}
	sc := &SegmentCache{
		footerMap:    make(map[string]*list.Element),
		footerLRU:    list.New(),
		maxFooters:   cfg.MaxFooters,
		chunkMap:     make(map[string]*list.Element),
		chunkLRU:     list.New(),
		dir:          dir,
		maxDiskBytes: cfg.MaxDiskBytes,
	}
	if dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}

	return sc
}

// GetFooter returns the cached footer for the given object key, or nil.
func (sc *SegmentCache) GetFooter(objectKey string) *segment.Footer {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	elem, ok := sc.footerMap[objectKey]
	if !ok {
		return nil
	}
	sc.footerLRU.MoveToFront(elem)

	return elem.Value.(*footerEntry).footer
}

// PutFooter caches a parsed footer for the given object key.
func (sc *SegmentCache) PutFooter(objectKey string, footer *segment.Footer) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if elem, ok := sc.footerMap[objectKey]; ok {
		elem.Value.(*footerEntry).footer = footer
		sc.footerLRU.MoveToFront(elem)

		return
	}

	entry := &footerEntry{key: objectKey, footer: footer}
	elem := sc.footerLRU.PushFront(entry)
	sc.footerMap[objectKey] = elem

	// Evict oldest if over limit (O(1) — remove from back).
	for len(sc.footerMap) > sc.maxFooters {
		back := sc.footerLRU.Back()
		if back == nil {
			break
		}
		evicted := sc.footerLRU.Remove(back).(*footerEntry)
		delete(sc.footerMap, evicted.key)
	}
}

// FooterCount returns the number of cached footers.
func (sc *SegmentCache) FooterCount() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return len(sc.footerMap)
}

// GetChunk returns a cached column chunk from disk.
func (sc *SegmentCache) GetChunk(segmentID string, rgIndex int, column string) ([]byte, bool) {
	key := chunkKey(segmentID, rgIndex, column)

	sc.mu.Lock()
	elem, ok := sc.chunkMap[key]
	if !ok {
		sc.mu.Unlock()

		return nil, false
	}
	entry := elem.Value.(*chunkEntry)
	path := entry.path
	sc.chunkLRU.MoveToFront(elem)
	sc.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		// File was deleted externally; remove from index.
		sc.mu.Lock()
		if elem, ok := sc.chunkMap[key]; ok {
			ce := sc.chunkLRU.Remove(elem).(*chunkEntry)
			sc.diskBytes -= ce.sizeBytes
			delete(sc.chunkMap, key)
		}
		sc.mu.Unlock()

		return nil, false
	}

	return data, true
}

// PutChunk writes a column chunk to disk cache and tracks it.
func (sc *SegmentCache) PutChunk(segmentID string, rgIndex int, column string, data []byte) {
	if sc.dir == "" {
		return
	}

	key := chunkKey(segmentID, rgIndex, column)
	path := sc.chunkPath(segmentID, rgIndex, column)

	dir := filepath.Dir(path)
	if _, ok := knownDirs.Load(dir); !ok {
		_ = os.MkdirAll(dir, 0o755)
		knownDirs.Store(dir, struct{}{})
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		slog.Warn("segment_cache: write chunk failed", "path", path, "error", err)

		return
	}

	size := int64(len(data))

	sc.mu.Lock()
	if elem, ok := sc.chunkMap[key]; ok {
		old := elem.Value.(*chunkEntry)
		sc.diskBytes -= old.sizeBytes
		old.sizeBytes = size
		old.path = path
		sc.chunkLRU.MoveToFront(elem)
	} else {
		entry := &chunkEntry{key: key, path: path, sizeBytes: size}
		elem := sc.chunkLRU.PushFront(entry)
		sc.chunkMap[key] = elem
	}
	sc.diskBytes += size

	// Evict oldest chunks until under budget (O(1) per eviction — remove from back).
	for sc.diskBytes > sc.maxDiskBytes {
		back := sc.chunkLRU.Back()
		if back == nil {
			break
		}
		evicted := sc.chunkLRU.Remove(back).(*chunkEntry)
		delete(sc.chunkMap, evicted.key)
		os.Remove(evicted.path)
		sc.diskBytes -= evicted.sizeBytes
	}
	sc.mu.Unlock()
}

// DiskBytes returns the total disk cache usage.
func (sc *SegmentCache) DiskBytes() int64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return sc.diskBytes
}

// ChunkCount returns the number of cached chunks.
func (sc *SegmentCache) ChunkCount() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	return len(sc.chunkMap)
}

// Clear removes all cached data.
func (sc *SegmentCache) Clear() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.footerMap = make(map[string]*list.Element)
	sc.footerLRU.Init()

	for _, elem := range sc.chunkMap {
		ce := elem.Value.(*chunkEntry)
		os.Remove(ce.path)
	}
	sc.chunkMap = make(map[string]*list.Element)
	sc.chunkLRU.Init()
	sc.diskBytes = 0
}

func chunkKey(segmentID string, rgIndex int, column string) string {
	return fmt.Sprintf("%s/rg%d/%s", segmentID, rgIndex, column)
}

func (sc *SegmentCache) chunkPath(segmentID string, rgIndex int, column string) string {
	return filepath.Join(sc.dir, segmentID, fmt.Sprintf("rg%d.%s.chunk", rgIndex, column))
}

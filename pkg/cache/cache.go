package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Key uniquely identifies a cached result.
type Key struct {
	IndexName    string
	SegmentID    string
	SegmentCRC32 uint32
	QueryHash    uint64
	TimeRange    [2]int64 // [earliest, latest] in nanoseconds
}

// Bytes returns deterministic serialization for the cache key.
func (k Key) Bytes() []byte {
	s := fmt.Sprintf("%s|%s|%08x|%016x|%d-%d",
		k.IndexName, k.SegmentID, k.SegmentCRC32, k.QueryHash,
		k.TimeRange[0], k.TimeRange[1])
	h := sha256.Sum256([]byte(s))

	return h[:]
}

// Hex returns the hex string of the key hash (for filenames).
func (k Key) Hex() string {
	return hex.EncodeToString(k.Bytes()[:16])
}

// HashQuery normalizes and hashes a query string.
func HashQuery(query string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(query))

	return h.Sum64()
}

// CachedBatch is a serializable batch of columnar results.
type CachedBatch struct {
	Columns map[string][]CachedValue
	Len     int
}

// CachedValue is a serializable event.Value.
type CachedValue struct {
	Type uint8
	Str  string
	Num  int64
	Flt  float64
}

// CachedResult holds cached query results.
type CachedResult struct {
	Batches    []CachedBatch
	CreatedAt  time.Time
	AccessedAt time.Time
	SizeBytes  int64
}

// Stats tracks cache performance.
type Stats struct {
	Hits       int64
	Misses     int64
	Evictions  int64
	SizeBytes  int64
	EntryCount int64
	HitRate    float64
}

// PoolReserver abstracts the UnifiedPool interface for cache integration.
// The cache calls ReserveForCache when inserting entries and ReleaseCache
// when evicting them. This allows elastic sharing with query execution.
type PoolReserver interface {
	ReserveForCache(n int64) error
	ReleaseCache(n int64)
}

// Store is an in-memory + optional filesystem cache with CLOCK eviction.
type Store struct {
	mu          sync.RWMutex
	entries     map[string]*cacheEntry
	clock       *clockBuffer
	dir         string
	maxBytes    int64
	ttl         time.Duration
	hits        atomic.Int64
	misses      atomic.Int64
	evictions   atomic.Int64
	currentSize int64 // running total of entry sizes, updated under mu

	// pool is the optional unified memory pool. When non-nil, cache
	// insertions call ReserveForCache and evictions call ReleaseCache.
	// Set via SetPool after construction to avoid import cycles.
	pool PoolReserver
}

type cacheEntry struct {
	result    *CachedResult
	key       string
	segmentID string // segment ID for granular invalidation
	sizeBytes int64
}

// NewStore creates a new cache store with CLOCK eviction.
func NewStore(dir string, maxBytes int64, ttl time.Duration) *Store {
	if maxBytes <= 0 {
		maxBytes = 1 << 30 // 1GB default
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	cs := &Store{
		entries:  make(map[string]*cacheEntry),
		clock:    newClockBuffer(16384), // 16K slots
		dir:      dir,
		maxBytes: maxBytes,
		ttl:      ttl,
	}
	// Load existing cache entries from disk
	if dir != "" {
		cs.loadFromDisk()
	}

	return cs
}

// SetPool sets the unified memory pool for elastic sharing. When set, cache
// insertions reserve memory from the pool and evictions release it back.
// Must be called before any Get/Put operations (typically during server startup).
func (cs *Store) SetPool(pool PoolReserver) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.pool = pool
}

// Get retrieves a cached result by key. Returns nil on miss.
func (cs *Store) Get(ctx context.Context, key Key) (*CachedResult, error) {
	hex := key.Hex()
	cs.mu.RLock()
	entry, ok := cs.entries[hex]
	cs.mu.RUnlock()

	if !ok {
		cs.misses.Add(1)

		return nil, nil
	}
	// Check TTL
	if time.Since(entry.result.CreatedAt) > cs.ttl {
		cs.mu.Lock()
		cs.removeEntryLocked(hex, entry)
		cs.mu.Unlock()
		cs.misses.Add(1)

		return nil, nil
	}
	// Mark as recently accessed in CLOCK.
	cs.mu.Lock()
	cs.clock.access(hex)
	entry.result.AccessedAt = time.Now()
	cs.mu.Unlock()

	cs.hits.Add(1)

	return entry.result, nil
}

// Put stores a result in the cache. When a unified pool is configured, it
// reserves memory from the pool before inserting. If the pool has no room
// (queries are using the space), the entry is silently dropped.
func (cs *Store) Put(ctx context.Context, key Key, result *CachedResult) error {
	hex := key.Hex()
	result.CreatedAt = time.Now()
	result.AccessedAt = result.CreatedAt

	size := estimateSize(result)
	result.SizeBytes = size

	cs.mu.Lock()

	// Track net size change for pool accounting.
	var existingSize int64
	if existing, ok := cs.entries[hex]; ok {
		existingSize = existing.sizeBytes
		cs.currentSize -= existing.sizeBytes
	}

	// If pool is set, reserve the net increase from the unified pool.
	if cs.pool != nil {
		netIncrease := size - existingSize
		if netIncrease > 0 {
			if err := cs.pool.ReserveForCache(netIncrease); err != nil {
				// Pool has no room — drop the entry silently.
				// Restore currentSize if we subtracted existing.
				cs.currentSize += existingSize
				cs.mu.Unlock()
				slog.Debug("cache: insert skipped, pool full",
					"key", hex, "size_bytes", size)

				return nil //nolint:nilerr // pool full, silently skip insert
			}
		} else if netIncrease < 0 {
			// New entry is smaller — release the difference.
			cs.pool.ReleaseCache(-netIncrease)
		}
	}

	cs.entries[hex] = &cacheEntry{
		result:    result,
		key:       hex,
		segmentID: key.SegmentID,
		sizeBytes: size,
	}
	cs.currentSize += size
	evicted := cs.clock.insert(hex)
	if evicted != "" {
		if evictedEntry, ok := cs.entries[evicted]; ok {
			cs.currentSize -= evictedEntry.sizeBytes
			if cs.pool != nil {
				cs.pool.ReleaseCache(evictedEntry.sizeBytes)
			}
		}
		delete(cs.entries, evicted)
		cs.evictions.Add(1)
		if cs.dir != "" {
			os.Remove(cs.diskPath(evicted))
		}
	}
	cs.mu.Unlock()

	// Evict if over max size
	cs.evictIfNeeded()

	// Persist to disk
	if cs.dir != "" {
		cs.writeToDisk(hex, result)
	}

	return nil
}

// EvictBytes evicts LRU entries until at least bytesNeeded bytes have been freed,
// or no more entries remain. Returns the total bytes actually freed.
//
// Called from the UnifiedPool's cacheEvictor callback. The caller holds its own
// mutex; this method acquires the cache's mu lock separately (no deadlock). Must
// NOT call back into the UnifiedPool — the pool adjusts cacheAllocated after
// the evictor returns.
func (cs *Store) EvictBytes(bytesNeeded int64) int64 {
	if bytesNeeded <= 0 {
		return 0
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	var freed int64
	for freed < bytesNeeded {
		evictedKey := cs.clock.evict()
		if evictedKey == "" {
			break
		}
		if entry, ok := cs.entries[evictedKey]; ok {
			freed += entry.sizeBytes
			cs.currentSize -= entry.sizeBytes
			// Do NOT call cs.pool.ReleaseCache here — this method is called
			// from the UnifiedPool's evictor, which adjusts cacheAllocated
			// itself based on the returned freed value.
		}
		delete(cs.entries, evictedKey)
		cs.evictions.Add(1)
		if cs.dir != "" {
			os.Remove(cs.diskPath(evictedKey))
		}
	}

	return freed
}

// OnFlush invalidates cache entries that belong to the memtable (no segment ID)
// while preserving entries for existing segments. This replaces Clear() on flush.
func (cs *Store) OnFlush(newSegmentIDs []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// Invalidate entries with empty segment ID (memtable results)
	// and entries whose segment ID matches a new segment (stale results).
	newIDs := make(map[string]bool, len(newSegmentIDs))
	for _, id := range newSegmentIDs {
		newIDs[id] = true
	}

	for hex, entry := range cs.entries {
		if entry.segmentID != "" && !newIDs[entry.segmentID] {
			continue
		}
		cs.removeEntryLocked(hex, entry)
	}
}

// OnCompaction invalidates cache entries for segments that were removed
// during compaction. Entries for new (merged) segments are not affected.
func (cs *Store) OnCompaction(removedIDs, addedIDs []string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	removed := make(map[string]bool, len(removedIDs))
	for _, id := range removedIDs {
		removed[id] = true
	}

	for hex, entry := range cs.entries {
		if !removed[entry.segmentID] {
			continue
		}
		cs.removeEntryLocked(hex, entry)
	}
}

// InvalidateSegment removes all cache entries for a specific segment ID.
func (cs *Store) InvalidateSegment(segmentID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for hex, entry := range cs.entries {
		if entry.segmentID == segmentID {
			cs.removeEntryLocked(hex, entry)
		}
	}
}

// Stats returns cache performance statistics.
func (cs *Store) Stats() Stats {
	cs.mu.RLock()
	totalSize := cs.currentSize
	entryCount := int64(len(cs.entries))
	cs.mu.RUnlock()

	hits := cs.hits.Load()
	misses := cs.misses.Load()
	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return Stats{
		Hits:       hits,
		Misses:     misses,
		Evictions:  cs.evictions.Load(),
		SizeBytes:  totalSize,
		EntryCount: entryCount,
		HitRate:    hitRate,
	}
}

// Clear removes all cache entries.
func (cs *Store) Clear() error {
	cs.mu.Lock()

	// Release all cache memory from pool before clearing.
	if cs.pool != nil && cs.currentSize > 0 {
		cs.pool.ReleaseCache(cs.currentSize)
	}

	cs.entries = make(map[string]*cacheEntry)
	cs.currentSize = 0
	cs.clock.clear()
	cs.mu.Unlock()

	if cs.dir != "" {
		_ = os.RemoveAll(cs.dir)
		_ = os.MkdirAll(cs.dir, 0o755)
	}

	return nil
}

// removeEntryLocked removes a cache entry and releases its memory from the pool.
// Must be called with cs.mu held.
func (cs *Store) removeEntryLocked(hex string, entry *cacheEntry) {
	cs.currentSize -= entry.sizeBytes
	if cs.pool != nil {
		cs.pool.ReleaseCache(entry.sizeBytes)
	}
	delete(cs.entries, hex)
	cs.clock.remove(hex)
	cs.evictions.Add(1)
	if cs.dir != "" {
		os.Remove(cs.diskPath(hex))
	}
}

func (cs *Store) evictIfNeeded() {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for cs.totalSize() > cs.maxBytes {
		evictedKey := cs.clock.evict()
		if evictedKey == "" {
			break
		}
		if entry, ok := cs.entries[evictedKey]; ok {
			cs.currentSize -= entry.sizeBytes
			if cs.pool != nil {
				cs.pool.ReleaseCache(entry.sizeBytes)
			}
		}
		delete(cs.entries, evictedKey)
		cs.evictions.Add(1)
		if cs.dir != "" {
			os.Remove(cs.diskPath(evictedKey))
		}
	}
}

func (cs *Store) totalSize() int64 {
	return cs.currentSize
}

func (cs *Store) diskPath(hex string) string {
	prefix := hex[:2]

	return filepath.Join(cs.dir, prefix, hex)
}

func (cs *Store) writeToDisk(hex string, result *CachedResult) {
	path := cs.diskPath(hex)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		slog.Debug("cache: mkdir failed", "path", filepath.Dir(path), "error", err)

		return
	}
	data, err := msgpack.Marshal(result)
	if err != nil {
		slog.Debug("cache: marshal failed", "error", err)

		return
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		slog.Debug("cache: write failed", "path", path, "error", err)
	}
}

func (cs *Store) loadFromDisk() {
	_ = filepath.Walk(cs.dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			slog.Debug("cache: skipping unreadable file", "path", path, "error", err)

			return nil //nolint:nilerr // skip unreadable cache files
		}
		var result CachedResult
		if err := msgpack.Unmarshal(data, &result); err != nil {
			slog.Debug("cache: skipping corrupt file", "path", path, "error", err)

			return nil //nolint:nilerr // skip corrupt cache files
		}
		hex := filepath.Base(path)
		// Extract segment ID from the filename prefix pattern.
		segID := ""
		if idx := strings.Index(hex, "|"); idx > 0 {
			segID = hex[:idx]
		}
		cs.entries[hex] = &cacheEntry{
			result:    &result,
			key:       hex,
			segmentID: segID,
			sizeBytes: result.SizeBytes,
		}
		cs.currentSize += result.SizeBytes
		cs.clock.insert(hex)

		return nil
	})
}

func estimateSize(result *CachedResult) int64 {
	size := int64(64) // overhead
	for _, b := range result.Batches {
		for _, col := range b.Columns {
			size += int64(len(col) * 48) // rough estimate per value
		}
	}

	return size
}

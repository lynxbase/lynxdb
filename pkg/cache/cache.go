package cache

import (
	"context"
	"encoding/binary"
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

	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/vmihailenco/msgpack/v5"
)

// GovernorPoolAdapter adapts a memgov.Governor to the PoolReserver interface.
type GovernorPoolAdapter struct {
	gov memgov.Governor
}

// NewGovernorPoolAdapter creates a PoolReserver backed by the given governor.
func NewGovernorPoolAdapter(gov memgov.Governor) *GovernorPoolAdapter {
	return &GovernorPoolAdapter{gov: gov}
}

func (a *GovernorPoolAdapter) ReserveForCache(n int64) error {
	return a.gov.Reserve(memgov.ClassPageCache, n)
}

func (a *GovernorPoolAdapter) ReleaseCache(n int64) {
	a.gov.Release(memgov.ClassPageCache, n)
}

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
	h := fnv.New128a()
	h.Write([]byte(k.IndexName))
	h.Write([]byte{0})
	h.Write([]byte(k.SegmentID))
	h.Write([]byte{0})
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[0:4], k.SegmentCRC32)
	binary.BigEndian.PutUint64(buf[4:12], k.QueryHash)
	h.Write(buf[:])
	var tbuf [16]byte
	binary.BigEndian.PutUint64(tbuf[0:8], uint64(k.TimeRange[0]))
	binary.BigEndian.PutUint64(tbuf[8:16], uint64(k.TimeRange[1]))
	h.Write(tbuf[:])
	return h.Sum(nil)
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
	Hits          int64
	Misses        int64
	Evictions     int64
	SizeBytes     int64
	EntryCount    int64
	HitRate       float64
	PoolFullDrops int64 // inserts dropped because governor pool was full
	RemoveErrors  int64 // os.Remove failures during eviction
}

// PoolReserver abstracts the Governor interface for cache integration.
// The cache calls ReserveForCache when inserting entries and ReleaseCache
// when evicting them. This allows elastic sharing with query execution.
type PoolReserver interface {
	ReserveForCache(n int64) error
	ReleaseCache(n int64)
}

// diskOp represents an async disk operation (write or remove).
type diskOp struct {
	hex    string
	result *CachedResult // nil = remove, non-nil = write
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

	// Observability counters for cache pressure and disk errors.
	poolFullDrops atomic.Int64 // inserts dropped because governor pool was full
	removeErrors  atomic.Int64 // os.Remove failures during eviction
	writeErrors   atomic.Int64 // os.WriteFile failures during disk persistence

	diskOps chan diskOp    // async disk write/remove queue
	diskWg  sync.WaitGroup // tracks background disk goroutine

	// pool is the optional memory pool. When non-nil, cache
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
	cs.diskOps = make(chan diskOp, 4096)
	cs.diskWg.Add(1)
	go cs.diskWorker()
	if dir != "" {
		cs.loadFromDisk()
	}

	return cs
}

// SetPool sets the memory pool for elastic sharing. When set, cache
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
	cs.mu.Lock()
	entry, ok := cs.entries[hex]
	if !ok {
		cs.mu.Unlock()
		cs.misses.Add(1)

		return nil, nil
	}
	if time.Since(entry.result.CreatedAt) > cs.ttl {
		cs.removeEntryLocked(hex, entry)
		cs.mu.Unlock()
		cs.misses.Add(1)

		return nil, nil
	}
	// Mark as recently accessed in CLOCK.
	cs.clock.access(hex)
	entry.result.AccessedAt = time.Now()
	cs.mu.Unlock()

	cs.hits.Add(1)

	return entry.result, nil
}

// Put stores a result in the cache. When a memory pool is configured, it
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

	// If pool is set, reserve the net increase from the memory pool.
	if cs.pool != nil {
		netIncrease := size - existingSize
		if netIncrease > 0 {
			if err := cs.pool.ReserveForCache(netIncrease); err != nil {
				// Pool has no room — drop the entry silently.
				// Restore currentSize if we subtracted existing.
				cs.currentSize += existingSize
				cs.poolFullDrops.Add(1)
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
			cs.removeDiskEntry(evicted)
		}
	}
	cs.mu.Unlock()

	cs.evictIfNeeded()

	if cs.dir != "" {
		cs.writeToDisk(hex, result)
	}

	return nil
}

// EvictBytes evicts LRU entries until at least bytesNeeded bytes have been freed,
// or no more entries remain. Returns the total bytes actually freed.
//
// Called from the Governor's cacheEvictor callback. The caller holds its own
// mutex; this method acquires the cache's mu lock separately (no deadlock). Must
// NOT call back into the Governor — the pool adjusts cacheAllocated after
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
			// from the Governor's evictor, which adjusts cacheAllocated
			// itself based on the returned freed value.
		}
		delete(cs.entries, evictedKey)
		cs.evictions.Add(1)
		if cs.dir != "" {
			cs.removeDiskEntry(evictedKey)
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
		Hits:          hits,
		Misses:        misses,
		Evictions:     cs.evictions.Load(),
		SizeBytes:     totalSize,
		EntryCount:    entryCount,
		HitRate:       hitRate,
		PoolFullDrops: cs.poolFullDrops.Load(),
		RemoveErrors:  cs.removeErrors.Load(),
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
		if err := os.RemoveAll(cs.dir); err != nil {
			return fmt.Errorf("cache: clear: remove: %w", err)
		}
		if err := os.MkdirAll(cs.dir, 0o755); err != nil {
			return fmt.Errorf("cache: clear: mkdir: %w", err)
		}
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
		cs.removeDiskEntry(hex)
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
			cs.removeDiskEntry(evictedKey)
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

// removeDiskEntry enqueues an async disk removal for the given cache entry.
func (cs *Store) removeDiskEntry(hex string) {
	select {
	case cs.diskOps <- diskOp{hex: hex}:
	default:
	}
}

func (cs *Store) writeToDisk(hex string, result *CachedResult) {
	select {
	case cs.diskOps <- diskOp{hex: hex, result: result}:
	default:
		// Channel full — drop the disk persistence (in-memory cache still valid).
	}
}

// diskWorker processes async disk writes and removes in the background.
func (cs *Store) diskWorker() {
	defer cs.diskWg.Done()
	// Pre-created dirs cache to avoid redundant MkdirAll calls.
	knownDirs := make(map[string]struct{})
	for op := range cs.diskOps {
		path := cs.diskPath(op.hex)
		if op.result == nil {
			// Remove.
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				cs.removeErrors.Add(1)
			}
		} else {
			// Write.
			dir := filepath.Dir(path)
			if _, ok := knownDirs[dir]; !ok {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					continue
				}
				knownDirs[dir] = struct{}{}
			}
			data, err := msgpack.Marshal(op.result)
			if err != nil {
				continue
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				cs.writeErrors.Add(1)
			}
		}
	}
}

// Close shuts down the background disk worker and waits for pending ops.
func (cs *Store) Close() {
	if cs.diskOps != nil {
		close(cs.diskOps)
		cs.diskWg.Wait()
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

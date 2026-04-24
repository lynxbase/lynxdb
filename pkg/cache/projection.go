package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// projectionKey uniquely identifies a cached decoded column.
type projectionKey struct {
	SegID  string
	RgIdx  int
	Column string
}

func (k projectionKey) String() string {
	return fmt.Sprintf("%s/rg%d/%s", k.SegID, k.RgIdx, k.Column)
}

// projectionEntry holds cached decoded column data.
// Exactly one of the typed slices is non-nil per entry.
type projectionEntry struct {
	strings  []string
	int64s   []int64
	float64s []float64
	size     int64
	refBit   atomic.Uint32 // CLOCK eviction: 1 on access, cleared by clock hand scan
}

// ProjectionCacheStats holds hit/miss/size statistics for the projection cache.
type ProjectionCacheStats struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Entries   int64 `json:"entries"`
	UsedBytes int64 `json:"used_bytes"`
	MaxBytes  int64 `json:"max_bytes"`
	Evictions int64 `json:"evictions"`
}

// ProjectionCache caches decoded column data to avoid repeated LZ4
// decompression and encoding decode across queries hitting the same segments.
//
// Implements segment.ColumnCache. Thread-safe for concurrent use.
//
// Eviction uses the CLOCK algorithm: a circular buffer of keys with a
// reference bit per entry. On eviction, the clock hand scans entries;
// if refBit is true it clears it and advances, if false it evicts.
type ProjectionCache struct {
	mu        sync.RWMutex
	entries   map[projectionKey]*projectionEntry
	clock     []projectionKey // circular buffer for CLOCK eviction
	clockOcc  []bool          // whether each clock slot is occupied
	clockHand int
	clockCap  int
	clockLen  int // number of occupied slots
	maxBytes  int64
	usedBytes int64
	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
}

// NewProjectionCache creates a new projection cache with the given byte limit.
// A maxBytes of 0 or negative disables the cache (all operations are no-ops).
func NewProjectionCache(maxBytes int64) *ProjectionCache {
	if maxBytes <= 0 {
		maxBytes = 0
	}
	// Size the clock buffer to accommodate roughly maxBytes / 16KB entries
	// (assuming ~16KB average column chunk). Minimum 1024 slots.
	clockCap := int(maxBytes / (16 * 1024))
	if clockCap < 1024 {
		clockCap = 1024
	}
	if clockCap > 1<<20 {
		clockCap = 1 << 20 // 1M slots max
	}

	return &ProjectionCache{
		entries:  make(map[projectionKey]*projectionEntry, clockCap/2),
		clock:    make([]projectionKey, clockCap),
		clockOcc: make([]bool, clockCap),
		clockCap: clockCap,
		maxBytes: maxBytes,
	}
}

// GetStrings returns cached decoded string column data.
func (pc *ProjectionCache) GetStrings(segID string, rgIdx int, col string) ([]string, bool) {
	if pc.maxBytes == 0 {
		return nil, false
	}
	key := projectionKey{SegID: segID, RgIdx: rgIdx, Column: col}
	pc.mu.RLock()
	entry, ok := pc.entries[key]
	var result []string
	if ok {
		entry.refBit.Store(1)
		result = entry.strings
	}
	pc.mu.RUnlock()

	if !ok {
		pc.misses.Add(1)

		return nil, false
	}

	pc.hits.Add(1)

	return result, true
}

// PutStrings stores decoded string column data in the cache.
func (pc *ProjectionCache) PutStrings(segID string, rgIdx int, col string, data []string) {
	if pc.maxBytes == 0 {
		return
	}
	key := projectionKey{SegID: segID, RgIdx: rgIdx, Column: col}
	size := estimateStringsSize(data)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	// If already present, update in place.
	if existing, ok := pc.entries[key]; ok {
		pc.usedBytes -= existing.size
		existing.strings = data
		existing.int64s = nil
		existing.float64s = nil
		existing.size = size
		existing.refBit.Store(1)
		pc.usedBytes += size
		pc.evictToFitLocked()

		return
	}

	entry := &projectionEntry{
		strings: data,
		size:    size,
	}
	entry.refBit.Store(1)
	pc.entries[key] = entry
	pc.usedBytes += size
	pc.clockInsertLocked(key)
	pc.evictToFitLocked()
}

// GetInt64s returns cached decoded int64 column data.
func (pc *ProjectionCache) GetInt64s(segID string, rgIdx int, col string) ([]int64, bool) {
	if pc.maxBytes == 0 {
		return nil, false
	}
	key := projectionKey{SegID: segID, RgIdx: rgIdx, Column: col}
	pc.mu.RLock()
	entry, ok := pc.entries[key]
	var result []int64
	if ok {
		entry.refBit.Store(1)
		result = entry.int64s
	}
	pc.mu.RUnlock()

	if !ok {
		pc.misses.Add(1)

		return nil, false
	}

	pc.hits.Add(1)

	return result, true
}

// PutInt64s stores decoded int64 column data in the cache.
func (pc *ProjectionCache) PutInt64s(segID string, rgIdx int, col string, data []int64) {
	if pc.maxBytes == 0 {
		return
	}
	key := projectionKey{SegID: segID, RgIdx: rgIdx, Column: col}
	size := estimateInt64sSize(data)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if existing, ok := pc.entries[key]; ok {
		pc.usedBytes -= existing.size
		existing.strings = nil
		existing.int64s = data
		existing.float64s = nil
		existing.size = size
		existing.refBit.Store(1)
		pc.usedBytes += size
		pc.evictToFitLocked()

		return
	}

	entry := &projectionEntry{
		int64s: data,
		size:   size,
	}
	entry.refBit.Store(1)
	pc.entries[key] = entry
	pc.usedBytes += size
	pc.clockInsertLocked(key)
	pc.evictToFitLocked()
}

// GetFloat64s returns cached decoded float64 column data.
func (pc *ProjectionCache) GetFloat64s(segID string, rgIdx int, col string) ([]float64, bool) {
	if pc.maxBytes == 0 {
		return nil, false
	}
	key := projectionKey{SegID: segID, RgIdx: rgIdx, Column: col}
	pc.mu.RLock()
	entry, ok := pc.entries[key]
	var result []float64
	if ok {
		entry.refBit.Store(1)
		result = entry.float64s
	}
	pc.mu.RUnlock()

	if !ok {
		pc.misses.Add(1)

		return nil, false
	}

	pc.hits.Add(1)

	return result, true
}

// PutFloat64s stores decoded float64 column data in the cache.
func (pc *ProjectionCache) PutFloat64s(segID string, rgIdx int, col string, data []float64) {
	if pc.maxBytes == 0 {
		return
	}
	key := projectionKey{SegID: segID, RgIdx: rgIdx, Column: col}
	size := estimateFloat64sSize(data)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	if existing, ok := pc.entries[key]; ok {
		pc.usedBytes -= existing.size
		existing.strings = nil
		existing.int64s = nil
		existing.float64s = data
		existing.size = size
		existing.refBit.Store(1)
		pc.usedBytes += size
		pc.evictToFitLocked()

		return
	}

	entry := &projectionEntry{
		float64s: data,
		size:     size,
	}
	entry.refBit.Store(1)
	pc.entries[key] = entry
	pc.usedBytes += size
	pc.clockInsertLocked(key)
	pc.evictToFitLocked()
}

// InvalidateSegment removes all cached entries for the given segment.
// Called when a segment is compacted away or deleted.
func (pc *ProjectionCache) InvalidateSegment(segID string) {
	if pc.maxBytes == 0 {
		return
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	for key, entry := range pc.entries {
		if key.SegID == segID {
			pc.usedBytes -= entry.size
			delete(pc.entries, key)
			pc.evictions.Add(1)
		}
	}

	// Clean clock slots for the invalidated segment.
	for i := 0; i < pc.clockCap; i++ {
		if pc.clockOcc[i] && pc.clock[i].SegID == segID {
			pc.clockOcc[i] = false
			pc.clock[i] = projectionKey{}
			pc.clockLen--
		}
	}
}

// EvictBytes evicts entries until at least target bytes have been freed,
// or no more entries remain. Returns the total bytes actually freed.
func (pc *ProjectionCache) EvictBytes(target int64) int64 {
	if target <= 0 || pc.maxBytes == 0 {
		return 0
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	var freed int64
	for freed < target {
		key, ok := pc.clockEvictLocked()
		if !ok {
			break
		}
		if entry, exists := pc.entries[key]; exists {
			freed += entry.size
			pc.usedBytes -= entry.size
			delete(pc.entries, key)
			pc.evictions.Add(1)
		}
	}

	return freed
}

// Stats returns cache performance statistics.
func (pc *ProjectionCache) Stats() ProjectionCacheStats {
	pc.mu.RLock()
	entries := int64(len(pc.entries))
	used := pc.usedBytes
	pc.mu.RUnlock()

	return ProjectionCacheStats{
		Hits:      pc.hits.Load(),
		Misses:    pc.misses.Load(),
		Entries:   entries,
		UsedBytes: used,
		MaxBytes:  pc.maxBytes,
		Evictions: pc.evictions.Load(),
	}
}

// clockInsertLocked adds a key to the CLOCK circular buffer.
// Must be called with pc.mu held.
func (pc *ProjectionCache) clockInsertLocked(key projectionKey) {
	if pc.clockLen >= pc.clockCap {
		// Clock is full — evict one entry first.
		pc.clockEvictLocked()
	}

	// Find an empty slot starting from the clock hand.
	for i := 0; i < pc.clockCap; i++ {
		idx := (pc.clockHand + i) % pc.clockCap
		if !pc.clockOcc[idx] {
			pc.clock[idx] = key
			pc.clockOcc[idx] = true
			pc.clockLen++
			pc.clockHand = (idx + 1) % pc.clockCap

			return
		}
	}
}

// clockEvictLocked removes one entry using the CLOCK algorithm.
// Returns the evicted key and true, or zero key and false if empty.
// Must be called with pc.mu held.
func (pc *ProjectionCache) clockEvictLocked() (projectionKey, bool) {
	// Scan at most 2 full revolutions.
	for i := 0; i < 2*pc.clockCap; i++ {
		idx := pc.clockHand
		pc.clockHand = (pc.clockHand + 1) % pc.clockCap

		if !pc.clockOcc[idx] {
			continue
		}

		key := pc.clock[idx]
		entry, ok := pc.entries[key]
		if !ok {
			// Stale clock slot — clean it up.
			pc.clockOcc[idx] = false
			pc.clock[idx] = projectionKey{}
			pc.clockLen--

			continue
		}

		if entry.refBit.Load() == 1 {
			entry.refBit.Store(0)

			continue
		}

		// Evict this entry.
		pc.clockOcc[idx] = false
		pc.clock[idx] = projectionKey{}
		pc.clockLen--

		return key, true
	}

	return projectionKey{}, false
}

// evictToFitLocked evicts entries until usedBytes <= maxBytes.
// Must be called with pc.mu held.
func (pc *ProjectionCache) evictToFitLocked() {
	for pc.usedBytes > pc.maxBytes {
		key, ok := pc.clockEvictLocked()
		if !ok {
			break
		}
		if entry, exists := pc.entries[key]; exists {
			pc.usedBytes -= entry.size
			delete(pc.entries, key)
			pc.evictions.Add(1)
		}
	}
}

// Size estimation helpers.
// Strings: sum of string lengths + 16 bytes per string (header + pointer).
// Int64s/Float64s: 8 bytes per value.
// Plus 64 bytes overhead per entry for the map entry, key, and pointer.

func estimateStringsSize(data []string) int64 {
	size := int64(64) // entry overhead
	for _, s := range data {
		size += int64(len(s)) + 16 // string data + string header
	}

	return size
}

func estimateInt64sSize(data []int64) int64 {
	return int64(64) + int64(len(data))*8
}

func estimateFloat64sSize(data []float64) int64 {
	return int64(64) + int64(len(data))*8
}

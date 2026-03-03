package stats

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// UnifiedPool manages a single memory pool shared between query execution and
// the segment cache. Inspired by Apache Spark's UnifiedMemoryManager (SPARK-10000):
// queries can evict cache entries down to a configurable floor, but the cache
// cannot evict query memory. This eliminates static partitioning — under heavy
// query load, cache shrinks; under light query load, cache grows to fill free space.
//
// Thread-safe. All methods may be called concurrently.
//
// Invariants:
//   - queryAllocated + cacheAllocated <= totalLimit (always)
//   - cacheAllocated >= cacheReserveFloor after eviction
//   - Queries can evict cache down to cacheReserveFloor
//   - Cache cannot evict query memory
type UnifiedPool struct {
	mu sync.Mutex

	totalLimit        int64 // total memory for queries + cache
	cacheReserveFloor int64 // minimum: cache cannot be evicted below this

	queryAllocated int64 // currently held by all queries
	cacheAllocated int64 // currently held by segment cache

	// cacheEvictor evicts cache entries to free memory for queries.
	// Called with mu held — implementations must not reacquire the pool lock.
	// Returns the number of bytes actually freed.
	cacheEvictor func(bytesNeeded int64) int64

	// Metrics (atomic for lock-free reads).
	evictionCount   atomic.Int64
	evictionBytes   atomic.Int64
	queryRejections atomic.Int64
}

// UnifiedPoolStats is a point-in-time snapshot of UnifiedPool metrics.
type UnifiedPoolStats struct {
	TotalLimitBytes        int64 `json:"total_pool_bytes"`
	QueryAllocatedBytes    int64 `json:"query_allocated_bytes"`
	CacheAllocatedBytes    int64 `json:"cache_allocated_bytes"`
	CacheReserveFloorBytes int64 `json:"cache_reserve_floor_bytes"`
	FreeBytes              int64 `json:"free_bytes"`
	CacheEvictionCount     int64 `json:"cache_eviction_count"`
	CacheEvictionBytes     int64 `json:"cache_eviction_bytes"`
	QueryRejections        int64 `json:"query_rejections"`
}

// NewUnifiedPool creates a unified memory pool with elastic sharing between
// query execution and segment cache.
//
// Parameters:
//   - totalLimit: total bytes available for both queries and cache combined.
//   - cacheReservePercent: minimum cache floor as a percentage of totalLimit (0-100, default 20).
//     Queries cannot evict cache below this floor.
//   - cacheEvictor: callback to evict cache entries. Called with the pool's lock held.
//     Must not reacquire the pool lock. Returns bytes actually freed.
//     May be nil if no cache is present (all memory goes to queries).
func NewUnifiedPool(totalLimit int64, cacheReservePercent int, cacheEvictor func(int64) int64) *UnifiedPool {
	if totalLimit <= 0 {
		totalLimit = 1 << 30 // 1GB default
	}
	if cacheReservePercent < 0 {
		cacheReservePercent = 0
	}
	if cacheReservePercent > 100 {
		cacheReservePercent = 100
	}

	floor := totalLimit * int64(cacheReservePercent) / 100

	if cacheEvictor == nil {
		cacheEvictor = func(int64) int64 { return 0 }
	}

	return &UnifiedPool{
		totalLimit:        totalLimit,
		cacheReserveFloor: floor,
		cacheEvictor:      cacheEvictor,
	}
}

// ReserveForQuery reserves n bytes for query execution. If free space is
// insufficient, evicts cache entries down to the cache reserve floor. Returns
// a *PoolExhaustedError if neither free space nor cache eviction can satisfy
// the request.
//
// Thread-safe.
func (p *UnifiedPool) ReserveForQuery(n int64) error {
	if p == nil || n <= 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	free := p.totalLimit - p.queryAllocated - p.cacheAllocated

	// Fast path: enough free space.
	if free >= n {
		p.queryAllocated += n

		return nil
	}

	// Compute how much we can evict from cache (down to the floor).
	evictable := p.cacheAllocated - p.cacheReserveFloor
	if evictable < 0 {
		evictable = 0
	}

	needed := n - free
	if needed > evictable {
		// Even full eviction won't help.
		p.queryRejections.Add(1)

		return &PoolExhaustedError{
			Pool:      "unified-pool",
			Requested: n,
			Current:   p.queryAllocated,
			Limit:     p.totalLimit,
		}
	}

	// Evict cache entries to make room.
	freed := p.cacheEvictor(needed)
	if freed > 0 {
		p.cacheAllocated -= freed
		if p.cacheAllocated < 0 {
			p.cacheAllocated = 0
		}
		p.evictionCount.Add(1)
		p.evictionBytes.Add(freed)
	}

	// Recheck after eviction.
	free = p.totalLimit - p.queryAllocated - p.cacheAllocated
	if free >= n {
		p.queryAllocated += n

		return nil
	}

	// Evictor couldn't free enough. This can happen if the cache's internal
	// accounting diverges from ours (e.g., concurrent invalidation).
	p.queryRejections.Add(1)

	return &PoolExhaustedError{
		Pool:      "unified-pool",
		Requested: n,
		Current:   p.queryAllocated,
		Limit:     p.totalLimit,
	}
}

// ReleaseQuery returns n bytes of query memory to the pool.
// Thread-safe.
func (p *UnifiedPool) ReleaseQuery(n int64) {
	if p == nil || n <= 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.queryAllocated -= n
	if p.queryAllocated < 0 {
		p.queryAllocated = 0
	}
}

// ReserveForCache reserves n bytes for the segment cache. The cache does NOT
// evict query memory — if there is not enough free space, the request fails.
// The cache should handle this by evicting its own LRU entries and retrying,
// or by not inserting the entry.
//
// Thread-safe.
func (p *UnifiedPool) ReserveForCache(n int64) error {
	if p == nil || n <= 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	free := p.totalLimit - p.queryAllocated - p.cacheAllocated
	if free >= n {
		p.cacheAllocated += n

		return nil
	}

	// Cache cannot evict query memory. Return error.
	return fmt.Errorf("unified pool: insufficient free space for cache reservation (%d requested, %d free)", n, free)
}

// ReleaseCache returns n bytes of cache memory to the pool.
// Thread-safe.
func (p *UnifiedPool) ReleaseCache(n int64) {
	if p == nil || n <= 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.cacheAllocated -= n
	if p.cacheAllocated < 0 {
		p.cacheAllocated = 0
	}
}

// Stats returns a point-in-time snapshot of pool metrics.
// Thread-safe.
func (p *UnifiedPool) Stats() UnifiedPoolStats {
	if p == nil {
		return UnifiedPoolStats{}
	}

	p.mu.Lock()
	queryAlloc := p.queryAllocated
	cacheAlloc := p.cacheAllocated
	total := p.totalLimit
	floor := p.cacheReserveFloor
	p.mu.Unlock()

	free := total - queryAlloc - cacheAlloc
	if free < 0 {
		free = 0
	}

	return UnifiedPoolStats{
		TotalLimitBytes:        total,
		QueryAllocatedBytes:    queryAlloc,
		CacheAllocatedBytes:    cacheAlloc,
		CacheReserveFloorBytes: floor,
		FreeBytes:              free,
		CacheEvictionCount:     p.evictionCount.Load(),
		CacheEvictionBytes:     p.evictionBytes.Load(),
		QueryRejections:        p.queryRejections.Load(),
	}
}

// TotalLimit returns the pool's total byte limit.
// Nil-safe: returns 0 if pool is nil.
func (p *UnifiedPool) TotalLimit() int64 {
	if p == nil {
		return 0
	}

	return p.totalLimit
}

// QueryAllocated returns the current bytes reserved by queries.
// Thread-safe. Nil-safe: returns 0 if pool is nil.
func (p *UnifiedPool) QueryAllocated() int64 {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.queryAllocated
}

// CacheAllocated returns the current bytes reserved by cache.
// Thread-safe. Nil-safe: returns 0 if pool is nil.
func (p *UnifiedPool) CacheAllocated() int64 {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.cacheAllocated
}

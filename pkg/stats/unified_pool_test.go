package stats

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestUnifiedPoolQueryAllocation(t *testing.T) {
	pool := NewUnifiedPool(1024, 20, nil)

	if err := pool.ReserveForQuery(256); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pool.QueryAllocated(); got != 256 {
		t.Fatalf("expected query=256, got %d", got)
	}

	pool.ReleaseQuery(100)
	if got := pool.QueryAllocated(); got != 156 {
		t.Fatalf("expected query=156, got %d", got)
	}

	pool.ReleaseQuery(156)
	if got := pool.QueryAllocated(); got != 0 {
		t.Fatalf("expected query=0, got %d", got)
	}
}

func TestUnifiedPoolCacheAllocation(t *testing.T) {
	pool := NewUnifiedPool(1024, 20, nil)

	if err := pool.ReserveForCache(512); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := pool.CacheAllocated(); got != 512 {
		t.Fatalf("expected cache=512, got %d", got)
	}

	pool.ReleaseCache(200)
	if got := pool.CacheAllocated(); got != 312 {
		t.Fatalf("expected cache=312, got %d", got)
	}

	pool.ReleaseCache(312)
	if got := pool.CacheAllocated(); got != 0 {
		t.Fatalf("expected cache=0, got %d", got)
	}
}

func TestUnifiedPoolQueryEvictsCache(t *testing.T) {
	// Total=1000, floor=20% = 200. Fill cache to 800, then query needs 500.
	// Evictable = 800 - 200 = 600. Should succeed.
	var evictCalled int64
	var evictRequested int64

	evictor := func(bytesNeeded int64) int64 {
		atomic.AddInt64(&evictCalled, 1)
		atomic.StoreInt64(&evictRequested, bytesNeeded)
		// Simulate evicting exactly what was requested.
		return bytesNeeded
	}

	pool := NewUnifiedPool(1000, 20, evictor)

	// Fill cache to 800.
	if err := pool.ReserveForCache(800); err != nil {
		t.Fatalf("cache reserve failed: %v", err)
	}

	// Free space = 1000 - 0 - 800 = 200. Query needs 500.
	// needed = 500 - 200 = 300. evictable = 800 - 200 = 600. 300 <= 600, ok.
	if err := pool.ReserveForQuery(500); err != nil {
		t.Fatalf("query reserve failed: %v", err)
	}

	if atomic.LoadInt64(&evictCalled) != 1 {
		t.Fatalf("expected evictor called once, got %d", atomic.LoadInt64(&evictCalled))
	}
	if atomic.LoadInt64(&evictRequested) != 300 {
		t.Fatalf("expected evictor requested 300, got %d", atomic.LoadInt64(&evictRequested))
	}

	// cache should be 800 - 300 = 500.
	if got := pool.CacheAllocated(); got != 500 {
		t.Fatalf("expected cache=500 after eviction, got %d", got)
	}
	if got := pool.QueryAllocated(); got != 500 {
		t.Fatalf("expected query=500, got %d", got)
	}

	// Verify eviction metrics.
	stats := pool.Stats()
	if stats.CacheEvictionCount != 1 {
		t.Fatalf("expected eviction count=1, got %d", stats.CacheEvictionCount)
	}
	if stats.CacheEvictionBytes != 300 {
		t.Fatalf("expected eviction bytes=300, got %d", stats.CacheEvictionBytes)
	}
}

func TestUnifiedPoolQueryCannotEvictBelowFloor(t *testing.T) {
	// Total=1000, floor=20% = 200. Cache at floor level (200).
	// Query at 800. Free = 0. Request 1 more byte -> fail.
	pool := NewUnifiedPool(1000, 20, func(int64) int64 { return 0 })

	if err := pool.ReserveForCache(200); err != nil {
		t.Fatalf("cache reserve failed: %v", err)
	}
	if err := pool.ReserveForQuery(800); err != nil {
		t.Fatalf("query reserve failed: %v", err)
	}

	// Free = 0, evictable = 200 - 200 = 0.
	err := pool.ReserveForQuery(1)
	if err == nil {
		t.Fatal("expected PoolExhaustedError, got nil")
	}
	if !IsPoolExhausted(err) {
		t.Fatalf("expected PoolExhaustedError, got %T: %v", err, err)
	}

	stats := pool.Stats()
	if stats.QueryRejections != 1 {
		t.Fatalf("expected rejections=1, got %d", stats.QueryRejections)
	}
}

func TestUnifiedPoolCacheCannotEvictQueries(t *testing.T) {
	// Total=1000. Queries fill 800. Cache tries to reserve 300 (only 200 free).
	pool := NewUnifiedPool(1000, 20, nil)

	if err := pool.ReserveForQuery(800); err != nil {
		t.Fatalf("query reserve failed: %v", err)
	}

	err := pool.ReserveForCache(300)
	if err == nil {
		t.Fatal("expected error when cache cannot fit, got nil")
	}

	// Cache should not have allocated anything.
	if got := pool.CacheAllocated(); got != 0 {
		t.Fatalf("expected cache=0 after failed reserve, got %d", got)
	}
}

func TestUnifiedPoolConcurrent(t *testing.T) {
	pool := NewUnifiedPool(1<<30, 20, nil) // 1GB, plenty of room

	const queryGoroutines = 50
	const cacheGoroutines = 20
	const bytesPerOp int64 = 1024

	var wg sync.WaitGroup
	wg.Add(queryGoroutines + cacheGoroutines)

	// Query goroutines: reserve then release.
	for i := 0; i < queryGoroutines; i++ {
		go func() {
			defer wg.Done()
			if err := pool.ReserveForQuery(bytesPerOp); err != nil {
				t.Errorf("query reserve failed: %v", err)

				return
			}
			pool.ReleaseQuery(bytesPerOp)
		}()
	}

	// Cache goroutines: reserve then release.
	for i := 0; i < cacheGoroutines; i++ {
		go func() {
			defer wg.Done()
			if err := pool.ReserveForCache(bytesPerOp); err != nil {
				t.Errorf("cache reserve failed: %v", err)

				return
			}
			pool.ReleaseCache(bytesPerOp)
		}()
	}

	wg.Wait()

	// After all goroutines complete, both should be 0.
	if got := pool.QueryAllocated(); got != 0 {
		t.Fatalf("expected query=0, got %d", got)
	}
	if got := pool.CacheAllocated(); got != 0 {
		t.Fatalf("expected cache=0, got %d", got)
	}

	// Invariant: queryAllocated + cacheAllocated <= totalLimit.
	stats := pool.Stats()
	if stats.QueryAllocatedBytes+stats.CacheAllocatedBytes > stats.TotalLimitBytes {
		t.Fatalf("invariant violated: query(%d) + cache(%d) > limit(%d)",
			stats.QueryAllocatedBytes, stats.CacheAllocatedBytes, stats.TotalLimitBytes)
	}
}

func TestUnifiedPoolEvictionMetrics(t *testing.T) {
	evictCount := 0
	evictor := func(bytesNeeded int64) int64 {
		evictCount++

		return bytesNeeded
	}

	pool := NewUnifiedPool(1000, 10, evictor) // floor = 100

	// Fill cache to 800.
	if err := pool.ReserveForCache(800); err != nil {
		t.Fatalf("cache reserve failed: %v", err)
	}

	// Reserve query: free=200, need 400 -> evict 200.
	if err := pool.ReserveForQuery(400); err != nil {
		t.Fatalf("query reserve failed: %v", err)
	}

	// Reserve more query: free=0, cache=600, floor=100, evictable=500.
	// Need 300 -> evict 300.
	if err := pool.ReserveForQuery(300); err != nil {
		t.Fatalf("query reserve failed: %v", err)
	}

	stats := pool.Stats()
	if stats.CacheEvictionCount != 2 {
		t.Fatalf("expected eviction count=2, got %d", stats.CacheEvictionCount)
	}
	if stats.CacheEvictionBytes != 500 {
		t.Fatalf("expected eviction bytes=500, got %d", stats.CacheEvictionBytes)
	}
	if stats.QueryRejections != 0 {
		t.Fatalf("expected rejections=0, got %d", stats.QueryRejections)
	}
}

func TestUnifiedPoolStats(t *testing.T) {
	pool := NewUnifiedPool(2000, 25, nil) // floor = 500

	if err := pool.ReserveForQuery(300); err != nil {
		t.Fatal(err)
	}
	if err := pool.ReserveForCache(700); err != nil {
		t.Fatal(err)
	}

	stats := pool.Stats()
	if stats.TotalLimitBytes != 2000 {
		t.Fatalf("expected total=2000, got %d", stats.TotalLimitBytes)
	}
	if stats.QueryAllocatedBytes != 300 {
		t.Fatalf("expected query=300, got %d", stats.QueryAllocatedBytes)
	}
	if stats.CacheAllocatedBytes != 700 {
		t.Fatalf("expected cache=700, got %d", stats.CacheAllocatedBytes)
	}
	if stats.CacheReserveFloorBytes != 500 {
		t.Fatalf("expected floor=500, got %d", stats.CacheReserveFloorBytes)
	}
	if stats.FreeBytes != 1000 {
		t.Fatalf("expected free=1000, got %d", stats.FreeBytes)
	}
}

func TestUnifiedPoolNilSafety(t *testing.T) {
	var pool *UnifiedPool

	// All nil-safe methods should not panic.
	if err := pool.ReserveForQuery(100); err != nil {
		t.Fatalf("nil reserve query should return nil: %v", err)
	}
	pool.ReleaseQuery(100)
	if err := pool.ReserveForCache(100); err != nil {
		t.Fatalf("nil reserve cache should return nil: %v", err)
	}
	pool.ReleaseCache(100)

	if pool.TotalLimit() != 0 {
		t.Fatal("nil TotalLimit should return 0")
	}
	if pool.QueryAllocated() != 0 {
		t.Fatal("nil QueryAllocated should return 0")
	}
	if pool.CacheAllocated() != 0 {
		t.Fatal("nil CacheAllocated should return 0")
	}

	stats := pool.Stats()
	if stats.TotalLimitBytes != 0 {
		t.Fatal("nil Stats should return zero values")
	}
}

func TestUnifiedPoolReleaseClampZero(t *testing.T) {
	pool := NewUnifiedPool(1000, 20, nil)

	if err := pool.ReserveForQuery(100); err != nil {
		t.Fatal(err)
	}
	// Release more than allocated — should clamp to 0.
	pool.ReleaseQuery(200)
	if got := pool.QueryAllocated(); got != 0 {
		t.Fatalf("expected query=0, got %d", got)
	}

	if err := pool.ReserveForCache(100); err != nil {
		t.Fatal(err)
	}
	pool.ReleaseCache(200)
	if got := pool.CacheAllocated(); got != 0 {
		t.Fatalf("expected cache=0, got %d", got)
	}
}

func TestUnifiedPoolEvictorCannotFreeEnough(t *testing.T) {
	// Evictor that only frees half of what's requested.
	evictor := func(bytesNeeded int64) int64 {
		return bytesNeeded / 2
	}

	pool := NewUnifiedPool(1000, 10, evictor) // floor = 100

	// Fill cache to 900.
	if err := pool.ReserveForCache(900); err != nil {
		t.Fatalf("cache reserve failed: %v", err)
	}

	// Free=100. Need 600. Evictable=900-100=800. 500 <= 800 so eviction attempted.
	// But evictor only frees 250. free becomes 100+250=350 < 600. Should fail.
	err := pool.ReserveForQuery(600)
	if err == nil {
		t.Fatal("expected error when evictor can't free enough")
	}
	if !IsPoolExhausted(err) {
		t.Fatalf("expected PoolExhaustedError, got %T: %v", err, err)
	}
}

func TestUnifiedPoolDefaultValues(t *testing.T) {
	// Zero totalLimit should default to 1GB.
	pool := NewUnifiedPool(0, 20, nil)
	if pool.TotalLimit() != 1<<30 {
		t.Fatalf("expected default 1GB, got %d", pool.TotalLimit())
	}
}

func TestUnifiedPoolZeroPercent(t *testing.T) {
	// 0% reserve: queries can evict ALL cache entries.
	evictor := func(bytesNeeded int64) int64 { return bytesNeeded }
	pool := NewUnifiedPool(1000, 0, evictor)

	if err := pool.ReserveForCache(1000); err != nil {
		t.Fatal(err)
	}

	// Query needs all 1000 bytes. Evictable = 1000 - 0 = 1000.
	if err := pool.ReserveForQuery(1000); err != nil {
		t.Fatalf("should succeed with 0%% reserve: %v", err)
	}
	if got := pool.CacheAllocated(); got != 0 {
		t.Fatalf("expected cache=0, got %d", got)
	}
}

func BenchmarkUnifiedPoolReserveRelease(b *testing.B) {
	pool := NewUnifiedPool(1<<30, 20, nil)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.ReserveForQuery(1024)
			pool.ReleaseQuery(1024)
		}
	})
}

func BenchmarkUnifiedPoolReserveReleaseContended(b *testing.B) {
	pool := NewUnifiedPool(1<<30, 20, nil)

	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.ReserveForQuery(1024)
			pool.ReleaseQuery(1024)
		}
	})
}

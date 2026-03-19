package cache

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestKeyDeterminism(t *testing.T) {
	k1 := Key{IndexName: "idx", SegmentID: "seg1", SegmentCRC32: 0x12345678, QueryHash: 42, TimeRange: [2]int64{100, 200}}
	k2 := Key{IndexName: "idx", SegmentID: "seg1", SegmentCRC32: 0x12345678, QueryHash: 42, TimeRange: [2]int64{100, 200}}
	if k1.Hex() != k2.Hex() {
		t.Errorf("same inputs should produce same key: %s != %s", k1.Hex(), k2.Hex())
	}
}

func TestKeyUniqueness(t *testing.T) {
	keys := []Key{
		{IndexName: "idx1", SegmentID: "seg1", QueryHash: 1},
		{IndexName: "idx2", SegmentID: "seg1", QueryHash: 1},                            // different index
		{IndexName: "idx1", SegmentID: "seg2", QueryHash: 1},                            // different segment
		{IndexName: "idx1", SegmentID: "seg1", QueryHash: 2},                            // different query
		{IndexName: "idx1", SegmentID: "seg1", QueryHash: 1, TimeRange: [2]int64{1, 2}}, // different time
	}
	seen := make(map[string]bool)
	for _, k := range keys {
		hex := k.Hex()
		if seen[hex] {
			t.Errorf("duplicate key: %s", hex)
		}
		seen[hex] = true
	}
}

func TestCacheHitReturnsCorrectResults(t *testing.T) {
	cs := NewStore("", 1<<20, time.Hour)
	ctx := context.Background()

	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}
	result := &CachedResult{
		Batches: []CachedBatch{
			{
				Columns: map[string][]CachedValue{
					"x": {{Type: 2, Num: 42}},
				},
				Len: 1,
			},
		},
	}
	cs.Put(ctx, key, result)

	got, err := cs.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected cache hit")
	}
	if len(got.Batches) != 1 {
		t.Errorf("expected 1 batch, got %d", len(got.Batches))
	}
	if got.Batches[0].Columns["x"][0].Num != 42 {
		t.Errorf("expected 42, got %v", got.Batches[0].Columns["x"][0].Num)
	}
}

func TestCacheInvalidationOnSegmentCRCChange(t *testing.T) {
	cs := NewStore("", 1<<20, time.Hour)
	ctx := context.Background()

	key1 := Key{IndexName: "idx", SegmentID: "seg1", SegmentCRC32: 0xAABBCCDD, QueryHash: 42}
	key2 := Key{IndexName: "idx", SegmentID: "seg1", SegmentCRC32: 0x11223344, QueryHash: 42} // different CRC

	cs.Put(ctx, key1, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	// Different CRC → different key → miss
	got, _ := cs.Get(ctx, key2)
	if got != nil {
		t.Error("expected cache miss for different CRC")
	}
}

func TestCacheTTLExpiration(t *testing.T) {
	cs := NewStore("", 1<<20, 50*time.Millisecond) // very short TTL
	ctx := context.Background()

	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}
	cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	// Should hit immediately
	got, _ := cs.Get(ctx, key)
	if got == nil {
		t.Error("expected hit before TTL")
	}

	// Wait for TTL
	time.Sleep(60 * time.Millisecond)

	// Should miss now
	got, _ = cs.Get(ctx, key)
	if got != nil {
		t.Error("expected miss after TTL")
	}
}

func TestCacheCLOCKEviction(t *testing.T) {
	// Create a small cache with CLOCK buffer capacity of 4.
	cs := NewStore("", 1<<30, time.Hour) // large byte limit
	cs.clock = newClockBuffer(4)         // small clock for testing
	ctx := context.Background()

	// Insert 4 items (fills the clock). All have refBit=1 from insert.
	for i := 0; i < 4; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	}

	if cs.clock.count != 4 {
		t.Fatalf("expected 4 entries in clock, got %d", cs.clock.count)
	}

	// Insert item 4: forces first eviction.
	// CLOCK scan: all refBit=1 → set to 0, then evict first item (0).
	key4 := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 4}
	cs.Put(ctx, key4, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	// Item 0 should be evicted (first victim after full refBit clear).
	got0, _ := cs.Get(ctx, Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 0})
	if got0 != nil {
		t.Error("item 0 should have been evicted")
	}

	// Now items 1,2,3 have refBit=0 (cleared during scan), item 4 has refBit=1.
	// Access item 2 to give it a second chance (refBit=1).
	cs.Get(ctx, Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 2})

	// Insert item 5: forces second eviction.
	// Clock hand at position after item 0's slot. Items 1(ref=0), 2(ref=1), 3(ref=0), 4(ref=1).
	// Eviction: item 1 (refBit=0) → evicted.
	key5 := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 5}
	cs.Put(ctx, key5, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	got1, _ := cs.Get(ctx, Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 1})
	if got1 != nil {
		t.Error("item 1 should have been evicted")
	}

	// Item 2 should survive (was accessed, refBit=1).
	got2, _ := cs.Get(ctx, Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 2})
	if got2 == nil {
		t.Error("item 2 should still be present (was accessed)")
	}

	// Items 3, 4, 5 should be present.
	for _, qh := range []uint64{3, 4, 5} {
		got, _ := cs.Get(ctx, Key{IndexName: "idx", SegmentID: "seg1", QueryHash: qh})
		if got == nil {
			t.Errorf("item %d should still be present", qh)
		}
	}

	if cs.clock.count != 4 {
		t.Errorf("expected 4 entries, got %d", cs.clock.count)
	}
}

func TestCacheSizeEviction(t *testing.T) {
	// Small cache: 1KB max
	cs := NewStore("", 1024, time.Hour)
	ctx := context.Background()

	// Insert enough entries to exceed limit
	for i := 0; i < 50; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{
			Batches: []CachedBatch{{
				Columns: map[string][]CachedValue{"x": make([]CachedValue, 10)},
				Len:     10,
			}},
		})
	}

	stats := cs.Stats()
	if stats.Evictions == 0 {
		t.Error("expected some evictions")
	}
	if stats.SizeBytes > 1024 {
		t.Errorf("size %d should be <= 1024 after eviction", stats.SizeBytes)
	}
}

func TestCacheOnFlush_GranularInvalidation(t *testing.T) {
	cs := NewStore("", 1<<20, time.Hour)
	ctx := context.Background()

	// Insert entries for different segments.
	segL1 := Key{IndexName: "idx", SegmentID: "seg-L1", QueryHash: 1}
	segL2 := Key{IndexName: "idx", SegmentID: "seg-L2", QueryHash: 2}
	memtable := Key{IndexName: "idx", SegmentID: "", QueryHash: 3} // memtable (empty segment ID)

	cs.Put(ctx, segL1, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	cs.Put(ctx, segL2, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	cs.Put(ctx, memtable, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	// Flush creates new segments "seg-new1" and "seg-new2".
	cs.OnFlush([]string{"seg-new1", "seg-new2"})

	// L1 and L2 segment entries should be preserved.
	gotL1, _ := cs.Get(ctx, segL1)
	gotL2, _ := cs.Get(ctx, segL2)
	if gotL1 == nil {
		t.Error("L1 segment entry should be preserved after flush")
	}
	if gotL2 == nil {
		t.Error("L2 segment entry should be preserved after flush")
	}

	// Memtable entry (empty segment ID) should be gone.
	gotMem, _ := cs.Get(ctx, memtable)
	if gotMem != nil {
		t.Error("memtable entry should be invalidated after flush")
	}
}

func TestCacheOnCompaction_GranularInvalidation(t *testing.T) {
	cs := NewStore("", 1<<20, time.Hour)
	ctx := context.Background()

	// Insert entries for segments that will be compacted.
	seg1 := Key{IndexName: "idx", SegmentID: "seg-old-1", QueryHash: 1}
	seg2 := Key{IndexName: "idx", SegmentID: "seg-old-2", QueryHash: 2}
	segKeep := Key{IndexName: "idx", SegmentID: "seg-keep", QueryHash: 3}

	cs.Put(ctx, seg1, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	cs.Put(ctx, seg2, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	cs.Put(ctx, segKeep, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	// Compaction: seg-old-1 + seg-old-2 → seg-merged.
	cs.OnCompaction([]string{"seg-old-1", "seg-old-2"}, []string{"seg-merged"})

	// Compacted segments should be removed.
	got1, _ := cs.Get(ctx, seg1)
	got2, _ := cs.Get(ctx, seg2)
	if got1 != nil {
		t.Error("seg-old-1 entry should be invalidated after compaction")
	}
	if got2 != nil {
		t.Error("seg-old-2 entry should be invalidated after compaction")
	}

	// Unrelated segment should be preserved.
	gotKeep, _ := cs.Get(ctx, segKeep)
	if gotKeep == nil {
		t.Error("seg-keep entry should be preserved after compaction")
	}
}

func TestCacheInvalidateSegment(t *testing.T) {
	cs := NewStore("", 1<<20, time.Hour)
	ctx := context.Background()

	// Multiple queries for the same segment.
	for i := 0; i < 5; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg-target", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	}
	// One for a different segment.
	other := Key{IndexName: "idx", SegmentID: "seg-other", QueryHash: 99}
	cs.Put(ctx, other, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	cs.InvalidateSegment("seg-target")

	// All entries for seg-target should be gone.
	for i := 0; i < 5; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg-target", QueryHash: uint64(i)}
		got, _ := cs.Get(ctx, key)
		if got != nil {
			t.Errorf("seg-target query %d should be invalidated", i)
		}
	}

	// seg-other should still be there.
	gotOther, _ := cs.Get(ctx, other)
	if gotOther == nil {
		t.Error("seg-other should be preserved")
	}
}

func TestCachePersistenceAcrossRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "lynxdb-cache-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}

	// Write to cache with persistence
	cs1 := NewStore(dir, 1<<20, time.Hour)
	cs1.Put(ctx, key, &CachedResult{
		Batches: []CachedBatch{{
			Columns: map[string][]CachedValue{"x": {{Type: 2, Num: 99}}},
			Len:     1,
		}},
	})

	// Create new cache store (simulates restart)
	cs2 := NewStore(dir, 1<<20, time.Hour)
	got, _ := cs2.Get(ctx, key)
	if got == nil {
		t.Fatal("expected cache hit after restart")
	}
	if got.Batches[0].Columns["x"][0].Num != 99 {
		t.Errorf("expected 99, got %v", got.Batches[0].Columns["x"][0].Num)
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	cs := NewStore("", 1<<20, time.Hour)
	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(id*100 + j)}
				cs.Put(ctx, key, &CachedResult{
					Batches: []CachedBatch{{Len: 1}},
				})
				cs.Get(ctx, key)
			}
		}(i)
	}
	wg.Wait()

	stats := cs.Stats()
	if stats.EntryCount == 0 {
		t.Error("expected some entries after concurrent access")
	}
}

func TestCacheClearCommand(t *testing.T) {
	dir, err := os.MkdirTemp("", "lynxdb-cache-clear-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cs := NewStore(dir, 1<<20, time.Hour)
	ctx := context.Background()

	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}
	cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	if err := cs.Clear(); err != nil {
		t.Fatal(err)
	}

	stats := cs.Stats()
	if stats.EntryCount != 0 {
		t.Errorf("expected 0 entries after clear, got %d", stats.EntryCount)
	}
}

func TestCacheStats(t *testing.T) {
	cs := NewStore("", 1<<20, time.Hour)
	ctx := context.Background()

	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}
	cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	// Hit
	cs.Get(ctx, key)
	// Miss
	cs.Get(ctx, Key{QueryHash: 999})

	stats := cs.Stats()
	if stats.Hits != 1 {
		t.Errorf("hits: got %d, want 1", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("misses: got %d, want 1", stats.Misses)
	}
	if stats.HitRate != 0.5 {
		t.Errorf("hit rate: got %v, want 0.5", stats.HitRate)
	}
}

func BenchmarkCacheEviction(b *testing.B) {
	cs := NewStore("", 1<<30, time.Hour)
	cs.clock = newClockBuffer(1024)
	ctx := context.Background()

	// Pre-fill cache to capacity.
	for i := 0; i < 1024; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(1024 + i)}
		cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	}
}

func BenchmarkCacheHitVsFreshScan(b *testing.B) {
	cs := NewStore("", 1<<30, time.Hour)
	ctx := context.Background()

	// Simulate a realistic cached result: 1000 events with 5 columns
	batches := make([]CachedBatch, 1)
	cols := map[string][]CachedValue{
		"_time":  make([]CachedValue, 1000),
		"host":   make([]CachedValue, 1000),
		"status": make([]CachedValue, 1000),
		"method": make([]CachedValue, 1000),
		"_raw":   make([]CachedValue, 1000),
	}
	for i := 0; i < 1000; i++ {
		cols["_time"][i] = CachedValue{Type: 5, Num: int64(1700000000 + i)}
		cols["host"][i] = CachedValue{Type: 1, Str: "web-01"}
		cols["status"][i] = CachedValue{Type: 2, Num: int64(200 + (i%5)*100)}
		cols["method"][i] = CachedValue{Type: 1, Str: "GET"}
		cols["_raw"][i] = CachedValue{Type: 1, Str: "2026-02-10T08:00:00 host=web-01 status=200 method=GET /api/v1/search"}
	}
	batches[0] = CachedBatch{Columns: cols, Len: 1000}

	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}
	cs.Put(ctx, key, &CachedResult{Batches: batches})

	b.Run("CacheHit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cs.Get(ctx, key)
		}
	})

	// Simulate fresh scan: read 1000 events, parse, filter, build batch.
	b.Run("FreshScan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			simulateFreshScan(1000)
		}
	})
}

// EvictBytes tests

func TestCacheEvictBytes(t *testing.T) {
	cs := NewStore("", 1<<30, time.Hour) // large max so evictIfNeeded doesn't interfere
	ctx := context.Background()

	// Insert 100 entries, each ~544 bytes (64 overhead + 10 * 48 per value).
	entrySize := int64(64 + 10*48) // 544 bytes
	for i := 0; i < 100; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{
			Batches: []CachedBatch{{
				Columns: map[string][]CachedValue{"x": make([]CachedValue, 10)},
				Len:     10,
			}},
		})
	}

	totalBefore := cs.Stats().SizeBytes
	halfTotal := totalBefore / 2

	freed := cs.EvictBytes(halfTotal)
	if freed < halfTotal {
		t.Fatalf("expected freed >= %d, got %d", halfTotal, freed)
	}

	// Verify approximately half the entries were evicted.
	after := cs.Stats()
	if after.EntryCount > 60 {
		t.Errorf("expected roughly half entries evicted, still have %d", after.EntryCount)
	}
	_ = entrySize // used for documentation
}

func TestCacheEvictBytesEmpty(t *testing.T) {
	cs := NewStore("", 1<<30, time.Hour)

	freed := cs.EvictBytes(1024)
	if freed != 0 {
		t.Fatalf("expected 0 freed from empty cache, got %d", freed)
	}
}

func TestCacheEvictBytesExceedsTotal(t *testing.T) {
	cs := NewStore("", 1<<30, time.Hour)
	ctx := context.Background()

	// Insert 10 entries.
	for i := 0; i < 10; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{
			Batches: []CachedBatch{{
				Columns: map[string][]CachedValue{"x": make([]CachedValue, 5)},
				Len:     5,
			}},
		})
	}

	totalBefore := cs.Stats().SizeBytes

	// Request more than total size.
	freed := cs.EvictBytes(totalBefore * 10)

	// Should evict everything.
	if freed != totalBefore {
		t.Fatalf("expected freed=%d (all entries), got %d", totalBefore, freed)
	}

	after := cs.Stats()
	if after.EntryCount != 0 {
		t.Fatalf("expected 0 entries, got %d", after.EntryCount)
	}
	if after.SizeBytes != 0 {
		t.Fatalf("expected 0 bytes, got %d", after.SizeBytes)
	}
}

// Pool integration tests

// mockPool implements PoolReserver for testing.
type mockPool struct {
	mu            sync.Mutex
	reserved      int64
	reserveCalls  int
	releaseCalls  int
	failOnReserve bool
}

func (m *mockPool) ReserveForCache(n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failOnReserve {
		return fmt.Errorf("pool full")
	}
	m.reserved += n
	m.reserveCalls++

	return nil
}

func (m *mockPool) ReleaseCache(n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.reserved -= n
	m.releaseCalls++
}

func (m *mockPool) snapshot() (int64, int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.reserved, m.reserveCalls, m.releaseCalls
}

func TestCachePutUsesGovernor(t *testing.T) {
	cs := NewStore("", 1<<30, time.Hour)
	pool := &mockPool{}
	cs.SetPool(pool)
	ctx := context.Background()

	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}
	cs.Put(ctx, key, &CachedResult{
		Batches: []CachedBatch{{
			Columns: map[string][]CachedValue{"x": {{Type: 2, Num: 1}}},
			Len:     1,
		}},
	})

	reserved, reserveCalls, _ := pool.snapshot()
	if reserved <= 0 {
		t.Fatalf("expected pool reserved > 0, got %d", reserved)
	}
	if reserveCalls != 1 {
		t.Fatalf("expected 1 reserve call, got %d", reserveCalls)
	}
}

func TestCachePutPoolFullSkipsInsert(t *testing.T) {
	cs := NewStore("", 1<<30, time.Hour)
	pool := &mockPool{failOnReserve: true}
	cs.SetPool(pool)
	ctx := context.Background()

	key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: 42}
	err := cs.Put(ctx, key, &CachedResult{
		Batches: []CachedBatch{{Len: 1}},
	})

	// Should not return error — silently dropped.
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Entry should not be in cache.
	got, _ := cs.Get(ctx, key)
	if got != nil {
		t.Fatal("entry should not be cached when pool is full")
	}
}

func TestCacheEvictionReleasesPool(t *testing.T) {
	// Small cache that will evict via CLOCK.
	cs := NewStore("", 512, time.Hour)
	pool := &mockPool{}
	cs.SetPool(pool)
	ctx := context.Background()

	// Insert enough to trigger eviction.
	for i := 0; i < 50; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{
			Batches: []CachedBatch{{
				Columns: map[string][]CachedValue{"x": make([]CachedValue, 5)},
				Len:     5,
			}},
		})
	}

	_, _, releaseCalls := pool.snapshot()
	if releaseCalls == 0 {
		t.Fatal("expected some release calls from eviction")
	}
}

func TestCacheClearReleasesPool(t *testing.T) {
	cs := NewStore("", 1<<30, time.Hour)
	pool := &mockPool{}
	cs.SetPool(pool)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})
	}

	reservedBefore, _, _ := pool.snapshot()
	if reservedBefore <= 0 {
		t.Fatal("expected some pool reservation")
	}

	cs.Clear()

	reservedAfter, _, _ := pool.snapshot()
	if reservedAfter != 0 {
		t.Fatalf("expected pool released to 0 after clear, got %d", reservedAfter)
	}
}

func TestCacheOnFlushReleasesPool(t *testing.T) {
	cs := NewStore("", 1<<30, time.Hour)
	pool := &mockPool{}
	cs.SetPool(pool)
	ctx := context.Background()

	// Insert memtable entry (empty segment ID).
	key := Key{IndexName: "idx", SegmentID: "", QueryHash: 1}
	cs.Put(ctx, key, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	// Insert segment entry (should survive).
	segKey := Key{IndexName: "idx", SegmentID: "seg-keep", QueryHash: 2}
	cs.Put(ctx, segKey, &CachedResult{Batches: []CachedBatch{{Len: 1}}})

	reservedBefore, _, _ := pool.snapshot()

	cs.OnFlush([]string{"seg-new"})

	reservedAfter, _, _ := pool.snapshot()
	// Should have released the memtable entry but kept the segment entry.
	if reservedAfter >= reservedBefore {
		t.Fatalf("expected pool reservation to decrease after flush, before=%d after=%d",
			reservedBefore, reservedAfter)
	}
	if reservedAfter <= 0 {
		t.Fatal("segment entry should still hold pool reservation")
	}
}

func BenchmarkCacheEvictBytes(b *testing.B) {
	cs := NewStore("", 1<<30, time.Hour)
	ctx := context.Background()

	// Pre-fill with 10K entries.
	for i := 0; i < 10000; i++ {
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(i)}
		cs.Put(ctx, key, &CachedResult{
			Batches: []CachedBatch{{
				Columns: map[string][]CachedValue{"x": make([]CachedValue, 10)},
				Len:     10,
			}},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Evict ~10% per iteration and refill.
		cs.EvictBytes(cs.Stats().SizeBytes / 10)
		// Refill one entry.
		key := Key{IndexName: "idx", SegmentID: "seg1", QueryHash: uint64(10000 + i)}
		cs.Put(ctx, key, &CachedResult{
			Batches: []CachedBatch{{
				Columns: map[string][]CachedValue{"x": make([]CachedValue, 10)},
				Len:     10,
			}},
		})
	}
}

// simulateFreshScan simulates the minimum cost of scanning events from a segment.
func simulateFreshScan(n int) []CachedBatch {
	cols := make(map[string][]CachedValue, 5)
	cols["_time"] = make([]CachedValue, 0, n)
	cols["host"] = make([]CachedValue, 0, n)
	cols["status"] = make([]CachedValue, 0, n)
	cols["method"] = make([]CachedValue, 0, n)
	cols["_raw"] = make([]CachedValue, 0, n)

	for i := 0; i < n; i++ {
		cols["_time"] = append(cols["_time"], CachedValue{Type: 5, Num: int64(1700000000 + i)})
		cols["host"] = append(cols["host"], CachedValue{Type: 1, Str: "web-01"})
		cols["status"] = append(cols["status"], CachedValue{Type: 2, Num: int64(200 + (i%5)*100)})
		cols["method"] = append(cols["method"], CachedValue{Type: 1, Str: "GET"})
		cols["_raw"] = append(cols["_raw"], CachedValue{Type: 1, Str: "2026-02-10T08:00:00 host=web-01 status=200 method=GET /api/v1/search"})
	}

	return []CachedBatch{{Columns: cols, Len: n}}
}

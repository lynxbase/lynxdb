package cache

import (
	"sync"
	"testing"
)

func TestProjectionCache_HitMiss(t *testing.T) {
	pc := NewProjectionCache(1 << 20) // 1MB

	// Put string data.
	data := []string{"hello", "world", "foo", "bar"}
	pc.PutStrings("seg1", 0, "_raw", data)

	// Hit: same key.
	got, ok := pc.GetStrings("seg1", 0, "_raw")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got) != len(data) {
		t.Fatalf("expected %d strings, got %d", len(data), len(got))
	}
	for i := range data {
		if got[i] != data[i] {
			t.Errorf("string[%d]: expected %q, got %q", i, data[i], got[i])
		}
	}

	// Miss: different column.
	_, ok = pc.GetStrings("seg1", 0, "_source")
	if ok {
		t.Error("expected cache miss for different column")
	}

	// Miss: different segment.
	_, ok = pc.GetStrings("seg2", 0, "_raw")
	if ok {
		t.Error("expected cache miss for different segment")
	}

	// Miss: different row group.
	_, ok = pc.GetStrings("seg1", 1, "_raw")
	if ok {
		t.Error("expected cache miss for different row group")
	}

	// Check stats.
	stats := pc.Stats()
	if stats.Hits != 1 {
		t.Errorf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 3 {
		t.Errorf("expected 3 misses, got %d", stats.Misses)
	}
	if stats.Entries != 1 {
		t.Errorf("expected 1 entry, got %d", stats.Entries)
	}
}

func TestProjectionCache_CompactionInvalidation(t *testing.T) {
	pc := NewProjectionCache(1 << 20)

	// Add entries for two segments.
	pc.PutStrings("seg-old", 0, "_raw", []string{"a", "b"})
	pc.PutStrings("seg-old", 0, "_source", []string{"c", "d"})
	pc.PutStrings("seg-old", 1, "_raw", []string{"e", "f"})
	pc.PutStrings("seg-keep", 0, "_raw", []string{"g", "h"})

	if pc.Stats().Entries != 4 {
		t.Fatalf("expected 4 entries, got %d", pc.Stats().Entries)
	}

	// Invalidate seg-old (compacted away).
	pc.InvalidateSegment("seg-old")

	// All seg-old entries should be gone.
	if _, ok := pc.GetStrings("seg-old", 0, "_raw"); ok {
		t.Error("seg-old rg0 _raw should be invalidated")
	}
	if _, ok := pc.GetStrings("seg-old", 0, "_source"); ok {
		t.Error("seg-old rg0 _source should be invalidated")
	}
	if _, ok := pc.GetStrings("seg-old", 1, "_raw"); ok {
		t.Error("seg-old rg1 _raw should be invalidated")
	}

	// seg-keep should survive.
	got, ok := pc.GetStrings("seg-keep", 0, "_raw")
	if !ok {
		t.Fatal("seg-keep should still be cached")
	}
	if len(got) != 2 || got[0] != "g" {
		t.Errorf("unexpected seg-keep data: %v", got)
	}

	if pc.Stats().Entries != 1 {
		t.Errorf("expected 1 entry after invalidation, got %d", pc.Stats().Entries)
	}
}

func TestProjectionCache_CLOCKEviction(t *testing.T) {
	// Small cache: 1KB.
	pc := NewProjectionCache(1024)

	// Each entry is roughly 64 + 4*16 + 4*5 = 148 bytes.
	// So ~6 entries should fill 1KB.
	for i := 0; i < 20; i++ {
		data := []string{"aaaa", "bbbb", "cccc", "dddd"}
		pc.PutStrings("seg1", i, "_raw", data)
	}

	stats := pc.Stats()
	if stats.UsedBytes > 1024 {
		t.Errorf("used bytes %d exceeds max %d", stats.UsedBytes, 1024)
	}
	if stats.Evictions == 0 {
		t.Error("expected evictions when exceeding cache size")
	}
	// Some entries should remain.
	if stats.Entries == 0 {
		t.Error("expected some entries to remain after eviction")
	}
}

func TestProjectionCache_Types(t *testing.T) {
	pc := NewProjectionCache(1 << 20)

	// Test int64s.
	int64Data := []int64{100, 200, 300, 400}
	pc.PutInt64s("seg1", 0, "_time", int64Data)

	got64, ok := pc.GetInt64s("seg1", 0, "_time")
	if !ok {
		t.Fatal("expected int64 cache hit")
	}
	if len(got64) != len(int64Data) {
		t.Fatalf("expected %d int64s, got %d", len(int64Data), len(got64))
	}
	for i := range int64Data {
		if got64[i] != int64Data[i] {
			t.Errorf("int64[%d]: expected %d, got %d", i, int64Data[i], got64[i])
		}
	}

	// Test float64s.
	floatData := []float64{1.1, 2.2, 3.3}
	pc.PutFloat64s("seg1", 0, "duration", floatData)

	gotF, ok := pc.GetFloat64s("seg1", 0, "duration")
	if !ok {
		t.Fatal("expected float64 cache hit")
	}
	if len(gotF) != len(floatData) {
		t.Fatalf("expected %d float64s, got %d", len(floatData), len(gotF))
	}
	for i := range floatData {
		if gotF[i] != floatData[i] {
			t.Errorf("float64[%d]: expected %f, got %f", i, floatData[i], gotF[i])
		}
	}

	// Cross-type miss: looking for strings at an int64 key returns nil values.
	gotS, ok := pc.GetStrings("seg1", 0, "_time")
	if !ok {
		t.Fatal("expected cache hit (entry exists)")
	}
	if gotS != nil {
		t.Error("expected nil strings for int64 entry")
	}

	// Verify stats.
	stats := pc.Stats()
	if stats.Entries != 2 {
		t.Errorf("expected 2 entries, got %d", stats.Entries)
	}
}

func TestProjectionCache_EvictBytes(t *testing.T) {
	pc := NewProjectionCache(1 << 30) // large limit

	// Insert 100 entries.
	for i := 0; i < 100; i++ {
		pc.PutInt64s("seg1", i, "_time", make([]int64, 100))
	}

	totalBefore := pc.Stats().UsedBytes
	half := totalBefore / 2

	freed := pc.EvictBytes(half)
	if freed < half {
		t.Fatalf("expected freed >= %d, got %d", half, freed)
	}

	after := pc.Stats()
	if after.Entries >= 100 {
		t.Errorf("expected some entries evicted, still have %d", after.Entries)
	}
}

func TestProjectionCache_ConcurrentAccess(t *testing.T) {
	pc := NewProjectionCache(1 << 20)
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				segID := "seg1"
				if gid%2 == 0 {
					segID = "seg2"
				}
				data := []string{"val"}
				pc.PutStrings(segID, i%10, "_raw", data)
				pc.GetStrings(segID, i%10, "_raw")
			}
		}(g)
	}

	// Also run invalidation concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			pc.InvalidateSegment("seg2")
		}
	}()

	wg.Wait()

	stats := pc.Stats()
	if stats.Hits+stats.Misses == 0 {
		t.Error("expected some cache activity")
	}
}

func TestProjectionCache_Disabled(t *testing.T) {
	pc := NewProjectionCache(0)

	pc.PutStrings("seg1", 0, "_raw", []string{"a"})
	_, ok := pc.GetStrings("seg1", 0, "_raw")
	if ok {
		t.Error("disabled cache should never hit")
	}

	pc.PutInt64s("seg1", 0, "_time", []int64{1})
	_, ok = pc.GetInt64s("seg1", 0, "_time")
	if ok {
		t.Error("disabled cache should never hit")
	}

	pc.PutFloat64s("seg1", 0, "dur", []float64{1.0})
	_, ok = pc.GetFloat64s("seg1", 0, "dur")
	if ok {
		t.Error("disabled cache should never hit")
	}

	// These should be no-ops without panicking.
	pc.InvalidateSegment("seg1")
	pc.EvictBytes(1000)
}

func TestProjectionCache_UpdateExistingEntry(t *testing.T) {
	pc := NewProjectionCache(1 << 20)

	// Insert original.
	pc.PutStrings("seg1", 0, "_raw", []string{"old"})

	// Update with new data.
	pc.PutStrings("seg1", 0, "_raw", []string{"new", "data"})

	got, ok := pc.GetStrings("seg1", 0, "_raw")
	if !ok {
		t.Fatal("expected cache hit after update")
	}
	if len(got) != 2 || got[0] != "new" {
		t.Errorf("expected updated data, got %v", got)
	}

	// Should still be a single entry.
	if pc.Stats().Entries != 1 {
		t.Errorf("expected 1 entry, got %d", pc.Stats().Entries)
	}
}

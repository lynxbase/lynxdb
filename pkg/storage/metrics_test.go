package storage

import (
	"encoding/json"
	"testing"
)

func TestStorageMetrics_Snapshot(t *testing.T) {
	m := NewMetrics()

	m.PartFlushes.Add(3)
	m.PartFlushBytes.Add(8192)

	m.SegmentCount.Store(5)
	m.SegmentTotalBytes.Store(1 << 20)
	m.SegmentReads.Add(50)
	m.SegmentReadBytes.Add(500000)

	m.CompactionRuns.Add(2)
	m.CompactionInputBytes.Add(2 << 20)
	m.CompactionOutputBytes.Add(1 << 20)

	m.CacheHits.Add(80)
	m.CacheMisses.Add(20)
	m.CacheEvictions.Add(5)
	m.CacheSizeBytes.Store(32768)

	m.TieringUploads.Add(1)
	m.TieringUploadBytes.Add(1 << 20)

	snap := m.Snapshot()

	if snap.Flush.Flushes != 3 {
		t.Errorf("Flush.Flushes: %d", snap.Flush.Flushes)
	}
	if snap.Segment.Count != 5 {
		t.Errorf("Segment.Count: %d", snap.Segment.Count)
	}
	if snap.Compaction.Runs != 2 {
		t.Errorf("Compaction.Runs: %d", snap.Compaction.Runs)
	}
	if snap.Cache.HitRate < 0.79 || snap.Cache.HitRate > 0.81 {
		t.Errorf("Cache.HitRate: %f", snap.Cache.HitRate)
	}
	if snap.Tiering.Uploads != 1 {
		t.Errorf("Tiering.Uploads: %d", snap.Tiering.Uploads)
	}
	if snap.UptimeSeconds <= 0 {
		t.Error("UptimeSeconds should be > 0")
	}
}

func TestStorageMetrics_JSON(t *testing.T) {
	m := NewMetrics()

	m.PartFlushes.Add(5)
	m.CacheHits.Add(42)
	m.CacheMisses.Add(8)
	m.SegmentCount.Store(3)

	data, err := m.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	// Parse it back to verify structure.
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Check top-level keys exist.
	for _, key := range []string{"uptime_seconds", "flush", "segment", "compaction", "cache", "tiering"} {
		if _, ok := result[key]; !ok {
			t.Errorf("missing top-level key: %s", key)
		}
	}

	// Check nested values.
	flushMap := result["flush"].(map[string]interface{})
	if flushMap["flushes"].(float64) != 5 {
		t.Errorf("flush.flushes: %v", flushMap["flushes"])
	}

	cacheMap := result["cache"].(map[string]interface{})
	hitRate := cacheMap["hit_rate"].(float64)
	if hitRate < 0.83 || hitRate > 0.85 {
		t.Errorf("cache.hit_rate: %f", hitRate)
	}
}

func TestStorageMetrics_ZeroCacheHitRate(t *testing.T) {
	m := NewMetrics()
	snap := m.Snapshot()
	if snap.Cache.HitRate != 0 {
		t.Errorf("zero queries should have 0 hit rate, got %f", snap.Cache.HitRate)
	}
}

func TestStorageMetrics_QuerySlowTotal(t *testing.T) {
	m := NewMetrics()

	m.QueryTotal.Add(10)
	m.QuerySlowTotal.Add(3)
	m.QueryErrors.Add(1)
	m.QueryTimeouts.Add(1)
	m.QueryCacheHits.Add(5)

	snap := m.Snapshot()

	if snap.Query.Total != 10 {
		t.Errorf("Query.Total: %d", snap.Query.Total)
	}

	if snap.Query.SlowTotal != 3 {
		t.Errorf("Query.SlowTotal: expected 3, got %d", snap.Query.SlowTotal)
	}

	if snap.Query.Errors != 1 {
		t.Errorf("Query.Errors: %d", snap.Query.Errors)
	}

	if snap.Query.Timeouts != 1 {
		t.Errorf("Query.Timeouts: %d", snap.Query.Timeouts)
	}

	if snap.Query.CacheHits != 5 {
		t.Errorf("Query.CacheHits: %d", snap.Query.CacheHits)
	}
}

func TestStorageMetrics_ConcurrentUpdate(t *testing.T) {
	m := NewMetrics()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			m.PartFlushes.Add(1)
			m.CacheHits.Add(1)
			m.SegmentReads.Add(1)
		}
		close(done)
	}()

	// Read concurrently.
	for i := 0; i < 100; i++ {
		_ = m.Snapshot()
	}

	<-done

	snap := m.Snapshot()
	if snap.Flush.Flushes != 10000 {
		t.Errorf("concurrent: Flush.Flushes = %d", snap.Flush.Flushes)
	}
}

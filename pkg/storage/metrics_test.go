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
	for _, key := range []string{"uptime_seconds", "flush", "segment", "compaction", "compaction_levels", "cache", "tiering", "pruning", "ingest", "query"} {
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

func TestStorageMetrics_WriteAmplification(t *testing.T) {
	m := NewMetrics()

	m.CompactionInputBytes.Add(100 << 20)  // 100 MiB in
	m.CompactionOutputBytes.Add(120 << 20) // 120 MiB out

	snap := m.Snapshot()

	// 120/100 = 1.2
	if snap.Compaction.WriteAmplification < 1.19 || snap.Compaction.WriteAmplification > 1.21 {
		t.Errorf("WriteAmplification: expected ~1.2, got %f", snap.Compaction.WriteAmplification)
	}
}

func TestStorageMetrics_WriteAmplificationZeroInput(t *testing.T) {
	m := NewMetrics()
	snap := m.Snapshot()
	if snap.Compaction.WriteAmplification != 0 {
		t.Errorf("WriteAmplification with zero input should be 0, got %f", snap.Compaction.WriteAmplification)
	}
}

func TestStorageMetrics_CompactionLevels(t *testing.T) {
	m := NewMetrics()

	m.CompactionL0ToL1Runs.Add(5)
	m.CompactionL0ToL1Bytes.Add(50 << 20)
	m.CompactionL1ToL2Runs.Add(2)
	m.CompactionL1ToL2Bytes.Add(200 << 20)

	snap := m.Snapshot()

	if snap.CompactionLevels.L0ToL1.Runs != 5 {
		t.Errorf("L0ToL1.Runs: expected 5, got %d", snap.CompactionLevels.L0ToL1.Runs)
	}
	if snap.CompactionLevels.L0ToL1.BytesOut != 50<<20 {
		t.Errorf("L0ToL1.BytesOut: expected %d, got %d", 50<<20, snap.CompactionLevels.L0ToL1.BytesOut)
	}
	if snap.CompactionLevels.L1ToL2.Runs != 2 {
		t.Errorf("L1ToL2.Runs: expected 2, got %d", snap.CompactionLevels.L1ToL2.Runs)
	}
	if snap.CompactionLevels.L1ToL2.BytesOut != 200<<20 {
		t.Errorf("L1ToL2.BytesOut: expected %d, got %d", 200<<20, snap.CompactionLevels.L1ToL2.BytesOut)
	}
}

func TestStorageMetrics_CompactionQueueDepth(t *testing.T) {
	m := NewMetrics()

	m.CompactionQueueDepth.Store(7)

	snap := m.Snapshot()
	if snap.Compaction.QueueDepth != 7 {
		t.Errorf("Compaction.QueueDepth: expected 7, got %d", snap.Compaction.QueueDepth)
	}
}

func TestStorageMetrics_Pruning(t *testing.T) {
	m := NewMetrics()

	m.PruningBloomSkips.Add(10)
	m.PruningTimeSkips.Add(20)
	m.PruningStatSkips.Add(5)
	m.PruningRangeSkips.Add(3)
	m.PruningIndexSkips.Add(8)

	snap := m.Snapshot()

	if snap.Pruning.BloomSkips != 10 {
		t.Errorf("Pruning.BloomSkips: expected 10, got %d", snap.Pruning.BloomSkips)
	}
	if snap.Pruning.TimeSkips != 20 {
		t.Errorf("Pruning.TimeSkips: expected 20, got %d", snap.Pruning.TimeSkips)
	}
	if snap.Pruning.StatSkips != 5 {
		t.Errorf("Pruning.StatSkips: expected 5, got %d", snap.Pruning.StatSkips)
	}
	if snap.Pruning.RangeSkips != 3 {
		t.Errorf("Pruning.RangeSkips: expected 3, got %d", snap.Pruning.RangeSkips)
	}
	if snap.Pruning.IndexSkips != 8 {
		t.Errorf("Pruning.IndexSkips: expected 8, got %d", snap.Pruning.IndexSkips)
	}
	if snap.Pruning.TotalSkips != 46 {
		t.Errorf("Pruning.TotalSkips: expected 46, got %d", snap.Pruning.TotalSkips)
	}
}

func TestStorageMetrics_IngestParseErrors(t *testing.T) {
	m := NewMetrics()

	m.IngestEvents.Add(1000)
	m.IngestParseErrors.Add(12)

	snap := m.Snapshot()
	if snap.Ingest.ParseErrors != 12 {
		t.Errorf("Ingest.ParseErrors: expected 12, got %d", snap.Ingest.ParseErrors)
	}
	if snap.Ingest.Events != 1000 {
		t.Errorf("Ingest.Events: expected 1000, got %d", snap.Ingest.Events)
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

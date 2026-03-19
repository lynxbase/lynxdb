package storage

import (
	"encoding/json"
	"sync/atomic"
	"time"
)

// Metrics collects observability metrics across all storage subsystems.
// Hot-path counters use sync/atomic for lock-free updates.
type Metrics struct {
	// Batcher metrics (direct-to-part model).
	BatcherSizeBytes atomic.Int64 // current buffered bytes in async batcher
	BatcherEvents    atomic.Int64 // current buffered events in async batcher
	PartFlushes      atomic.Int64 // part flush counter
	PartFlushBytes   atomic.Int64 // bytes written by part flushes

	// Segment metrics.
	SegmentCount      atomic.Int64
	SegmentTotalBytes atomic.Int64
	SegmentReads      atomic.Int64
	SegmentReadBytes  atomic.Int64

	// Compaction metrics.
	CompactionRuns        atomic.Int64
	CompactionInputBytes  atomic.Int64
	CompactionOutputBytes atomic.Int64
	CompactionErrors      atomic.Int64
	CompactionQueueDepth  atomic.Int64 // current number of pending compaction jobs

	// Per-level compaction metrics.
	CompactionL0ToL1Runs  atomic.Int64
	CompactionL0ToL1Bytes atomic.Int64
	CompactionL1ToL2Runs  atomic.Int64
	CompactionL1ToL2Bytes atomic.Int64
	CompactionL2ToL3Runs  atomic.Int64
	CompactionL2ToL3Bytes atomic.Int64

	// Cache metrics.
	CacheHits      atomic.Int64
	CacheMisses    atomic.Int64
	CacheEvictions atomic.Int64
	CacheSizeBytes atomic.Int64

	// Tiering metrics.
	TieringUploads       atomic.Int64
	TieringUploadBytes   atomic.Int64
	TieringDownloads     atomic.Int64
	TieringDownloadBytes atomic.Int64

	// Pruning observability.
	PruningBloomSkips atomic.Int64 // segments skipped by bloom filter
	PruningTimeSkips  atomic.Int64 // segments skipped by time range
	PruningStatSkips  atomic.Int64 // segments skipped by column stats
	PruningRangeSkips atomic.Int64 // segments skipped by range predicates
	PruningIndexSkips atomic.Int64 // segments skipped by index name

	// Ingestion metrics.
	IngestEvents      atomic.Int64 // total events ingested
	IngestBatches     atomic.Int64 // total ingest batches processed
	IngestBytes       atomic.Int64 // total raw bytes ingested
	IngestErrors      atomic.Int64 // total ingest errors
	IngestDedupDrops  atomic.Int64 // events dropped by ingest dedup
	IngestParseErrors atomic.Int64 // events with parse failures (non-fatal, continued)

	// Query execution metrics (O2: query observability).
	QueryTotal     atomic.Int64 // total queries submitted
	QueryErrors    atomic.Int64 // queries that ended in error
	QueryTimeouts  atomic.Int64 // queries that exceeded max_query_runtime
	QueryCacheHits atomic.Int64 // queries served from cache (O3)
	QuerySlowTotal atomic.Int64 // queries exceeding slow query threshold

	// Buffer pool metrics (unified buffer manager, populated externally by the engine).
	BufferPoolTotalPages atomic.Int64
	BufferPoolFreePages  atomic.Int64
	BufferPoolHits       atomic.Int64
	BufferPoolMisses     atomic.Int64
	BufferPoolEvictions  atomic.Int64

	// Unified memory pool metrics (populated externally by the engine).
	PoolTotalBytes        atomic.Int64
	PoolQueryAllocated    atomic.Int64
	PoolCacheAllocated    atomic.Int64
	PoolCacheReserveFloor atomic.Int64
	PoolFreeBytes         atomic.Int64
	PoolEvictionCount     atomic.Int64
	PoolEvictionBytes     atomic.Int64
	PoolQueryRejections   atomic.Int64

	startTime time.Time
}

// NewMetrics creates a new metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// MetricsSnapshot is the JSON-serializable form of Metrics.
type MetricsSnapshot struct {
	UptimeSeconds float64 `json:"uptime_seconds"`

	Flush struct {
		Flushes    int64 `json:"flushes"`
		FlushBytes int64 `json:"flush_bytes"`
	} `json:"flush"`

	Segment struct {
		Count      int64 `json:"count"`
		TotalBytes int64 `json:"total_bytes"`
		Reads      int64 `json:"reads"`
		ReadBytes  int64 `json:"read_bytes"`
	} `json:"segment"`

	Compaction struct {
		Runs               int64   `json:"runs"`
		InputBytes         int64   `json:"input_bytes"`
		OutputBytes        int64   `json:"output_bytes"`
		Errors             int64   `json:"errors"`
		QueueDepth         int64   `json:"queue_depth"`
		WriteAmplification float64 `json:"write_amplification"` // output_bytes / input_bytes (ideal = 1.0)
	} `json:"compaction"`

	CompactionLevels struct {
		L0ToL1 struct {
			Runs     int64 `json:"runs"`
			BytesOut int64 `json:"bytes_out"`
		} `json:"l0_to_l1"`
		L1ToL2 struct {
			Runs     int64 `json:"runs"`
			BytesOut int64 `json:"bytes_out"`
		} `json:"l1_to_l2"`
		L2ToL3 struct {
			Runs     int64 `json:"runs"`
			BytesOut int64 `json:"bytes_out"`
		} `json:"l2_to_l3"`
	} `json:"compaction_levels"`

	Cache struct {
		Hits      int64   `json:"hits"`
		Misses    int64   `json:"misses"`
		Evictions int64   `json:"evictions"`
		SizeBytes int64   `json:"size_bytes"`
		HitRate   float64 `json:"hit_rate"`
	} `json:"cache"`

	Tiering struct {
		Uploads       int64 `json:"uploads"`
		UploadBytes   int64 `json:"upload_bytes"`
		Downloads     int64 `json:"downloads"`
		DownloadBytes int64 `json:"download_bytes"`
	} `json:"tiering"`

	Pruning struct {
		BloomSkips int64 `json:"bloom_skips"`
		TimeSkips  int64 `json:"time_skips"`
		StatSkips  int64 `json:"stat_skips"`
		RangeSkips int64 `json:"range_skips"`
		IndexSkips int64 `json:"index_skips"`
		TotalSkips int64 `json:"total_skips"`
	} `json:"pruning"`

	Ingest struct {
		Events      int64 `json:"events"`
		Batches     int64 `json:"batches"`
		Bytes       int64 `json:"bytes"`
		Errors      int64 `json:"errors"`
		DedupDrops  int64 `json:"dedup_drops"`
		ParseErrors int64 `json:"parse_errors"`
	} `json:"ingest"`

	Query struct {
		Total     int64 `json:"total"`
		Errors    int64 `json:"errors"`
		Timeouts  int64 `json:"timeouts"`
		CacheHits int64 `json:"cache_hits"`
		SlowTotal int64 `json:"slow_total"`
	} `json:"query"`

	Pool       *PoolSnapshot       `json:"pool,omitempty"`
	BufferPool *BufferPoolSnapshot `json:"buffer_pool,omitempty"`
}

// BufferPoolSnapshot holds buffer manager metrics.
type BufferPoolSnapshot struct {
	TotalPages int64 `json:"total_pages"`
	FreePages  int64 `json:"free_pages"`
	Hits       int64 `json:"hits"`
	Misses     int64 `json:"misses"`
	Evictions  int64 `json:"evictions"`
}

// PoolSnapshot holds unified memory pool metrics.
type PoolSnapshot struct {
	TotalBytes        int64 `json:"total_bytes"`
	QueryAllocated    int64 `json:"query_allocated_bytes"`
	CacheAllocated    int64 `json:"cache_allocated_bytes"`
	CacheReserveFloor int64 `json:"cache_reserve_floor_bytes"`
	FreeBytes         int64 `json:"free_bytes"`
	EvictionCount     int64 `json:"eviction_count"`
	EvictionBytes     int64 `json:"eviction_bytes"`
	QueryRejections   int64 `json:"query_rejections"`
}

// Snapshot returns a consistent point-in-time view of all metrics.
func (m *Metrics) Snapshot() *MetricsSnapshot {
	snap := &MetricsSnapshot{
		UptimeSeconds: time.Since(m.startTime).Seconds(),
	}

	snap.Flush.Flushes = m.PartFlushes.Load()
	snap.Flush.FlushBytes = m.PartFlushBytes.Load()

	snap.Segment.Count = m.SegmentCount.Load()
	snap.Segment.TotalBytes = m.SegmentTotalBytes.Load()
	snap.Segment.Reads = m.SegmentReads.Load()
	snap.Segment.ReadBytes = m.SegmentReadBytes.Load()

	snap.Compaction.Runs = m.CompactionRuns.Load()
	snap.Compaction.InputBytes = m.CompactionInputBytes.Load()
	snap.Compaction.OutputBytes = m.CompactionOutputBytes.Load()
	snap.Compaction.Errors = m.CompactionErrors.Load()
	snap.Compaction.QueueDepth = m.CompactionQueueDepth.Load()
	if snap.Compaction.InputBytes > 0 {
		snap.Compaction.WriteAmplification = float64(snap.Compaction.OutputBytes) / float64(snap.Compaction.InputBytes)
	}

	snap.CompactionLevels.L0ToL1.Runs = m.CompactionL0ToL1Runs.Load()
	snap.CompactionLevels.L0ToL1.BytesOut = m.CompactionL0ToL1Bytes.Load()
	snap.CompactionLevels.L1ToL2.Runs = m.CompactionL1ToL2Runs.Load()
	snap.CompactionLevels.L1ToL2.BytesOut = m.CompactionL1ToL2Bytes.Load()
	snap.CompactionLevels.L2ToL3.Runs = m.CompactionL2ToL3Runs.Load()
	snap.CompactionLevels.L2ToL3.BytesOut = m.CompactionL2ToL3Bytes.Load()

	snap.Cache.Hits = m.CacheHits.Load()
	snap.Cache.Misses = m.CacheMisses.Load()
	snap.Cache.Evictions = m.CacheEvictions.Load()
	snap.Cache.SizeBytes = m.CacheSizeBytes.Load()
	total := snap.Cache.Hits + snap.Cache.Misses
	if total > 0 {
		snap.Cache.HitRate = float64(snap.Cache.Hits) / float64(total)
	}

	snap.Tiering.Uploads = m.TieringUploads.Load()
	snap.Tiering.UploadBytes = m.TieringUploadBytes.Load()
	snap.Tiering.Downloads = m.TieringDownloads.Load()
	snap.Tiering.DownloadBytes = m.TieringDownloadBytes.Load()

	snap.Pruning.BloomSkips = m.PruningBloomSkips.Load()
	snap.Pruning.TimeSkips = m.PruningTimeSkips.Load()
	snap.Pruning.StatSkips = m.PruningStatSkips.Load()
	snap.Pruning.RangeSkips = m.PruningRangeSkips.Load()
	snap.Pruning.IndexSkips = m.PruningIndexSkips.Load()
	snap.Pruning.TotalSkips = snap.Pruning.BloomSkips + snap.Pruning.TimeSkips +
		snap.Pruning.StatSkips + snap.Pruning.RangeSkips + snap.Pruning.IndexSkips

	snap.Ingest.Events = m.IngestEvents.Load()
	snap.Ingest.Batches = m.IngestBatches.Load()
	snap.Ingest.Bytes = m.IngestBytes.Load()
	snap.Ingest.Errors = m.IngestErrors.Load()
	snap.Ingest.DedupDrops = m.IngestDedupDrops.Load()
	snap.Ingest.ParseErrors = m.IngestParseErrors.Load()

	snap.Query.Total = m.QueryTotal.Load()
	snap.Query.Errors = m.QueryErrors.Load()
	snap.Query.Timeouts = m.QueryTimeouts.Load()
	snap.Query.CacheHits = m.QueryCacheHits.Load()
	snap.Query.SlowTotal = m.QuerySlowTotal.Load()

	// Unified memory pool (only populated when the pool is active).
	if poolTotal := m.PoolTotalBytes.Load(); poolTotal > 0 {
		snap.Pool = &PoolSnapshot{
			TotalBytes:        poolTotal,
			QueryAllocated:    m.PoolQueryAllocated.Load(),
			CacheAllocated:    m.PoolCacheAllocated.Load(),
			CacheReserveFloor: m.PoolCacheReserveFloor.Load(),
			FreeBytes:         m.PoolFreeBytes.Load(),
			EvictionCount:     m.PoolEvictionCount.Load(),
			EvictionBytes:     m.PoolEvictionBytes.Load(),
			QueryRejections:   m.PoolQueryRejections.Load(),
		}
	}

	// Buffer pool (only populated when buffer manager is active).
	if bpTotal := m.BufferPoolTotalPages.Load(); bpTotal > 0 {
		snap.BufferPool = &BufferPoolSnapshot{
			TotalPages: bpTotal,
			FreePages:  m.BufferPoolFreePages.Load(),
			Hits:       m.BufferPoolHits.Load(),
			Misses:     m.BufferPoolMisses.Load(),
			Evictions:  m.BufferPoolEvictions.Load(),
		}
	}

	return snap
}

// MarshalJSON returns the JSON representation of the metrics snapshot.
func (m *Metrics) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Snapshot())
}

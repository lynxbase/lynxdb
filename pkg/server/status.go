package server

import (
	"sort"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/buffer"
	"github.com/OrlovEvgeny/Lynxdb/pkg/cache"
	"github.com/OrlovEvgeny/Lynxdb/pkg/model"
	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage"
)

// MemoryPoolStats returns a point-in-time snapshot of the unified memory pool.
// Returns nil if the engine is running in legacy mode (no unified pool).
func (e *Engine) MemoryPoolStats() *stats.UnifiedPoolStats {
	if e.unifiedPool == nil {
		return nil
	}

	s := e.unifiedPool.Stats()

	return &s
}

// BufferPoolStats returns buffer pool metrics, or nil if buffer manager is disabled.
func (e *Engine) BufferPoolStats() *buffer.PoolStats {
	if e.bufferPool == nil {
		return nil
	}

	s := e.bufferPool.Stats()

	return &s
}

// ClusterStatusInfo holds cluster status data.
type ClusterStatusInfo struct {
	Status         string `json:"status"`
	NodeCount      int    `json:"node_count"`
	IndexCount     int    `json:"index_count"`
	SegmentCount   int    `json:"segment_count"`
	BufferedSize   int64  `json:"buffered_size"`
	BufferedEvents int64  `json:"buffered_events"`
	DataDir        string `json:"data_dir"`
}

// ClusterStatus returns the current cluster status.
func (e *Engine) ClusterStatus() ClusterStatusInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	dataDir := e.dataDir
	if dataDir == "" {
		dataDir = "(in-memory)"
	}

	var bufferedEvents int64
	var bufferedBytes int64
	if e.batcher != nil {
		bufferedEvents = e.batcher.BufferedEvents()
		bufferedBytes = e.batcher.BufferedBytes()
	}

	return ClusterStatusInfo{
		Status:         "healthy",
		NodeCount:      1,
		IndexCount:     len(e.indexes),
		SegmentCount:   len(e.currentEpoch.segments),
		BufferedSize:   bufferedBytes,
		BufferedEvents: bufferedEvents,
		DataDir:        dataDir,
	}
}

// SourceEntry holds a source name and event count.
type SourceEntry struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// StatsInfo holds server statistics.
type StatsInfo struct {
	UptimeSeconds  int64         `json:"uptime_seconds"`
	StorageBytes   int64         `json:"storage_bytes"`
	TotalEvents    int64         `json:"total_events"`
	EventsToday    int64         `json:"events_today"`
	IndexCount     int           `json:"index_count"`
	SegmentCount   int           `json:"segment_count"`
	BufferedEvents int64         `json:"buffered_events"`
	Sources        []SourceEntry `json:"sources"`
	OldestEvent    string        `json:"oldest_event,omitempty"`
}

// Stats returns server statistics.
func (e *Engine) Stats() StatsInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var totalEvents int64
	var oldestEvent time.Time
	var storageBytes int64
	sourceCounts := make(map[string]int64)

	// Count events from segments.
	for _, sh := range e.currentEpoch.segments {
		totalEvents += sh.meta.EventCount
		storageBytes += sh.meta.SizeBytes

		if sh.meta.MinTime.IsZero() {
			continue
		}
		if oldestEvent.IsZero() || sh.meta.MinTime.Before(oldestEvent) {
			oldestEvent = sh.meta.MinTime
		}

		// Count source distribution from segment readers.
		if sh.reader != nil {
			for _, col := range sh.reader.Stats() {
				if col.Name == "source" {
					sourceCounts[col.MinValue] += sh.meta.EventCount
				}
			}
		}
	}

	// No separate buffer to scan — all committed data is in segments.

	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	var eventsToday int64

	// Count today's events from segments.
	for _, sh := range e.currentEpoch.segments {
		if !sh.meta.MaxTime.Before(todayStart) {
			if !sh.meta.MinTime.Before(todayStart) {
				eventsToday += sh.meta.EventCount
			} else if sh.meta.EventCount > 0 {
				totalDur := sh.meta.MaxTime.Sub(sh.meta.MinTime)
				todayDur := sh.meta.MaxTime.Sub(todayStart)
				if totalDur > 0 {
					eventsToday += int64(float64(sh.meta.EventCount) * float64(todayDur) / float64(totalDur))
				}
			}
		}
	}

	// Build source distribution as sorted list.
	sources := make([]SourceEntry, 0, len(sourceCounts))
	for name, count := range sourceCounts {
		sources = append(sources, SourceEntry{Name: name, Count: count})
	}
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].Count > sources[j].Count
	})
	if len(sources) > 10 {
		sources = sources[:10]
	}

	uptime := time.Since(e.startTime)

	info := StatsInfo{
		UptimeSeconds:  int64(uptime.Seconds()),
		StorageBytes:   storageBytes,
		TotalEvents:    totalEvents,
		EventsToday:    eventsToday,
		IndexCount:     len(e.indexes),
		SegmentCount:   len(e.currentEpoch.segments),
		BufferedEvents: e.BufferedEventCount(),
		Sources:        sources,
	}
	if !oldestEvent.IsZero() {
		info.OldestEvent = oldestEvent.Format(time.RFC3339)
	}

	return info
}

// CacheClear clears the query cache.
func (e *Engine) CacheClear() {
	_ = e.cache.Clear()
}

// CacheStats returns cache statistics.
func (e *Engine) CacheStats() cache.Stats {
	return e.cache.Stats()
}

// Metrics returns the storage metrics, populating dynamic values first.
func (e *Engine) Metrics() *storage.Metrics {
	// Populate dynamic metrics before returning.
	e.mu.RLock()
	var batcherBytes, batcherEvents int64
	if e.batcher != nil {
		batcherBytes = e.batcher.BufferedBytes()
		batcherEvents = e.batcher.BufferedEvents()
	}
	e.metrics.BatcherSizeBytes.Store(batcherBytes)
	e.metrics.BatcherEvents.Store(batcherEvents)
	e.metrics.SegmentCount.Store(int64(len(e.currentEpoch.segments)))
	var segBytes int64
	for _, sh := range e.currentEpoch.segments {
		segBytes += sh.meta.SizeBytes
	}
	e.metrics.SegmentTotalBytes.Store(segBytes)
	e.mu.RUnlock()

	// Cache metrics.
	cacheStats := e.cache.Stats()
	e.metrics.CacheHits.Store(cacheStats.Hits)
	e.metrics.CacheMisses.Store(cacheStats.Misses)
	e.metrics.CacheEvictions.Store(cacheStats.Evictions)
	e.metrics.CacheSizeBytes.Store(cacheStats.SizeBytes)

	// Unified memory pool metrics.
	if e.unifiedPool != nil {
		ps := e.unifiedPool.Stats()
		e.metrics.PoolTotalBytes.Store(ps.TotalLimitBytes)
		e.metrics.PoolQueryAllocated.Store(ps.QueryAllocatedBytes)
		e.metrics.PoolCacheAllocated.Store(ps.CacheAllocatedBytes)
		e.metrics.PoolCacheReserveFloor.Store(ps.CacheReserveFloorBytes)
		e.metrics.PoolFreeBytes.Store(ps.FreeBytes)
		e.metrics.PoolEvictionCount.Store(ps.CacheEvictionCount)
		e.metrics.PoolEvictionBytes.Store(ps.CacheEvictionBytes)
		e.metrics.PoolQueryRejections.Store(ps.QueryRejections)
	}

	// Buffer pool metrics (unified buffer manager).
	if e.bufferPool != nil {
		bps := e.bufferPool.Stats()
		e.metrics.BufferPoolTotalPages.Store(int64(bps.TotalPages))
		e.metrics.BufferPoolFreePages.Store(int64(bps.FreePages))
		e.metrics.BufferPoolHits.Store(bps.Hits)
		e.metrics.BufferPoolMisses.Store(bps.Misses)
		e.metrics.BufferPoolEvictions.Store(bps.Evictions)
	}

	return e.metrics
}

// ListIndexes returns all configured indexes.
func (e *Engine) ListIndexes() []model.IndexConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]model.IndexConfig, 0, len(e.indexes))
	for _, idx := range e.indexes {
		result = append(result, idx)
	}

	return result
}

// CreateIndex creates a new index.
func (e *Engine) CreateIndex(cfg model.IndexConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.indexes[cfg.Name] = cfg
}

// FieldInfo holds field metadata for the field catalog.
type FieldInfo = storage.FieldInfo

// ListFields returns the field catalog built from segments.
func (e *Engine) ListFields() []FieldInfo {
	e.mu.RLock()
	defer e.mu.RUnlock()

	fc := storage.NewFieldCatalog()

	// Add stats from segments.
	for _, sh := range e.currentEpoch.segments {
		if sh.reader != nil {
			fc.AddSegmentStats(sh.reader.Stats(), sh.meta.EventCount)
		}
	}

	// No separate buffer to scan — all committed data is in segments.

	return fc.Build()
}

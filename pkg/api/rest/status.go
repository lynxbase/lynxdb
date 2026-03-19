package rest

import (
	"net/http"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	info := s.engine.ClusterStatus()
	respondData(w, http.StatusOK, map[string]interface{}{
		"status":          info.Status,
		"node_count":      info.NodeCount,
		"index_count":     info.IndexCount,
		"segment_count":   info.SegmentCount,
		"buffered_size":   info.BufferedSize,
		"buffered_events": info.BufferedEvents,
		"data_dir":        info.DataDir,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	info := s.engine.Stats()
	response := map[string]interface{}{
		"uptime_seconds":  info.UptimeSeconds,
		"storage_bytes":   info.StorageBytes,
		"total_events":    info.TotalEvents,
		"events_today":    info.EventsToday,
		"index_count":     info.IndexCount,
		"segment_count":   info.SegmentCount,
		"buffered_events": info.BufferedEvents,
		"sources":         info.Sources,
	}
	if info.OldestEvent != "" {
		response["oldest_event"] = info.OldestEvent
	}
	respondData(w, http.StatusOK, response)
}

// handleStatus returns a unified server status combining stats and cluster info.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.engine.Stats()
	cluster := s.engine.ClusterStatus()

	mvTotal := 0
	mvActive := 0
	mvDefs := s.engine.ListMV()
	if mvDefs != nil {
		mvTotal = len(mvDefs)
		for _, d := range mvDefs {
			if d.Status == "active" {
				mvActive++
			}
		}
	}

	data := map[string]interface{}{
		"version":        buildinfo.Version,
		"uptime_seconds": stats.UptimeSeconds,
		"health":         cluster.Status,
		"storage": map[string]interface{}{
			"used_bytes": stats.StorageBytes,
		},
		"events": map[string]interface{}{
			"total": stats.TotalEvents,
			"today": stats.EventsToday,
		},
		"queries": map[string]interface{}{
			"active": s.engine.ActiveJobs(),
		},
		"views": map[string]interface{}{
			"total":  mvTotal,
			"active": mvActive,
		},
		"tail": map[string]interface{}{
			"active_sessions":      s.activeTailSessions.Load(),
			"subscriber_count":     s.engine.EventBus().SubscriberCount(),
			"total_dropped_events": s.engine.EventBus().DroppedEvents(),
		},
	}
	if stats.OldestEvent != "" {
		data["retention"] = map[string]interface{}{
			"oldest_event": stats.OldestEvent,
		}
	}

	// Memory governance stats.
	memorySection := map[string]interface{}{}

	if govStats := s.engine.GovernorStats(); govStats != nil {
		govMap := map[string]interface{}{
			"allocated_bytes": govStats.Allocated,
			"peak_bytes":      govStats.Peak,
			"limit_bytes":     govStats.Limit,
		}
		classes := map[string]interface{}{}
		for i := 0; i < len(govStats.ByClass); i++ {
			cs := govStats.ByClass[i]
			if cs.Allocated > 0 || cs.Peak > 0 || cs.Limit > 0 {
				classes[memgov.MemoryClass(i).String()] = map[string]interface{}{
					"allocated_bytes": cs.Allocated,
					"peak_bytes":      cs.Peak,
					"limit_bytes":     cs.Limit,
				}
			}
		}
		if len(classes) > 0 {
			govMap["by_class"] = classes
		}
		memorySection["governor"] = govMap
	}

	if bmStats := s.engine.BufMgrStats(); bmStats != nil {
		memorySection["buffer_manager"] = map[string]interface{}{
			"total_frames":    bmStats.TotalFrames,
			"free_frames":     bmStats.FreeFrames,
			"clean_frames":    bmStats.CleanFrames,
			"dirty_frames":    bmStats.DirtyFrames,
			"pinned_frames":   bmStats.PinnedFrames,
			"eviction_count":  bmStats.EvictionCount,
			"writeback_count": bmStats.WritebackCount,
			"hit_count":       bmStats.HitCount,
			"miss_count":      bmStats.MissCount,
			"seg_cache_frames": bmStats.SegCacheFrames,
		}
	}

	if len(memorySection) > 0 {
		data["memory"] = memorySection
	}

	respondData(w, http.StatusOK, data)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "healthy"
	if s.degraded.Load() {
		status = "degraded"
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"status":   status,
		"degraded": s.degraded.Load(),
		"version":  buildinfo.Version,
	})
}

func (s *Server) handleCacheClear(w http.ResponseWriter, r *http.Request) {
	s.engine.CacheClear()
	respondData(w, http.StatusOK, map[string]interface{}{
		"status": "cleared",
	})
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	stats := s.engine.CacheStats()
	respondData(w, http.StatusOK, map[string]interface{}{
		"hits":       stats.Hits,
		"misses":     stats.Misses,
		"hit_rate":   stats.HitRate,
		"entries":    stats.EntryCount,
		"size_bytes": stats.SizeBytes,
		"evictions":  stats.Evictions,
	})
}

// handleMetrics returns storage observability metrics as JSON.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.engine.Metrics()
	respondData(w, http.StatusOK, metrics)
}

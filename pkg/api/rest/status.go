package rest

import (
	"net/http"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
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

	if poolStats := s.engine.MemoryPoolStats(); poolStats != nil {
		data["memory_pool"] = poolStats
	}

	if bpStats := s.engine.BufferPoolStats(); bpStats != nil {
		data["buffer_pool"] = bpStats
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

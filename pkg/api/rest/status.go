package rest

import (
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
)

// storeTotals sums the stats of the indexes.
type storeTotals struct {
	events         uint64
	bytes          int64
	parts          int
	oldest, newest time.Time
}

func sumStats(stats []columnar.IndexStats) storeTotals {
	var t storeTotals
	for _, ix := range stats {
		t.events += ix.Rows
		t.bytes += ix.Bytes
		t.parts += ix.Parts
		if ix.Parts == 0 {
			continue
		}
		if t.oldest.IsZero() || ix.First.Before(t.oldest) {
			t.oldest = ix.First
		}
		if ix.Last.After(t.newest) {
			t.newest = ix.Last
		}
	}

	return t
}

// jobCounts counts the jobs the database keeps by their state.
func (s *Server) jobCounts() map[string]int {
	counts := map[string]int{
		string(database.JobRunning):  0,
		string(database.JobDone):     0,
		string(database.JobFailed):   0,
		string(database.JobCanceled): 0,
	}
	for _, job := range s.db.Jobs() {
		counts[string(job.Status().State)]++
	}

	return counts
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	s.syncForRead()
	stats := s.db.Stats()
	totals := sumStats(stats)
	indexes := make([]map[string]interface{}, 0, len(stats))
	for _, ix := range stats {
		indexes = append(indexes, indexEntry(ix))
	}
	response := map[string]interface{}{
		"uptime_seconds": int(time.Since(s.started).Seconds()),
		"storage_bytes":  totals.bytes,
		"total_events":   totals.events,
		"index_count":    len(stats),
		"part_count":     totals.parts,
		"indexes":        indexes,
		"jobs":           s.jobCounts(),
	}
	if !totals.oldest.IsZero() {
		response["oldest_event"] = totals.oldest.Format(time.RFC3339)
		response["newest_event"] = totals.newest.Format(time.RFC3339)
	}
	respondData(w, http.StatusOK, response)
}

// handleStatus returns a unified server status from the stats of the
// indexes and the jobs.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.syncForRead()
	totals := sumStats(s.db.Stats())
	jobs := s.jobCounts()

	data := map[string]interface{}{
		"version":        buildinfo.Version,
		"uptime_seconds": int(time.Since(s.started).Seconds()),
		"health":         "healthy",
		"storage": map[string]interface{}{
			"used_bytes": totals.bytes,
		},
		"events": map[string]interface{}{
			"total": totals.events,
		},
		"queries": map[string]interface{}{
			"active": jobs[string(database.JobRunning)],
		},
	}
	retention := map[string]interface{}{}
	s.cfgMu.RLock()
	if s.runtimeCfg.Retention > 0 {
		retention["policy"] = time.Duration(s.runtimeCfg.Retention).String()
	}
	s.cfgMu.RUnlock()
	if !totals.oldest.IsZero() {
		retention["oldest_event"] = totals.oldest.Format(time.RFC3339)
	}
	if len(retention) > 0 {
		data["retention"] = retention
	}

	respondData(w, http.StatusOK, data)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondData(w, http.StatusOK, map[string]interface{}{
		"status":  "healthy",
		"version": buildinfo.Version,
	})
}

package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/server"
)

// handleGetJob returns the status/results of a job by string ID.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := requirePathValue(r, w, "id")
	if !ok {
		return
	}
	job, ok := s.engine.GetJob(jobID)
	if !ok {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, "job not found")

		return
	}

	snap := job.Snapshot()

	if snap.Status == server.JobStatusRunning {
		data := map[string]interface{}{
			"type": "job", "job_id": snap.ID, "status": server.JobStatusRunning,
			"query": snap.Query,
		}
		if p := job.Progress.Load(); p != nil {
			data["progress"] = p
		}
		respondData(w, http.StatusOK, data)

		return
	}
	if snap.Status == server.JobStatusError || snap.Status == server.JobStatusCanceled {
		errCode := "QUERY_ERROR"
		if snap.ErrorCode != "" {
			errCode = snap.ErrorCode
		}
		respondData(w, http.StatusOK, map[string]interface{}{
			"type": "job", "job_id": snap.ID, "status": snap.Status,
			"error": map[string]interface{}{
				"code":    errCode,
				"message": snap.Error,
			},
		})

		return
	}

	// status == "done" — wrap results in a job envelope so the client
	// always sees {type: "job", status: "done", job_id: "...", results: {...}}.
	var results interface{}
	switch snap.ResultType {
	case server.ResultTypeAggregate, server.ResultTypeTimechart:
		results = buildAggregateResponse(snap.ResultType, snap.Results)
	default:
		defaultLimit := s.queryCfg.DefaultResultLimit
		if defaultLimit == 0 {
			defaultLimit = 1000
		}
		results = buildEventsResponse(snap.Results, defaultLimit, 0)
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"type":         "job",
		"job_id":       snap.ID,
		"status":       "done",
		"query":        snap.Query,
		"created_at":   snap.CreatedAt,
		"completed_at": snap.DoneAt,
		"results":      results,
	},
		WithTookMS(snap.Stats.ElapsedMS),
		WithScanned(snap.Stats.RowsScanned),
		WithQueryID(snap.ID),
		WithSearchStats(searchStatsToMeta(&snap.Stats)))
}

// handleCancelJob cancels a running job.
func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	jobID, ok := requirePathValue(r, w, "id")
	if !ok {
		return
	}
	if !s.engine.CancelJob(jobID) {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, "job not found")

		return
	}

	respondData(w, http.StatusOK, map[string]interface{}{
		"job_id": jobID, "status": "canceled",
	})
}

// handleListJobs returns all active/recent jobs.
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := s.engine.ListJobs()
	entries := make([]map[string]interface{}, len(jobs))
	for i, j := range jobs {
		entries[i] = map[string]interface{}{
			"job_id": j.ID, "query": j.Query,
			"status": j.Status, "created_at": j.CreatedAt,
		}
	}
	respondData(w, http.StatusOK, map[string]interface{}{"jobs": entries})
}

// handleJobStream sends SSE events for job progress and results.
func (s *Server) handleJobStream(w http.ResponseWriter, r *http.Request) {
	jobID, ok := requirePathValue(r, w, "id")
	if !ok {
		return
	}
	job, ok := s.engine.GetJob(jobID)
	if !ok {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, "job not found")

		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		respondInternalError(w, "streaming not supported")

		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	ctx := r.Context()
	enc := json.NewEncoder(w)

	writeSSE := func(event string, data interface{}) {
		fmt.Fprintf(w, "event: %s\n", event)
		fmt.Fprintf(w, "data: ")
		if err := enc.Encode(data); err != nil {
			slog.Warn("rest: job stream json encode failed", "error", err)
		}
		fmt.Fprintf(w, "\n")
		flusher.Flush()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		snap := job.Snapshot()

		switch snap.Status {
		case server.JobStatusRunning:
			progress := map[string]interface{}{
				"phase":   "scanning",
				"percent": 0.0,
			}
			if p := job.Progress.Load(); p != nil {
				total := p.SegmentsTotal
				scanned := p.SegmentsScanned
				pct := 0.0
				if total > 0 {
					pct = float64(scanned) / float64(total) * 100
				}
				progress = map[string]interface{}{
					"phase":          string(p.Phase),
					"scanned":        p.RowsReadSoFar,
					"percent":        pct,
					"events_matched": p.RowsReadSoFar,
					"elapsed_ms":     p.ElapsedMS,
				}
				if pct > 0 {
					progress["eta_ms"] = p.ElapsedMS / pct * (100 - pct)
				}
			}
			writeSSE("progress", progress)

		case server.JobStatusDone:
			var data interface{}
			switch snap.ResultType {
			case server.ResultTypeAggregate, server.ResultTypeTimechart:
				data = buildAggregateResponse(snap.ResultType, snap.Results)
			default:
				defaultLimit := s.queryCfg.DefaultResultLimit
				if defaultLimit == 0 {
					defaultLimit = 1000
				}
				data = buildEventsResponse(snap.Results, defaultLimit, 0)
			}
			writeSSE("complete", data)

			return

		case server.JobStatusError:
			errCode := "QUERY_ERROR"
			if snap.ErrorCode != "" {
				errCode = snap.ErrorCode
			}
			writeSSE("failed", map[string]interface{}{
				"code": errCode, "message": snap.Error,
			})

			return

		case server.JobStatusCanceled:
			progress := map[string]interface{}{}
			if p := job.Progress.Load(); p != nil {
				progress["scanned"] = p.RowsReadSoFar
				progress["elapsed_ms"] = p.ElapsedMS
			}
			writeSSE("canceled", progress)

			return
		}

		select {
		case <-job.Done():
			// Job finished; loop once more to emit terminal event.
			continue
		case <-ticker.C:
			// Periodic progress update.
		case <-ctx.Done():
			return
		}
	}
}

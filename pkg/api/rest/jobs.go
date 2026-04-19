package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/server"
)

func normalizeJobStatusFilter(raw string) (string, error) {
	status := strings.ToLower(strings.TrimSpace(raw))
	switch status {
	case "":
		return "", nil
	case server.JobStatusRunning, server.JobStatusDone, server.JobStatusError, server.JobStatusCanceled:
		return status, nil
	case "complete":
		return server.JobStatusDone, nil
	case "failed":
		return server.JobStatusError, nil
	case "cancelled":
		return server.JobStatusCanceled, nil
	default:
		return "", fmt.Errorf(
			"invalid job status %q; expected one of running, done, error, canceled, complete, failed, cancelled",
			raw,
		)
	}
}

// handleGetJob returns the status/results of a job by string ID.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
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
	defaultLimit := s.currentQueryConfig().DefaultResultLimit
	if defaultLimit == 0 {
		defaultLimit = 1000
	}
	var results interface{}
	switch snap.ResultType {
	case server.ResultTypeAggregate, server.ResultTypeTimechart:
		results = buildAggregateResponse(snap.ResultType, snap.Results, defaultLimit, 0)
	default:
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
	if !s.requireScope(w, r, auth.ScopeAdmin) {
		return
	}
	jobID, ok := requirePathValue(r, w, "id")
	if !ok {
		return
	}
	result, ok := s.engine.CancelJob(jobID)
	if !ok {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, "job not found")

		return
	}

	data := map[string]interface{}{
		"job_id":   jobID,
		"status":   result.Snapshot.Status,
		"canceled": result.Canceled,
	}
	if !result.Snapshot.DoneAt.IsZero() {
		data["completed_at"] = result.Snapshot.DoneAt
	}
	respondData(w, http.StatusOK, data)
}

// handleListJobs returns all active/recent jobs.
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	status, err := normalizeJobStatusFilter(r.URL.Query().Get("status"))
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	}
	jobs := s.engine.ListJobs(status)
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
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
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

	var lastPreviewVersion int64

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
					"scanned":        p.SegmentsScanned,
					"segments_total": p.SegmentsTotal,
					"percent":        pct,
					"events_matched": p.RowsReadSoFar,
					"elapsed_ms":     p.ElapsedMS,
				}
				if pct > 0 {
					progress["eta_ms"] = p.ElapsedMS / pct * (100 - pct)
				}
			}
			if snap := job.Preview.Load(); snap != nil && snap.Version > lastPreviewVersion {
				progress["preview"] = snap.Rows
				progress["preview_version"] = snap.Version
				lastPreviewVersion = snap.Version
			}
			writeSSE("progress", progress)

		case server.JobStatusDone:
			defaultLimit := s.currentQueryConfig().DefaultResultLimit
			if defaultLimit == 0 {
				defaultLimit = 1000
			}
			var data interface{}
			switch snap.ResultType {
			case server.ResultTypeAggregate, server.ResultTypeTimechart:
				data = buildAggregateResponse(snap.ResultType, snap.Results, defaultLimit, 0)
			default:
				data = buildEventsResponse(snap.Results, defaultLimit, 0)
			}
			// Build stats meta for the complete event.
			// Include took_ms and scanned at the envelope level (matching
			// the sync response structure from writeSyncResultFromUsecase)
			// so the frontend can read server-side execution time and row
			// count without falling back to client-side elapsedMs.
			statsEnvelope := searchStatsToMeta(&snap.Stats)
			writeSSE("complete", map[string]interface{}{
				"data": data,
				"meta": map[string]interface{}{
					"took_ms": snap.Stats.ElapsedMS,
					"scanned": snap.Stats.RowsScanned,
					"stats":   statsEnvelope,
				},
			})

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

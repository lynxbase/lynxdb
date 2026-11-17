package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/query"
)

func normalizeJobStatusFilter(raw string) (database.JobState, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "running":
		return database.JobRunning, nil
	case "done":
		return database.JobDone, nil
	case "error":
		return database.JobFailed, nil
	case "canceled":
		return database.JobCanceled, nil
	default:
		return "", fmt.Errorf("invalid job status %q; expected one of running, done, error, canceled", raw)
	}
}

// jobResultType returns the shape of the rows of a job's query.
func jobResultType(job *database.Job) ResultType {
	c, err := query.Compile(job.Query)
	if err != nil {
		return ResultTypeEvents
	}

	return DetectResultType(c.Plan)
}

func (s *Server) jobResultLimit() int {
	if limit := s.currentQueryConfig().DefaultResultLimit; limit > 0 {
		return limit
	}

	return defaultResultLimit
}

func jobError(err error) map[string]interface{} {
	code, _ := queryErrorCode(err)

	return map[string]interface{}{"code": string(code), "message": err.Error()}
}

// handleGetJob returns the state of a job and, once it is done, its result.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	jobID, ok := requirePathValue(r, w, "id")
	if !ok {
		return
	}
	job, ok := s.db.Job(jobID)
	if !ok {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, "job not found")

		return
	}

	st := job.Status()
	data := map[string]interface{}{
		"type":       "job",
		"job_id":     st.ID,
		"status":     string(st.State),
		"query":      st.Query,
		"created_at": st.Started.UTC(),
	}
	switch st.State {
	case database.JobRunning:
		data["progress"] = jobProgress(st)
		respondData(w, http.StatusOK, data, WithQueryID(st.ID))
	case database.JobFailed, database.JobCanceled:
		data["error"] = jobError(st.Err)
		respondData(w, http.StatusOK, data, WithQueryID(st.ID), WithTook(st.Elapsed))
	default:
		res, _ := job.Result()
		results := appendResultData(nil, res, jobResultType(job), s.jobResultLimit(), 0)
		data["completed_at"] = st.Started.Add(st.Elapsed).UTC()
		data["results"] = json.RawMessage(results)
		respondData(w, http.StatusOK, data,
			WithTook(st.Elapsed), WithQueryID(st.ID), WithRows(st.Rows))
	}
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
	job, ok := s.db.Job(jobID)
	if !ok {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, "job not found")

		return
	}

	running := job.Status().State == database.JobRunning
	job.Cancel()
	select {
	case <-job.Done():
	case <-time.After(time.Second):
	}
	st := job.Status()
	data := map[string]interface{}{
		"job_id":   jobID,
		"status":   string(st.State),
		"canceled": running,
	}
	if st.State != database.JobRunning {
		data["completed_at"] = st.Started.Add(st.Elapsed).UTC()
	}
	respondData(w, http.StatusOK, data)
}

// handleListJobs lists the jobs the database keeps, the latest first.
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	state, err := normalizeJobStatusFilter(r.URL.Query().Get("status"))
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	}
	entries := []map[string]interface{}{}
	for _, job := range s.db.Jobs() {
		st := job.Status()
		if state != "" && st.State != state {
			continue
		}
		entries = append(entries, map[string]interface{}{
			"job_id": st.ID, "query": st.Query,
			"status": string(st.State), "created_at": st.Started.UTC(),
		})
	}
	respondData(w, http.StatusOK, map[string]interface{}{"jobs": entries})
}

// handleJobStream sends SSE progress events with the state and elapsed time
// of a job every second, then its result or error.
func (s *Server) handleJobStream(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	jobID, ok := requirePathValue(r, w, "id")
	if !ok {
		return
	}
	job, ok := s.db.Job(jobID)
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

	writeSSE := func(event string, data []byte) {
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		flusher.Flush()
	}
	writeSSEJSON := func(event string, v interface{}) {
		data, err := json.Marshal(v)
		if err != nil {
			slog.Warn("rest: job stream json encode failed", "error", err)
			return
		}
		writeSSE(event, data)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		st := job.Status()
		switch st.State {
		case database.JobRunning:
			writeSSEJSON("progress", jobProgress(st))
		case database.JobDone:
			res, _ := job.Result()
			meta, err := json.Marshal(buildMeta([]MetaOpt{WithTook(st.Elapsed), WithQueryID(st.ID), WithRows(st.Rows)}))
			if err != nil {
				slog.Warn("rest: job stream json encode failed", "error", err)
				return
			}
			out := append([]byte(`{"data":`), appendResultData(nil, res, jobResultType(job), s.jobResultLimit(), 0)...)
			out = append(append(append(out, `,"meta":`...), meta...), '}')
			writeSSE("complete", out)

			return
		case database.JobFailed:
			writeSSEJSON("failed", jobError(st.Err))

			return
		case database.JobCanceled:
			writeSSEJSON("canceled", jobProgress(st))

			return
		}

		select {
		case <-job.Done():
		case <-ticker.C:
		case <-r.Context().Done():
			return
		}
	}
}

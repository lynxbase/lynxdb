package rest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
)

// ingestPipeline returns the ingest pipeline selected by config.
// When mode is "lightweight", only metadata fields are extracted at ingest time;
// all other fields stay in _raw for query-time extraction via REX/spath.
func (s *Server) ingestPipeline() *pipeline.Pipeline {
	if s.ingestCfg.Mode == "lightweight" {
		return pipeline.LightweightPipeline()
	}
	return pipeline.DefaultPipeline()
}

// scannerBufPool reuses scanner buffers across ingest requests to reduce
// per-request allocations and GC pressure under high concurrency.
var scannerBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64*1024)
		return &buf
	},
}

const structuredIngestSuggestion = "Use POST /api/v1/ingest with a JSON array of objects containing event. Use /api/v1/ingest/raw for newline-delimited logs or NDJSON, and /api/v1/es/_bulk for Elasticsearch bulk payloads."

// respondIngestError maps engine errors to the appropriate HTTP status and error code.
// Returns true if an error response was written, false if err is nil.
func respondIngestError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, server.ErrShuttingDown):
		respondError(w, ErrCodeShuttingDown, http.StatusServiceUnavailable, "server is shutting down")
	case errors.Is(err, part.ErrTooManyParts):
		w.Header().Set("Retry-After", "1")
		respondError(w, ErrCodeBackpressure, http.StatusServiceUnavailable, "ingest backpressure: compaction falling behind")
	default:
		slog.Warn("ingest: internal error", "error", err)
		respondInternalError(w, "ingest failed: internal error")
	}

	return true
}

func (s *Server) handleIngestEvents(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}

	decoder := json.NewDecoder(r.Body)
	batchSize := s.ingestCfg.MaxBatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	pipe := s.ingestPipeline()
	batch := make([]*event.Event, 0, batchSize)
	accepted := 0
	failed := 0

	tok, err := decoder.Token()
	if err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest,
			fmt.Sprintf("invalid JSON: %s", sanitizeErrorMessage(err.Error())),
			WithSuggestion(structuredIngestSuggestion))

		return
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '[' {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest,
			"invalid JSON: /api/v1/ingest expects a top-level JSON array of structured event objects",
			WithSuggestion(structuredIngestSuggestion))
		return
	}

	writeSummary := func(warning string) {
		resp := map[string]interface{}{
			"accepted": accepted,
			"failed":   failed,
		}
		if warning != "" {
			resp["warning"] = warning
		}
		respondData(w, http.StatusOK, resp)
	}

	flushBatch := func() bool {
		if len(batch) == 0 {
			return true
		}

		processed, processErr := pipe.Process(batch)
		if processErr != nil {
			slog.Warn("ingest: pipeline processing failed", "error", processErr, "batch_size", len(batch))
			if accepted == 0 && failed == 0 {
				respondInternalError(w, "ingest processing failed")
				return false
			}
			failed += len(batch)
			batch = batch[:0]

			return true
		}
		if ingestErr := s.engine.IngestContext(r.Context(), processed); ingestErr != nil {
			if accepted == 0 {
				if respondIngestError(w, ingestErr) {
					return false
				}
			}
			failed += len(processed)
			batch = batch[:0]

			return true
		}

		accepted += len(processed)
		batch = batch[:0]

		return true
	}

	for decoder.More() {
		var payload receiver.EventPayload
		if err := decoder.Decode(&payload); err != nil {
			msg := fmt.Sprintf("invalid JSON: %s", sanitizeErrorMessage(err.Error()))
			if accepted == 0 && failed == 0 {
				respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, msg, WithSuggestion(structuredIngestSuggestion))

				return
			}

			writeSummary("Request body contained invalid JSON after earlier events were already accepted.")

			return
		}

		batch = append(batch, payload.ToEvent())
		if len(batch) >= batchSize && !flushBatch() {
			return
		}
	}

	endTok, err := decoder.Token()
	if err != nil {
		msg := fmt.Sprintf("invalid JSON: %s", sanitizeErrorMessage(err.Error()))
		if accepted == 0 && failed == 0 {
			respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, msg, WithSuggestion(structuredIngestSuggestion))

			return
		}

		writeSummary("Request body ended with invalid JSON after earlier events were already accepted.")

		return
	}
	endDelim, ok := endTok.(json.Delim)
	if !ok || endDelim != ']' {
		msg := "invalid JSON: expected end of array"
		if accepted == 0 && failed == 0 {
			respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, msg, WithSuggestion(structuredIngestSuggestion))

			return
		}

		writeSummary("Request body ended unexpectedly after earlier events were already accepted.")

		return
	}

	if !flushBatch() {
		return
	}

	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		msg := "invalid JSON: trailing data after array"
		if err != nil {
			msg = fmt.Sprintf("invalid JSON: %s", sanitizeErrorMessage(err.Error()))
		}
		if accepted == 0 && failed == 0 {
			respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, msg, WithSuggestion(structuredIngestSuggestion))

			return
		}

		writeSummary("Request body contained trailing data after earlier events were already accepted.")

		return
	}

	writeSummary("")
}

func (s *Server) handleIngestRaw(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}

	source := r.Header.Get("X-Source")
	if source == "" {
		source = "http"
	} else if len(source) > 256 {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest,
			"X-Source header exceeds maximum length of 256 characters")
		return
	}
	sourceType := r.Header.Get("X-Source-Type")
	if sourceType == "" {
		sourceType = "raw"
	} else if len(sourceType) > 256 {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest,
			"X-Source-Type header exceeds maximum length of 256 characters")
		return
	}
	indexName := r.Header.Get("X-Index")
	if indexName == "" {
		indexName = "main"
	} else if len(indexName) > 256 {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest,
			"X-Index header exceeds maximum length of 256 characters")
		return
	}

	buildEvent := func(line string) *event.Event {
		e := event.NewEvent(time.Time{}, line)
		e.Source = source
		e.SourceType = sourceType
		e.Index = indexName

		return e
	}

	accepted, failed, truncated, err := s.processBatched(w, r, buildEvent)
	if err != nil && accepted == 0 {
		return // response already written by processBatched
	}

	resp := map[string]interface{}{
		"accepted": accepted,
		"failed":   failed,
	}
	if truncated {
		resp["truncated"] = true
		resp["warning"] = "Request body exceeded max_body_size limit. Some events were not processed. " +
			"Use smaller batches or increase ingest.max_body_size."
	}
	respondData(w, http.StatusOK, resp)
}

func (s *Server) handleIngestHEC(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}

	var skippedLines int64

	buildEvent := func(line string) *event.Event {
		var hec receiver.HECEvent
		if err := json.Unmarshal([]byte(line), &hec); err != nil {
			skippedLines++

			return nil // skip unparseable lines (Splunk HEC behavior)
		}

		return hec.ToEvent()
	}

	_, _, _, err := s.processBatched(w, r, buildEvent)
	if err != nil {
		return // response already written
	}

	// HEC: Splunk-compatible response format (no envelope).
	// Include skipped_lines count so clients know if parsing failures occurred.
	resp := map[string]interface{}{
		"text": "Success",
		"code": 0,
	}
	if skippedLines > 0 {
		resp["skipped_lines"] = skippedLines
	}
	respondJSON(w, http.StatusOK, resp)
}

// processBatched reads lines from the request body, builds events using buildEvent,
// and ingests them in batches. If buildEvent returns nil, the line is skipped.
//
// H1 fix: tracks accepted/failed counts across batches. If batch N succeeds but batch N+1
// fails, the already-committed events from batch N are reported as accepted (not silently lost).
// Returns (accepted, failed, truncated, error). Truncated is true when the request body
// exceeded MaxBytesReader and was cut short. On a fatal error with 0 accepted, it writes
// the HTTP response directly and returns a non-nil error.
func (s *Server) processBatched(w http.ResponseWriter, r *http.Request, buildEvent func(string) *event.Event) (int, int, bool, error) {
	batchSize := s.ingestCfg.MaxBatchSize
	if batchSize == 0 {
		batchSize = 1000
	}
	scanner := bufio.NewScanner(r.Body)
	bufp := scannerBufPool.Get().(*[]byte)
	maxLineBytes := s.ingestCfg.MaxLineBytes
	if maxLineBytes <= 0 {
		maxLineBytes = 1 << 20 // 1 MB default
	}
	scanner.Buffer(*bufp, maxLineBytes)
	defer scannerBufPool.Put(bufp)

	pipe := s.ingestPipeline()
	batch := make([]*event.Event, 0, batchSize)
	accepted := 0
	failed := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		ev := buildEvent(line)
		if ev == nil {
			continue
		}
		batch = append(batch, ev)

		if len(batch) >= batchSize {
			processed, err := pipe.Process(batch)
			if err != nil {
				slog.Warn("ingest: batch processing failed", "error", err.Error(), "batch_size", len(batch)) //nolint:gosec // structured slog key-value, not format string injection
				failed += len(batch)
				batch = batch[:0]

				continue // pipeline error: skip this batch, try next
			}
			if err := s.engine.IngestContext(r.Context(), processed); err != nil {
				// Retry on WAL backpressure — the ring buffer may drain within
				// one flush cycle (100ms). Three retries at 50ms intervals covers
				// one full flush cycle, turning transient backpressure into a brief
				// pause instead of permanent data loss.
				if errors.Is(err, part.ErrTooManyParts) {
					retried := false
					for attempt := 0; attempt < 3; attempt++ {
						backoff := time.Duration(50<<uint(attempt)) * time.Millisecond // 50ms, 100ms, 200ms
						if sleepErr := sleepWithContext(r.Context(), backoff); sleepErr != nil {
							return accepted, failed, false, sleepErr
						}
						if retryErr := s.engine.IngestContext(r.Context(), processed); retryErr == nil {
							accepted += len(processed)
							retried = true

							break
						}
					}
					if retried {
						batch = batch[:0]

						continue
					}
				}
				failed += len(processed)
				// H1: if we already accepted some events, don't write an error response.
				// Instead, continue counting failures and return partial success.
				if accepted == 0 {
					// First batch failed — check if it's a fatal error.
					if respondIngestError(w, err) {
						return 0, failed, false, err
					}
				}
				batch = batch[:0]

				continue
			}
			accepted += len(processed)
			batch = batch[:0]
		}
	}

	truncated := false
	if err := scanner.Err(); err != nil {
		if accepted == 0 {
			respondError(w, ErrCodeInvalidRequest, http.StatusBadRequest, "request body read error")

			return 0, 0, false, err
		}
		// Partial success: body was truncated (likely MaxBytesReader limit hit).
		truncated = true
		slog.Warn("ingest: request body truncated", //nolint:gosec // err is from bufio.Scanner, not user-controlled
			"accepted_so_far", accepted,
			"error", err.Error())
	}

	// Flush remaining.
	if len(batch) > 0 {
		processed, err := pipe.Process(batch)
		if err != nil {
			failed += len(batch)
		} else if ingestErr := s.engine.IngestContext(r.Context(), processed); ingestErr != nil {
			// Retry on WAL backpressure (same logic as main loop).
			retried := false
			if errors.Is(ingestErr, part.ErrTooManyParts) {
				for attempt := 0; attempt < 3; attempt++ {
					backoff := time.Duration(50<<uint(attempt)) * time.Millisecond
					if sleepErr := sleepWithContext(r.Context(), backoff); sleepErr != nil {
						return accepted, failed, truncated, sleepErr
					}
					if retryErr := s.engine.IngestContext(r.Context(), processed); retryErr == nil {
						accepted += len(processed)
						retried = true

						break
					}
				}
			}
			if !retried {
				failed += len(processed)
				if accepted == 0 {
					if respondIngestError(w, ingestErr) {
						return 0, failed, false, ingestErr
					}
				}
			}
		} else {
			accepted += len(processed)
		}
	}

	return accepted, failed, truncated, nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// sanitizeErrorMessage replaces control characters in error messages to prevent
// log injection. User-provided JSON parse errors may contain arbitrary bytes
// from the input that could confuse log parsers or terminals.
func sanitizeErrorMessage(msg string) string {
	var b strings.Builder
	b.Grow(len(msg))
	for _, r := range msg {
		if r < 0x20 && r != '\n' && r != '\r' && r != '\t' {
			b.WriteRune('?')
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

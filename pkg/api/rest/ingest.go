package rest

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver"
	"github.com/lynxbase/lynxdb/pkg/server"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
)

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
	var payload []receiver.EventPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))

		return
	}

	events := make([]*event.Event, len(payload))
	for i, p := range payload {
		events[i] = p.ToEvent()
	}

	pipe := pipeline.DefaultPipeline()
	processed, err := pipe.Process(events)
	if err != nil {
		slog.Warn("ingest: pipeline processing failed", "error", err)
		respondInternalError(w, "ingest processing failed")

		return
	}

	if respondIngestError(w, s.engine.Ingest(processed)) {
		return
	}

	respondData(w, http.StatusOK, map[string]interface{}{
		"accepted": len(processed),
		"failed":   0,
	})
}

func (s *Server) handleIngestRaw(w http.ResponseWriter, r *http.Request) {
	source := r.Header.Get("X-Source")
	if source == "" {
		source = "http"
	} else if len(source) > 256 {
		slog.Warn("ingest: field truncated", "field", "source", "original_len", len(source))
		source = source[:256]
	}
	sourceType := r.Header.Get("X-Source-Type")
	if sourceType == "" {
		sourceType = "raw"
	} else if len(sourceType) > 256 {
		slog.Warn("ingest: field truncated", "field", "sourcetype", "original_len", len(sourceType))
		sourceType = sourceType[:256]
	}
	indexName := r.Header.Get("X-Index")
	if indexName == "" {
		indexName = "main"
	} else if len(indexName) > 256 {
		slog.Warn("ingest: field truncated", "field", "index", "original_len", len(indexName))
		indexName = indexName[:256]
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
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	pipe := pipeline.DefaultPipeline()
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
			if err := s.engine.Ingest(processed); err != nil {
				// Retry on WAL backpressure — the ring buffer may drain within
				// one flush cycle (100ms). Three retries at 50ms intervals covers
				// one full flush cycle, turning transient backpressure into a brief
				// pause instead of permanent data loss.
				if errors.Is(err, part.ErrTooManyParts) {
					retried := false
					for attempt := 0; attempt < 3; attempt++ {
						backoff := time.Duration(50<<uint(attempt)) * time.Millisecond // 50ms, 100ms, 200ms
						time.Sleep(backoff)
						if retryErr := s.engine.Ingest(processed); retryErr == nil {
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
		} else if ingestErr := s.engine.Ingest(processed); ingestErr != nil {
			// Retry on WAL backpressure (same logic as main loop).
			retried := false
			if errors.Is(ingestErr, part.ErrTooManyParts) {
				for attempt := 0; attempt < 3; attempt++ {
					backoff := time.Duration(50<<uint(attempt)) * time.Millisecond
					time.Sleep(backoff)
					if retryErr := s.engine.Ingest(processed); retryErr == nil {
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

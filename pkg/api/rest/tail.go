package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/planner"
	"github.com/OrlovEvgeny/Lynxdb/pkg/usecases"
)

func (s *Server) handleTail(w http.ResponseWriter, r *http.Request) {
	// Enforce concurrent session limit.
	if maxSessions := s.tailCfg.MaxConcurrentSessions; maxSessions > 0 {
		if s.activeTailSessions.Load() >= int64(maxSessions) {
			w.Header().Set("Retry-After", "5")
			httpError(w, "too many active tail sessions", http.StatusTooManyRequests)

			return
		}
	}

	q := r.URL.Query().Get("q")
	if q == "" {
		httpError(w, "missing required parameter: q", http.StatusBadRequest)

		return
	}

	count := 100
	if v := r.URL.Query().Get("count"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 10000 {
				n = 10000
			}
			count = n
		}
	}
	from := r.URL.Query().Get("from")
	if from == "" {
		from = "-1h"
	}

	plan, err := s.tailService.Plan(usecases.TailRequest{
		Query: q,
		Count: count,
		From:  from,
	})
	if err != nil {
		var pe *planner.ParseError
		var tve *planner.TailValidationError
		switch {
		case errors.As(err, &pe):
			resp := map[string]string{"error": pe.Message}
			if pe.Suggestion != "" {
				resp["suggestion"] = pe.Suggestion
			}
			w.WriteHeader(http.StatusBadRequest)
			if encErr := json.NewEncoder(w).Encode(resp); encErr != nil {
				slog.Warn("rest: tail json encode failed", "error", encErr)
			}
		case errors.As(err, &tve):
			w.WriteHeader(http.StatusUnprocessableEntity)
			if encErr := json.NewEncoder(w).Encode(map[string]interface{}{
				"error":       tve.Error(),
				"unsupported": tve.Unsupported,
			}); encErr != nil {
				slog.Warn("rest: tail json encode failed", "error", encErr)
			}
		default:
			httpError(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		httpError(w, "streaming not supported", http.StatusInternalServerError)

		return
	}

	// Set SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Track active session count for lifecycle management.
	s.activeTailSessions.Add(1)
	sessionStart := time.Now()
	var liveEventsSent int64

	logger := s.engine.Logger()
	logger.Info("tail session started",
		"query", q,
		"count", count,
		"from", from,
		"remote", r.RemoteAddr,
		"active_sessions", s.activeTailSessions.Load(),
	)

	defer func() {
		s.activeTailSessions.Add(-1)
		logger.Info("tail session ended",
			"query", q,
			"duration", time.Since(sessionStart).String(),
			"events_sent", liveEventsSent,
			"remote", r.RemoteAddr,
			"active_sessions", s.activeTailSessions.Load(),
		)
	}()

	ctx := r.Context()

	// Enforce max session duration if configured.
	if d := s.tailCfg.MaxSessionDuration; d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	// Subscribe to the EventBus BEFORE running catchup. This guarantees no
	// events are lost between the storage snapshot and the live stream.
	// The returned session's iterator has a dedup cursor so events already
	// included in catchup rows are not duplicated.
	rows, session, err := s.tailService.SubscribeAndCatchup(ctx, plan)
	if err != nil {
		writeSSE(w, "error", map[string]string{"error": err.Error()})
		flusher.Flush()

		return
	}
	defer session.Cleanup()

	// Stream catchup results.
	for _, row := range rows {
		writeSSE(w, "result", rowToJSON(row))
	}
	writeSSE(w, "catchup_done", map[string]int{"count": len(rows)})
	flusher.Flush()

	// Live — stream new events through the SPL2 pipeline.
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	var lastDropped int64

	for {
		// Check for client disconnect or session timeout before pulling next batch.
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				writeSSE(w, "close", map[string]string{"reason": "max_session_duration"})
				flusher.Flush()
			}

			return
		case <-heartbeat.C:
			writeSSE(w, "heartbeat", map[string]string{"ts": time.Now().UTC().Format(time.RFC3339)})
			flusher.Flush()

			continue
		default:
		}

		batch, err := session.Iter.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					writeSSE(w, "close", map[string]string{"reason": "max_session_duration"})
					flusher.Flush()
				}

				return
			}
			writeSSE(w, "error", map[string]string{"error": err.Error()})
			flusher.Flush()

			return
		}
		if batch == nil {
			// Channel closed — subscription ended.
			return
		}

		for i := 0; i < batch.Len; i++ {
			writeSSE(w, "result", rowToJSON(batch.Row(i)))
			liveEventsSent++
		}

		// Notify client if events were dropped due to slow consumption.
		if dropped := session.Bus.DroppedEventsForSubscriber(session.SubID); dropped > lastDropped {
			writeSSE(w, "warning", map[string]interface{}{
				"type":          "events_dropped",
				"count":         dropped - lastDropped,
				"total_dropped": dropped,
			})
			lastDropped = dropped
		}

		flusher.Flush()
	}
}

// writeSSE writes a single SSE event to the writer.
func writeSSE(w io.Writer, eventType string, data interface{}) {
	fmt.Fprintf(w, "event: %s\n", eventType)
	fmt.Fprint(w, "data: ")
	if err := json.NewEncoder(w).Encode(data); err != nil { // includes trailing \n
		slog.Warn("rest: SSE json encode failed", "event", eventType, "error", err)
	}
	fmt.Fprint(w, "\n")
}

// rowToJSON converts a pipeline row to a JSON-friendly map.
func rowToJSON(row map[string]event.Value) map[string]interface{} {
	m := make(map[string]interface{}, len(row))
	for k, v := range row {
		m[k] = v.Interface()
	}

	return m
}

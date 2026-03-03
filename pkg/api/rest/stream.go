package rest

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/usecases"
)

func (s *Server) handleQueryStream(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}
	query := req.effectiveQuery()
	if query == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query is required")

		return
	}

	start := time.Now()

	iter, stats, err := s.queryService.Stream(r.Context(), usecases.StreamRequest{
		Query: query,
		From:  req.effectiveFrom(),
		To:    req.effectiveTo(),
	})
	if err != nil {
		handlePlanError(w, err)

		return
	}
	defer iter.Close()

	// Set streaming headers before first write.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	total := 0

	for {
		if err := r.Context().Err(); err != nil {
			return // client disconnected
		}

		batch, err := iter.Next(r.Context())
		if err != nil {
			// Write error as last line.
			if encErr := enc.Encode(map[string]interface{}{
				"__error": map[string]interface{}{
					"code":    "STREAM_ERROR",
					"message": err.Error(),
				},
			}); encErr != nil {
				slog.Warn("rest: stream json encode failed", "error", encErr)
			}
			if flusher != nil {
				flusher.Flush()
			}

			return
		}
		if batch == nil {
			break
		}

		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			out := rowToInterface(row)
			if encErr := enc.Encode(out); encErr != nil {
				slog.Warn("rest: stream json encode failed", "error", encErr)
			}
			total++
		}
		if flusher != nil {
			flusher.Flush()
		}
	}

	// Write meta line.
	elapsed := time.Since(start)
	if encErr := enc.Encode(map[string]interface{}{
		"__meta": map[string]interface{}{
			"total":   total,
			"scanned": stats.RowsScanned,
			"took_ms": elapsed.Milliseconds(),
		},
	}); encErr != nil {
		slog.Warn("rest: stream json encode failed", "error", encErr)
	}
	if flusher != nil {
		flusher.Flush()
	}
}

// rowToInterface converts an event.Value map to a plain map for JSON serialization.
func rowToInterface(row map[string]event.Value) map[string]interface{} {
	out := make(map[string]interface{}, len(row))
	for k, v := range row {
		out[k] = v.Interface()
	}

	return out
}

package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/usecases"
)

type queryStreamRequest struct {
	Q         string            `json:"q"`
	Query     string            `json:"query"`
	From      string            `json:"from"`
	To        string            `json:"to"`
	Earliest  string            `json:"earliest"`
	Latest    string            `json:"latest"`
	Variables map[string]string `json:"variables,omitempty"`
	Limit     *int              `json:"limit"`
	Offset    *int              `json:"offset"`
	Format    *string           `json:"format"`
	Wait      *float64          `json:"wait"`
	Profile   *string           `json:"profile"`
}

func (r queryStreamRequest) toQueryRequest() QueryRequest {
	return QueryRequest{
		Q:         r.Q,
		Query:     r.Query,
		From:      r.From,
		To:        r.To,
		Earliest:  r.Earliest,
		Latest:    r.Latest,
		Variables: r.Variables,
	}
}

func (r queryStreamRequest) unsupportedFields() []string {
	fields := make([]string, 0, 5)
	if r.Wait != nil {
		fields = append(fields, "wait")
	}
	if r.Limit != nil {
		fields = append(fields, "limit")
	}
	if r.Offset != nil {
		fields = append(fields, "offset")
	}
	if r.Profile != nil {
		fields = append(fields, "profile")
	}
	if r.Format != nil {
		fields = append(fields, "format")
	}

	return fields
}

func (s *Server) handleQueryStream(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}

	var rawReq queryStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&rawReq); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}
	if unsupported := rawReq.unsupportedFields(); len(unsupported) > 0 {
		respondError(
			w,
			ErrCodeValidationError,
			http.StatusBadRequest,
			fmt.Sprintf("unsupported fields for /query/stream: %s", strings.Join(unsupported, ", ")),
			WithSuggestion("Use only q/query, from/to, earliest/latest, and variables with /query/stream."),
		)

		return
	}
	req := rawReq.toQueryRequest()
	query := req.effectiveQuery()
	if query == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query is required")

		return
	}
	query = substituteVariables(query, req.Variables)
	if !s.checkQueryLength(w, query) {
		return
	}

	if ucErr := spl2.CheckUnsupportedCommands(query); ucErr != nil {
		respondError(w, ErrCodeUnsupportedCommand, http.StatusBadRequest,
			ucErr.Error(), WithSuggestion(ucErr.Hint))

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

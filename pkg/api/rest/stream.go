package rest

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/lynxbase/lynxdb/pkg/api/apicontracts"
	"github.com/lynxbase/lynxdb/pkg/auth"
)

type queryStreamRequest struct {
	Q        string   `json:"q"`
	Query    string   `json:"query"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	Earliest string   `json:"earliest"`
	Latest   string   `json:"latest"`
	Limit    *int     `json:"limit"`
	Offset   *int     `json:"offset"`
	Format   *string  `json:"format"`
	Wait     *float64 `json:"wait"`
}

func (r queryStreamRequest) toQueryRequest() QueryRequest {
	return QueryRequest{
		Q:        r.Q,
		Query:    r.Query,
		From:     r.From,
		To:       r.To,
		Earliest: r.Earliest,
		Latest:   r.Latest,
	}
}

func (r queryStreamRequest) unsupportedFields() []string {
	fields := make([]string, 0, len(apicontracts.QueryStreamUnsupportedFields))
	for _, field := range apicontracts.QueryStreamUnsupportedFields {
		switch field {
		case "wait":
			if r.Wait != nil {
				fields = append(fields, field)
			}
		case "limit":
			if r.Limit != nil {
				fields = append(fields, field)
			}
		case "offset":
			if r.Offset != nil {
				fields = append(fields, field)
			}
		case "format":
			if r.Format != nil {
				fields = append(fields, field)
			}
		}
	}

	return fields
}

// handleQueryStream runs a query and writes its rows as NDJSON, one object
// per line, then a __meta line.
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
			apicontracts.UnsupportedQueryStreamFieldsMessage(unsupported),
			WithSuggestion(apicontracts.QueryStreamUnsupportedFieldsSuggestion),
		)

		return
	}
	req := rawReq.toQueryRequest()
	src := req.effectiveQuery()
	if src == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query is required")

		return
	}
	if !s.checkQueryLength(w, src) {
		return
	}
	cq, ok := compileQuery(w, src, req.effectiveFrom(), req.effectiveTo())
	if !ok {
		return
	}

	start := time.Now()
	if err := s.db.Sync(); err != nil {
		respondQueryFailure(w, err)
		return
	}
	res, err := s.db.Query(r.Context(), cq.c, cq.opts)
	took := time.Since(start)
	errType := ""
	if err != nil {
		code, _ := queryErrorCode(err)
		errType = string(code)
	}
	s.promMetrics.RecordQuery(string(DetectResultType(cq.c.Plan)), took, errType)
	if err != nil {
		respondQueryFailure(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	const flushBytes = 1 << 20
	var buf []byte
	write := func() bool {
		if _, err := w.Write(buf); err != nil {
			slog.Warn("rest: stream write failed", "error", err)
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		buf = buf[:0]
		return true
	}
	n := res.Len()
	for i := 0; i < n; i++ {
		buf = append(res.AppendJSON(buf, i), '\n')
		if len(buf) >= flushBytes && !write() {
			return
		}
	}
	buf = append(buf, `{"__meta":{"total":`...)
	buf = strconv.AppendInt(buf, int64(n), 10)
	buf = append(buf, `,"took_ms":`...)
	buf = strconv.AppendInt(buf, took.Milliseconds(), 10)
	buf = append(buf, "}}\n"...)
	write()
}

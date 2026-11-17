package rest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
	"github.com/lynxbase/lynxdb/pkg/ingest/ndjson"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// maxHeaderValue bounds the X-Index, X-Source, X-Source-Type and X-Host
// headers of raw ingest.
const maxHeaderValue = 256

func ingestPipelineForConfig(cfg config.IngestConfig) *pipeline.Pipeline {
	if cfg.Mode == "lightweight" {
		return pipeline.LightweightPipeline()
	}
	return pipeline.DefaultPipeline()
}

// respondIngestError maps database errors to the appropriate HTTP status and error code.
// Returns true if an error response was written, false if err is nil.
func respondIngestError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, columnar.ErrIndexName):
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
	case errors.Is(err, columnar.ErrClosed), errors.Is(err, colstore.ErrClosed):
		w.Header().Set("Retry-After", "5")
		respondError(w, ErrCodeShuttingDown, http.StatusServiceUnavailable, "server is shutting down")
	case memgov.IsMemoryExhausted(err):
		w.Header().Set("Retry-After", "5")
		respondError(w, ErrCodeBackpressure, http.StatusServiceUnavailable, "ingest backpressure: memory exhausted")
	default:
		slog.Warn("ingest: internal error", "error", err)
		respondInternalError(w, "ingest failed: internal error")
	}

	return true
}

// requestIndex returns the index a request names in ?index= or the X-Index
// header, or the default index.
func requestIndex(r *http.Request, headerFirst bool) string {
	query, header := r.URL.Query().Get("index"), r.Header.Get("X-Index")
	if headerFirst {
		query, header = header, query
	}
	switch {
	case query != "":
		return query
	case header != "":
		return header
	}

	return database.DefaultSource
}

// countLines returns the number of lines of b that hold more than blanks.
func countLines(b []byte) int {
	n := 0
	for len(b) > 0 {
		line := b
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			line, b = b[:i], b[i+1:]
		} else {
			b = nil
		}
		if len(bytes.TrimSpace(line)) > 0 {
			n++
		}
	}

	return n
}

// handleIngestEvents takes NDJSON, one event per line, and appends it to an
// index as it is.
func (s *Server) handleIngestEvents(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}
	index := requestIndex(r, false)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respondError(w, ErrCodeInvalidRequest, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit),
				WithSuggestion("Send smaller batches or raise ingest.max_body_size."))
			return
		}
		respondError(w, ErrCodeInvalidRequest, http.StatusBadRequest, "request body read error")
		return
	}
	lines := countLines(body)
	rejected, err := s.db.Append(index, body)
	if err != nil {
		s.promMetrics.RecordIngestError()
		respondIngestError(w, err)
		return
	}
	accepted := lines - int(rejected)
	s.promMetrics.RecordIngest(accepted, len(body))
	respondData(w, http.StatusOK, map[string]interface{}{
		"accepted": accepted,
		"failed":   rejected,
	})
}

// handleIngestRaw takes lines of log text; the ingest pipeline extracts the
// time and fields of each line that is not a JSON object.
func (s *Server) handleIngestRaw(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}

	source := r.Header.Get("X-Source")
	if source == "" {
		source = "http"
	}
	sourceType := r.Header.Get("X-Source-Type")
	if sourceType == "" {
		sourceType = "raw"
	}
	host := r.Header.Get("X-Host")
	index := requestIndex(r, true)
	for name, v := range map[string]string{"X-Source": source, "X-Source-Type": sourceType, "X-Host": host, "X-Index": index} {
		if len(v) > maxHeaderValue {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest,
				fmt.Sprintf("%s header exceeds maximum length of %d characters", name, maxHeaderValue))
			return
		}
	}

	conv, err := ndjson.NewText("")
	if err != nil {
		respondInternalError(w, err.Error())
		return
	}
	conv.Source, conv.SourceType, conv.Host = source, sourceType, host

	accepted, failed := 0, 0
	var appendErr error
	readErr := conv.Read(r.Body, func(chunk []byte) error {
		lines := countLines(chunk)
		rejected, err := s.db.Append(index, chunk)
		if err != nil {
			appendErr = err
			return err
		}
		accepted += lines - int(rejected)
		failed += int(rejected)
		s.promMetrics.RecordIngest(lines-int(rejected), len(chunk))
		return nil
	})
	if appendErr != nil {
		s.promMetrics.RecordIngestError()
		if accepted == 0 {
			respondIngestError(w, appendErr)
			return
		}
	}
	truncated := false
	if readErr != nil && appendErr == nil {
		if accepted == 0 {
			respondError(w, ErrCodeInvalidRequest, http.StatusBadRequest, "request body read error")
			return
		}
		truncated = true
		slog.Warn("ingest: request body truncated", "accepted_so_far", accepted, "error", readErr.Error()) //nolint:gosec // err is from the body reader, not user-controlled
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
	s.splunkHECHandler(false).HandleEvent(w, r)
}

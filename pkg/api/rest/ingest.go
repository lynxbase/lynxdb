package rest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/database"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
	"github.com/lynxbase/lynxdb/pkg/ingest/ndjson"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
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

// bodyPiece is the input that a body is read in before it is appended.
const bodyPiece = 1 << 20

// pieces holds the buffers that bodies are read into.
var pieces = sync.Pool{New: func() any {
	b := make([]byte, bodyPiece)

	return &b
}}

// readLines reads r in pieces of whole lines, the last of which may lack its
// newline, and calls fn with each. A line longer than a piece grows the
// piece until it holds the line.
func readLines(r io.Reader, fn func(lines []byte) error) error {
	held := pieces.Get().(*[]byte)
	buf := *held
	defer func() {
		if cap(buf) == bodyPiece {
			*held = buf
			pieces.Put(held)
		}
	}()
	n := 0
	for {
		read, err := io.ReadFull(r, buf[n:])
		n += read
		switch {
		case err == io.EOF || err == io.ErrUnexpectedEOF:
			if n == 0 {
				return nil
			}

			return fn(buf[:n])
		case err != nil:
			return err
		}
		end := bytes.LastIndexByte(buf, '\n') + 1
		if end == 0 {
			buf = slices.Grow(buf, len(buf))[:2*len(buf)]

			continue
		}
		if err := fn(buf[:end]); err != nil {
			return err
		}
		n = copy(buf, buf[end:])
	}
}

// handleIngestEvents takes NDJSON, one event per line, and appends it to an
// index as it is. The body is appended as it is read, so the lines before a
// body that turns out too large are taken.
func (s *Server) handleIngestEvents(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}
	index := requestIndex(r, false)
	var accepted, rejected int64
	size := 0
	var appendErr error
	readErr := readLines(r.Body, func(lines []byte) error {
		rows, failed, err := s.db.AppendRows(index, lines)
		accepted, rejected, size = accepted+rows, rejected+failed, size+len(lines)
		appendErr = err

		return err
	})
	if accepted > 0 {
		s.promMetrics.RecordIngest(int(accepted), size)
	}
	if appendErr != nil {
		s.promMetrics.RecordIngestError()
		respondIngestError(w, appendErr)
		return
	}
	if readErr != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(readErr, &tooLarge) {
			msg := fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit)
			if accepted+rejected > 0 {
				msg += fmt.Sprintf("; the %d lines before the limit were taken", accepted+rejected)
			}
			respondError(w, ErrCodeInvalidRequest, http.StatusRequestEntityTooLarge, msg,
				WithSuggestion("Send smaller batches or raise ingest.max_body_size."))
			return
		}
		respondError(w, ErrCodeInvalidRequest, http.StatusBadRequest, "request body read error")
		return
	}
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
		rows, rejected, err := s.db.AppendRows(index, chunk)
		if err != nil {
			appendErr = err
			return err
		}
		accepted += int(rows)
		failed += int(rejected)
		s.promMetrics.RecordIngest(int(rows), len(chunk))
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

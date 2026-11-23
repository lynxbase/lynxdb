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
	"github.com/lynxbase/lynxdb/pkg/ingest/limits"
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
	return respondIngestErrorAfter(w, err, "")
}

// respondIngestErrorAfter is respondIngestError for a body that was appended
// as it was read: taken ends the message with the lines appended before err.
func respondIngestErrorAfter(w http.ResponseWriter, err error, taken string) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, columnar.ErrIndexName):
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error()+taken)
	case errors.Is(err, columnar.ErrClosed), errors.Is(err, colstore.ErrClosed):
		w.Header().Set("Retry-After", "5")
		respondError(w, ErrCodeShuttingDown, http.StatusServiceUnavailable, "server is shutting down"+taken)
	default:
		slog.Warn("ingest: internal error", "error", err)
		respondInternalError(w, "ingest failed: internal error"+taken)
	}

	return true
}

// linesTaken says how many lines of a body were appended before what
// stopped it, for the message of the error; a client that sends the body
// again repeats those lines.
func linesTaken(lines int64, before string) string {
	if lines == 0 {
		return ""
	}

	return fmt.Sprintf("; the %d lines before %s were taken", lines, before)
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

// piece is lines read from a body into the buffer held.
type piece struct {
	held  *[]byte
	lines []byte
}

// readLines reads r in pieces of whole lines, the last of which may lack its
// newline, and calls fn with each while it reads the next, so that reading
// and decoding a body take a core of their own. A line longer than a piece
// grows the piece until it holds the line. It returns the first error of
// fn, or else of reading, once it has stopped reading r; the lines read
// since the last piece are dropped when reading fails, because the last of
// them may be cut.
func readLines(r io.Reader, fn func(lines []byte) error) error {
	free := make(chan *[]byte, 2)
	free <- pieces.Get().(*[]byte)
	free <- pieces.Get().(*[]byte)
	full, stop := make(chan piece), make(chan struct{})
	var readErr error
	go func() {
		defer close(full)
		readErr = fillPieces(r, free, full, stop)
	}()
	var err error
	for p := range full {
		if err == nil {
			if err = fn(p.lines); err != nil {
				close(stop)
			}
		}
		free <- p.held
	}
	for range 2 {
		if held := <-free; cap(*held) == bodyPiece {
			pieces.Put(held)
		}
	}
	if err != nil {
		return err
	}

	return readErr
}

// fillPieces reads r into the buffers of free and sends the lines of each to
// full, keeping the line a buffer ends inside for the next; every buffer it
// takes goes back to free or to full.
func fillPieces(r io.Reader, free chan *[]byte, full chan<- piece, stop <-chan struct{}) error {
	cur, n := <-free, 0
	defer func() {
		if cur != nil {
			free <- cur
		}
	}()
	for {
		buf := *cur
		// io.ReadFull would turn the end of r inside a piece into
		// io.ErrUnexpectedEOF, which is also what a body cut short of its
		// length or of its compressed stream gives.
		var err error
		for n < len(buf) && err == nil {
			var read int
			read, err = r.Read(buf[n:])
			n += read
		}
		switch {
		case err == io.EOF:
			if n > 0 {
				select {
				case full <- piece{cur, buf[:n]}:
					cur = nil
				case <-stop:
				}
			}

			return nil
		case err != nil:
			return err
		}
		end := bytes.LastIndexByte(buf, '\n') + 1
		if end == 0 {
			*cur = slices.Grow(buf, len(buf))[:2*len(buf)]

			continue
		}
		var next *[]byte
		select {
		case next = <-free:
		case <-stop:
			return nil
		}
		if tail := len(buf) - end; tail >= len(*next) {
			*next = slices.Grow((*next)[:0], 2*tail)[:2*tail]
		}
		n = copy(*next, buf[end:])
		select {
		case full <- piece{cur, buf[:end]}:
			cur = next
		case <-stop:
			free <- next

			return nil
		}
	}
}

// handleIngestEvents takes NDJSON, one JSON object per line, and appends it
// to an index as it is; a line that is not a JSON object is counted as
// failed, and a body without one object is refused. The body is appended as
// it is read, so the lines before a body turns out too large, cut short or
// not to decode are taken, and the error says how many.
func (s *Server) handleIngestEvents(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest) {
		return
	}
	index := requestIndex(r, false)
	var accepted, rejected, expired int64
	size := 0
	var appendErr error
	readErr := readLines(r.Body, func(lines []byte) error {
		done, err := s.db.AppendObjects(index, lines)
		accepted, rejected, expired = accepted+done.Rows, rejected+done.Rejected, expired+done.Expired
		size += len(lines)
		appendErr = err

		return err
	})
	if accepted > 0 {
		s.promMetrics.RecordIngest(int(accepted), size)
	}
	if appendErr != nil {
		s.promMetrics.RecordIngestError()
		respondIngestErrorAfter(w, appendErr, linesTaken(accepted+rejected+expired, "the error"))
		return
	}
	if readErr != nil {
		var wire *http.MaxBytesError
		var decoded *limits.TooLargeError
		switch {
		case errors.As(readErr, &wire):
			respondError(w, ErrCodeInvalidRequest, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", wire.Limit)+linesTaken(accepted+rejected+expired, "the limit"),
				WithSuggestion("Send smaller batches or raise ingest.max_body_size."))
		case errors.As(readErr, &decoded):
			respondError(w, ErrCodeInvalidRequest, http.StatusRequestEntityTooLarge,
				decoded.Error()+linesTaken(accepted+rejected+expired, "the limit"),
				WithSuggestion("Send smaller batches or raise ingest.max_body_size."))
		default:
			respondError(w, ErrCodeInvalidRequest, http.StatusBadRequest,
				"request body is cut short or does not decode"+linesTaken(accepted+rejected+expired, "that"))
		}
		return
	}
	if accepted == 0 && rejected > 0 {
		respondError(w, ErrCodeInvalidRequest, http.StatusBadRequest,
			fmt.Sprintf("none of the %d lines is a JSON object", rejected),
			WithSuggestion("Send one JSON object per line, or send log text to /api/v1/ingest/raw."))
		return
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"accepted": accepted,
		"failed":   rejected,
		"expired":  expired,
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

	accepted, failed, expired := 0, 0, 0
	var appendErr error
	readErr := conv.Read(r.Body, func(chunk []byte) error {
		done, err := s.db.AppendLines(index, chunk)
		if err != nil {
			appendErr = err
			return err
		}
		accepted += int(done.Rows)
		failed += int(done.Rejected)
		expired += int(done.Expired)
		s.promMetrics.RecordIngest(int(done.Rows), len(chunk))
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
		"expired":  expired,
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

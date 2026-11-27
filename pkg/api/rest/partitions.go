package rest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
)

func (s *Server) handleListPartitions(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery, auth.ScopeAdmin) {
		return
	}
	parts, err := s.db.ListPartitions(r.Context(), r.PathValue("index"))
	s.partitionResponse(w, map[string]any{"partitions": parts}, err)
}

func (s *Server) handleDropPartition(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeAdmin) {
		return
	}
	start, ok := partitionStart(w, r)
	if !ok {
		return
	}
	p, err := s.db.DropPartition(r.Context(), r.PathValue("index"), start)
	s.partitionResponse(w, p, err)
}

func (s *Server) handleDetachPartition(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeAdmin) {
		return
	}
	start, ok := partitionStart(w, r)
	if !ok {
		return
	}
	var dest struct {
		To string `json:"to"`
	}
	if !decodeArchiveRequest(w, r, &dest) {
		return
	}
	p, err := s.db.DetachPartition(r.Context(), r.PathValue("index"), start, dest.To)
	s.partitionResponse(w, p, err)
}

func (s *Server) handleAttachPartition(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeAdmin) {
		return
	}
	var src struct {
		From string `json:"from"`
	}
	if !decodeArchiveRequest(w, r, &src) {
		return
	}
	p, err := s.db.AttachPartition(r.Context(), r.PathValue("index"), src.From)
	s.partitionResponse(w, p, err)
}

func partitionStart(w http.ResponseWriter, r *http.Request) (time.Time, bool) {
	value := r.PathValue("start")
	t, err := time.Parse(time.DateOnly, value)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, value)
	}
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "start must be a UTC date or RFC3339 timestamp")
		return time.Time{}, false
	}
	return t.UTC(), true
}

func decodeArchiveRequest(w http.ResponseWriter, r *http.Request, dest any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid archive request: "+err.Error())
		return false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "expected one JSON object")
		return false
	}
	return true
}

func (s *Server) partitionResponse(w http.ResponseWriter, data any, err error) {
	switch {
	case err == nil:
		respondData(w, http.StatusOK, data)
	case errors.Is(err, colstore.ErrPartitionNotFound), errors.Is(err, columnar.ErrNoIndex):
		respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())
	case errors.Is(err, colstore.ErrPartitionExists), errors.Is(err, colstore.ErrIncompatibleArchive), errors.Is(err, colstore.ErrArchiveExists):
		respondError(w, ErrCodeConflict, http.StatusConflict, err.Error())
	case errors.Is(err, colstore.ErrInvalidPartition), errors.Is(err, colstore.ErrInvalidArchive):
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
	case errors.Is(err, columnar.ErrClosed), errors.Is(err, colstore.ErrClosed):
		w.Header().Set("Retry-After", "5")
		respondError(w, ErrCodeShuttingDown, http.StatusServiceUnavailable, "server is shutting down")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		respondError(w, ErrCodeInvalidRequest, http.StatusRequestTimeout, err.Error())
	default:
		s.logger.Warn("partition operation failed", "error", err)
		respondInternalError(w, "partition operation failed: internal error")
	}
}

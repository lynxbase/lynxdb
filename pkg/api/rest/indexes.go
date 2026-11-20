package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
)

// indexSettingsRequest is the body of PUT /api/v1/indexes/{index}; a field
// left out takes the server's storage setting.
type indexSettingsRequest struct {
	SortKey    []string `json:"sort_key"`
	TimeFields []string `json:"time_fields"`
	Partition  *string  `json:"partition"`
}

// handleCreateIndex creates an index with the settings of the body, or
// answers with the index when it exists with every setting the body gives.
// PUT /api/v1/indexes/{index}.
func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest, auth.ScopeAdmin) {
		return
	}
	index, ok := requirePathValue(r, w, "index")
	if !ok {
		return
	}
	var req indexSettingsRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON: "+err.Error())

		return
	}
	want := columnar.Settings{SortKey: req.SortKey, TimeFields: req.TimeFields}
	if req.Partition != nil {
		span, err := config.ParseDuration(*req.Partition)
		if err != nil || span <= 0 {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest,
				fmt.Sprintf("partition %q is not a positive span such as 1h or 1d", *req.Partition))

			return
		}
		want.Partition = span.Duration()
	}
	settings, created, err := s.db.CreateIndex(index, want)
	switch {
	case errors.Is(err, columnar.ErrSettings), errors.Is(err, columnar.ErrIndexName):
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	case errors.Is(err, columnar.ErrSettingsConflict):
		respondError(w, ErrCodeConflict, http.StatusConflict, err.Error(),
			WithSuggestion("The settings of an index do not change; create another index for another design."))

		return
	case errors.Is(err, columnar.ErrClosed), errors.Is(err, colstore.ErrClosed):
		w.Header().Set("Retry-After", "5")
		respondError(w, ErrCodeShuttingDown, http.StatusServiceUnavailable, "server is shutting down")

		return
	case err != nil:
		s.logger.Warn("create index failed", "index", index, "error", err)
		respondInternalError(w, "create index failed: internal error")

		return
	}
	stats, ok := s.db.Index(index)
	if !ok {
		stats = columnar.IndexStats{Name: index, Settings: settings}
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	respondData(w, status, indexEntry(stats))
}

// handleGetIndex describes an index with its settings.
// GET /api/v1/indexes/{index}.
func (s *Server) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	index, ok := requirePathValue(r, w, "index")
	if !ok {
		return
	}
	s.syncForRead()
	stats, ok := s.db.Index(index)
	if !ok {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, fmt.Sprintf("index %q not found", index))

		return
	}
	respondData(w, http.StatusOK, indexEntry(stats))
}

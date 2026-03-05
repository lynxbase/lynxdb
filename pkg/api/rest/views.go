package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/storage/views"
	"github.com/lynxbase/lynxdb/pkg/usecases"
)

func (s *Server) handleCreateMV(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string `json:"name"`
		Query     string `json:"query"`
		Retention string `json:"retention"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}
	if req.Name == "" || req.Query == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "name and query are required")

		return
	}

	if err := s.viewService.Create(usecases.CreateViewRequest{
		Name:      req.Name,
		Query:     req.Query,
		Retention: req.Retention,
	}); err != nil {
		if errors.Is(err, views.ErrViewAlreadyExists) {
			respondError(w, ErrCodeAlreadyExists, http.StatusConflict, err.Error())
		} else {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		}

		return
	}

	respondData(w, http.StatusCreated, map[string]interface{}{
		"name":    req.Name,
		"status":  "created",
		"message": "Materialized view created",
	})
}

func (s *Server) handleListMV(w http.ResponseWriter, r *http.Request) {
	defs := s.viewService.List()

	result := make([]map[string]interface{}, len(defs))
	for i, d := range defs {
		result[i] = map[string]interface{}{
			"name":       d.Name,
			"status":     d.Status,
			"query":      d.Query,
			"type":       d.Type,
			"created_at": d.CreatedAt,
			"updated_at": d.UpdatedAt,
		}
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"views": result,
	})
}

func (s *Server) handleGetMV(w http.ResponseWriter, r *http.Request) {
	name, ok := requirePathValue(r, w, "name")
	if !ok {
		return
	}

	detail, err := s.viewService.Get(name)
	if err != nil {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

		return
	}

	resp := map[string]interface{}{
		"name":       detail.Name,
		"status":     detail.Status,
		"query":      detail.Query,
		"type":       detail.Type,
		"filter":     detail.Filter,
		"columns":    detail.Columns,
		"retention":  detail.Retention.String(),
		"created_at": detail.CreatedAt,
		"updated_at": detail.UpdatedAt,
	}
	if detail.BackfillProgress != nil {
		resp["backfill"] = detail.BackfillProgress
	}

	respondData(w, http.StatusOK, resp)
}

func (s *Server) handleDeleteMV(w http.ResponseWriter, r *http.Request) {
	name, ok := requirePathValue(r, w, "name")
	if !ok {
		return
	}

	if err := s.viewService.Delete(name); err != nil {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

		return
	}

	respondData(w, http.StatusOK, map[string]interface{}{
		"status": "dropped",
		"name":   name,
	})
}

func (s *Server) handlePatchView(w http.ResponseWriter, r *http.Request) {
	name, ok := requirePathValue(r, w, "name")
	if !ok {
		return
	}

	var patch struct {
		Retention *string `json:"retention,omitempty"`
		Paused    *bool   `json:"paused,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}

	result, err := s.viewService.Patch(name, usecases.PatchViewRequest{
		Retention: patch.Retention,
		Paused:    patch.Paused,
	})
	if err != nil {
		if errors.Is(err, views.ErrViewNotFound) {
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())
		} else {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		}

		return
	}

	respondData(w, http.StatusOK, map[string]interface{}{
		"name":    result.Name,
		"status":  result.Status,
		"updated": true,
	})
}

func (s *Server) handleViewBackfill(w http.ResponseWriter, r *http.Request) {
	name, ok := requirePathValue(r, w, "name")
	if !ok {
		return
	}

	switch r.Method {
	case http.MethodPost:
		// Trigger a manual backfill.
		if err := s.viewService.TriggerBackfill(name); err != nil {
			if errors.Is(err, views.ErrViewNotFound) {
				respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())
			} else {
				respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
			}

			return
		}

		respondData(w, http.StatusAccepted, map[string]interface{}{
			"name":    name,
			"status":  "backfill",
			"message": "Backfill triggered",
		})
	default:
		// GET: return current backfill status from the view definition.
		detail, err := s.viewService.Get(name)
		if err != nil {
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

			return
		}

		resp := map[string]interface{}{
			"name":       name,
			"status":     detail.Status,
			"updated_at": detail.UpdatedAt,
		}
		if detail.BackfillProgress != nil {
			resp["backfill"] = detail.BackfillProgress
		}

		respondData(w, http.StatusOK, resp)
	}
}

package rest

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/model"
)

func (s *Server) handleListIndexes(w http.ResponseWriter, r *http.Request) {
	indexes := s.engine.ListIndexes()
	result := make([]map[string]interface{}, len(indexes))
	for i, idx := range indexes {
		result[i] = map[string]interface{}{
			"name":               idx.Name,
			"retention_period":   idx.RetentionPeriod.String(),
			"replication_factor": idx.ReplicationFactor,
		}
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"indexes": result,
	})
}

func (s *Server) handleListFields(w http.ResponseWriter, r *http.Request) {
	fields := s.engine.ListFields()
	respondData(w, http.StatusOK, map[string]interface{}{
		"fields": fields,
	})
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string `json:"name"`
		RetentionDays     int    `json:"retention_days,omitempty"`
		ReplicationFactor int    `json:"replication_factor,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}

	cfg := model.DefaultIndexConfig(req.Name)
	if req.RetentionDays > 0 {
		cfg.RetentionPeriod = time.Duration(req.RetentionDays) * 24 * time.Hour
	}
	if req.ReplicationFactor > 0 {
		cfg.ReplicationFactor = req.ReplicationFactor
	}

	if err := cfg.Validate(); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	}

	s.engine.CreateIndex(cfg)

	respondData(w, http.StatusCreated, map[string]interface{}{
		"name":   cfg.Name,
		"status": "created",
	})
}

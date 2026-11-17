package rest

import (
	"net/http"
	"strings"
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
	if prefix := strings.TrimSpace(r.URL.Query().Get("prefix")); prefix != "" {
		filtered := fields[:0]
		for _, field := range fields {
			if strings.HasPrefix(field.Name, prefix) {
				filtered = append(filtered, field)
			}
		}
		fields = filtered
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"fields": fields,
	})
}

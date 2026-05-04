package rest

import (
	"encoding/json"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/ingest/receiver"
)

func (s *Server) handleOTLPLogs(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct == "application/x-protobuf" {
		respondError(w, ErrCodeInvalidRequest, http.StatusUnsupportedMediaType, "protobuf not supported, use application/json")

		return
	}

	var req receiver.OTLPLogsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON: "+err.Error())

		return
	}

	events := req.ToEvents()
	if len(events) == 0 {
		respondJSON(w, http.StatusOK, map[string]interface{}{})

		return
	}

	pipe := s.ingestPipeline()
	processed, err := pipe.Process(events)
	if err != nil {
		respondInternalError(w, err.Error())

		return
	}

	if respondIngestError(w, s.submitShipperEvents(r.Context(), processed)) {
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{})
}

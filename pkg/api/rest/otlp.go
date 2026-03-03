package rest

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/OrlovEvgeny/Lynxdb/pkg/ingest/pipeline"
	"github.com/OrlovEvgeny/Lynxdb/pkg/ingest/receiver"
)

func (s *Server) handleOTLPLogs(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct == "application/x-protobuf" {
		respondError(w, ErrCodeInvalidRequest, http.StatusUnsupportedMediaType, "protobuf not supported, use application/json")

		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "failed to read body")

		return
	}

	var req receiver.OTLPLogsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON: "+err.Error())

		return
	}

	events := req.ToEvents()
	if len(events) == 0 {
		respondJSON(w, http.StatusOK, map[string]interface{}{})

		return
	}

	pipe := pipeline.DefaultPipeline()
	processed, err := pipe.Process(events)
	if err != nil {
		respondInternalError(w, err.Error())

		return
	}

	if respondIngestError(w, s.engine.Ingest(processed)) {
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{})
}

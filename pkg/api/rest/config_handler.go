package rest

import (
	"encoding/json"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/config"
)

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	s.cfgMu.RLock()
	cfg := s.runtimeCfg
	s.cfgMu.RUnlock()
	respondData(w, http.StatusOK, cfg)
}

func (s *Server) handlePatchConfig(w http.ResponseWriter, r *http.Request) {
	var patch map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}

	if len(patch) == 0 {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "empty patch")

		return
	}

	var restartRequired []string

	for key := range patch {
		switch key {
		case "retention", "log_level":
			// Runtime adjustable, no restart needed.
		case "listen":
			restartRequired = append(restartRequired, "listen")
		case "query", "ingest", "storage", "http":
			// Sub-configs are runtime adjustable.
		default:
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, "unknown config key: "+key)

			return
		}
	}

	// Apply patches to a copy of the config.
	s.cfgMu.Lock()
	raw, err := json.Marshal(s.runtimeCfg)
	if err != nil {
		s.cfgMu.Unlock()
		respondError(w, ErrCodeInternalError, http.StatusInternalServerError, "failed to marshal config: "+err.Error())

		return
	}
	var updated config.Config
	if err := json.Unmarshal(raw, &updated); err != nil {
		s.cfgMu.Unlock()
		respondError(w, ErrCodeInternalError, http.StatusInternalServerError, "failed to unmarshal config: "+err.Error())

		return
	}

	patchRaw, err := json.Marshal(patch)
	if err != nil {
		s.cfgMu.Unlock()
		respondError(w, ErrCodeInternalError, http.StatusInternalServerError, "failed to marshal patch: "+err.Error())

		return
	}
	if err := json.Unmarshal(patchRaw, &updated); err != nil {
		s.cfgMu.Unlock()
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "failed to apply patch: "+err.Error())

		return
	}

	// Validate the patched config before applying.
	if err := updated.Validate(); err != nil {
		s.cfgMu.Unlock()
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "invalid config: "+err.Error())

		return
	}

	s.runtimeCfg = &updated
	s.cfgMu.Unlock()

	result := map[string]interface{}{
		"config": updated,
	}
	if len(restartRequired) > 0 {
		result["restart_required"] = restartRequired
	}
	respondData(w, http.StatusOK, result)
}

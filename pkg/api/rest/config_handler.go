package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
)

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeAdmin) {
		return
	}
	s.cfgMu.RLock()
	cfg := s.runtimeCfg
	s.cfgMu.RUnlock()
	respondData(w, http.StatusOK, cfg)
}

func (s *Server) handlePatchConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireRoot(w, r) {
		return
	}
	var patch map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}

	if len(patch) == 0 {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "empty patch")

		return
	}

	for key := range patch {
		switch key {
		case "retention", "log_level":
			// Runtime adjustable, no restart needed.
		case "listen":
			// Known key but requires restart — will be flagged in the response.
		case "query", "ingest", "storage", "http":
			// Sub-configs are runtime adjustable.
		default:
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, "unknown config key: "+key)

			return
		}
	}

	// Apply patches to a copy of the current runtime config.
	s.cfgMu.RLock()
	raw, err := json.Marshal(s.runtimeCfg)
	s.cfgMu.RUnlock()
	if err != nil {
		respondError(w, ErrCodeInternalError, http.StatusInternalServerError, "failed to marshal config: "+err.Error())

		return
	}

	var updated config.Config
	if err := json.Unmarshal(raw, &updated); err != nil {
		respondError(w, ErrCodeInternalError, http.StatusInternalServerError, "failed to unmarshal config: "+err.Error())

		return
	}

	patchRaw, err := json.Marshal(patch)
	if err != nil {
		respondError(w, ErrCodeInternalError, http.StatusInternalServerError, "failed to marshal patch: "+err.Error())

		return
	}
	if err := json.Unmarshal(patchRaw, &updated); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "failed to apply patch: "+err.Error())

		return
	}

	restartRequired, err := s.ReloadConfig(&updated)
	if err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	}

	result := map[string]interface{}{
		"config": updated,
	}
	if len(restartRequired) > 0 {
		result["restart_required"] = restartRequired
	}
	respondData(w, http.StatusOK, result)
}

// ReloadConfig validates the supplied config and applies hot-reloadable fields.
// It runs validation outside the config mutex, then swaps the snapshot under
// the write lock, mirroring the SIGHUP reload path. Returns the list of fields
// that changed but require a restart to take effect (currently listen and
// data_dir).
func (s *Server) ReloadConfig(updated *config.Config) ([]string, error) {
	if updated == nil {
		return nil, fmt.Errorf("nil config")
	}
	if err := updated.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	s.cfgMu.Lock()
	old := s.runtimeCfg
	s.runtimeCfg = updated
	s.cfgMu.Unlock()

	if s.levelVar != nil && old.LogLevel != updated.LogLevel {
		s.levelVar.Set(parseLogLevel(updated.LogLevel))
		if s.logger != nil {
			s.logger.Info("reloaded log_level", "old", old.LogLevel, "new", updated.LogLevel)
		}
	}

	if s.engine != nil {
		s.engine.ReloadConfig(updated)
	}

	if s.logger != nil && old.Retention != updated.Retention {
		s.logger.Info("reloaded retention", "old", old.Retention.String(), "new", updated.Retention.String())
	}

	var restartRequired []string
	if old.Listen != updated.Listen {
		restartRequired = append(restartRequired, "listen")
		if s.logger != nil {
			s.logger.Warn("listen changed, restart required", "old", old.Listen, "new", updated.Listen)
		}
	}
	if old.DataDir != updated.DataDir {
		restartRequired = append(restartRequired, "data_dir")
		if s.logger != nil {
			s.logger.Warn("data_dir changed, restart required", "old", old.DataDir, "new", updated.DataDir)
		}
	}

	return restartRequired, nil
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

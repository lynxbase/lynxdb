package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

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

// ReloadConfig validates the supplied config, applies the runtime-safe subset,
// and returns the fields that changed but still require a restart.
func (s *Server) ReloadConfig(updated *config.Config) ([]string, error) {
	if updated == nil {
		return nil, fmt.Errorf("nil config")
	}
	if err := updated.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	s.cfgMu.Lock()
	old := s.runtimeCfg
	changes := config.ClassifyReloadChanges(old, updated)
	s.runtimeCfg = updated
	s.queryCfg = updated.Query
	s.ingestCfg = updated.Ingest
	s.tailCfg = updated.Tail
	s.shutdownTimeout = updated.HTTP.ShutdownTimeout
	if s.shutdownTimeout == 0 {
		s.shutdownTimeout = 30 * time.Second
	}
	s.alertShutdownTimeout = updated.HTTP.AlertShutdownTimeout
	if s.alertShutdownTimeout == 0 {
		s.alertShutdownTimeout = 10 * time.Second
	}
	if s.httpServer != nil {
		s.httpServer.IdleTimeout = updated.HTTP.IdleTimeout
		if s.httpServer.IdleTimeout == 0 {
			s.httpServer.IdleTimeout = 120 * time.Second
		}
		s.httpServer.ReadHeaderTimeout = updated.HTTP.ReadHeaderTimeout
		if s.httpServer.ReadHeaderTimeout == 0 {
			s.httpServer.ReadHeaderTimeout = 10 * time.Second
		}
	}
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
	if s.queryService != nil {
		s.queryService.ReloadConfig(updated.Query)
	}
	if s.logger != nil {
		for _, field := range changes.RestartRequired {
			s.logger.Warn("config changed but still requires restart", "field", field)
		}
	}

	return changes.RestartRequired, nil
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

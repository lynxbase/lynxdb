package rest

import (
	"bytes"
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

// handlePatchConfig applies a JSON patch of the settings that take effect
// at once. A patch that changes a setting read only at startup is refused,
// so that GET /config reports only settings in effect.
func (s *Server) handlePatchConfig(w http.ResponseWriter, r *http.Request) {
	if !s.requireRoot(w, r) {
		return
	}
	var patch map[string]json.RawMessage
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
		case "listen", "data_dir", "retention", "log_level", "storage", "query", "ingest", "http", "syslog", "tls", "auth":
		default:
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, "unknown config key: "+key)

			return
		}
	}

	// Apply the patch to a copy of the current runtime config.
	s.cfgMu.RLock()
	current := s.runtimeCfg
	raw, err := json.Marshal(current)
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
	dec := json.NewDecoder(bytes.NewReader(patchRaw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&updated); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "failed to apply patch: "+err.Error())

		return
	}

	if restart := config.ClassifyReloadChanges(current, &updated).RestartRequired; len(restart) > 0 {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest,
			"these settings take effect only at startup: "+strings.Join(restart, ", "),
			WithSuggestion("Change them in the config file and restart the server"))

		return
	}

	if _, err := s.ReloadConfig(&updated); err != nil {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())

		return
	}

	s.cfgMu.RLock()
	applied := s.runtimeCfg
	s.cfgMu.RUnlock()
	respondData(w, http.StatusOK, map[string]interface{}{"config": applied})
}

// ReloadConfig validates the supplied config, applies the settings that
// take effect at once, and returns the changed settings that need a restart.
// Those keep their running values, which GET /config reports.
func (s *Server) ReloadConfig(updated *config.Config) ([]string, error) {
	if updated == nil {
		return nil, fmt.Errorf("nil config")
	}
	if err := updated.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	s.cfgMu.Lock()
	old := s.runtimeCfg
	applied, changes := config.Reload(old, updated)
	if err := applied.Validate(); err != nil {
		s.cfgMu.Unlock()

		return nil, fmt.Errorf("invalid config without the settings that need a restart (%s): %w",
			strings.Join(changes.RestartRequired, ", "), err)
	}
	s.runtimeCfg = applied
	s.queryCfg = applied.Query
	s.ingestCfg = applied.Ingest
	s.shutdownTimeout = defaultShutdownTimeout(applied.HTTP.ShutdownTimeout)
	s.cfgMu.Unlock()

	if s.levelVar != nil && old.LogLevel != applied.LogLevel {
		s.levelVar.Set(parseLogLevel(applied.LogLevel))
		if s.logger != nil {
			s.logger.Info("reloaded log_level", "old", old.LogLevel, "new", applied.LogLevel)
		}
	}

	if s.syslogReceiver != nil {
		if err := s.syslogReceiver.ReloadConfig(applied.Syslog); err != nil {
			return nil, fmt.Errorf("reload syslog config: %w", err)
		}
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

package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
)

// handleCreateKey creates a new API key. Requires a root key.
// POST /api/v1/auth/keys.
func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireRoot(w, r) {
		return
	}

	var input struct {
		Name        string `json:"name"`
		Scope       string `json:"scope"`
		Description string `json:"description"`
		ExpiresIn   string `json:"expires_in"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "Invalid JSON body")

		return
	}

	if input.Name == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "name is required")

		return
	}

	// Default scope.
	scope := auth.ScopeFull
	if input.Scope != "" {
		if !auth.ValidScope(input.Scope) {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest,
				fmt.Sprintf("invalid scope %q: must be one of ingest, query, admin, full", input.Scope))
			return
		}
		scope = auth.Scope(input.Scope)
	}

	// Parse expiration.
	var expiresAt time.Time
	if input.ExpiresIn != "" && input.ExpiresIn != "never" {
		dur, err := parseExpiresIn(input.ExpiresIn)
		if err != nil {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest,
				fmt.Sprintf("invalid expires_in %q: %v", input.ExpiresIn, err))
			return
		}
		expiresAt = time.Now().Add(dur)
	}

	created, err := s.keyStore.CreateKeyWithOpts(auth.CreateKeyOpts{
		Name:        input.Name,
		IsRoot:      false,
		Scope:       scope,
		Description: input.Description,
		ExpiresAt:   expiresAt,
	})
	if err != nil {
		respondInternalError(w, "Failed to create API key")

		return
	}

	respondData(w, http.StatusCreated, created)
}

// parseExpiresIn parses a human-readable duration string like "30d", "90d", "1y".
func parseExpiresIn(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "never" {
		return 0, nil
	}

	// Try standard Go duration first (e.g. "24h", "168h").
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Parse custom suffixes: d (days), w (weeks), y (years).
	if len(s) < 2 {
		return 0, fmt.Errorf("unsupported duration format")
	}

	suffix := s[len(s)-1]
	numStr := s[:len(s)-1]
	var num int
	if _, err := fmt.Sscanf(numStr, "%d", &num); err != nil || num <= 0 {
		return 0, fmt.Errorf("unsupported duration format")
	}

	switch suffix {
	case 'd':
		return time.Duration(num) * 24 * time.Hour, nil
	case 'w':
		return time.Duration(num) * 7 * 24 * time.Hour, nil
	case 'y':
		return time.Duration(num) * 365 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported duration suffix %q (use d, w, y, or Go duration)", string(suffix))
	}
}

// handleListKeys lists all API keys (without tokens or hashes). Requires root.
// GET /api/v1/auth/keys.
func (s *Server) handleListKeys(w http.ResponseWriter, r *http.Request) {
	if !s.requireRoot(w, r) {
		return
	}

	keys := s.keyStore.List()

	respondData(w, http.StatusOK, map[string]interface{}{
		"keys": keys,
	})
}

// handleRevokeKey revokes an API key by ID. Requires root.
// DELETE /api/v1/auth/keys/{id}.
func (s *Server) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireRoot(w, r) {
		return
	}

	id := r.PathValue("id")
	if id == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "key ID is required")

		return
	}

	if err := s.keyStore.Revoke(id); err != nil {
		if errors.Is(err, auth.ErrLastRootKey) {
			respondError(w, ErrCodeLastRootKey, http.StatusConflict,
				"Cannot revoke the last root key. Use rotate-root instead.")

			return
		}

		respondError(w, ErrCodeNotFound, http.StatusNotFound, "Key not found")

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleRotateRoot rotates the root key. Requires root.
// POST /api/v1/auth/rotate-root.
func (s *Server) handleRotateRoot(w http.ResponseWriter, r *http.Request) {
	if !s.requireRoot(w, r) {
		return
	}

	// Use the caller's own key as the one to rotate.
	info := auth.KeyInfoFromContext(r.Context())
	if info == nil {
		respondInternalError(w, "Failed to determine current key")

		return
	}

	created, err := s.keyStore.RotateRoot(info.ID)
	if err != nil {
		respondInternalError(w, "Failed to rotate root key")

		return
	}

	respondData(w, http.StatusOK, map[string]interface{}{
		"id":             created.ID,
		"name":           created.Name,
		"prefix":         created.Prefix,
		"token":          created.Token,
		"is_root":        created.IsRoot,
		"revoked_key_id": info.ID,
		"created_at":     created.CreatedAt,
	})
}

// requireRoot checks that the request was authenticated with a root key.
// Writes an error response and returns false if not.
func (s *Server) requireRoot(w http.ResponseWriter, r *http.Request) bool {
	if !auth.IsRoot(r.Context()) {
		respondError(w, ErrCodeForbidden, http.StatusForbidden,
			"This operation requires a root key")

		return false
	}

	return true
}

// authDisabledHandler returns 404 for all auth endpoints when auth is not enabled.
func authDisabledHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		respondError(w, ErrCodeNotFound, http.StatusNotFound, "Resource not found")
	}
}

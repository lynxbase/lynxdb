package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/auth"
)

// handleCreateKey creates a new API key. Requires a root key.
// POST /api/v1/auth/keys.
func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	if !s.requireRoot(w, r) {
		return
	}

	var input struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "Invalid JSON body")

		return
	}

	if input.Name == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "name is required")

		return
	}

	created, err := s.keyStore.CreateKey(input.Name, false)
	if err != nil {
		respondInternalError(w, "Failed to create API key")

		return
	}

	respondData(w, http.StatusCreated, created)
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

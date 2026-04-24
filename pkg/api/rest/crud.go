package rest

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/auth"
)

// CRUDStore defines the interface for a generic CRUD store.
type CRUDStore[T any] interface {
	List() []T
	Get(id string) (*T, error)
	Create(item *T) error
	Update(item *T) error
	Delete(id string) error
}

// CRUDOpts configures the registerCRUD handler builder.
type CRUDOpts[T any, Input any] struct {
	Store       CRUDStore[T]
	IDParam     string             // path parameter name; default "id"
	ConflictErr error              // error indicating duplicate/conflict
	NewEntity   func(Input) *T     // converts validated input to entity
	MergeEntity func(*T, Input) *T // merges input into existing entity

	// ServerRef provides access to the server for auth checks.
	// Required so that write endpoints can enforce scope.
	ServerRef *Server

	// MutateScope is the scope required for create/update/delete operations.
	// If empty, defaults to ScopeAdmin.
	MutateScope auth.Scope
}

// registerCRUD registers standard List/Create/Get/Update/Delete endpoints.
// Input must implement Validate() error. Use CRUDOpts.NewEntity and
// CRUDOpts.MergeEntity to customize entity creation and update logic.
func registerCRUD[T any, Input interface{ Validate() error }](
	mux *http.ServeMux,
	basePath string,
	opts CRUDOpts[T, Input],
) {
	idParam := opts.IDParam
	if idParam == "" {
		idParam = "id"
	}
	mutateScope := opts.MutateScope
	if mutateScope == "" {
		mutateScope = auth.ScopeAdmin
	}
	requireAuth := opts.ServerRef != nil

	mux.HandleFunc("GET "+basePath, func(w http.ResponseWriter, r *http.Request) {
		respondData(w, http.StatusOK, opts.Store.List())
	})

	// POST basePath — create
	mux.HandleFunc("POST "+basePath, func(w http.ResponseWriter, r *http.Request) {
		if requireAuth && !opts.ServerRef.requireScope(w, r, mutateScope) {
			return
		}
		var input Input
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

			return
		}
		if err := input.Validate(); err != nil {
			respondError(w, ErrCodeValidationError, http.StatusUnprocessableEntity, err.Error())

			return
		}
		entity := opts.NewEntity(input)
		if err := opts.Store.Create(entity); err != nil {
			if opts.ConflictErr != nil && errors.Is(err, opts.ConflictErr) {
				respondError(w, ErrCodeAlreadyExists, http.StatusConflict, err.Error())

				return
			}
			respondInternalError(w, err.Error())

			return
		}
		respondData(w, http.StatusCreated, entity)
	})

	// GET basePath/{id} — get by ID
	mux.HandleFunc("GET "+basePath+"/{"+idParam+"}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue(idParam)
		item, err := opts.Store.Get(id)
		if err != nil {
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

			return
		}
		respondData(w, http.StatusOK, item)
	})

	// PUT basePath/{id} — update
	mux.HandleFunc("PUT "+basePath+"/{"+idParam+"}", func(w http.ResponseWriter, r *http.Request) {
		if requireAuth && !opts.ServerRef.requireScope(w, r, mutateScope) {
			return
		}
		id := r.PathValue(idParam)
		existing, err := opts.Store.Get(id)
		if err != nil {
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

			return
		}
		var input Input
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

			return
		}
		if err := input.Validate(); err != nil {
			respondError(w, ErrCodeValidationError, http.StatusUnprocessableEntity, err.Error())

			return
		}
		updated := opts.MergeEntity(existing, input)
		if err := opts.Store.Update(updated); err != nil {
			respondInternalError(w, err.Error())

			return
		}
		respondData(w, http.StatusOK, updated)
	})

	// DELETE basePath/{id} — delete
	mux.HandleFunc("DELETE "+basePath+"/{"+idParam+"}", func(w http.ResponseWriter, r *http.Request) {
		if requireAuth && !opts.ServerRef.requireScope(w, r, mutateScope) {
			return
		}
		id := r.PathValue(idParam)
		if err := opts.Store.Delete(id); err != nil {
			respondError(w, ErrCodeNotFound, http.StatusNotFound, err.Error())

			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

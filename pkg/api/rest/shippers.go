package rest

import (
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/auth"
)

func (s *Server) handleListShippers(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	respondData(w, http.StatusOK, s.shipperRegistry.List())
}

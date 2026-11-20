package rest

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
)

// handleCompactIndex makes the events appended to an index durable, merges
// each of its partitions into one sorted run and answers once that is done.
// POST /api/v1/indexes/{index}/compact.
func (s *Server) handleCompactIndex(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeIngest, auth.ScopeAdmin) {
		return
	}
	index, ok := requirePathValue(r, w, "index")
	if !ok {
		return
	}
	c, found, err := s.db.Compact(index)
	switch {
	case errors.Is(err, columnar.ErrClosed), errors.Is(err, colstore.ErrClosed):
		w.Header().Set("Retry-After", "5")
		respondError(w, ErrCodeShuttingDown, http.StatusServiceUnavailable, "server is shutting down")

		return
	case err != nil:
		s.logger.Warn("compact failed", "index", index, "error", err)
		respondInternalError(w, "compact failed: internal error")

		return
	case !found:
		respondError(w, ErrCodeNotFound, http.StatusNotFound, fmt.Sprintf("index %q not found", index))

		return
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"index":        index,
		"parts_before": c.PartsBefore,
		"parts":        c.Parts,
		"bytes_before": c.BytesBefore,
		"bytes":        c.Bytes,
		"seconds":      c.Took.Seconds(),
	})
}

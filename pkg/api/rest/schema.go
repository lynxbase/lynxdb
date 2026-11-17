package rest

import (
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
)

// indexEntry describes an index from its stats; an index whose events are
// not yet written has no first or last event.
func indexEntry(ix columnar.IndexStats) map[string]interface{} {
	entry := map[string]interface{}{
		"name":         ix.Name,
		"events":       ix.Rows,
		"bytes":        ix.Bytes,
		"filter_bytes": ix.FilterBytes,
		"parts":        ix.Parts,
	}
	if ix.Parts > 0 {
		entry["first_event"] = ix.First.Format(time.RFC3339Nano)
		entry["last_event"] = ix.Last.Format(time.RFC3339Nano)
	}

	return entry
}

func (s *Server) handleListIndexes(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}
	s.syncForRead()
	stats := s.db.Stats()
	indexes := make([]map[string]interface{}, 0, len(stats))
	for _, ix := range stats {
		indexes = append(indexes, indexEntry(ix))
	}
	respondData(w, http.StatusOK, map[string]interface{}{
		"indexes": indexes,
	})
}

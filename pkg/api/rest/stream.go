package rest

import (
	"encoding/json"
	"net/http"

	"github.com/lynxbase/lynxdb/pkg/api/apicontracts"
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/event"
)

type queryStreamRequest struct {
	Q        string   `json:"q"`
	Query    string   `json:"query"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	Earliest string   `json:"earliest"`
	Latest   string   `json:"latest"`
	Limit    *int     `json:"limit"`
	Offset   *int     `json:"offset"`
	Format   *string  `json:"format"`
	Wait     *float64 `json:"wait"`
	Profile  *string  `json:"profile"`
}

func (r queryStreamRequest) toQueryRequest() QueryRequest {
	return QueryRequest{
		Q:        r.Q,
		Query:    r.Query,
		From:     r.From,
		To:       r.To,
		Earliest: r.Earliest,
		Latest:   r.Latest,
	}
}

func (r queryStreamRequest) unsupportedFields() []string {
	fields := make([]string, 0, len(apicontracts.QueryStreamUnsupportedFields))
	for _, field := range apicontracts.QueryStreamUnsupportedFields {
		switch field {
		case "wait":
			if r.Wait != nil {
				fields = append(fields, field)
			}
		case "limit":
			if r.Limit != nil {
				fields = append(fields, field)
			}
		case "offset":
			if r.Offset != nil {
				fields = append(fields, field)
			}
		case "profile":
			if r.Profile != nil {
				fields = append(fields, field)
			}
		case "format":
			if r.Format != nil {
				fields = append(fields, field)
			}
		}
	}

	return fields
}

func (s *Server) handleQueryStream(w http.ResponseWriter, r *http.Request) {
	if !s.requireScope(w, r, auth.ScopeQuery) {
		return
	}

	var rawReq queryStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&rawReq); err != nil {
		respondError(w, ErrCodeInvalidJSON, http.StatusBadRequest, "invalid JSON")

		return
	}
	if unsupported := rawReq.unsupportedFields(); len(unsupported) > 0 {
		respondError(
			w,
			ErrCodeValidationError,
			http.StatusBadRequest,
			apicontracts.UnsupportedQueryStreamFieldsMessage(unsupported),
			WithSuggestion(apicontracts.QueryStreamUnsupportedFieldsSuggestion),
		)

		return
	}
	req := rawReq.toQueryRequest()
	query := req.effectiveQuery()
	if query == "" {
		respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query is required")

		return
	}
	if !s.checkQueryLength(w, query) {
		return
	}

	s.runQueryStream(w, r, query, req)
}

// rowToInterface converts an event.Value map to a plain map for JSON serialization.
func rowToInterface(row map[string]event.Value) map[string]interface{} {
	out := make(map[string]interface{}, len(row))
	for k, v := range row {
		out[k] = v.Interface()
	}

	return out
}

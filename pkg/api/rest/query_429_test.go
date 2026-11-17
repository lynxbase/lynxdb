package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/database"
)

// TestRespondQueryFailure_TooManyQueriesSetsRetryAfter verifies that the query
// concurrency-limit error maps to HTTP 429 and carries a Retry-After hint so
// clients back off instead of hammering the endpoint.
func TestRespondQueryFailure_TooManyQueriesSetsRetryAfter(t *testing.T) {
	for name, err := range map[string]error{
		"sentinel": database.ErrTooManyQueries,
		"wrapped":  fmt.Errorf("submit: %w", database.ErrTooManyQueries),
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			respondQueryFailure(rec, err)

			if rec.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
			}
			if got := rec.Header().Get("Retry-After"); got != "1" {
				t.Errorf("Retry-After = %q, want %q", got, "1")
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Error.Code != string(ErrCodeTooManyRequests) {
				t.Errorf("code = %q, want %s", body.Error.Code, ErrCodeTooManyRequests)
			}
		})
	}
}

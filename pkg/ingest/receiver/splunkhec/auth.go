package splunkhec

import (
	"errors"
	"net/http"
	"strings"
)

var (
	ErrMissingToken = errors.New("splunk token is required")
	ErrInvalidToken = errors.New("splunk token is invalid")
	ErrForbidden    = errors.New("splunk token may not send events")
)

// AuthConfig checks the Authorization: Splunk <token> header of requests.
type AuthConfig struct {
	// Check decides whether a token may send events and returns nil,
	// ErrInvalidToken or ErrForbidden. A nil Check accepts every request.
	Check func(token string) error
}

func (a AuthConfig) Authorize(r *http.Request) error {
	if a.Check == nil {
		return nil
	}
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Splunk ")
	token = strings.TrimSpace(token)
	if !ok || token == "" {
		return ErrMissingToken
	}
	return a.Check(token)
}

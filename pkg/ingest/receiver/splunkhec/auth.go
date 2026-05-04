package splunkhec

import (
	"errors"
	"net/http"
	"strings"
)

var (
	ErrMissingToken = errors.New("splunk token is required")
	ErrInvalidToken = errors.New("splunk token is invalid")
)

type AuthConfig struct {
	Enabled      bool
	RequireToken bool
	Tokens       map[string]struct{}
}

func (a AuthConfig) Authorize(r *http.Request) error {
	if !a.Enabled {
		return nil
	}
	const prefix = "Splunk "
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, prefix) {
		return ErrMissingToken
	}
	token := strings.TrimSpace(strings.TrimPrefix(h, prefix))
	if token == "" {
		return ErrMissingToken
	}
	if !a.RequireToken {
		return nil
	}
	if _, ok := a.Tokens[token]; !ok {
		return ErrInvalidToken
	}
	return nil
}

//go:build dev

// Package webui serves the embedded LynxDB Web UI as a single-page application.
// In dev builds (go build -tags dev), requests are proxied to a Vite dev server
// running on localhost:5173 for hot-reload during frontend development.
package webui

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

// Enabled always returns true in dev mode.
func Enabled() bool { return true }

// Handler returns a reverse proxy to the Vite dev server.
func Handler() http.Handler {
	target, _ := url.Parse("http://localhost:5173")
	return httputil.NewSingleHostReverseProxy(target)
}

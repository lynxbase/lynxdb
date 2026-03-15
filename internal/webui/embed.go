//go:build !dev

// Package webui serves the embedded LynxDB Web UI as a single-page application.
// In production builds, static assets are compiled into the binary via embed.FS.
// In dev builds (go build -tags dev), requests are proxied to a Vite dev server.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// Enabled reports whether embedded UI assets are available.
// Returns false when dist/ contains only the .gitkeep placeholder.
func Enabled() bool {
	entries, err := fs.ReadDir(distFS, "dist")
	if err != nil {
		return false
	}
	for _, e := range entries {
		if e.Name() != ".gitkeep" {
			return true
		}
	}
	return false
}

// Handler returns an http.Handler that serves the embedded SPA.
// Static assets under /assets/ are served with immutable cache headers.
// All other paths fall back to index.html for client-side routing.
func Handler() http.Handler {
	sub, _ := fs.Sub(distFS, "dist")
	fileServer := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" {
			path = "index.html"
		} else {
			path = strings.TrimPrefix(path, "/")
		}

		if f, err := sub.Open(path); err == nil {
			f.Close()
			if strings.HasPrefix(r.URL.Path, "/assets/") {
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			}
			fileServer.ServeHTTP(w, r)
			return
		}

		// SPA fallback: serve index.html for unmatched routes.
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	})
}

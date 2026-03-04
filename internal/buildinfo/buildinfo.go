// Package buildinfo exposes build-time metadata (version, commit, date)
// injected via -ldflags at compile time. All other packages should import
// this package instead of maintaining their own version variables.
//
// Build example:
//
//	go build -ldflags "-X github.com/lynxbase/lynxdb/internal/buildinfo.Version=1.0.0
//	  -X github.com/lynxbase/lynxdb/internal/buildinfo.Commit=abc1234
//	  -X github.com/lynxbase/lynxdb/internal/buildinfo.Date=2026-02-25T12:00:00Z"
//	  ./cmd/lynxdb/
package buildinfo

import (
	"fmt"
	"runtime"
)

// Version, Commit, and Date are set at build time via -ldflags.
// They default to safe values for development builds.
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// Short returns a one-line human-readable version string,
// e.g. "LynxDB v1.2.3 (abc1234) built 2026-02-25T12:00:00Z".
func Short() string {
	return fmt.Sprintf("LynxDB %s (%s) built %s", Version, Commit, Date)
}

// Runtime returns Go version and platform info,
// e.g. "go1.25.4 linux/amd64".
func Runtime() string {
	return fmt.Sprintf("%s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

// IsDev reports whether this is a development build (not built via GoReleaser).
func IsDev() bool {
	return Version == "dev"
}

// UserAgent returns the User-Agent string for HTTP requests,
// e.g. "lynxdb/v0.5.0" or "lynxdb/dev".
func UserAgent() string {
	return "lynxdb/" + Version
}

// FullVersion returns a detailed version string including commit and build date,
// e.g. "lynxdb v0.5.0 (commit: abc1234, built: 2026-06-15T10:30:00Z)".
func FullVersion() string {
	return fmt.Sprintf("lynxdb %s (commit: %s, built: %s)", Version, Commit, Date)
}

// Info returns a structured map of build information suitable for JSON
// serialization. Keys: version, commit, date, go, os, arch.
func Info() map[string]string {
	return map[string]string{
		"name":    "LynxDB",
		"version": Version,
		"commit":  Commit,
		"date":    Date,
		"go":      runtime.Version(),
		"os":      runtime.GOOS,
		"arch":    runtime.GOARCH,
	}
}

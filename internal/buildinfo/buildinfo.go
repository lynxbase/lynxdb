// Package buildinfo exposes build-time metadata (version, commit, date)
// injected via -ldflags at compile time. All other packages should import
// this package instead of maintaining their own version variables.
//
//	go build -ldflags "-X github.com/lynxbase/lynxdb/internal/buildinfo.Version=26.11.1.80
//	  -X github.com/lynxbase/lynxdb/internal/buildinfo.Commit=abc1234def56
//	  -X github.com/lynxbase/lynxdb/internal/buildinfo.Date=2026-11-24T12:00:00Z"
//	  ./cmd/lynxdb/
//
// tools/dist/ldflags.sh prints these flags for the current tree. A version
// reads year.month.patch.build; a build between releases adds its distance
// from the last one and its commit: 26.11.1.80+5.gabc1234.
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

// Runtime returns Go version and platform info,
// e.g. "go1.26.3 linux/amd64".
func Runtime() string {
	return fmt.Sprintf("%s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH)
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

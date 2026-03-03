// Package upgrade implements LynxDB's self-update mechanism.
//
// Architecture:
//
//	manifest.json (CDN) -> check() -> download() -> verify() -> swap()
//
// The manifest is a lightweight JSON file (~500 bytes) hosted at
// dl.lynxdb.org/manifest.json that contains the latest version,
// per-platform download URLs, and SHA256 checksums.
//
// The upgrade flow:
//  1. Fetch manifest.json from CDN (with fallback to GitHub)
//  2. Compare versions (semver)
//  3. Download the platform-specific archive to a temp file
//  4. Verify SHA256 checksum
//  5. Extract binary from archive
//  6. Atomic replace: rename new -> old (same filesystem)
package upgrade

import "errors"

var (
	// ErrAlreadyLatest indicates the current version is already the latest.
	ErrAlreadyLatest = errors.New("already running the latest version")

	// ErrPlatformNotFound indicates no build is available for the current platform.
	ErrPlatformNotFound = errors.New("no build available for this platform")

	// ErrChecksumMismatch indicates the downloaded archive did not match the expected SHA256.
	ErrChecksumMismatch = errors.New("checksum verification failed")
)

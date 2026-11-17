# Releasing

LynxDB publishes stable releases and nightly prereleases. Both are built by GoReleaser (pinned in `mise.toml`) from a version tag.

## Stable Release

1. Make sure `main` is green and `CHANGELOG.md` describes the release.
2. Tag the release commit with `vX.Y.Z` and push the tag.

Pushing a tag that matches `v*` runs `.github/workflows/release.yaml`, which accepts only `vX.Y.Z`:

1. **Build & Publish**: `goreleaser check`, then `goreleaser release` with `.goreleaser.yaml`: binaries and archives for every platform, `checksums.txt`, the GitHub release, Docker images `ghcr.io/lynxbase/lynxdb:X.Y.Z` and `:latest` (amd64 and arm64), and the Homebrew formula in `lynxbase/homebrew-tap`.
2. **Sync to CDN**: uploads the archives and checksums to the release bucket behind `dl.lynxdb.org`, regenerates `manifest.json` (`scripts/generate-manifest.py`), uploads `install.sh`, and verifies the published artifacts.

The install script reads `https://dl.lynxdb.org/manifest.json` to find the latest stable version.

## Nightly Prerelease

`.github/workflows/nightly-trigger.yaml` runs daily at 02:00 UTC and on manual dispatch:

1. Skips the run when `main` has no commits since the latest stable tag.
2. Computes the tag `vX.Y.Z-nightly.YYYYMMDD.gSHA` with `scripts/nightly-version.sh`. The base version is the `base_version` dispatch input, else the `NIGHTLY_BASE_VERSION` repository variable, else the latest stable tag with the patch number incremented.
3. Runs `go test ./...` and `goreleaser check --config .goreleaser-nightly.yaml`.
4. Pushes the tag and dispatches `.github/workflows/nightly-release.yaml` for it.

The nightly release publishes with `.goreleaser-nightly.yaml`: a GitHub prerelease, Docker images `ghcr.io/lynxbase/lynxdb:<version>`, `:nightly` and `:sha-<commit>` (never `:latest`; the workflow fails if `:latest` changes), immutable artifacts and `nightly/manifest.json` on the CDN, and the `lynxdb-nightly` Homebrew formula.

To cut a nightly by hand, dispatch the Nightly Trigger workflow, optionally with `base_version` (for example `0.7.0`):

```bash
gh workflow run nightly-trigger.yaml -f base_version=0.7.0
```

## Checks Before Publishing

`.github/workflows/release-dry-run.yaml` runs on pull requests that touch the GoReleaser configs, the release workflows and scripts, `cmd/`, `internal/`, `go.mod`/`go.sum` or `Dockerfile.release`. It validates both GoReleaser configs, asserts that the nightly config publishes none of the stable outputs, and builds a stable snapshot.

Locally:

```bash
goreleaser check
goreleaser check --config .goreleaser-nightly.yaml
goreleaser release --snapshot --clean
```

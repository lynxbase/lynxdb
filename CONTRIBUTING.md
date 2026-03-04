# Contributing to LynxDB

## Setup

```bash
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb
make build
make test
```

Go 1.25.4+. No other dependencies.

## Branches

Branch off `main`. Name format: `<type>/<short-description>`.

| Prefix    | When to use                          |
|-----------|--------------------------------------|
| `feat/`   | New feature                          |
| `fix/`    | Bug fix                              |
| `chore/`  | Build, CI, deps, cleanup             |
| `refactor/` | Code change that doesn't fix a bug or add a feature |
| `docs/`   | Documentation only                   |
| `test/`   | Adding or fixing tests               |
| `perf/`   | Performance improvement              |

Examples: `feat/streaming-join`, `fix/wal-replay-crash`, `chore/bump-go-version`.

## Commits

Follow the same prefixes for commit messages:

```
feat: add JOIN command to pipeline
fix: WAL replay skips corrupted entries instead of panicking
chore: update roaring bitmap to v2.4
```

One logical change per commit. Keep commits small and reviewable.

## Pull Requests

1. One PR = one logical change.
2. PR title follows the same `type: description` format.
3. All checks must pass: `make build`, `make test`, `make vet`.
4. Add tests for new code. No exceptions.
5. If you're adding a new pipeline operator or SPL2 command, add acceptance tests in `test/acceptance/`.

## Code Style

- `go vet ./...` must pass.
- `golangci-lint` via `make lint` if configured.
- Exported symbols require godoc comments.
- Errors are wrapped with context: `fmt.Errorf("component.Op: %w", err)`.
- `context.Context` is the first parameter for any I/O or blocking function.
- No `init()`, no global mutable state, no `panic` for control flow.

## Testing

```bash
make test          # all tests
go test ./pkg/spl2/... -run TestParser   # specific package/test
go test ./... -bench .                   # benchmarks
```

Test locations:
- Unit tests: next to the code (`*_test.go` in the same package).
- Acceptance tests: `test/acceptance/`.
- Integration tests: `test/integration/`.
- E2E tests: `test/e2e/`.
- Regression tests: `test/regression/`.

## Project Structure

Code lives in three top-level directories:

- `cmd/lynxdb/` -- CLI entry point.
- `pkg/` -- Public packages (storage, query engine, API, SPL2 parser, etc.).
- `internal/` -- Internal packages not intended for external use.

## Documentation

- User-facing docs site: `docs/site/` (Docusaurus).
- Changelog: `CHANGELOG.md`

## Reporting Issues

Open a GitHub issue. Include:
- What you did (query, config, input data).
- What you expected.
- What happened instead.
- LynxDB version (`lynxdb --version`).

# Contributing to LynxDB

## Setup

LynxDB pins every local and CI toolchain in `mise.toml`: Go, Node (docs site),
Python (release scripts), GoReleaser, golangci-lint, rclone and the GitHub CLI.

```bash
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb
curl https://mise.run | sh
mise install
mise run build
mise run test
```

If you already have the toolchains installed, the plain `make` targets are
equivalent:

```bash
make build
make test
make vet
```

For quick local Go-only checks:

```bash
go build -o lynxdb ./cmd/lynxdb/
go test ./...
```

See the [development setup guide](docs/site/docs/contributing/development-setup.md)
for running a local server, golden files and benchmarks.

## Branches

Branch off `main`. Name format: `<type>/<short-description>`.

| Prefix      | When to use                                          |
|-------------|------------------------------------------------------|
| `feat/`     | New feature                                          |
| `fix/`      | Bug fix                                              |
| `chore/`    | Build, CI, deps, cleanup                             |
| `refactor/` | Code change that doesn't fix a bug or add a feature  |
| `docs/`     | Documentation only                                   |
| `test/`     | Adding or fixing tests                               |
| `perf/`     | Performance improvement                              |

Examples: `feat/streaming-join`, `fix/batcher-flush-retry`, `chore/bump-go-version`.

## Commits

Use the same prefixes for commit messages, `<type>: <subject>`, imperative and
lowercase after the prefix, under 72 characters:

```
feat: add trendline command
fix: batcher flush recovers from interrupted part writes
chore: update roaring bitmap to v2.4
```

One logical change per commit. Keep commits small and reviewable.

## Pull Requests

1. One PR = one logical change.
2. The PR title follows the same `type: description` format.
3. All checks must pass: `make build`, `make test`, `make vet`.
4. Add tests for new code. No exceptions.
5. For a PPL command or function, update its catalog entry in `pkg/ppl`, add
   compile goldens under `pkg/ppl/testdata/compile/` and execution tests, and
   regenerate the reference pages with `go run ./internal/docgen`.

## Code Style

- `go vet ./...` must pass; run `golangci-lint run ./...` with `.golangci.yml`.
- Exported symbols have doc comments.
- Errors are wrapped with the failed operation: `fmt.Errorf("read segment %s: %w", id, err)`.
- `context.Context` is the first parameter of any I/O or blocking function.
- Avoid mutable package-level state; no `panic` for control flow.
- The query language stays in `pkg/ppl`; the optimizer, physical planner and
  executor work on `pkg/ir` only.

See [coding guidelines](docs/site/docs/contributing/coding-guidelines.md).

## Testing

```bash
make test                                   # unit + e2e + CLI tests
make test-unit                              # Go unit tests with the race detector
make test-e2e                               # end-to-end server tests
make test-cli                               # tests against the built binary
go test ./pkg/ppl/ -run TestCompileGolden   # one package and test
go test ./pkg/ppl/ -run TestCompileGolden -update   # rewrite compile goldens
go test ./pkg/vm/... -bench . -benchmem     # benchmarks
```

Run tests from the repository root with package paths; tests read testdata
relative to their package.

Test locations:

- Unit tests: next to the code (`*_test.go` in the same package).
- E2E tests: `test/e2e/` (shipper compatibility in `test/e2e/shippers/`).
- CLI tests: `test/cli/`.
- Cluster integration tests: `test/integration/` (`integration` build tag).
- Regression tests: `test/regression/`.
- Chaos and simulation tests: `test/chaos/`, `test/simulation/`.

## Common Tasks

```bash
mise run build          # make build
mise run test           # make test
mise run bench          # make bench-micro
make docs-gen           # regenerate the PPL reference pages from the catalogs
make clean              # remove local build artifacts
```

## Project Structure

- `cmd/lynxdb/` -- CLI entry point.
- `pkg/` -- Core packages: PPL frontend (`pkg/ppl`), IR and optimizer
  (`pkg/ir`), execution (`pkg/engine`, `pkg/vm`), storage, ingest, API,
  cluster.
- `internal/` -- Packages not meant for import outside the module.

See the [project structure guide](docs/site/docs/contributing/project-structure.md).

## Documentation

- User-facing docs site: `docs/site/` (Docusaurus).
- Generated PPL reference pages: `docs/site/docs/ppl/commands.md`,
  `functions.md`, `aggregations.md` (`go run ./internal/docgen`).
- Changelog: `CHANGELOG.md`.

## Reporting Issues

Open a GitHub issue. Include:

- What you did (query, config, input data).
- What you expected.
- What happened instead.
- LynxDB version (`lynxdb version`).

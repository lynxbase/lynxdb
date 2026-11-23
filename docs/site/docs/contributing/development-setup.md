---
sidebar_position: 1
title: Development Setup
description: Clone, build, test, and run LynxDB from source. Toolchain, mise tasks, and development workflow.
---

# Development Setup

## Prerequisites

| Requirement | Version | Check |
|------------|---------|-------|
| **Go** | 1.26 (the exact release is in `mise.lock`) | `go version` |
| **Git** | 2.x | `git --version` |
| **mise** | 2026.11 or later | `mise --version` |
| **Docker** | Optional: shipper, conformance, installer and image tests | `docker version` |

`mise.toml` names every tool the project uses (Go, golangci-lint, shellcheck, the workflow linters, Node for the docs site) and `mise.lock` records their exact versions and checksums. With [mise](https://mise.jdx.dev) installed, `mise install` sets them up, `mise tasks` lists the project's commands, and `mise x -- <command>` runs any command with the pinned tools.

LynxDB needs no C libraries and no JVM.

## Clone and Build

```bash
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb

# Build the binary with version metadata (output: ./lynxdb)
mise run build

# Or a plain build
go build -o lynxdb ./cmd/lynxdb/

./lynxdb version
```

The binary is a static executable; copy it to any machine with the same OS and architecture.

## Tasks

```bash
mise run build             # Build ./lynxdb with version, commit and date
mise run ci                # Every linter and the unit tests
mise run lint              # golangci-lint, go.mod tidiness, shellcheck, commit subjects, workflow linters
mise run fmt               # Format the Go sources
mise run test              # go test ./...
mise run test:race         # The same under the race detector
mise run test:e2e          # Server tests over the REST API (e2e build tag)
mise run test:cli          # test/cli against a binary it builds (clitest build tag), goldens included
mise run test:compat       # Shipper compatibility tests (Filebeat, Fluent Bit, Vector, OTel Collector, HEC); needs Docker
mise run test:conformance  # Full shipper conformance matrix; needs Docker
mise run bench             # Micro benchmarks of the row and ingest pipelines; writes artifacts/bench-micro.txt
mise run bench:fixtures    # Generate benchmark fixtures (tools/dev/genbench)
mise run gen               # Regenerate the PPL reference pages from the catalogs
mise run docs              # Build the documentation site
```

Arguments after `--` go to the command of the task: `mise run test -- -run TestCompileGolden`.

The release binaries, archives and container image are built by tasks too, which is also how continuous integration builds them:

```bash
mise run dist              # Cross-compile every platform into dist/stage
mise run package           # Archives, install.sh and SHA256SUMS in dist/
mise run test:package      # Install from those archives on this machine and in containers; needs Docker
mise run test:image        # Build the container image and check the server in it; needs Docker
mise run version           # The version this tree builds as
```

## Running Locally

### Pipe and File Mode (No Server)

The fastest way to try a change is to query a file or stdin; the query runs in process:

```bash
go run ./cmd/lynxdb/ query --file testdata/logs/access.log 'source=main | stats count() by level'

echo '{"level":"error","msg":"test"}' | go run ./cmd/lynxdb/ query 'source=main | stats count() by level'
```

Input is loaded into the index `main`. `--explain` prints the logical plan, the optimized plan and the optimizer rules without running the query:

```bash
go run ./cmd/lynxdb/ query --explain 'source=main level=ERROR | stats count() by host'
```

### Server Mode

Start a server that keeps its data in a temporary directory, deleted on exit:

```bash
go run ./cmd/lynxdb/ server --data-dir ""

# In another terminal: ingest and query
./lynxdb ingest testdata/logs/access.log
./lynxdb query 'source=main level=ERROR | stats count() by host'
```

Without `--data-dir`, the server uses the configured data directory (default `~/.local/share/lynxdb`). For a separate development directory:

```bash
mkdir -p /tmp/lynxdb-dev
go run ./cmd/lynxdb/ server --data-dir /tmp/lynxdb-dev --log-level debug
```

`testdata/logs/` holds sample logs (`access.log`, `nginx_access.log`, `OpenSSH_2k.log`, `mixed_json.log`, and others) for manual testing.

## Running Tests

Tests read testdata with paths relative to the package directory, so run `go test` from the repository root with package paths.

### Unit Tests

```bash
# All unit tests
go test ./...

# One package tree
go test ./pkg/ppl/...
go test ./pkg/ir/...

# One test
go test -v -run TestCompileGolden ./pkg/ppl/
```

### Golden Files

The PPL compiler and the query corpus compare against golden files:

- `pkg/ppl/testdata/compile/` -- compiled plans and errors for PPL queries (`TestCompileGolden`, `TestCompileErrors`)
- `pkg/query/testdata/corpus_plans.sum` -- optimized plans of the query corpus (`TestCorpusPlansGolden`)

After an intended plan change, rewrite them with `-update` and review the diff:

```bash
go test ./pkg/ppl/ -run TestCompileGolden -update
go test ./pkg/query/ -run TestCorpusPlansGolden -update
```

### Generated Reference Pages

`docs/site/docs/ppl/commands.md`, `functions.md` and `aggregations.md` are generated from the catalogs in `pkg/ppl` and the IR function registry. After changing a catalog entry, regenerate them:

```bash
go run ./internal/docgen   # or: mise run gen
```

`TestGeneratedPagesAreCurrent` in `internal/docgen` fails when the committed pages differ from the catalogs.

### Race Detector

```bash
go test -race ./...
```

### End-to-End and CLI Tests

```bash
mise run test:e2e   # starts servers and exercises the REST API
mise run test:cli   # runs the built binary as a subprocess: output, exit codes, goldens
```

### Regression

```bash
go test ./test/regression/...   # query correctness regressions
```

## Benchmarks

```bash
# Micro benchmarks (row pipeline, ingest pipeline)
mise run bench

# One package
go test -run '^$' -bench . -benchmem ./pkg/vec/...
go test -run '^$' -bench . -benchmem ./pkg/engine/pipeline/...
```

`BENCHMARKS.md` at the repository root describes every benchmark suite and how to run it.

## IDE Setup

### VS Code

- **Go** (`golang.go`) for language support, debugging and tests.

```json
{
  "go.testFlags": ["-race", "-count=1"],
  "go.lintTool": "golangci-lint",
  "editor.formatOnSave": true,
  "[go]": {
    "editor.defaultFormatter": "golang.go"
  }
}
```

### GoLand / IntelliJ

- Open the repository root (the directory with `go.mod`).
- Add `-race` to "Go tool arguments" in run configurations.

## Development Workflow

1. **Create a branch** off `main`: `git checkout -b feat/my-change` (see [Coding Guidelines](/docs/contributing/coding-guidelines#branches-and-commits)).
2. **Change code** in `pkg/`, `cmd/` or `internal/`.
3. **Run the package tests**: `go test ./pkg/path/to/package/...`
4. **Update goldens and generated pages** if plans or catalogs changed.
5. **Lint**: `mise run lint`
6. **Run the suite**: `mise run test`, then `mise run test:e2e` and `mise run test:cli`
7. **Try it**: `lynxdb query --file ...` or `lynxdb server`.
8. **Commit** small, single-purpose commits and open a pull request.

## Cross-Compilation

```bash
GOOS=linux GOARCH=amd64 go build -o lynxdb-linux-amd64 ./cmd/lynxdb/
GOOS=linux GOARCH=arm64 go build -o lynxdb-linux-arm64 ./cmd/lynxdb/
GOOS=darwin GOARCH=arm64 go build -o lynxdb-darwin-arm64 ./cmd/lynxdb/
```

`mise run dist` builds every released platform this way, stamped with the version.

## Related

- [Project Structure](/docs/contributing/project-structure) -- navigate the codebase
- [Coding Guidelines](/docs/contributing/coding-guidelines) -- style and conventions
- [Architecture Overview](/docs/architecture/overview) -- understand the system before changing it

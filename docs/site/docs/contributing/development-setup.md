---
sidebar_position: 1
title: Development Setup
description: Clone, build, test, and run LynxDB from source. Toolchain, make targets, and development workflow.
---

# Development Setup

This page covers building, testing, and running LynxDB from source.

## Prerequisites

| Requirement | Version | Check |
|------------|---------|-------|
| **Go** | 1.26.3 (pinned in `mise.toml`) | `go version` |
| **Git** | 2.x | `git --version` |
| **Make** | 3.x | `make --version` |
| **Docker** | Optional: shipper and conformance tests | `docker version` |

`mise.toml` pins every tool the project uses (Go, golangci-lint, goreleaser, Node for the docs site). With [mise](https://mise.jdx.dev) installed, `mise install` sets them up, and `mise x -- <command>` runs a command with them.

LynxDB needs no C libraries and no JVM.

## Clone and Build

```bash
git clone https://github.com/lynxbase/lynxdb.git
cd lynxdb

# Build the binary with version metadata (output: ./lynxdb)
make build

# Or a plain build
go build -o lynxdb ./cmd/lynxdb/

./lynxdb version
```

The binary is a static executable; copy it to any machine with the same OS and architecture.

## Make Targets

```bash
make build             # Build ./lynxdb with version, commit and date
make test              # Unit + e2e + CLI tests
make test-unit         # go test ./... with the race detector
make test-e2e          # Server tests over the REST API (e2e build tag)
make test-cli          # Build, then run test/cli against the binary (clitest build tag)
make test-compat       # Shipper compatibility tests (Filebeat, Fluent Bit, Vector, OTel Collector, HEC); needs Docker
make test-conformance  # Full shipper conformance matrix; needs Docker
make bench-micro       # Micro benchmarks for pipeline, VM and ingest; writes artifacts/bench-micro.txt
make bench-fixtures    # Generate benchmark fixtures (scripts/genbench)
make vet               # go vet ./...
make docs-gen          # Regenerate the PPL reference pages from the catalogs
make clean             # Remove build artifacts
```

Lint with the pinned golangci-lint and `.golangci.yml`:

```bash
golangci-lint run ./...
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
go run ./internal/docgen   # or: make docs-gen
```

`TestGeneratedPagesAreCurrent` in `internal/docgen` fails when the committed pages differ from the catalogs.

### Race Detector

```bash
go test -race ./...
```

### End-to-End and CLI Tests

```bash
make test-e2e   # starts servers and exercises the REST API
make test-cli   # runs the built binary as a subprocess: output, exit codes, goldens
```

### Regression

```bash
go test ./test/regression/...   # query correctness regressions
```

## Benchmarks

```bash
# Micro benchmarks (pipeline, VM, ingest)
make bench-micro

# One package
go test -bench=. -benchmem ./pkg/vm/...
go test -bench=. -benchmem ./pkg/engine/pipeline/...
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
5. **Lint**: `go vet ./...` and `golangci-lint run ./...`
6. **Run the suite**: `make test`
7. **Try it**: `lynxdb query --file ...` or `lynxdb server`.
8. **Commit** small, single-purpose commits and open a pull request.

## Cross-Compilation

```bash
GOOS=linux GOARCH=amd64 go build -o lynxdb-linux-amd64 ./cmd/lynxdb/
GOOS=linux GOARCH=arm64 go build -o lynxdb-linux-arm64 ./cmd/lynxdb/
GOOS=darwin GOARCH=arm64 go build -o lynxdb-darwin-arm64 ./cmd/lynxdb/
```

## Related

- [Project Structure](/docs/contributing/project-structure) -- navigate the codebase
- [Coding Guidelines](/docs/contributing/coding-guidelines) -- style and conventions
- [Architecture Overview](/docs/architecture/overview) -- understand the system before changing it

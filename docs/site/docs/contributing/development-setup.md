---
sidebar_position: 1
title: Development Setup
description: Clone, build, test, and run LynxDB from source. Go version requirements, make targets, and development workflow.
---

# Development Setup

This guide covers everything you need to build, test, and run LynxDB from source.

## Prerequisites

| Requirement | Minimum Version | Check |
|------------|----------------|-------|
| **Go** | 1.25.4+ | `go version` |
| **Git** | 2.x | `git --version` |
| **Make** | 3.x | `make --version` |

LynxDB has no other external dependencies. No JVM, no C libraries, no protobuf compiler. The Go toolchain is all you need.

## Clone and Build

```bash
# Clone the repository
git clone https://github.com/OrlovEvgeny/Lynxdb.git
cd lynxdb

# Build the binary
go build -o lynxdb ./cmd/lynxdb/

# Verify it works
./lynxdb version
```

The built binary is a static executable with no runtime dependencies. You can copy it to any machine with the same OS/architecture and run it.

## Make Targets

The project includes a Makefile with common development tasks:

```bash
# Build the binary (output: ./lynxdb)
make build

# Run all tests
make test

# Run tests with race detector
make test-race

# Run only unit tests (fast)
make test-unit

# Run integration tests (starts a server)
make test-integration

# Run acceptance tests (10 canonical queries)
make test-acceptance

# Run end-to-end tests
make test-e2e

# Run the full test suite (unit + integration + acceptance + e2e)
make test-all

# Run benchmarks
make bench

# Run the linter (golangci-lint)
make lint

# Format code
make fmt

# Generate code (if applicable)
make generate

# Clean build artifacts
make clean

# Build for all platforms (linux/darwin, amd64/arm64)
make build-all
```

## Running Locally

### Pipe Mode (No Server)

The fastest way to test changes -- query local files or stdin:

```bash
# Build and run a query against a log file
go run ./cmd/lynxdb/ query --file /var/log/syslog '| stats count by level'

# Pipe data through
echo '{"level":"error","msg":"test"}' | go run ./cmd/lynxdb/ query '| stats count by level'
```

### Server Mode

Start a local server with an in-memory data directory (data lost on exit):

```bash
# Start the server (foreground)
go run ./cmd/lynxdb/ server

# In another terminal: ingest data
./lynxdb ingest testdata/sample.log

# Query
./lynxdb query 'level=error | stats count'
```

Start with a persistent data directory:

```bash
# Create a temp directory for development
mkdir -p /tmp/lynxdb-dev

# Start with persistence
go run ./cmd/lynxdb/ server --data-dir /tmp/lynxdb-dev --log-level debug
```

### Demo Mode

Run the built-in demo to generate realistic log data:

```bash
# Terminal 1: Start the demo (generates 200 events/sec from 4 sources)
go run ./cmd/lynxdb/ demo

# Terminal 2: Query the demo data
./lynxdb query 'source=nginx | stats count by status'
./lynxdb query 'level=error | timechart count span=1m'
```

## Running Tests

### Unit Tests

Unit tests are fast and do not require a running server:

```bash
# All unit tests
go test ./...

# A specific package
go test ./pkg/storage/segment/...
go test ./pkg/spl2/...
go test ./pkg/vm/...

# With verbose output
go test -v ./pkg/spl2/...

# A specific test function
go test -v -run TestParseStatsCommand ./pkg/spl2/...
```

### With Race Detector

Always run tests with the race detector during development:

```bash
go test -race ./...
```

### Integration Tests

Integration tests start a real HTTP server and exercise the REST API:

```bash
go test ./test/integration/...
```

### Acceptance Tests

The acceptance test suite runs 10 canonical queries against a known test dataset and verifies exact result correctness:

```bash
go test ./test/acceptance/...
```

### Regression Tests

Regression tests verify that specific bug fixes remain in place:

```bash
go test ./test/regression/...
```

### End-to-End Tests

E2E tests exercise the full system from CLI invocation through query results:

```bash
go test ./test/e2e/...
```

## Benchmarks

Run performance benchmarks:

```bash
# All benchmarks
go test -bench=. -benchmem ./...

# VM benchmarks (expression evaluation)
go test -bench=. -benchmem ./pkg/vm/...

# Storage benchmarks (segment read/write)
go test -bench=. -benchmem ./pkg/storage/segment/...

# Pipeline benchmarks (query execution)
go test -bench=. -benchmem ./pkg/engine/pipeline/...
```

Run the built-in benchmark command (ingest + query throughput):

```bash
./lynxdb bench --events 100000
./lynxdb bench --events 1000000
```

## IDE Setup

### VS Code

Recommended extensions:

- **Go** (`golang.go`) -- language support, debugging, testing
- **Go Test Explorer** -- test discovery in the sidebar

Recommended `settings.json`:

```json
{
  "go.testFlags": ["-race", "-count=1"],
  "go.lintTool": "golangci-lint",
  "go.lintFlags": ["--fast"],
  "editor.formatOnSave": true,
  "[go]": {
    "editor.defaultFormatter": "golang.go"
  }
}
```

### GoLand / IntelliJ

- Open the project root directory (the one containing `go.mod`).
- Go settings are auto-detected from `go.mod`.
- Enable the race detector in run configurations: add `-race` to "Go tool arguments".

## Development Workflow

A typical development cycle:

1. **Create a branch**: `git checkout -b feature/my-change`
2. **Make changes**: Edit code in `pkg/`, `cmd/`, or `internal/`.
3. **Run unit tests**: `go test ./pkg/path/to/changed/package/...`
4. **Run the linter**: `make lint`
5. **Run the full test suite**: `make test-all`
6. **Test manually**: Run `lynxdb demo` or `lynxdb server` and verify behavior.
7. **Commit and push**: Follow the [coding guidelines](/docs/contributing/coding-guidelines).

## Cross-Compilation

Build for a different OS/architecture:

```bash
# Linux AMD64
GOOS=linux GOARCH=amd64 go build -o lynxdb-linux-amd64 ./cmd/lynxdb/

# Linux ARM64
GOOS=linux GOARCH=arm64 go build -o lynxdb-linux-arm64 ./cmd/lynxdb/

# macOS ARM64 (Apple Silicon)
GOOS=darwin GOARCH=arm64 go build -o lynxdb-darwin-arm64 ./cmd/lynxdb/
```

## Related

- [Project Structure](/docs/contributing/project-structure) -- navigate the codebase
- [Coding Guidelines](/docs/contributing/coding-guidelines) -- style and conventions
- [Architecture Overview](/docs/architecture/overview) -- understand the system before changing it

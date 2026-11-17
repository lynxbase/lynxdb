---
sidebar_position: 3
title: Coding Guidelines
description: Go coding standards, error handling, context usage, testing, query-language changes, and commit conventions for contributing to LynxDB.
---

# Coding Guidelines

This page describes the conventions of the LynxDB codebase.

## Go Version and Style

LynxDB builds with the Go version pinned in `mise.toml` (1.26.3). Use current Go idioms.

### Formatting

All code is `gofmt`-formatted:

```bash
gofmt -w .
# or, with import management
goimports -w .
```

### Linting

`go vet ./...` must pass; CI runs it on every push. `.golangci.yml` turns on `errcheck`, `govet`, `ineffassign`, `staticcheck` and `unused`:

```bash
golangci-lint run ./...
```

### Naming

- **Exported names**: `PascalCase` -- `OpenStore`, `IndexStats`, `KeyPlan`.
- **Unexported names**: `camelCase` -- `blockBudget`, `pruneScan`, `planCache`.
- **Acronyms** keep one case: `VM`, `AST`, `IR`, `PPL`, `HTTP`, `JSON`.
- **Interfaces**: no `I` prefix. One-method interfaces take the `-er` suffix (`Reader`, `Flusher`); others a descriptive noun (`Tables`, `Iterator`).
- **Tests** name the behavior they check: `TestCompileGolden`, `TestStoreServesReadersWhileItMerges`, `TestPickMergesGroupsRunsByTier`.

### Package Organization

- Package names are short lowercase nouns: `part`, `shred`, `pipeline`, `vm`, `ppl`, `ir`.
- No `util`, `helpers`, `common` or `misc` packages; put a function where it belongs.
- Code not meant for import outside the module goes in `internal/`.
- The query language stays in `pkg/ppl`. Everything after the frontend (`pkg/ir/opt`, `pkg/engine/columnar`, `pkg/colstore/exec/irplan`, `pkg/engine/physical`, `pkg/engine/pipeline`, `pkg/vm`) works on `pkg/ir` and never imports `pkg/ppl`.

## Error Handling

### Check Every Error

`errcheck` enforces it (with exclusions for `Close`, `os.Remove` and `fmt.Fprint*` listed in `.golangci.yml`).

```go
// Wrong
json.Unmarshal(data, &result)

// Right
if err := json.Unmarshal(data, &result); err != nil {
    return fmt.Errorf("unmarshal event: %w", err)
}
```

### Wrap with the Operation

Wrap with `fmt.Errorf` and `%w`, naming the operation that failed:

```go
// Wrong: adds nothing
return fmt.Errorf("error: %w", err)

// Wrong: loses the cause
return errors.New("failed to open index")

// Right
return fmt.Errorf("columnar: index %s: %w", index, err)
```

### Sentinel and Typed Errors

Define sentinel errors for conditions callers test with `errors.Is`, such as `colstore.ErrManifest` or `database.ErrTooManyQueries`, and typed errors for conditions that carry data, tested with `errors.As`.

Query compile errors are `*ppl.Error` values with a kind (syntax, semantic, type, unsupported command, function or feature), a start and end position, a message, an optional hint and, for syntax errors, the expected tokens. The REST layer maps the kind to an error code (`SYNTAX_ERROR`, `SEMANTIC_ERROR`, ...) and the HTTP status. A compile error should point at the offending token and, where it can, say how to fix it:

```go
return semanticErr(n, "the rex pattern has no named groups; use (?<name>...)")
```

### API Errors

HTTP handlers answer errors through `respondError` in `pkg/api/rest/response.go`, with an `ErrorCode` constant, the HTTP status, a message, and optional details such as `WithSuggestion`:

```go
respondError(w, ErrCodeValidationError, http.StatusBadRequest, "query is required")
```

Every error response carries a code and a message.

## Context Usage

### Pass Context

Functions that do I/O, run queries, or may block take `context.Context` as their first parameter:

```go
func (db *DB) Query(ctx context.Context, c *query.Compiled, opts QueryOptions) (*columnar.Result, error)
```

Iterators receive the context on every `Next(ctx)` call.

### Respect Cancellation

Check for cancellation in loops and before expensive work:

```go
for {
    if err := ctx.Err(); err != nil {
        return err
    }
    batch, err := it.Next(ctx)
    if err != nil {
        return err
    }
    if batch == nil {
        return nil
    }
    // process batch
}
```

### Context Values

Do not pass data between functions with `context.WithValue`; use parameters. Context values are for cross-cutting concerns such as request IDs, auth identity and trace spans.

## Concurrency

### Goroutine Lifecycle

Every goroutine has a shutdown path: a context or a channel that stops it, and a `sync.WaitGroup` or channel to wait on.

```go
// maintain drops expired events and finished jobs until Close.
func (db *DB) maintain() {
    defer db.wg.Done()
    tick := time.NewTicker(time.Minute)
    defer tick.Stop()
    for {
        db.expire(time.Now())
        select {
        case <-db.stop:
            return
        case <-tick.C:
        }
    }
}
```

### Mutexes

- Keep critical sections small.
- Do not hold a mutex during I/O or calls into unknown code.
- Use `sync.RWMutex` when reads far outnumber writes.
- Say what a mutex protects:

```go
type DB struct {
    mu     sync.RWMutex // guards stores and closed
    stores map[string]*colstore.Store
    closed bool
}
```

### Package-Level State

Avoid package-level mutable state; state belongs to structs that are passed explicitly. Read-only tables built once (catalogs, registries) and bounded caches with their own locking are the exceptions.

## Performance-Sensitive Code

Hot paths are the block loops of the columnar executor (`pkg/colstore/exec`), JSON shredding (`pkg/colstore/shred`), part encoding and decoding (`pkg/colstore/part`), the VM evaluation loop (`pkg/vm`) and the row operators (`pkg/engine/pipeline`). Cold paths are configuration, setup, merge selection and API handler wiring.

### Hot Path Rules

- **No per-row allocations**: no `make`, `new`, growing `append`, `fmt.Sprintf` or interface boxing per row. Allocate per block, batch or query, from an arena or a pool, and reuse.
- **Concrete types** in inner loops; interface dispatch per row adds up.
- **No `reflect`**: use type switches.
- **Work per batch**: process a batch of rows per call, not one row.
- **Measure**: back a hot-path change with a benchmark (`b.ReportAllocs()`, `-benchmem`) and compare before and after.

### Cold Path Rules

Prefer clarity: allocations, interfaces for testability and `fmt.Errorf` are fine.

## Query Language Changes

PPL support is declared in catalogs, and the documentation and API read the same catalogs:

- Commands: `pkg/ppl/catalog.go` (support level and notes)
- Scalar functions: `pkg/ppl/functions.go`, `pkg/ppl/relevance.go`
- Aggregations: `pkg/ppl/aggs.go`

When you add or change a command or function:

1. Lower it to existing IR operators and functions where possible; add an IR operator or registry function only when no combination expresses it.
2. Update its catalog entry. Never mark something supported that is not; unsupported options fail with `UNSUPPORTED_FEATURE`.
3. Add golden cases under `pkg/ppl/testdata/compile/` and execution tests.
4. Regenerate the reference pages with `go run ./internal/docgen`.

See [Adding commands](../ppl/adding-commands.md) and [Adding functions](../ppl/adding-functions.md).

Optimizer rules in `pkg/ir/opt` must preserve the plan's result; rules that hand predicates to a scan keep them in the filter above it. Cover each rule in `TestOptimizerRules` and check the plan corpus golden (`pkg/query/testdata/corpus_plans.sum`).

## Testing

### Location

Tests live next to the code they test:

```
pkg/ppl/lexer/
├── lexer.go
└── lexer_test.go
```

Cross-package suites live in `test/` (see [Project Structure](/docs/contributing/project-structure#test)).

### Table-Driven Tests

Use table-driven tests for functions with several input/output cases:

```go
func TestParseTimeModifier(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        wantErr bool
    }{
        {name: "relative hour", input: "-1h"},
        {name: "snap to day", input: "-1d@d"},
        {name: "invalid unit", input: "-1fortnight", wantErr: true},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // call the function and compare
        })
    }
}
```

### Golden Files

Compiled plans and errors are compared against golden files. Rewrite them with `-update` only for intended changes, and review the diff:

```bash
go test ./pkg/ppl/ -run TestCompileGolden -update
```

### Test Helpers

Call `t.Helper()` in helpers so failures report the caller's line:

```go
func assertRowCount(t *testing.T, rows []map[string]event.Value, want int) {
    t.Helper()
    if len(rows) != want {
        t.Fatalf("got %d rows, want %d", len(rows), want)
    }
}
```

### Independence

Tests do not depend on each other or on global state. Use `t.TempDir()` for files, open fresh databases and stores, and close them after each test. Run `go test` from the repository root with package paths, since tests read testdata relative to their package.

## Comments and Documentation

### Package Comments

Every package has a package comment:

```go
// Package opt rewrites canonical IR plans into cheaper equivalent plans.
package opt
```

### Exported Identifiers

Exported types and functions have a doc comment that starts with the name and says what it does in one or two sentences:

```go
// Sync makes the events appended so far durable and visible.
func (db *DB) Sync() error
```

### Other Comments

A comment explains what the code cannot: the reason for a non-obvious choice, an invariant, a unit, a workaround. Prefer a better name or a smaller function to a comment that restates the code. Comments describe the code as it is, not how it changed.

```go
// A VM caches per-program state that is not keyed by program, so a
// recycled one starts from zero.
*m = vm.VM{}
```

## Branches and Commits

Branch off `main` with `<type>/<short-description>`: `feat/`, `fix/`, `chore/`, `refactor/`, `docs/`, `test/`, `perf/`.

Commit messages use the same types, `<type>: <subject>`, imperative, lowercase after the prefix, under 72 characters, one logical change per commit:

```
feat: add trendline command
fix: keep scan time range when merging filters
perf: skip blocks by their token filter for string equality
```

See `CONTRIBUTING.md` for pull request requirements.

## Related

- [Development Setup](/docs/contributing/development-setup) -- build and test the project
- [Project Structure](/docs/contributing/project-structure) -- navigate the codebase
- [Architecture Overview](/docs/architecture/overview) -- understand the system

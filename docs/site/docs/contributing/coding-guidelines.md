---
sidebar_position: 3
title: Coding Guidelines
description: Go coding standards, error handling patterns, context usage, testing guidelines, and conventions for contributing to LynxDB.
---

# Coding Guidelines

This page describes the coding conventions and standards for the LynxDB codebase. Following these guidelines ensures consistency, readability, and maintainability across contributions.

## Go Version and Style

LynxDB targets **Go 1.25.4+**. Use modern Go idioms and features.

### Formatting

All code must pass `gofmt` (or `goimports`). The CI pipeline rejects unformatted code.

```bash
# Format all files
gofmt -w .

# Or use goimports for automatic import management
goimports -w .
```

### Linting

All code must pass `golangci-lint`. Run it locally before pushing:

```bash
make lint
```

The linter configuration is in `.golangci.yml` at the project root. Key enabled linters:

- `govet` -- correctness checks
- `errcheck` -- unchecked errors
- `staticcheck` -- advanced static analysis
- `gosimple` -- simplification suggestions
- `ineffassign` -- unused assignments
- `misspell` -- typos in comments and strings

### Naming

Follow standard Go naming conventions:

- **Exported names**: `PascalCase` -- `SegmentWriter`, `ParseQuery`, `EventCount`.
- **Unexported names**: `camelCase` -- `segmentHandle`, `parseSearchPredicate`, `eventPool`.
- **Acronyms**: All caps for short acronyms -- `WAL`, `VM`, `FST`, `AST`, `SSE`, `LRU`. Mixed case for longer ones -- `Http` is wrong, `HTTP` is correct.
- **Interface names**: Do not use `I` prefix. Use the `-er` suffix when the interface has one method -- `Reader`, `Writer`, `Flusher`. For multi-method interfaces, use a descriptive noun -- `ObjectStore`, `Operator`.
- **Test functions**: `TestParseStatsCommand`, `TestSegmentWriter_FlushV2`, `BenchmarkVMSimplePredicate`.

### Package Organization

- One package per directory. No multi-file packages sharing a directory with unrelated code.
- Package names are singular lowercase nouns: `segment`, `memtable`, `pipeline`, `vm`.
- Avoid `util`, `helpers`, `common`, and `misc` packages. Put functions where they belong.
- Internal types that should not be imported outside the module go in `internal/`.

## Error Handling

### Always Check Errors

Every function that returns an error must have its error checked. The `errcheck` linter enforces this.

```go
// Wrong
file.Close()
json.Unmarshal(data, &result)

// Right
if err := file.Close(); err != nil {
    return fmt.Errorf("close segment file: %w", err)
}

if err := json.Unmarshal(data, &result); err != nil {
    return fmt.Errorf("unmarshal event: %w", err)
}
```

### Error Wrapping

Wrap errors with context using `fmt.Errorf` and the `%w` verb. The wrapping message should describe the operation that failed, not repeat the error:

```go
// Wrong: repeats the underlying error
if err != nil {
    return fmt.Errorf("error: %w", err)
}

// Wrong: loses the error chain
if err != nil {
    return errors.New("failed to read segment")
}

// Right: describes the operation, wraps the cause
if err != nil {
    return fmt.Errorf("read segment %s: %w", seg.ID, err)
}
```

### Sentinel Errors

Define sentinel errors for conditions that callers need to check programmatically:

```go
var (
    ErrSegmentNotFound = errors.New("segment not found")
    ErrQueryTimeout    = errors.New("query timeout exceeded")
    ErrInvalidQuery    = errors.New("invalid query")
)
```

Check sentinel errors with `errors.Is`:

```go
if errors.Is(err, ErrSegmentNotFound) {
    // handle missing segment
}
```

### Structured Errors

For errors returned to HTTP clients, use the structured error type from `pkg/model`:

```go
return &model.APIError{
    Code:       "INVALID_QUERY",
    Message:    "Unknown command 'staats'.",
    Suggestion: "stats",
    DocsURL:    "https://lynxdb.io/docs/spl2/overview",
}
```

Every API error must include `Code` and `Message`. `Suggestion` and `DocsURL` are optional but strongly encouraged.

## Context Usage

### Pass Context Everywhere

All functions that perform I/O, query execution, or long-running work must accept a `context.Context` as their first parameter:

```go
func (e *Engine) Query(ctx context.Context, q string) (*Result, error) {
    // ...
}

func (s *SegmentReader) ReadColumn(ctx context.Context, name string) ([]byte, error) {
    // ...
}
```

### Respect Cancellation

Check context cancellation in loops and before expensive operations:

```go
func (p *Pipeline) Execute(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }

        batch, err := p.root.Next(ctx)
        if err != nil {
            return err
        }
        if batch == nil {
            return nil // done
        }
        // process batch
    }
}
```

### Context Values

Do not use `context.WithValue` for passing data between functions. Use explicit parameters. Context values are reserved for cross-cutting concerns like request IDs and trace spans.

## Concurrency

### Goroutine Lifecycle

Every goroutine must have a clear shutdown path. Use `context.Context` for cancellation and `sync.WaitGroup` or channels for join:

```go
func (e *Engine) startCompaction(ctx context.Context) {
    e.wg.Add(1)
    go func() {
        defer e.wg.Done()
        ticker := time.NewTicker(e.compactionInterval)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                e.runCompaction(ctx)
            }
        }
    }()
}

func (e *Engine) Close() error {
    e.cancel() // signal all goroutines to stop
    e.wg.Wait() // wait for them to finish
    return nil
}
```

### Mutex Usage

- Use `sync.Mutex` for protecting shared state.
- Keep the critical section as small as possible.
- Never hold a mutex while performing I/O or calling external code.
- Use `sync.RWMutex` when reads significantly outnumber writes.
- Document what each mutex protects with a comment:

```go
type Registry struct {
    mu       sync.RWMutex // protects segments and meta
    segments map[string]*segmentHandle
    meta     *Metadata
}
```

### Avoid Global State

No package-level mutable variables. All state is owned by structs and passed explicitly. This makes testing straightforward (no need to reset global state between tests).

## Performance-Sensitive Code

LynxDB has clear hot paths (the VM evaluation loop, the scan operator, the pipeline batch processing) and cold paths (configuration loading, segment flush, API handler setup). Different standards apply.

### Hot Path Rules

The following rules apply to code in `pkg/vm/`, `pkg/engine/pipeline/` (scan, filter, aggregate), and `pkg/storage/segment/` (reader):

- **Zero allocations**: No `make`, `new`, `append`, `fmt.Sprintf`, or interface conversions on the hot path. Pre-allocate buffers and reuse them.
- **No interfaces**: Use concrete types. Interface dispatch adds ~2ns per call, which matters at 22ns/op.
- **Avoid `reflect`**: Reflection is slow. Use type switches or code generation.
- **Batch processing**: Process 1024 rows at a time, not one at a time. This amortizes function call overhead and improves cache locality.

### Cold Path Rules

For cold paths (configuration, setup, flush, compaction), prioritize clarity over performance:

- Allocations are fine.
- Interfaces are encouraged for testability.
- Use `fmt.Errorf` freely for error wrapping.

## Testing

### Test File Location

Test files live alongside the code they test:

```
pkg/spl2/
├── parser.go
├── parser_test.go
├── lexer.go
└── lexer_test.go
```

### Table-Driven Tests

Use table-driven tests for functions with multiple input/output cases:

```go
func TestParseTimeRange(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        wantFrom time.Time
        wantTo   time.Time
        wantErr  bool
    }{
        {
            name:     "relative hour",
            input:    "-1h",
            wantFrom: now.Add(-time.Hour),
            wantTo:   now,
        },
        {
            name:    "invalid input",
            input:   "not-a-time",
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            from, to, err := ParseTimeRange(tt.input)
            if tt.wantErr {
                if err == nil {
                    t.Fatal("expected error, got nil")
                }
                return
            }
            if err != nil {
                t.Fatalf("unexpected error: %v", err)
            }
            // assert from and to
        })
    }
}
```

### Test Helpers

Use `t.Helper()` in test helper functions so that failure messages report the caller's line number:

```go
func assertEventCount(t *testing.T, result *Result, expected int) {
    t.Helper()
    if len(result.Events) != expected {
        t.Fatalf("expected %d events, got %d", expected, len(result.Events))
    }
}
```

### Benchmarks

Write benchmarks for performance-sensitive code. Use `b.ReportAllocs()` to track allocations:

```go
func BenchmarkVMSimplePredicate(b *testing.B) {
    program := compile("status >= 500")
    event := testEvent(map[string]interface{}{"status": 503})

    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        program.Eval(event)
    }
}
```

### Test Independence

Every test must be independent -- it must not depend on the outcome of another test or on global state. Use `t.TempDir()` for temporary directories, create fresh instances of engines and stores, and clean up after each test.

## Comments and Documentation

### Package Comments

Every package must have a package comment in `doc.go` or at the top of the primary file:

```go
// Package segment implements the columnar .lsg segment format for LynxDB.
// It provides a Writer for creating segments and a Reader for querying them.
package segment
```

### Exported Type and Function Comments

Every exported type, function, and method must have a Go doc comment:

```go
// SegmentWriter writes events to a columnar .lsg V2 segment file.
// It encodes each column with type-specific encoding (delta-varint for
// timestamps, dictionary for strings, Gorilla for floats, LZ4 for raw text)
// and builds a bloom filter and inverted index.
type SegmentWriter struct {
    // ...
}

// Write writes a batch of events to the segment. Events must be sorted
// by timestamp. Returns the number of events written.
func (w *SegmentWriter) Write(events []*Event) (int, error) {
    // ...
}
```

### Internal Comments

Use comments to explain **why**, not **what**. The code shows what; the comment should explain non-obvious reasoning:

```go
// Use interpolation search instead of binary search because timestamps
// are approximately uniformly distributed, giving O(log log n) expected
// complexity vs O(log n) for binary search. Benchmarks show 4.4x speedup.
idx := interpolationSearch(timestamps, target)
```

## Commit Messages

- Use imperative mood: "Add partial aggregation support", not "Added" or "Adds".
- First line: concise summary (under 72 characters).
- Blank line, then a longer description if needed.
- Reference issues: `Fixes #123` or `Closes #456`.

```
Add bloom filter segment skipping to scan operator

When a query includes literal search terms, check each segment's bloom
filter before scanning. Segments where the bloom filter returns false
for any search term are skipped entirely.

Benchmarks show 80-95% of segments are skipped for selective queries,
reducing full-text search latency by 10-50x.

Fixes #87
```

## Related

- [Development Setup](/docs/contributing/development-setup) -- build and test the project
- [Project Structure](/docs/contributing/project-structure) -- navigate the codebase
- [Architecture Overview](/docs/architecture/overview) -- understand the system

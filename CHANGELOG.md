# Changelog

All notable changes to LynxDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Query language

- **Query language: OpenSearch PPL.** Queries are OpenSearch Piped Processing
  Language pipelines (`source=main status>=500 | stats count() by host`)
  everywhere a query is accepted: `lynxdb query` against a server, over
  `--file` input or standard input, `lynxdb explain`, `lynxdb tail`, and the
  REST API.
- **PPL frontend** (`pkg/ppl`): lexer and recursive-descent parser over the
  official PPL grammar, semantic analysis against each command's output
  schema, static type checks, and lowering to IR. Limits: 256 KiB per query,
  nesting depth 256, 1000 commands per pipeline.
- **Catalogs with explicit support levels**: every OpenSearch PPL command,
  scalar function and aggregation is listed as full, partial (with its
  supported options) or unsupported (with the reason) in `pkg/ppl/catalog.go`,
  `pkg/ppl/functions.go`, `pkg/ppl/relevance.go` and `pkg/ppl/aggs.go`.
  `go run ./internal/docgen` generates the reference tables from them, and
  `GET /api/v1/catalog` serves them as JSON.
- **Commands**: search with free-text, phrase, wildcard and field terms and
  time modifiers; `where`, `fields`, `table`, `rename`, `eval`, `convert`,
  `replace`, `fillnull`, `stats`, `eventstats`, `streamstats`, `bin`,
  `timechart`, `chart`, `trendline`, `sort`, `reverse`, `head`, `dedup`,
  `top`, `rare`, `parse`, `grok`, `rex`, `regex`, `spath`, `patterns`,
  `join`, `lookup`, `append`, `appendcol`, `appendpipe`, `union`,
  `multisearch`, `describe`, `makeresults`, `expand`, `flatten`,
  `mvexpand`, `mvcombine`, `nomv`, `addtotals`, `addcoltotals`,
  `transpose`, `xyseries` and the `explain` prefix.
- **Functions**: the PPL math, string, condition, conversion, date and time,
  JSON, collection (with lambdas), IP, cryptographic and system functions;
  single-field `match`, `match_phrase` and `match_phrase_prefix` as filters
  without scoring; uncorrelated `IN`, `EXISTS` and scalar subqueries.

### Query engine

- **Canonical IR** (`pkg/ir`): typed expressions, schemas, logical operators
  and a function registry with NULL behavior, volatility and partial
  aggregation merge kinds, validated by `ir.Validate`. The engine consumes IR
  only.
- **Optimizer** (`pkg/ir/opt`): constant folding, filter merging, scan pruning
  predicates, time ranges from `_time` comparisons, `sort` + `head` as TopN,
  scan row limits and column pruning.
- **Storage hints** (`pkg/engine/physical/hints.go`, `pkg/planner`): the
  server turns each plan's scans into time bounds, term-index terms, token
  globs, field predicates, required columns and row limits, and uses them to
  skip segments and select rows.
- **Physical planner** (`pkg/engine/physical`): IR operators to pipeline
  iterators, expressions to VM bytecode, subqueries evaluated once; sort and
  aggregation spill to disk under memory pressure.
- **Single entry point** (`pkg/query`): `Compile`, `Run` and `Explain`, with
  a cache of the 1024 most recently compiled plans.

### Errors

- Compile errors carry a kind, a line and column, the expected tokens and a
  suggestion. REST codes: `SYNTAX_ERROR`, `SEMANTIC_ERROR`, `TYPE_ERROR` and
  `PLANNING_ERROR` (400); `UNSUPPORTED_COMMAND`, `UNSUPPORTED_FUNCTION` and
  `UNSUPPORTED_FEATURE` (422); `EXECUTION_ERROR` (500). Streaming queries end
  with an `__error` record on failure.
- `GET /api/v1/query/explain` returns the logical plan, the optimized plan,
  the optimizer rules that fired and the scan hints, or the errors of an
  invalid query; `analyze=true` adds execution statistics.

### Interfaces

- **CLI**: `query`, `explain`, `tail`, `fields`, `ingest`, `import`, `server`,
  `status`, `health`, `indexes`, `cache`, `jobs`, `config`, `auth`, `login`,
  `logout`, `completion` and `version`.
- **REST API**: query (sync, async jobs and hybrid), streaming, explain,
  catalog, jobs, fields, sources, histogram, live tail (SSE), ingest, indexes,
  stats, cache, config, status, API keys and metrics.

### Storage and ingest

- **Storage engine**: columnar segment format (`.lsg` format-major v1 with
  `LSG1` magic) with delta-varint timestamps, LZ4/ZSTD compression,
  dictionary-encoded strings, Gorilla-encoded floats, region magics, and a
  `FORMAT` marker.
- **Full-text search**: FST-based inverted index with roaring bitmap posting
  lists and bloom filters for segment skipping.
- **Direct-to-part ingest**: `AsyncBatcher` buffers events in memory and
  flushes immutable `.lsg` parts via atomic rename; configurable `fsync`
  policy per part write.
- **Compaction**: size-tiered compaction (L0 -> L1 -> L2) with rate limiting.
- **Ingest compatibility**: Elasticsearch `_bulk` API, OpenTelemetry
  OTLP/HTTP and gRPC, Splunk HEC receivers, and syslog.
- **File and pipe queries**: query local files and standard input with the
  same compiler and execution engine as the server; combined, syslog and
  docker formats are detected and `key=value` pairs extracted while loading.
- **Live tail**: SSE streaming with historical catch-up for queries without
  blocking operators.
- **Field catalog**: automatic field discovery with types, coverage and top
  values.

### Distribution

- **Install script**: `curl -fsSL https://lynxdb.org/install.sh | sh` with
  platform detection and checksum verification.
- **Container images**: multi-arch (`amd64`/`arm64`) images on
  `ghcr.io/lynxbase/lynxdb`.
- **Homebrew tap**: `brew install lynxbase/tap/lynxdb`.

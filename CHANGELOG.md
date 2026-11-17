# Changelog

All notable changes to LynxDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Query language

- **Query language: OpenSearch PPL.** Queries are OpenSearch Piped Processing
  Language pipelines (`source=main status>=500 | stats count() by host`)
  everywhere a query is accepted: `lynxdb query` against a server, over
  `--file` input or standard input, `lynxdb explain`, and the REST API.
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
- **Columnar executor** (`pkg/colstore/exec`, `pkg/engine/columnar`): every
  part of a plan it takes, from a scan up (filters, computed columns, one
  aggregate, a choice of columns, a sort, a limit, the first row of each
  key), runs over the column blocks of the stores on every core, skipping
  blocks by time range, column statistics and token filters. Membership
  tests against subqueries of up to 1024 values run as lists of values.
- **Row operators** (`pkg/engine/physical`, `pkg/engine/pipeline`,
  `pkg/vm`): the rest of a plan runs on pull-based iterators over batches of
  rows, with expressions compiled to VM bytecode and uncorrelated subqueries
  evaluated once.
- **Single entry point** (`pkg/query`): `Compile` and `Explain`, with a cache
  of the 1024 most recently compiled plans.

### Errors

- Compile errors carry a kind, a line and column, the expected tokens and a
  suggestion. REST codes: `SYNTAX_ERROR`, `SEMANTIC_ERROR`, `TYPE_ERROR` and
  `PLANNING_ERROR` (400); `UNSUPPORTED_COMMAND`, `UNSUPPORTED_FUNCTION` and
  `UNSUPPORTED_FEATURE` (422); `EXECUTION_ERROR` (500); `TOO_MANY_REQUESTS`
  (429) when `query.max_concurrent` queries run.
- `GET /api/v1/query/explain` returns the logical plan, the optimized plan,
  the optimizer rules that fired, the fields read and the source scope, or
  the errors of an invalid query; `analyze=true` runs the query and adds its
  time and row count.

### Interfaces

- **CLI**: `query`, `explain`, `fields`, `ingest`, `import`, `server`,
  `status`, `health`, `indexes`, `jobs`, `config`, `auth`, `login`, `logout`,
  `completion` and `version`.
- **REST API**: query (sync, async jobs and hybrid), NDJSON streaming,
  explain, catalog, jobs, fields, histogram, ingest, indexes, stats,
  config, status, API keys and Prometheus metrics. Query responses carry
  `took_ms`, `query_id` and `rows` in `meta`.

### Storage and ingest

- **Columnar store** (`pkg/colstore`, `pkg/database`): one store per index
  under the data directory, created with the index's first events. JSON is
  parsed once into typed columns by field path; a line that is not a JSON
  object is kept with its text in `_raw`. Rows are partitioned by time
  (`storage.partition`, 24h), written as sorted runs within
  `storage.flush_interval` or on sync, made durable by a manifest record, and
  merged in the background. Parts hold blocks with dictionary, constant and
  time encodings, zstd or LZ4 compression and per-block token filters.
- **Event time** from the first of `storage.time_fields` an event holds
  (`_time`, `timestamp`, `@timestamp`, `time`, `ts`), otherwise the time of
  arrival.
- **Retention** drops whole partitions older than `retention`, at startup and
  once a minute, and logs each removal at `WARN` with the index, the event
  count and the cutoff.
- **Ingest**: NDJSON appended as it is (`POST /api/v1/ingest`); log text with
  timestamp and `key=value` extraction (`POST /api/v1/ingest/raw`);
  Elasticsearch `_bulk` documents as they are; OpenTelemetry OTLP/HTTP and
  gRPC, Splunk HEC and syslog decoded into one flat JSON object per event.
  The native ingest routes are bounded by `ingest.max_body_size`, the
  shipper routes by `ingest.limits.*`; a `413` names the setting.
- **File and pipe queries**: query local files and standard input with the
  same compiler and executors as the server, over a database in a temporary
  directory; combined and syslog formats are detected and `key=value` pairs
  extracted while loading.
- **Field catalog**: the fields of every index and the top values of a
  field.

### Distribution

- **Install script**: `curl -fsSL https://lynxdb.org/install.sh | sh` with
  platform detection and checksum verification.
- **Container images**: multi-arch (`amd64`/`arm64`) images on
  `ghcr.io/lynxbase/lynxdb`.
- **Homebrew tap**: `brew install lynxbase/tap/lynxdb`.

---
title: Query Settings
description: Concurrency, timeouts, result limits, async jobs, memory and spill, and advisory lint thresholds for the LynxDB query engine.
---

# Query Settings

The `query` section controls how the server runs queries. Every `query.*` key takes effect on [reload](./overview.md#reloading-a-running-server) except `global_query_pool_bytes`, `spill_dir` and `max_temp_dir_size_bytes`, which need a restart.

## Concurrency and Timeouts

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `query.max_concurrent` | `LYNXDB_QUERY_MAX_CONCURRENT` | `32` | Queries running at once. Further queries get `429 TOO_MANY_REQUESTS` with `Retry-After: 1`. |
| `query.sync_timeout` | `LYNXDB_QUERY_SYNC_TIMEOUT` | `30s` | How long a synchronous `POST /api/v1/query` waits. A query still running after this is returned as an async job (`{"data":{"job_id":"qry_...","status":"running","type":"job"}}`). Minimum `1s`. |
| `query.max_query_runtime` | `LYNXDB_QUERY_MAX_QUERY_RUNTIME` | `5m` | Hard limit on one query's run time, sync or async. `0` means unlimited; otherwise at least `1s`. |
| `query.max_branch_parallelism` | `LYNXDB_QUERY_MAX_BRANCH_PARALLELISM` | `0` | Goroutines that run the branches of `append`, `multisearch` and `union` in parallel. `0` uses `GOMAXPROCS`. |
| `query.slow_query_threshold_ms` | `LYNXDB_QUERY_SLOW_QUERY_THRESHOLD_MS` | `1000` | Queries slower than this are logged at `WARN` with their execution statistics. `0` disables the log. |

## Result Limits

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `query.default_result_limit` | `LYNXDB_QUERY_DEFAULT_RESULT_LIMIT` | `1000` | Rows per response when the request sets no `limit`. |
| `query.max_result_limit` | `LYNXDB_QUERY_MAX_RESULT_LIMIT` | `50000` | Largest `limit` a request can ask for. Event queries without a final `head` are also capped at this many rows while results are collected. Must be at least `default_result_limit`. |
| `query.max_query_length` | none | `1048576` | Longest accepted query text in bytes; longer queries get `400 QUERY_TOO_LARGE`. `0` disables the check. The PPL parser rejects queries over 256 KiB on its own. |

Aggregation results are not capped by `max_result_limit` during collection, since their size depends on group cardinality. To export many raw events, use `POST /api/v1/query/stream`, which writes NDJSON rows as they are produced.

## Async Jobs

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `query.job_ttl` | `LYNXDB_QUERY_JOB_TTL` | `5m` | How long a finished job and its results stay available under `/api/v1/query/jobs/{id}`. |
| `query.job_gc_interval` | `LYNXDB_QUERY_JOB_GC_INTERVAL` | `30s` | How often expired jobs are deleted. |
| `query.preview_size` | none | `50` | Rows included in progress previews of a running job. `0` disables previews. |

## Memory and Spill

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `query.max_query_memory_bytes` | none | `LYNXDB_QUERY_MAX_QUERY_MEMORY_BYTES` | `1gb` | Memory budget of one query. Sorts and aggregations spill to disk when they approach it; a query that still exceeds it fails with `400 QUERY_MEMORY_EXCEEDED`. |
| `query.spill_dir` | `--spill-dir` | `LYNXDB_QUERY_SPILL_DIR` | system temp directory | Directory for spill files. Orphaned spill files are deleted at startup. |
| `query.max_temp_dir_size_bytes` | none | `LYNXDB_QUERY_MAX_TEMP_DIR_SIZE_BYTES` | `10gb` | Disk quota for spill files across all queries. `0` means unlimited. |
| `query.global_query_pool_bytes` | `--max-query-pool` | `LYNXDB_QUERY_GLOBAL_QUERY_POOL_BYTES` | `0` | Validated but not read by the server. The shared pool is `server.total_memory_pool_bytes`. |

The total memory of all running queries and the query cache is bounded by [`server.total_memory_pool_bytes`](./server.md#memory-pool). When that pool is exhausted a query fails with `503 QUERY_POOL_EXHAUSTED`.

`lynxdb query --file` and piped input run the query in the CLI process, not on the server. Limit that process with `--max-memory`:

```bash
lynxdb query --file access.log --max-memory 512mb 'source=main | stats count() by host'
```

## Scan Tuning

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `query.bitmap_selectivity_threshold` | `LYNXDB_QUERY_BITMAP_SELECTIVITY_THRESHOLD` | `0.9` | When an inverted-index bitmap matches more than this fraction of a segment's rows, the scan reads the segment sequentially instead. `1.0` always uses the bitmap. Range 0.0 to 1.0. |
| `query.dedup_exact` | `LYNXDB_QUERY_DEDUP_EXACT` | `false` | Validated but not read by the server. |

## Advisory Lints

The server adds advisory lints to a query response (`meta.lints`) when the request does not turn lints off (`"lint": false`). Two of them depend on these thresholds and apply only to queries over every source (`source=*` or `index=*`):

| Key | Env var | Default | Lint |
|-----|---------|---------|------|
| `query.broad_source_lint_threshold` | `LYNXDB_QUERY_BROAD_SOURCE_LINT_THRESHOLD` | `10` | `ALL_SOURCES_HIGH_VOLUME` when the query reads at least this many sources. |
| `query.broad_segment_lint_threshold` | `LYNXDB_QUERY_BROAD_SEGMENT_LINT_THRESHOLD` | `1000` | `BROAD_SEARCH` when a free-text or regex search over all sources touches at least this many segments, or reaches the source threshold. |

`0` falls back to the default. Lints never change results.

For a query such as `source=* | stats count() by host` on a server with many sources, naming the source (`source=main | stats count() by host`) clears the lint.

## Complete Example

```yaml
query:
  max_concurrent: 32
  sync_timeout: "30s"
  max_query_runtime: "10m"
  default_result_limit: 1000
  max_result_limit: 50000
  max_query_memory_bytes: "2gb"
  spill_dir: "/var/lib/lynxdb/spill"
  max_temp_dir_size_bytes: "20gb"
  job_ttl: "15m"
  slow_query_threshold_ms: 2000
```

## Tuning Guidelines

| Workload | Change |
|----------|--------|
| Many concurrent users | Raise `max_concurrent` with the CPU count; raise `server.total_memory_pool_bytes` so queries do not contend for memory. |
| Long time ranges, large aggregations | Raise `max_query_runtime` and `max_query_memory_bytes`; put `spill_dir` on a fast local disk. |
| API clients that need fast answers | Lower `sync_timeout` and poll the returned job, or send `"wait": 0` to get a job immediately. |
| Bulk export of raw events | Use `POST /api/v1/query/stream` instead of raising `max_result_limit`. |

For writing faster queries, see [PPL performance](../ppl/performance.md).

## Related

- [PPL overview](../ppl/overview.md)
- [Query API](../api/query.md)
- [Server Settings](./server.md)
- [Performance Tuning](../operations/performance-tuning.md)

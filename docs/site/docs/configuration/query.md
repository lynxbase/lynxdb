---
title: Query Settings
description: Configure LynxDB query engine -- concurrency limits, timeouts, memory limits, and result limits.
---

# Query Settings

The `query` section controls how the query engine allocates resources, enforces limits, and manages async jobs.

## Concurrency

### Max Concurrent Queries

The maximum number of queries that can execute simultaneously.

| Config Key | `query.max_concurrent` |
|---|---|
| **Env Var** | `LYNXDB_QUERY_MAX_CONCURRENT` |
| **Default** | `10` |
| **Hot-Reloadable** | Yes |

```yaml
query:
  max_concurrent: 10
```

When the limit is reached, new queries are queued. Increase this on machines with more CPU cores and memory. A good starting point is 2x the number of CPU cores.

```bash
# Change at runtime
lynxdb config set query.max_concurrent 20
lynxdb config reload
```

## Timeouts

### Sync Timeout

Maximum wait time for synchronous query execution. If the query does not complete within this time, the server returns a job ID for async polling.

| Config Key | `query.sync_timeout` |
|---|---|
| **Env Var** | `LYNXDB_QUERY_SYNC_TIMEOUT` |
| **Default** | `30s` |

```yaml
query:
  sync_timeout: "30s"
```

This is the server-side timeout for `POST /api/v1/query`. The CLI also supports a client-side `--timeout` flag.

### Max Query Runtime

Hard limit on how long any single query can run, regardless of execution mode.

| Config Key | `query.max_query_runtime` |
|---|---|
| **Env Var** | `LYNXDB_QUERY_MAX_QUERY_RUNTIME` |
| **Default** | `5m` |
| **Hot-Reloadable** | Yes |

```yaml
query:
  max_query_runtime: "10m"
```

Queries that exceed this limit are cancelled with an error. Increase for workloads that involve scanning large time ranges or complex aggregations.

## Result Limits

### Default Result Limit

The default number of result rows returned when the query does not include a `HEAD` or `LIMIT` command.

| Config Key | `query.default_result_limit` |
|---|---|
| **Env Var** | `LYNXDB_QUERY_DEFAULT_RESULT_LIMIT` |
| **Default** | `1000` |
| **Hot-Reloadable** | Yes |

```yaml
query:
  default_result_limit: 1000
```

### Max Result Limit

The hard cap on result rows, even if the query explicitly requests more.

| Config Key | `query.max_result_limit` |
|---|---|
| **Env Var** | `LYNXDB_QUERY_MAX_RESULT_LIMIT` |
| **Default** | `50000` |
| **Hot-Reloadable** | Yes |

```yaml
query:
  max_result_limit: 50000
```

For exporting large datasets, use the streaming endpoint (`POST /api/v1/query/stream`) which is not subject to this limit.

## Memory Limits

### Server-Side Memory Pool

The global memory pool shared across all concurrent queries.

| CLI Flag | `--max-query-pool` |
|---|---|
| **Default** | (unlimited) |

```bash
lynxdb server --max-query-pool 4gb
```

When the pool is exhausted, queries spill intermediate results to disk.

### Spill Directory

Directory for temporary spill files when query memory is exceeded.

| CLI Flag | `--spill-dir` |
|---|---|
| **Default** | OS temp directory |

```bash
lynxdb server --max-query-pool 2gb --spill-dir /data/lynxdb/tmp
```

### Client-Side Memory Limit

For pipe/file mode queries, limit the ephemeral engine's memory usage.

```bash
# Limit ephemeral engine to 512MB
lynxdb query --file huge.log '| stats count by host' --max-memory 512mb
```

## Async Job Management

### Job TTL

How long completed async job results are kept before garbage collection.

| Config Key | `query.job_ttl` |
|---|---|
| **Env Var** | `LYNXDB_QUERY_JOB_TTL` |
| **Default** | `10m` |

```yaml
query:
  job_ttl: "10m"
```

### Job GC Interval

How often the server cleans up expired async jobs.

| Config Key | `query.job_gc_interval` |
|---|---|
| **Env Var** | `LYNXDB_QUERY_JOB_GC_INTERVAL` |
| **Default** | `1m` |

```yaml
query:
  job_gc_interval: "1m"
```

## Complete Example

```yaml
query:
  sync_timeout: "30s"
  max_query_runtime: "10m"
  max_concurrent: 20
  default_result_limit: 1000
  max_result_limit: 50000
  job_ttl: "10m"
  job_gc_interval: "1m"
```

## Tuning Guidelines

| Workload | Recommendation |
|---|---|
| High query concurrency | Increase `max_concurrent` to 30-50, ensure enough CPU cores |
| Large time-range scans | Increase `max_query_runtime` to `30m`, set `--max-query-pool` |
| API integrations with strict latency | Decrease `sync_timeout` to `10s`, use async mode for slow queries |
| Exporting large datasets | Use `/api/v1/query/stream`, not the result limit settings |
| CI/CD pipelines | Use `--timeout` on the client side, `--fail-on-empty` for assertions |

## Next Steps

- [Performance Tuning](/docs/operations/performance-tuning) -- end-to-end optimization guide
- [Storage Settings](/docs/configuration/storage) -- cache and compaction tuning
- [Monitoring](/docs/operations/monitoring) -- track query performance metrics

---
title: Query Settings
description: Concurrency, the synchronous wait, result limits and job lifetime of LynxDB queries.
---

# Query Settings

The `query` section controls how the server runs queries. Every query runs as a job; these settings bound how many run at once, how long a request waits for one, how many rows a response carries and how long a finished job stays.

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `query.max_concurrent` | `LYNXDB_QUERY_MAX_CONCURRENT` | `32` | Queries running at once. A query beyond it is refused with `429 TOO_MANY_REQUESTS` and `Retry-After: 1`. Minimum `1`. Needs a restart. |
| `query.sync_timeout` | `LYNXDB_QUERY_SYNC_TIMEOUT` | `30s` | How long `POST /api/v1/query` without `wait` waits for the result. A query still running then is answered with its job handle (`202`, `{"data":{"type":"job","job_id":"qry_...","status":"running",...}}`) and keeps running. Minimum `1s`. Reloadable. |
| `query.job_ttl` | `LYNXDB_QUERY_JOB_TTL` | `5m` | How long a finished job and its rows stay available under `/api/v1/query/jobs/{id}`. Finished jobs are swept once a minute. Minimum `1s`. Needs a restart. |
| `query.default_result_limit` | `LYNXDB_QUERY_DEFAULT_RESULT_LIMIT` | `1000` | Rows per response when the request sets no `limit`. Minimum `1`. Reloadable. |
| `query.max_result_limit` | `LYNXDB_QUERY_MAX_RESULT_LIMIT` | `50000` | Largest `limit` a request can ask for; a larger one is lowered to it. At least `default_result_limit`. Reloadable. |
| `query.max_query_length` | none | `1048576` | Longest accepted query text in bytes; a longer one gets `400 QUERY_TOO_LARGE`. `0` disables the check. Reloadable. |

A job keeps running when its request returns, until it finishes, `DELETE /api/v1/query/jobs/{id}` cancels it, or the server shuts down. The result limits shape the response only: a query computes all its rows, and the response pages through them with `limit` and `offset` and reports `has_more`. To export many rows at once, use `POST /api/v1/query/stream`, which writes NDJSON rows without a limit.

`lynxdb query --file`, including `--file -` for standard input, runs the query in the CLI process, not on the server, so these settings do not apply to it.

## Complete Example

```yaml
query:
  max_concurrent: 32
  sync_timeout: "30s"
  job_ttl: "15m"
  default_result_limit: 1000
  max_result_limit: 50000
  max_query_length: 1048576
```

## Tuning Guidelines

| Workload | Change |
|----------|--------|
| Many concurrent users or dashboards | Raise `max_concurrent`; queries beyond it are refused, not queued. |
| API clients that need fast answers | Lower `sync_timeout` and poll the returned job, or send `"wait": 0` to get a job at once. |
| Clients that come back for results later | Raise `job_ttl`. |
| Bulk export of raw events | Use `POST /api/v1/query/stream` instead of raising `max_result_limit`. |

For writing faster queries, see [PPL performance](../ppl/performance.md).

## Related

- [PPL overview](../ppl/overview.md)
- [Query API](../api/query.md)
- [Server Settings](./server.md)
- [Performance Tuning](../operations/performance-tuning.md)

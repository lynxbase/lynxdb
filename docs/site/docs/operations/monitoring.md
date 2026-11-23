---
title: Monitoring
description: Monitor LynxDB health and performance -- health, status and stats endpoints, the status command, Prometheus metrics, request logs and query profiles.
---

# Monitoring LynxDB

LynxDB reports its health and load through REST endpoints, a Prometheus `/metrics` endpoint, its request log and CLI commands. This page lists what each one shows and what to watch.

## Health Check

`GET /health` is meant for load balancers and container orchestrators. It needs no API key:

```bash
curl http://localhost:3100/health
```

```json
{"data":{"status":"healthy","version":"dev"}}
```

It answers `200 OK` while the server accepts requests. Use it for load balancer health checks, Kubernetes liveness and readiness probes, and uptime monitoring. The CLI calls the same endpoint:

```bash
lynxdb health
```

## Server Status

`GET /api/v1/status` sums up the server:

```bash
curl -s http://localhost:3100/api/v1/status
```

```json
{
  "data": {
    "events": {"total": 60},
    "health": "healthy",
    "queries": {"active": 0},
    "retention": {"oldest_event": "2026-02-10T08:12:03Z"},
    "storage": {"used_bytes": 6803},
    "uptime_seconds": 8,
    "version": "dev"
  }
}
```

| Field | Meaning |
|-------|---------|
| `events.total` | Events held by all indexes |
| `storage.used_bytes` | Bytes of the part files of all indexes |
| `queries.active` | Query jobs running now |
| `retention.policy` | The `retention` setting, such as `168h0m0s`; absent when retention is `0` |
| `retention.oldest_event` | Time of the oldest event held |

`GET /api/v1/stats` breaks the same numbers down by index and counts the query jobs the server keeps:

```bash
curl -s http://localhost:3100/api/v1/stats
```

```json
{
  "data": {
    "index_count": 2,
    "indexes": [
      {"name": "main", "events": 26, "bytes": 3978, "parts": 1,
       "first_event": "2026-02-10T08:12:03.045Z", "last_event": "2026-02-10T10:45:00.5Z"},
      {"name": "nginx", "events": 34, "bytes": 2825, "parts": 1,
       "first_event": "2026-11-17T15:43:19.088137Z", "last_event": "2026-11-17T15:43:19.088137Z"}
    ],
    "jobs": {"canceled": 0, "done": 0, "error": 0, "running": 0},
    "newest_event": "2026-11-17T15:43:19Z",
    "oldest_event": "2026-02-10T08:12:03Z",
    "part_count": 2,
    "storage_bytes": 6803,
    "total_events": 60,
    "uptime_seconds": 8
  }
}
```

Both endpoints sync the database before they answer, so they count every event ingested before the request.

`parts` is the number of immutable part files an index holds. The store merges parts in the background, so the count rises with ingest and falls again as merges finish. A count that only grows means merges cannot keep up with ingest.

The CLI shows the same information:

```bash
lynxdb status
```

```text
  LynxDB dev — uptime 0m — healthy

  Storage  6.6 KB
  Events   60 total
  Parts    2
  Indexes  2
  Indexes        main (43%), nginx (57%)
  Oldest         2026-02-10T08:12:03Z
  Newest         2026-11-17T15:43:19Z
```

```bash
lynxdb status --format json   # the fields of /api/v1/stats
lynxdb indexes                # one row per index: events, bytes, parts, first and last event
```

## Prometheus Metrics

`GET /metrics` serves the Prometheus text format without authentication. Besides the standard `go_*` and `process_*` series, it exports these LynxDB series. A series with labels appears once it has been recorded.

### Ingest

| Metric | Labels | Description |
|--------|--------|-------------|
| `lynxdb_ingest_events_total` | | Events ingested |
| `lynxdb_ingest_events_expired_total` | | Events refused for being older than the retention |
| `lynxdb_ingest_batches_total` | | Batches ingested |
| `lynxdb_ingest_bytes_total` | | NDJSON bytes taken by `/api/v1/ingest`, `/api/v1/ingest/raw` and the bulk routes |
| `lynxdb_ingest_errors_total` | | Batches the database refused |
| `lynxdb_ingest_dropped_records_total` | `source`, `reason` | Records dropped by a receiver |
| `lynxdb_ingest_decompression_rejected_total` | `stage`, `encoding` | Requests refused with `413` by `ingest.max_body_size` or `ingest.limits.*` |
| `lynxdb_ingest_listener_up` | `listener` | `1` while the `es`, `otlp_http` or `otlp_grpc` listener is bound |
| `lynxdb_ingest_es_bulk_requests_total` | `result` | Elasticsearch bulk requests |
| `lynxdb_ingest_es_bulk_items_total` | `action`, `result` | Elasticsearch bulk items |
| `lynxdb_ingest_es_bulk_duration_seconds` | | Time per bulk request (histogram) |
| `lynxdb_ingest_es_handshake_total` | `kind` | Elasticsearch handshake and setup probes |
| `lynxdb_ingest_otlp_requests_total` | `signal`, `encoding`, `result` | OTLP requests |
| `lynxdb_ingest_otlp_records_total` | `signal`, `result` | OTLP log records |
| `lynxdb_ingest_otlp_request_bytes` | | Decompressed OTLP/HTTP body sizes (histogram) |
| `lynxdb_syslog_messages_received_total` | `transport`, `dialect` | Syslog messages, when syslog is on |
| `lynxdb_syslog_messages_dropped_total` | `transport`, `reason` | Dropped syslog messages |
| `lynxdb_syslog_parse_errors_total` | `dialect` | Syslog messages that did not parse |
| `lynxdb_syslog_active_connections` | | Open TCP syslog connections |

### Queries

| Metric | Labels | Description |
|--------|--------|-------------|
| `lynxdb_query_duration_seconds` | `result_type` (`events`, `aggregate`) | Query duration (histogram) |
| `lynxdb_query_errors_total` | `type` | Failed queries by error code, such as `EXECUTION_ERROR` |

### What to Watch

- The rate of `lynxdb_ingest_events_total`: a drop points at shippers or the network.
- `lynxdb_ingest_errors_total` and `lynxdb_ingest_dropped_records_total`: any sustained increase.
- `lynxdb_ingest_listener_up` at `0` for a listener you configured.
- The p99 of `lynxdb_query_duration_seconds`.
- `lynxdb_query_errors_total` by `type`: queries that compiled but failed while they ran.
- `status=429` in the [request log](#request-log): queries refused because `query.max_concurrent` queries were running.

### Scrape Configuration

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'lynxdb'
    scrape_interval: 30s
    metrics_path: '/metrics'
    static_configs:
      - targets: ['lynxdb:3100']
```

Kubernetes `ServiceMonitor`:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: lynxdb
  namespace: lynxdb
spec:
  selector:
    matchLabels:
      app: lynxdb
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
```

## Request Log

The server logs every HTTP request except `/health` at `INFO` to stdout, with its method, path, status, `duration_ms`, remote address, request ID and, for queries, the `query_id`:

```text
time=2026-11-17T16:49:30.931+01:00 level=INFO msg="http request" method=POST path=/api/v1/query status=200 duration_ms=18 remote=127.0.0.1:60671 query_id=qry_0df6d3917e47b479 request_id=18e8dfd823b-0002
```

The `query_id` is the same one that query responses carry in `meta.query_id` and the `X-Query-ID` header.

## Query Profiling

`--analyze` adds a profile to a query's output:

```bash
lynxdb query 'source=main level=ERROR | stats count() by service' --analyze
```

```text
  ══════ Query Profile ══════

  Phase Breakdown:
    exec            207µs    12%   ████████████████████
                 ──────────
    total             1ms
```

`exec` is the time the server spent running the query; `total` includes the round trip. `lynxdb explain` prints the plan the server runs, with the columns each scan reads; see [Performance Tuning](/docs/operations/performance-tuning#query-shape).

## Monitoring Checklist

For production deployments, monitor at least:

- [ ] `/health`: uptime
- [ ] The rate of `lynxdb_ingest_events_total`: shipper failures
- [ ] Disk use of the data directory, and `storage_bytes` in `/api/v1/stats`: capacity
- [ ] The p99 of `lynxdb_query_duration_seconds`: latency
- [ ] `queries.active` in `/api/v1/status` against `query.max_concurrent`: concurrency
- [ ] `part_count` in `/api/v1/stats`: merges keeping up with ingest

## Next Steps

- [Performance Tuning](/docs/operations/performance-tuning) -- act on what the metrics show
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose common issues
- [Retention Policies](/docs/operations/retention) -- manage data lifecycle
- [Query Settings](/docs/configuration/query) -- tune concurrency and timeouts

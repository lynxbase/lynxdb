---
title: Monitoring
description: Monitor LynxDB health and performance -- health and status endpoints, the status command, Prometheus metrics, cache statistics, and query profiling.
---

# Monitoring LynxDB

LynxDB reports its health and performance through REST endpoints, a Prometheus `/metrics` endpoint, and CLI commands. This page lists what each one shows and what to watch.

## Health Check

The `/health` endpoint is meant for load balancers and container orchestrators:

```bash
curl http://localhost:3100/health
# {"data":{"degraded":false,"status":"healthy","version":"X.Y.Z"}}
```

It returns `200 OK` when the server accepts requests. Use it for:

- load balancer health checks
- Kubernetes liveness and readiness probes
- uptime monitoring

```bash
# CLI health check
lynxdb health
```

## Server Status

`GET /api/v1/stats` returns storage and event counters:

```bash
curl -s http://localhost:3100/api/v1/stats | jq .
```

```json
{
  "data": {
    "buffered_events": 0,
    "events_today": 114,
    "index_count": 2,
    "oldest_event": "2025-01-15T08:00:00Z",
    "segment_count": 4,
    "sources": [],
    "storage_bytes": 242686,
    "total_events": 1003,
    "uptime_seconds": 394
  }
}
```

`GET /api/v1/status` combines these counters with health, memory governor usage per class, active queries, and live tail sessions.

The CLI shows the same information:

```bash
lynxdb status
```

```
  LynxDB X.Y.Z — uptime 2d 5h — healthy

  Storage   1.2 GB
  Events    3,456,789 total  123,456 today
  Segments  42  buffer: 8,200 events
  Indexes   3
  Oldest    2025-01-08T10:30:00Z
```

For machine-readable output:

```bash
lynxdb status --format json
```

## Prometheus Metrics

`GET /metrics` serves the Prometheus text format. Besides the standard `go_*` and `process_*` series, it exports these LynxDB series:

### Ingest

| Metric | Description | What to watch |
|--------|-------------|---------------|
| `lynxdb_ingest_events_total` | Events ingested | Rate drops point at shipper or network problems |
| `lynxdb_ingest_bytes_total` | Raw bytes ingested | Capacity planning |
| `lynxdb_ingest_batches_total` | Ingest batches processed | |
| `lynxdb_ingest_errors_total` | Ingest errors | Any sustained increase |
| `lynxdb_ingest_listener_up` | Whether an ingest listener (OTLP, syslog) is bound | `0` for a listener you configured |
| `lynxdb_ingest_staging_bytes`, `lynxdb_ingest_staging_events` | Pending data in the shipper staging buffer | Steady growth means the sink falls behind |
| `lynxdb_ingest_staging_age_seconds` | Age of the oldest staged event | Should stay below `ingest.staging.max_age` |
| `lynxdb_ingest_staging_overflows_total` | Staging buffer overflows | Non-zero means shippers receive rejections |
| `lynxdb_ingest_es_bulk_requests_total`, `lynxdb_ingest_es_bulk_items_total` | Elasticsearch bulk requests and items by result | Rejected items |
| `lynxdb_ingest_otlp_request_bytes` | OTLP HTTP request body sizes (histogram) | |

### Storage and Compaction

| Metric | Description | What to watch |
|--------|-------------|---------------|
| `lynxdb_compaction_queue_depth` | Pending compaction jobs | A growing queue means compaction cannot keep up; add workers or raise the rate limit |
| `lynxdb_compaction_runs_total`, `lynxdb_compaction_errors_total` | Compaction runs and failures | Any errors |
| `lynxdb_compaction_input_bytes_total`, `lynxdb_compaction_output_bytes_total` | Bytes read and written by compaction | Write amplification |
| `lynxdb_compaction_duration_seconds_total` | Cumulative compaction time | |
| `lynxdb_tiering_uploads_total`, `lynxdb_tiering_upload_bytes_total` | Segments and bytes uploaded to the warm/cold tier | Object-store costs |
| `lynxdb_tiering_downloads_total`, `lynxdb_tiering_download_bytes_total` | Segments and bytes fetched from the warm/cold tier | High download rates mean the segment cache is too small |

### Queries

| Metric | Description | What to watch |
|--------|-------------|---------------|
| `lynxdb_query_duration_seconds` | Query duration (histogram) | p99 latency |
| `lynxdb_query_scan_duration_seconds`, `lynxdb_query_pipeline_duration_seconds` | Time in segment scan and in pipeline execution | Which phase dominates |
| `lynxdb_query_rows_scanned` | Rows scanned per query (histogram) | Queries without selective filters |
| `lynxdb_query_peak_memory_bytes` | Peak memory per query (histogram) | Queries close to `query.max_query_memory_bytes` |
| `lynxdb_query_errors_total` | Query errors by type | Timeouts and memory errors |
| `lynxdb_query_slow_total` | Queries above `query.slow_query_threshold_ms` | |
| `lynxdb_segments_scanned_total` | Segments read by queries | |
| `lynxdb_segments_skipped_time_total`, `lynxdb_segments_skipped_bloom_total`, `lynxdb_segments_skipped_column_stats_total` | Segments skipped by time range, bloom filter, and column min/max | A low skip ratio means queries lack time bounds or selective terms |
| `lynxdb_cache_hits_total`, `lynxdb_cache_misses_total`, `lynxdb_cache_evictions_total`, `lynxdb_cache_size_bytes` | Query result cache | Hit rate and evictions |
| `lynxdb_spill_bytes_active`, `lynxdb_spill_files_active`, `lynxdb_spill_bytes_total` | Spill files of blocking operators | Disk use against `query.max_temp_dir_size_bytes` |

### Memory

| Metric | Description | What to watch |
|--------|-------------|---------------|
| `lynxdb_memgov_class_bytes` | Bytes held per memory governor class | Growth of one class |
| `lynxdb_memgov_class_peak_bytes` | Peak bytes per class | |
| `lynxdb_memgov_pressure_events_total` | Memory pressure events | Sustained increases mean the memory pool is too small |

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

## Cache Statistics

```bash
lynxdb cache stats
```

```
  Cache Statistics

  Hits      12,456
  Misses    3,789
  Hit Rate  76.7%
  Entries   1,234
  Size      456 MB
  Evictions 89
```

```bash
# Machine-readable
lynxdb cache stats --format json
```

The result cache key includes an ingest generation counter, so new data makes earlier entries miss. A low hit rate is expected under constant ingest. When the same queries repeat over data that does not change, raise `storage.cache_max_bytes` or `storage.cache_ttl`.

## Query Profiling

Profile a single query to find where it spends time:

```bash
# Phase breakdown, segments scanned and skipped, rows, optimizer rules
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze

# Adds per-operator timing
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze=full

# Adds per-segment detail
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze=trace
```

Pass the level with `=`: a bare `--analyze` means `basic`. The profile shows:

- segments scanned and skipped, with the skip reason (time, index, column stats, bloom)
- rows scanned and rows returned
- the optimizer rules that rewrote the plan
- peak memory and CPU time
- recommendations, such as adding a time range

`lynxdb explain --analyze '<query>'` runs the query on the server and prints the logical and optimized plans together with its profile. See [PPL performance](../ppl/performance.md) for how plan shape affects cost.

## Monitoring Checklist

For production deployments, monitor at least:

- [ ] `/health` -- uptime
- [ ] `lynxdb_ingest_events_total` rate -- shipper failures
- [ ] Disk usage and growth -- capacity planning
- [ ] `lynxdb_query_duration_seconds` p99 -- latency
- [ ] `queries.active` in `/api/v1/status` against `query.max_concurrent` -- concurrency saturation
- [ ] `lynxdb_compaction_queue_depth` -- compaction backlog
- [ ] `buffered_events` in `/api/v1/stats` -- flush health
- [ ] `lynxdb_memgov_pressure_events_total` -- memory pressure

## Next Steps

- [Performance Tuning](/docs/operations/performance-tuning) -- act on what the metrics show
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose common issues
- [Retention Policies](/docs/operations/retention) -- manage data lifecycle
- [Query Settings](/docs/configuration/query) -- tune concurrency and timeouts

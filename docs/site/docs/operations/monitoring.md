---
title: Monitoring
description: Monitor LynxDB health and performance -- /stats endpoint, status command, key metrics, and Prometheus integration.
---

# Monitoring LynxDB

LynxDB exposes comprehensive health and performance metrics through its REST API and CLI commands. This guide covers what to monitor, how to access metrics, and how to set up alerts.

## Health Check

The `/health` endpoint is designed for load balancers and container orchestrators:

```bash
curl http://localhost:3100/health
# {"status": "ok"}
```

Returns `200 OK` when the server is ready to accept requests. Use this for:
- Load balancer health checks
- Kubernetes liveness/readiness probes
- Uptime monitoring

```bash
# CLI health check
lynxdb health
```

## Server Status

The `GET /api/v1/stats` endpoint returns detailed server metrics:

```bash
curl -s http://localhost:3100/api/v1/stats | jq .
```

CLI shorthand:

```bash
lynxdb status

# Output:
#   LynxDB v0.1.0 -- uptime 2d 5h 30m -- healthy
#
#   Storage:     1.2 GB
#   Events:      3,456,789 total    123,456 today
#   Segments:    42    Memtable: 8200 events
#   Sources:     nginx (45%), api-gateway (30%), postgres (25%)
#   Oldest:      2025-01-08T10:30:00Z
#   Indexes:     3
```

For machine-readable output:

```bash
lynxdb status --format json
```

## Live Dashboard

The `lynxdb top` command provides a full-screen live TUI dashboard:

```bash
lynxdb top
lynxdb top --interval 5s
```

Shows four panels:
- **Ingest**: Events/sec, events today, total events
- **Queries**: Active queries, cache hit rate, materialized view count, tail sessions
- **Storage**: Total size, segment count, memtable size, index count
- **Sources**: Bar chart of events by source

Press `q` or `Ctrl+C` to exit.

## Key Metrics

### Ingest Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| Events ingested/sec | Current ingest rate | Sudden drops indicate pipeline issues |
| Events today | Events ingested since midnight | Compare to baseline |
| Total events | All events in storage | Growth rate |
| WAL size | Current WAL size | Growing WAL means flush is slow |

### Storage Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| Total storage size | Disk usage for all segments | Capacity planning |
| Segment count | Number of `.lsg` segments | High L0 count = compaction backlog |
| Memtable size | In-memory buffer size | Should stay below `flush_threshold` |
| Compaction backlog | Pending compaction work | Growing backlog = increase workers |

### Query Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| Active queries | Currently executing queries | Near `max_concurrent` = bottleneck |
| Cache hit rate | Percentage of queries served from cache | Low rate = increase `cache_max_bytes` |
| Query latency (p50, p99) | Query execution time | Spikes indicate performance issues |
| Bloom filter skip rate | Segments skipped by bloom filters | Higher is better |

### Tiering Metrics (S3)

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| Segments in hot tier | Segments on local SSD | Capacity planning |
| Segments in warm tier | Segments in S3 | Storage costs |
| Segment cache hit rate | Local cache for warm segments | Low rate = increase cache |
| Upload/download bytes | S3 transfer volume | Cost monitoring |

## Cluster Metrics

In cluster mode, additional metrics are available for monitoring distributed operations.

### Shard Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| `shard_active` | Shards in active state | Should match expected partition count |
| `shard_draining` | Shards being drained | Non-zero during rebalance |
| `shard_migrating` | Shards being migrated | Non-zero during rebalance or split |
| `shard_splitting` | Shards being split | Non-zero during hot partition split |
| `shard_map_epoch` | Current shard map version | Monotonically increasing |

### Rebalance Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| `rebalance_total` | Total rebalances applied | Increasing during topology changes |
| `rebalance_move_total` | Total shard moves across all rebalances | Large numbers indicate frequent topology changes |
| `rebalance_duration_ns` | Duration of last rebalance | Growing duration may indicate large clusters |

### Node Health Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| `nodes_alive` | Nodes sending heartbeats normally | Should match cluster size |
| `nodes_suspect` | Nodes with missed heartbeats | Non-zero may indicate network issues |
| `nodes_dead` | Nodes declared dead | Should be 0 in healthy cluster |
| `leader_changes_total` | Raft leader transitions | Frequent changes indicate instability |

### Split Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| `split_total` | Total partition splits proposed | Increasing under hot-spot load |
| `split_duration_ns` | Duration of last split | |

### Meta Loss Metrics

| Metric | Description | What to Watch |
|--------|-------------|---------------|
| `meta_loss_duration_ns` | Duration of current/last meta-loss episode | Should be 0 normally |
| `meta_loss_duplicate_parts` | Duplicate partitions detected during meta loss | Non-zero indicates potential conflicts |

## Cache Statistics

```bash
lynxdb cache stats

# Output:
#   Query Cache
#   Hits:       12,456
#   Misses:     3,789
#   Hit Rate:   76.7%
#   Entries:    1,234
#   Size:       456 MB / 1.0 GB
#   Evictions:  89
```

```bash
# Machine-readable
lynxdb cache stats --format json
```

If the hit rate is consistently below 50%, consider increasing `storage.cache_max_bytes` or `storage.cache_ttl`.

## Diagnostics

The `lynxdb doctor` command runs a comprehensive health check:

```bash
lynxdb doctor

# Output:
#   ok Binary        v0.1.0 (linux/amd64, go1.25.4)
#   ok Config        /home/user/.config/lynxdb/config.yaml (valid)
#   ok Data dir      /var/lib/lynxdb (42 GB free)
#   ok Server        localhost:3100 (healthy, uptime 2d 5h)
#   ok Events        3.4M total
#   ok Storage       1.2 GB
#   ok Retention     7d
#   ok Completion    zsh detected
#
#   All checks passed.
```

```bash
# Machine-readable
lynxdb doctor --format json
```

## Query Profiling

Profile individual queries to identify performance bottlenecks:

```bash
# Basic profiling
lynxdb query 'level=error | stats count by source' --analyze

# Full profiling with per-operator timing
lynxdb query 'level=error | stats count by source' --analyze full

# Trace-level profiling
lynxdb query 'level=error | stats count by source' --analyze trace
```

The `--analyze` output shows:
- Segments scanned vs skipped (with skip reasons: bloom, time, index, stats)
- Rows read vs filtered
- Per-operator execution time
- Memory usage

## Monitoring with External Tools

### Prometheus Scraping

Poll the `/api/v1/stats` endpoint at regular intervals:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'lynxdb'
    scrape_interval: 30s
    metrics_path: '/api/v1/stats'
    static_configs:
      - targets: ['lynxdb:3100']
```

### Kubernetes ServiceMonitor

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
      path: /api/v1/stats
      interval: 30s
```

### Alerting on Metrics

Use the `lynxdb watch` command for quick metric monitoring:

```bash
# Watch error rate every 30 seconds
lynxdb watch 'level=error | stats count' --interval 30s --diff
```

Set up server-side alerts for infrastructure monitoring:

```bash
# Alert when disk usage is high
curl -X POST localhost:3100/api/v1/alerts -d '{
  "name": "High disk usage",
  "q": "| from _internal | where metric=\"storage_bytes\" | where value > 100000000000",
  "interval": "5m",
  "channels": [
    {"type": "slack", "config": {"webhook_url": "https://hooks.slack.com/..."}}
  ]
}'
```

## Recommended Monitoring Checklist

For production deployments, monitor these at minimum:

- [ ] `/health` endpoint -- uptime monitoring
- [ ] Ingest rate -- detect pipeline failures
- [ ] Disk usage and growth rate -- capacity planning
- [ ] Query latency (p99) -- performance SLA
- [ ] Active queries vs `max_concurrent` -- concurrency saturation
- [ ] Cache hit rate -- query performance optimization
- [ ] WAL size -- flush health
- [ ] Compaction backlog -- storage health

**Cluster-specific items** (when running in cluster mode):
- [ ] `nodes_alive` -- all nodes healthy
- [ ] `nodes_dead` -- should be 0
- [ ] `shard_draining` + `shard_migrating` -- rebalance in progress
- [ ] `leader_changes_total` -- Raft leader stability
- [ ] `meta_loss_duration_ns` -- meta quorum health

## Next Steps

- [Performance Tuning](/docs/operations/performance-tuning) -- optimize based on metrics
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose common issues
- [Retention Policies](/docs/operations/retention) -- manage data lifecycle
- [Query Settings](/docs/configuration/query) -- tune concurrency and timeouts

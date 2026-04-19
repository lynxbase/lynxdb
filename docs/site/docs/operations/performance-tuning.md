---
title: Performance Tuning
description: Optimize LynxDB performance -- ingest throughput, query latency, compaction, memory, and cache tuning.
---

# Performance Tuning

LynxDB is designed to perform well with default settings. This guide covers tuning for specific workloads when you need to push beyond defaults.

## Baseline Performance

With default settings on commodity hardware:

| Metric | Value |
|--------|-------|
| Single-node ingest throughput | 300K+ events/sec |
| Pipeline throughput (WHERE + STATS) | ~2.1M events/sec |
| VM simple predicate | 22ns/op, 0 allocs |
| Cache hit latency | 299ns |
| Streaming `head 10` on 100K events | 0.23ms |
| MV-accelerated query | ~400x speedup |

If you are already achieving acceptable performance, there is no need to tune further.

## Ingest Throughput

### Increase Flush Threshold

Larger flush targets can reduce small-part churn and improve compaction efficiency:

```yaml
storage:
  flush_threshold: "1gb"      # Default: 512mb
```

Trade-off: Higher memory usage and more data buffered before a flush completes.

### Part Flush Durability

For maximum throughput, reduce synchronous flush cost on non-critical pipelines:

```yaml
ingest:
  fsync: false                # Lower flush latency, higher power-loss risk
```

For stricter durability, keep `ingest.fsync: true`.

### Batch Size

For the CLI `ingest` command, increase the client-side batch size:

```bash
lynxdb ingest huge.log --batch-size 10000  # Default: 5000
```

For the HTTP API, send larger request bodies:

```yaml
ingest:
  max_body_size: "50mb"       # Default: 100mb
  max_batch_size: 5000        # Default: 1000
```

## Query Latency

### Increase Cache Size

The query cache can dramatically reduce latency for repeated queries:

```yaml
storage:
  cache_max_bytes: "4gb"      # Default: 1gb
  cache_ttl: "10m"            # Default: 5m
```

Monitor the cache hit rate:

```bash
lynxdb cache stats
```

If the hit rate is below 50%, increase `cache_max_bytes`.

### Increase Concurrent Queries

Allow more queries to run in parallel:

```yaml
query:
  max_concurrent: 30          # Default: 10
```

Rule of thumb: 2-3x the number of CPU cores, depending on available memory.

### Use Materialized Views

For repeated aggregation queries, materialized views provide ~400x speedup:

```bash
# Create a view for a common query pattern
lynxdb mv create mv_errors_5m \
  'level=error | stats count, avg(duration) by source, time_bucket(timestamp, "5m") AS bucket' \
  --retention 90d

# Queries matching this pattern are automatically accelerated
lynxdb query 'level=error | stats count by source'
# meta.accelerated_by: {view: mv_errors_5m, speedup: "~400x"}
```

### Segment Cache for S3 Tiering

When using S3 tiering, increase the local segment cache to avoid S3 round-trips:

```yaml
storage:
  segment_cache_size: "50gb"  # Default: 10gb
```

Size this to hold 2-4x your most frequently queried warm-tier data.

### Query Profiling

Use `--analyze` to identify bottlenecks in specific queries:

```bash
# Basic profiling
lynxdb query 'level=error | stats count by source' --analyze

# Full profiling with per-operator timing
lynxdb query 'level=error | stats count by source' --analyze full
```

Look for:
- **High scan count / low skip count**: Bloom filters or time-range indexes are not effective. Consider more selective queries.
- **High filter ratio**: Most events are being scanned and discarded. Add materialized views for this query pattern.
- **Slow operators**: Identify which pipeline operator is the bottleneck.

## Compaction

### Reduce Compaction Interval

More frequent compaction checks keep L0 file count low:

```yaml
storage:
  compaction_interval: "15s"  # Default: 30s
```

### Increase Compaction Workers

More workers allow parallel compaction:

```yaml
storage:
  compaction_workers: 4       # Default: 2
```

Trade-off: More CPU and I/O during compaction.

### Rate Limiting

If compaction I/O affects query latency, apply a rate limit:

```yaml
storage:
  compaction_rate_limit_mb: 100  # Default: 100
```

### Level Thresholds

Tune when compaction triggers:

```yaml
storage:
  l0_threshold: 4             # Default: 4 (trigger L0->L1 compaction)
  l1_threshold: 4             # Default: 4 (trigger L1->L2 compaction)
  l2_target_size: "1gb"       # Default: 1gb (target size for L2 segments)
```

Lower `l0_threshold` for more aggressive L0 compaction (reduces query latency but increases CPU). Higher `l2_target_size` for fewer, larger segments (better for sequential scan performance).

## Memory

### Memory Pool

Set a global memory pool to prevent queries from using all available memory:

```bash
lynxdb server --max-query-pool 4gb
```

When the pool is exhausted, queries spill intermediate results to disk.

### Spill Directory

Use a fast disk for spill files:

```bash
lynxdb server --max-query-pool 4gb --spill-dir /data/lynxdb/tmp
```

### Compression

Switch to zstd for lower memory usage during decompression (at the cost of slightly more CPU):

```yaml
storage:
  compression: "zstd"         # Default: lz4
```

## Workload-Specific Tuning

### High-Throughput Ingest (>100K events/sec)

```yaml
storage:
  flush_threshold: "1gb"
  compaction_workers: 4
  compaction_interval: "15s"

ingest:
  max_body_size: "50mb"
  max_batch_size: 5000
  fsync: false
```

### Dashboard-Heavy (Many Concurrent Queries)

```yaml
query:
  max_concurrent: 50
  default_result_limit: 500

storage:
  cache_max_bytes: "8gb"
  cache_ttl: "10m"
```

Create materialized views for all dashboard panels:

```bash
lynxdb mv create mv_dashboard_errors \
  'level=error | stats count by source, time_bucket(timestamp, "5m") AS bucket' \
  --retention 90d
```

### Long-Range Scans (Weeks/Months)

```yaml
query:
  max_query_runtime: "30m"    # Default: 5m

storage:
  segment_cache_size: "50gb"  # Large cache for warm-tier data
```

```bash
lynxdb server --max-query-pool 8gb
```

### Resource-Constrained (Small VMs)

```yaml
storage:
  flush_threshold: "128mb"
  compaction_workers: 1
  cache_max_bytes: "256mb"

query:
  max_concurrent: 5
```

## Benchmarking

Run the built-in benchmark to establish a baseline:

```bash
lynxdb bench
lynxdb bench --events 1000000
```

Compare results after tuning:

```
LynxDB Benchmark -- 100,000 events
============================================================
Generating events... 100000 lines

Ingest:  100,000 events in 245ms (408,163 events/sec)

QUERY                                         RESULTS       TIME
Filtered aggregate                                  1       12ms
Full scan aggregate                                 5        8ms
Full-text search                                  340       15ms
Range filter + top                                 10       11ms
Time bucketed                                      96       14ms

Done.
```

## Next Steps

- [Monitoring](/docs/operations/monitoring) -- track performance metrics
- [Storage Settings](/docs/configuration/storage) -- detailed storage configuration
- [Query Settings](/docs/configuration/query) -- query engine tuning
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose performance issues

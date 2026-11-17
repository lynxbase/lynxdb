---
title: Performance Tuning
description: Tune LynxDB for ingest throughput, query latency, compaction, memory, and cache use, and write queries that skip data.
---

# Performance Tuning

LynxDB performs well with default settings. This page covers the settings to change for specific workloads, and how to write queries that read less data. Measure before and after each change with `--analyze` (see [Monitoring](/docs/operations/monitoring#query-profiling)).

## Query Shape

The largest wins come from the query itself. The server skips whole segments, and rows inside them, using the time range, search terms and string equality predicates of the query. See [Indexing](/docs/architecture/indexing) for how each one is used.

| Do | Why |
|----|-----|
| Bound the time range: `source=main earliest=-1h ...`, or `--since 1h` on the CLI | Segments outside the range are never opened |
| Put search terms and field comparisons in the source command: `source=main level=ERROR timeout` | Terms are checked against bloom filters and the term index; string equality is checked against column min/max |
| Filter early, before `eval`, `stats` or `sort` | Filters directly above the scan become scan hints |
| End event searches with `head N` | The pipeline stops pulling rows after N instead of reading every segment |
| Name the fields you need with `fields` | Only those columns are read |
| Use `sort ... | head N` instead of a full `sort` | The optimizer turns it into a top-N heap |

```bash
# Reads only the segments of the last hour that may contain the token "timeout"
lynxdb query 'source=main earliest=-1h level=ERROR timeout | head 50'

# Top 10 failing paths on one host
lynxdb query 'source=main host=web-01 status>=500
  | stats count() as errors by path
  | sort - errors
  | head 10'
```

`lynxdb explain '<query>'` prints the optimized plan. Check that the `Scan` line carries `prune=` predicates, a time range and a `columns=` list:

```
Optimized plan:
Project(`count()`, host)
└─ Aggregate(`count()` = count() BY host)
   └─ Filter(level = "ERROR")
      └─ Scan(main) columns=[_time, host, level] prune=level = "ERROR"
```

See [PPL performance](../ppl/performance.md) for the full list of plan rewrites.

## Ingest Throughput

### Flush Threshold

A larger flush target writes fewer, larger parts and reduces compaction work:

```yaml
storage:
  flush_threshold: "1gb"      # Default: 512mb
```

Trade-off: more memory in use and more data buffered before a flush completes.

### Part Flush Durability

For the highest throughput on pipelines that can resend data, skip the fsync before the part rename:

```yaml
ingest:
  fsync: false                # Lower flush latency, higher power-loss risk
```

Keep `ingest.fsync: true` when you need parts to survive power loss.

### Batch Size

For the CLI `ingest` command, raise the client-side batch size:

```bash
lynxdb ingest huge.log --batch-size 10000  # Default: 5000
```

For the HTTP API, send larger request bodies:

```yaml
ingest:
  max_body_size: "100mb"      # Default: 100mb
  max_batch_size: 5000        # Default: 1000
```

## Query Latency

### Result Cache

The server caches complete query results. The cache key includes the query text, the time range and an ingest generation counter, so a cached result is only reused while no new data arrives. It helps most for repeated queries over historical data:

```yaml
storage:
  cache_max_bytes: "4gb"      # Default: 1gb
  cache_ttl: "10m"            # Default: 5m
```

Check the hit rate with `lynxdb cache stats`. Queries whose result depends on the current time are never cached: relative time bounds, `now()` and other non-deterministic functions, and requests that pass `from`/`to`.

### Concurrent Queries

Allow more queries to run in parallel:

```yaml
query:
  max_concurrent: 64          # Default: 32
```

Each running query holds its own memory budget (`query.max_query_memory_bytes`, default 1gb), so raise concurrency together with the memory pool.

### Segment Cache for S3 Tiering

With S3 tiering, a larger local segment cache avoids object-store round trips:

```yaml
storage:
  segment_cache_size: "50gb"  # Default: 10gb
```

Size it to hold the warm-tier data you query most often.

### Reading a Profile

```bash
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze=full
```

Look for:

- **Few skipped segments**: the query has no time range or no selective term. Add `earliest=` or a search term.
- **Many rows scanned, few returned**: the filter is applied after the scan instead of narrowing it. Move string equality and terms into the source command.
- **One slow operator**: per-operator timing shows which stage dominates.

## Compaction

### Compaction Interval

More frequent compaction checks keep the L0 part count low:

```yaml
storage:
  compaction_interval: "15s"  # Default: 30s
```

### Compaction Workers

More workers run compactions in parallel:

```yaml
storage:
  compaction_workers: 4       # Default: 2
```

Trade-off: more CPU and I/O during compaction.

### Rate Limiting

If compaction I/O affects query latency, lower its bandwidth:

```yaml
storage:
  compaction_rate_limit_mb: 50  # Default: 100
```

### Level Thresholds

```yaml
storage:
  l0_threshold: 4             # Default: 4 (L0 -> L1 trigger)
  l1_threshold: 4             # Default: 4 (L1 -> L2 trigger)
  l2_target_size: "1gb"       # Default: 1gb
```

A lower `l0_threshold` compacts L0 sooner (fewer parts per query, more CPU). A higher `l2_target_size` produces fewer, larger parts.

## Memory

### Query Memory Pool

Bound the memory all queries share:

```bash
lynxdb server --max-query-pool 4gb
```

Aggregation and sort spill to disk when their share of the budget runs out. Other operators that hold rows (join, eventstats, top-N, reverse) are bounded by the same budget; a query that exceeds it fails with `QUERY_MEMORY_EXCEEDED`.

### Spill Directory

Put spill files on a fast disk:

```bash
lynxdb server --max-query-pool 4gb --spill-dir /data/lynxdb/tmp
```

`query.max_temp_dir_size_bytes` (default 10gb) caps the disk space spill files use.

### Compression

`zstd` produces smaller parts than `lz4` at the cost of more CPU on write and read:

```yaml
storage:
  compression: "zstd"         # Default: lz4
```

## Workload Profiles

### High-Throughput Ingest

```yaml
storage:
  flush_threshold: "1gb"
  compaction_workers: 4
  compaction_interval: "15s"

ingest:
  max_batch_size: 5000
  fsync: false
```

### Many Concurrent Queries

```yaml
query:
  max_concurrent: 64
  default_result_limit: 500

storage:
  cache_max_bytes: "8gb"
  cache_ttl: "10m"
```

### Long-Range Scans

```yaml
query:
  max_query_runtime: "30m"    # Default: 5m

storage:
  segment_cache_size: "50gb"
```

```bash
lynxdb server --max-query-pool 8gb
```

### Small VMs

```yaml
storage:
  flush_threshold: "128mb"
  compaction_workers: 1
  cache_max_bytes: "256mb"

query:
  max_concurrent: 4
  max_query_memory_bytes: "256mb"
```

## Next Steps

- [Monitoring](/docs/operations/monitoring) -- metrics to watch while tuning
- [Storage Settings](/docs/configuration/storage) -- storage configuration reference
- [Query Settings](/docs/configuration/query) -- query engine configuration reference
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose performance issues

---
title: Storage Settings
description: Configure LynxDB storage engine -- compression, WAL, memtable, compaction, and partition settings.
---

# Storage Settings

The `storage` section controls how LynxDB writes, compresses, and compacts data on disk. These settings affect write throughput, disk usage, and query performance.

## Compression

| Config Key | `storage.compression` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_COMPRESSION` |
| **Default** | `lz4` |

```yaml
storage:
  compression: "lz4"
```

Valid values:

| Value | Description |
|-------|-------------|
| `lz4` | Fast compression, good for high-throughput ingest. Default. |
| `zstd` | Higher compression ratio, slightly more CPU. Good for long-term storage. |

LZ4 is recommended for most workloads. Switch to zstd if disk space is a primary concern and you can afford slightly higher CPU usage during compaction.

## Row Group Size

Controls how many rows are stored per column chunk in segments.

| Config Key | `storage.row_group_size` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_ROW_GROUP_SIZE` |
| **Default** | `65536` |

```yaml
storage:
  row_group_size: 65536
```

Larger values improve compression but increase memory usage during reads. The default of 65536 is a good balance for most workloads.

## Flush Threshold

The memtable is flushed to a segment on disk when it reaches this size.

| Config Key | `storage.flush_threshold` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_FLUSH_THRESHOLD` |
| **Default** | `512mb` |

```yaml
storage:
  flush_threshold: "512mb"
```

A larger threshold means fewer, larger segments (better for queries) but higher memory usage and more data at risk during crashes. A smaller threshold means more frequent flushes with smaller segments.

## Memtable Shards

Number of concurrent memtable shards for parallel ingestion.

| Config Key | `storage.memtable_shards` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_MEMTABLE_SHARDS` |
| **Default** | `0` (auto = number of CPUs) |

```yaml
storage:
  memtable_shards: 0
```

Set to `0` for auto-detection (one shard per CPU core). This provides lock-free concurrent ingestion.

## Max Immutable Memtables

Maximum number of immutable memtables waiting to be flushed before backpressure is applied to ingestion.

| Config Key | `storage.max_immutable` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_MAX_IMMUTABLE` |
| **Default** | `2` |

```yaml
storage:
  max_immutable: 2
```

## WAL (Write-Ahead Log)

The WAL ensures durability by recording every write before it enters the memtable.

### Sync Mode

| Config Key | `storage.wal_sync_mode` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_WAL_SYNC_MODE` |
| **Default** | `write` |

```yaml
storage:
  wal_sync_mode: "write"
```

| Value | Description | Durability | Performance |
|-------|-------------|------------|-------------|
| `none` | No explicit sync. OS decides when to flush. | Lowest -- data loss on power failure | Highest throughput |
| `write` | Batch sync every `wal_sync_interval`. Default. | Good -- at most `wal_sync_interval` of data at risk | Good throughput |
| `fsync` | fsync after every write. | Highest -- no data loss | Lowest throughput |

### Sync Interval

How often the WAL is synced to disk (when `wal_sync_mode` is `write`).

| Config Key | `storage.wal_sync_interval` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_WAL_SYNC_INTERVAL` |
| **Default** | `100ms` |

```yaml
storage:
  wal_sync_interval: "100ms"
```

### Sync Bytes

Sync the WAL after this many bytes written (in addition to the interval).

| Config Key | `storage.wal_sync_bytes` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_WAL_SYNC_BYTES` |
| **Default** | `0` (interval-only) |

```yaml
storage:
  wal_sync_bytes: "0"
```

### Max Segment Size

WAL segment rotation size. When a WAL segment reaches this size, a new segment is created.

| Config Key | `storage.wal_max_segment_size` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_WAL_MAX_SEGMENT_SIZE` |
| **Default** | `256mb` |

```yaml
storage:
  wal_max_segment_size: "256mb"
```

## Compaction

Compaction merges small segments into larger ones, improving query performance and reclaiming space.

LynxDB uses size-tiered compaction with three levels:
- **L0** -- Recently flushed segments (may overlap in time range)
- **L1** -- Merged, non-overlapping segments
- **L2** -- Fully compacted segments (~1GB each)

### Compaction Interval

How often the compaction scheduler checks for work.

| Config Key | `storage.compaction_interval` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_COMPACTION_INTERVAL` |
| **Default** | `30s` |

```yaml
storage:
  compaction_interval: "30s"
```

### Compaction Workers

Number of concurrent compaction threads.

| Config Key | `storage.compaction_workers` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_COMPACTION_WORKERS` |
| **Default** | `2` |

```yaml
storage:
  compaction_workers: 2
```

Increase for faster compaction at the cost of more CPU and I/O. Decrease to reduce resource contention on busy servers.

### Compaction Rate Limit

Maximum disk write speed for compaction (in MB/s). Prevents compaction from starving queries.

| Config Key | `storage.compaction_rate_limit_mb` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB` |
| **Default** | `0` (unlimited) |

```yaml
storage:
  compaction_rate_limit_mb: 100
```

### Level Thresholds

| Config Key | Env Var | Default | Description |
|---|---|---|---|
| `storage.l0_threshold` | `LYNXDB_STORAGE_L0_THRESHOLD` | `4` | L0 files before compaction triggers |
| `storage.l1_threshold` | `LYNXDB_STORAGE_L1_THRESHOLD` | `10` | L1 files before L1-to-L2 compaction |
| `storage.l2_target_size` | `LYNXDB_STORAGE_L2_TARGET_SIZE` | `1gb` | Target size for L2 segments |

```yaml
storage:
  l0_threshold: 4
  l1_threshold: 10
  l2_target_size: "1gb"
```

## Query Cache

The filesystem-based segment query cache reduces repeated query costs.

| Config Key | Env Var | Default | Description |
|---|---|---|---|
| `storage.cache_max_bytes` | `LYNXDB_STORAGE_CACHE_MAX_BYTES` | `1gb` | Max cache size |
| `storage.cache_ttl` | `LYNXDB_STORAGE_CACHE_TTL` | `5m` | Cache entry TTL |

```yaml
storage:
  cache_max_bytes: "4gb"
  cache_ttl: "5m"
```

The cache is persistent across restarts and uses TTL + LRU eviction. Cache keys are based on `(segment_id, CRC32, query_hash, time_range)`.

## Complete Example

```yaml
storage:
  compression: "lz4"
  row_group_size: 65536
  flush_threshold: "512mb"
  memtable_shards: 0
  max_immutable: 2
  wal_sync_mode: "write"
  wal_sync_interval: "100ms"
  wal_max_segment_size: "256mb"
  compaction_interval: "30s"
  compaction_workers: 2
  compaction_rate_limit_mb: 0
  l0_threshold: 4
  l1_threshold: 10
  l2_target_size: "1gb"
  cache_max_bytes: "1gb"
  cache_ttl: "5m"
```

## Next Steps

- [S3 Tiering](/docs/configuration/s3-tiering) -- warm/cold storage with S3
- [Performance Tuning](/docs/operations/performance-tuning) -- optimize for your workload
- [Server Settings](/docs/configuration/server) -- listen address, retention, TLS

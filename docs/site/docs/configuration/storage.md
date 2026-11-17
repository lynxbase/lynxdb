---
title: Storage Settings
description: Configure part writing, partitioning, and compaction in the storage section of the LynxDB config file.
---

# Storage Settings

The `storage` section controls how LynxDB writes parts, lays them out on disk, and compacts them.

## Write Path

There is no write-ahead log. Accepted events go into an in-memory batcher. A flush writes the batch to a temporary `.lsg` part file, optionally fsyncs it (`ingest.fsync`), and renames it into place, which makes the events durable and visible to queries. Events accepted but not yet flushed are lost if the process crashes.

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `storage.flush_threshold` | `LYNXDB_STORAGE_FLUSH_THRESHOLD` | `512mb` | Buffered size that triggers a flush. Minimum `1mb`. |
| `storage.flush_idle_timeout` | `LYNXDB_STORAGE_FLUSH_IDLE_TIMEOUT` | `200ms` | Flush after this long without incoming events. `0` disables the idle flush. |
| `storage.row_group_size` | `LYNXDB_STORAGE_ROW_GROUP_SIZE` | `65536` | Rows per row group in a part. |
| `storage.max_columns_per_part` | `LYNXDB_STORAGE_MAX_COLUMNS_PER_PART` | `256` | Maximum user fields stored as columns in one part. When a batch has more distinct fields, the most frequent ones become columns and the rest stay searchable through `_raw`. `0` removes the cap. |
| `storage.compression` | `LYNXDB_STORAGE_COMPRESSION` | `lz4` | Validated (`lz4` or `zstd`), but parts are always written with LZ4. |

## Partitioning

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `storage.partition_by` | `LYNXDB_STORAGE_PARTITION_BY` | `daily` | Time partition of part directories: `daily`, `hourly`, `weekly`, `monthly` or `none`. Retention deletes whole partitions, and queries skip partitions outside their time range. |

## Compaction

Compaction merges small parts into larger ones. L0 parts come from flushes; when an index has `l0_threshold` L0 parts they merge into L1, and `l1_threshold` L1 parts merge into L2 parts of about `l2_target_size`.

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `storage.compaction_interval` | `LYNXDB_STORAGE_COMPACTION_INTERVAL` | `30s` | How often the compaction scheduler runs. Minimum `1s`. |
| `storage.compaction_workers` | `LYNXDB_STORAGE_COMPACTION_WORKERS` | `2` | Parallel compaction workers. |
| `storage.compaction_rate_limit_mb` | `LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB` | `100` | Upper bound of the compaction rate in MB/s. The rate adapts below this bound (down to 10 MB/s) when slow queries or GC load increase. `0` uses a 200 MB/s bound. Reloadable. |
| `storage.l0_threshold` | `LYNXDB_STORAGE_L0_THRESHOLD` | `4` | L0 parts that trigger L0 to L1 compaction. |
| `storage.l1_threshold` | `LYNXDB_STORAGE_L1_THRESHOLD` | `4` | L1 parts that trigger L1 to L2 compaction. |
| `storage.l2_target_size` | `LYNXDB_STORAGE_L2_TARGET_SIZE` | `1gb` | Target size of L2 parts. Minimum `1mb`. |
| `storage.compaction_disable_bsi_on_output` | `LYNXDB_STORAGE_COMPACTION_DISABLE_BSI_ON_OUTPUT` | `false` | Skip writing range indexes (BSI) into compacted parts. |

Besides the scheduled runs, a flush that leaves an index with `l0_threshold` or more L0 parts triggers compaction for that index right away.

## Query Result Cache

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `storage.cache_max_bytes` | `LYNXDB_STORAGE_CACHE_MAX_BYTES` | `1gb` | Validated but not read: the query result cache is sized by the server (256 MB, 30 s entry lifetime) and shrinks under memory pressure from `server.total_memory_pool_bytes`. |
| `storage.cache_ttl` | `LYNXDB_STORAGE_CACHE_TTL` | `5m` | Validated but not read. |

`lynxdb cache stats` shows cache usage and `lynxdb cache clear` empties it.

## Complete Example

```yaml
storage:
  flush_threshold: "512mb"
  flush_idle_timeout: "200ms"
  row_group_size: 65536
  max_columns_per_part: 256
  partition_by: "daily"
  compaction_interval: "30s"
  compaction_workers: 4
  compaction_rate_limit_mb: 200
  l0_threshold: 4
  l1_threshold: 4
  l2_target_size: "1gb"
```

## Related

- [Ingest Settings](./ingest.md)
- [Storage Engine](../architecture/storage-engine.md)
- [Performance Tuning](../operations/performance-tuning.md)

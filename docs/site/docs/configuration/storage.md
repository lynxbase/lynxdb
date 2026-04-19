---
title: Storage Settings
description: Configure LynxDB storage behavior -- compression, partitioning, compaction, cache, and direct-to-part ingest buffering.
---

# Storage Settings

The `storage` section controls how LynxDB lays out data on disk, compacts immutable parts, and caches query results. The current write path is a direct-to-part model, so older WAL-specific settings do not apply.

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
|---|---|
| `lz4` | Fast ingest and compaction. Default. |
| `zstd` | Better compression ratio with more CPU cost. |

## Max Columns Per Part

Limit how many user-defined fields are materialized as columns in a part.

| Config Key | `storage.max_columns_per_part` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_MAX_COLUMNS_PER_PART` |
| **Default** | `256` |

```yaml
storage:
  max_columns_per_part: 256
```

Fields beyond the cap remain searchable through `_raw`, but they are not stored as dedicated columns.

## Partitioning

Choose how LynxDB groups part files on disk.

| Config Key | `storage.partition_by` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_PARTITION_BY` |
| **Default** | `daily` |

```yaml
storage:
  partition_by: "daily"
```

Valid values: `daily`, `hourly`, `weekly`, `monthly`, `none`.

## Ingest Buffering

LynxDB no longer exposes WAL tuning under `storage.*`.

The write path is:

1. accept events into an in-memory `AsyncBatcher`
2. flush a batch to a temporary `.lsg` file
3. optionally `fsync`
4. atomically rename the part into place

Operator-facing knobs for that path now live on [Ingest Settings](/docs/configuration/ingest), especially:

- `ingest.max_body_size`
- `ingest.max_batch_size`
- `ingest.fsync`

## Compaction

Compaction merges smaller parts into larger ones to reduce query fan-out.

### Scheduler

| Config Key | `storage.compaction_interval` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_COMPACTION_INTERVAL` |
| **Default** | `30s` |

```yaml
storage:
  compaction_interval: "30s"
```

### Workers

| Config Key | `storage.compaction_workers` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_COMPACTION_WORKERS` |
| **Default** | `2` |

```yaml
storage:
  compaction_workers: 2
```

### Rate Limit

| Config Key | `storage.compaction_rate_limit_mb` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB` |
| **Default** | `100` |

```yaml
storage:
  compaction_rate_limit_mb: 100
```

### Level Thresholds

| Config Key | Env Var | Default | Description |
|---|---|---|---|
| `storage.l0_threshold` | `LYNXDB_STORAGE_L0_THRESHOLD` | `4` | L0 parts before L0-to-L1 compaction |
| `storage.l1_threshold` | `LYNXDB_STORAGE_L1_THRESHOLD` | `4` | L1 parts before L1-to-L2 compaction |
| `storage.l2_target_size` | `LYNXDB_STORAGE_L2_TARGET_SIZE` | `1gb` | Target size for L2 parts |

```yaml
storage:
  l0_threshold: 4
  l1_threshold: 4
  l2_target_size: "1gb"
```

## Query Cache

| Config Key | Env Var | Default | Description |
|---|---|---|---|
| `storage.cache_max_bytes` | `LYNXDB_STORAGE_CACHE_MAX_BYTES` | `1gb` | Maximum on-disk query cache size |
| `storage.cache_ttl` | `LYNXDB_STORAGE_CACHE_TTL` | `5m` | Cache entry TTL |

```yaml
storage:
  cache_max_bytes: "1gb"
  cache_ttl: "5m"
```

## Complete Example

```yaml
storage:
  compression: "lz4"
  max_columns_per_part: 256
  partition_by: "daily"
  compaction_interval: "30s"
  compaction_workers: 2
  compaction_rate_limit_mb: 100
  l0_threshold: 4
  l1_threshold: 4
  l2_target_size: "1gb"
  cache_max_bytes: "1gb"
  cache_ttl: "5m"
```

## Next Steps

- [Ingest Settings](/docs/configuration/ingest)
- [S3 Tiering](/docs/configuration/s3-tiering)
- [Performance Tuning](/docs/operations/performance-tuning)
- [Storage Engine](/docs/architecture/storage-engine)

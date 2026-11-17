---
title: Retention and Data Lifecycle
description: Configure LynxDB data retention -- partition-based expiration, tiered storage lifecycle, and storage estimates.
---

# Retention and Data Lifecycle

LynxDB deletes old data on a schedule and, with S3 tiering, moves older segments to cheaper storage. Both run in the background.

## Retention Policy

The global `retention` setting controls how long data is kept.

| Config Key | `retention` |
|---|---|
| **Env Var** | `LYNXDB_RETENTION` |
| **Default** | `7d` |
| **Hot-Reloadable** | Yes |

```yaml
retention: "30d"
```

```bash
# Set through the environment
LYNXDB_RETENTION=30d lynxdb server

# Change at runtime (no restart)
lynxdb config set retention 30d
lynxdb config reload
```

Accepted duration formats:

- `7d` -- 7 days
- `4w` -- 4 weeks (28 days)
- `6h` -- 6 hours (for testing)
- `90d` -- 90 days
- `365d` -- 1 year

## How Retention Works

Retention deletes whole time partitions. Parts are stored in one directory per partition, sized by `storage.partition_by` (`daily` by default; also `hourly`, `weekly`, `monthly`, or `none`).

1. A retention pass runs every hour.
2. For each index, it lists the partition directories and parses the start time from each name (`2026-03-01` for daily partitions).
3. A partition whose start time is older than `now - retention` is removed from the part registry; its files are deleted once no running query holds them open.

This means:

- Deletion happens at partition granularity. With daily partitions and `retention: 7d`, the partition of a day is deleted once the day started more than 7 days ago.
- Retention does not depend on compaction.
- With `partition_by: none`, all parts of an index live in one partition without a time, and retention never deletes them.

## Tiered Storage Lifecycle

When S3 tiering is configured, segments move through tiers by age:

```
Hot (local disk)  -->  Warm (object store)  -->  Cold (object store / archive policy)
  Recent data           Older data                 Archive data
```

### Configuration

LynxDB moves segments from hot to warm on its own. Archive storage classes underneath the cold tier are set with S3 Lifecycle rules:

```yaml
# LynxDB config -- hot to warm
storage:
  s3_bucket: "my-lynxdb-logs"
  tiering_interval: "5m"      # Check every 5 minutes
```

```bash
# AWS S3 Lifecycle -- Glacier after 90 days, delete after 365 days
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-lynxdb-logs \
  --lifecycle-configuration '{
    "Rules": [
      {
        "ID": "GlacierTransition",
        "Status": "Enabled",
        "Filter": {},
        "Transitions": [
          {"Days": 90, "StorageClass": "GLACIER"}
        ]
      },
      {
        "ID": "DeleteAfter365",
        "Status": "Enabled",
        "Filter": {},
        "Expiration": {"Days": 365}
      }
    ]
  }'
```

Segments in Glacier storage classes must be restored before LynxDB can read them. Keep the S3 expiration at least as long as `retention`, or queries over the retention window lose data.

### Example Policies

**Cost-sensitive:**

```yaml
retention: "30d"
storage:
  s3_bucket: "my-logs"
  # S3 Lifecycle: delete after 30 days
```

**Compliance:**

```yaml
retention: "365d"
storage:
  s3_bucket: "my-logs"
  # S3 Lifecycle: Glacier after 90 days, delete after 365 days
```

**Long-term archive:**

```yaml
retention: "2555d"  # 7 years
storage:
  s3_bucket: "my-logs"
  # S3 Lifecycle: IA after 30 days, Glacier after 90 days, Deep Archive after 365 days
```

## Monitoring Retention

Check the oldest event and storage use:

```bash
lynxdb status
#   Oldest    2026-02-01T10:30:00Z

lynxdb status --format json | jq '{total_events, storage_bytes, oldest_event}'
```

## Changing Retention

Retention is hot-reloadable. A new value applies from the next retention pass:

```bash
# Increase retention
lynxdb config set retention 90d
lynxdb config reload

# Decrease retention (expired partitions are deleted at the next pass)
lynxdb config set retention 7d
lynxdb config reload
```

:::caution
Decreasing retention deletes every partition outside the new window at the next pass. This cannot be undone. Take a backup first if you may need the data.
:::

## Estimating Storage Needs

Rule of thumb for storage estimation:

| Raw Log Size | LynxDB Storage (LZ4) | Compression Ratio |
|---|---|---|
| 1 GB/day raw logs | ~200-400 MB/day on disk | 2.5-5x |
| 10 GB/day raw logs | ~2-4 GB/day on disk | 2.5-5x |
| 100 GB/day raw logs | ~20-40 GB/day on disk | 2.5-5x |

**Example**: 10 GB/day raw logs with 30-day retention = ~60-120 GB total disk usage.

With S3 tiering:

- Hot tier (7 days on local disk): ~14-28 GB
- Warm tier (remaining 23 days in S3): ~46-92 GB at S3 Standard pricing

## Next Steps

- [S3 Tiering Configuration](/docs/configuration/s3-tiering) -- configure tiered storage
- [Storage Settings](/docs/configuration/storage) -- `partition_by` and compaction settings
- [Backup and Restore](/docs/operations/backup-restore) -- protect against data loss
- [Performance Tuning](/docs/operations/performance-tuning) -- tune compaction

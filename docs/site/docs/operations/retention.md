---
title: Retention and Data Lifecycle
description: Configure LynxDB data retention policies -- automatic expiration, tiered storage lifecycle, and materialized view retention.
---

# Retention and Data Lifecycle

LynxDB automatically manages data lifecycle through retention policies, tiered storage, and materialized view retention. Older data is deleted or moved to cheaper storage without manual intervention.

## Retention Policy

The global `retention` setting controls how long data is kept before automatic deletion.

| Config Key | `retention` |
|---|---|
| **Env Var** | `LYNXDB_RETENTION` |
| **Default** | `7d` |
| **Hot-Reloadable** | Yes |

```yaml
retention: "30d"
```

```bash
# Set via CLI flag
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

Segments older than the retention period are deleted during compaction:

1. The compaction scheduler runs at `storage.compaction_interval` (default: every 30 seconds)
2. It checks each segment's time range
3. Segments where **all** events are older than the retention period are deleted
4. Segments that partially overlap the retention boundary are compacted -- old events are dropped, recent events are kept

This means:
- Deletion is not exact to the second -- it depends on segment boundaries
- A segment spanning the retention boundary is not deleted until compaction splits it
- Compaction must be running for retention to take effect

## Tiered Storage Lifecycle

When S3 tiering is enabled, data moves through three tiers based on age:

```
Hot (local SSD)  -->  Warm (S3 Standard)  -->  Cold (S3 Glacier)
  Recent data          Older data               Archive data
```

### Configuration

LynxDB handles hot-to-warm transitions automatically. Warm-to-cold transitions use S3 Lifecycle rules:

```yaml
# LynxDB config -- hot to warm
storage:
  s3_bucket: "my-lynxdb-logs"
  tiering_interval: "5m"      # Check every 5 minutes
```

```bash
# AWS S3 Lifecycle -- warm to cold (Glacier after 90 days)
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

### Example Lifecycle Policies

**Startup (cost-sensitive):**

```yaml
retention: "30d"
storage:
  s3_bucket: "my-logs"
  # S3 Lifecycle: delete after 30 days
```

**Mid-size company (compliance):**

```yaml
retention: "365d"
storage:
  s3_bucket: "my-logs"
  # S3 Lifecycle: Glacier after 90 days, delete after 365 days
```

**Enterprise (long-term archive):**

```yaml
retention: "2555d"  # 7 years
storage:
  s3_bucket: "my-logs"
  # S3 Lifecycle: IA after 30 days, Glacier after 90 days, Deep Archive after 365 days
```

## Materialized View Retention

Materialized views have their own retention policy, independent of the raw data retention:

```bash
# Create a view with 90-day retention
lynxdb mv create mv_errors_5m \
  'level=error | stats count, avg(duration) by source, time_bucket(timestamp, "5m") AS bucket' \
  --retention 90d

# Create a cascading view with longer retention
lynxdb mv create mv_errors_1h \
  '| from mv_errors_5m | stats sum(count) AS count by source, time_bucket(bucket, "1h") AS hour' \
  --retention 365d
```

This pattern lets you keep detailed data for a short period and pre-aggregated summaries for much longer:

| Data | Retention | Query Speed |
|------|-----------|-------------|
| Raw events | 7d | Normal |
| 5-minute aggregates (MV) | 90d | ~400x faster |
| 1-hour aggregates (cascading MV) | 365d | ~400x faster |

## Monitoring Retention

Check current data age and storage usage:

```bash
# View data age range
lynxdb status
# Oldest: 2026-02-01T10:30:00Z

# Check storage breakdown
lynxdb status --format json | jq '{total_events, storage_bytes, oldest_event}'
```

## Changing Retention

Retention is hot-reloadable. Changing it takes effect at the next compaction cycle:

```bash
# Increase retention
lynxdb config set retention 90d
lynxdb config reload

# Decrease retention (data will be deleted at next compaction)
lynxdb config set retention 7d
lynxdb config reload
```

:::caution
Decreasing retention causes immediate data deletion at the next compaction cycle. This cannot be undone. Ensure you have backups if needed.
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
- Hot tier (7 days on SSD): ~14-28 GB
- Warm tier (remaining 23 days in S3): ~46-92 GB at S3 Standard pricing

## Next Steps

- [S3 Tiering Configuration](/docs/configuration/s3-tiering) -- configure tiered storage
- [Materialized Views](/docs/guides/materialized-views) -- pre-aggregate for long-term retention
- [Backup and Restore](/docs/operations/backup-restore) -- protect against data loss
- [Performance Tuning](/docs/operations/performance-tuning) -- optimize compaction

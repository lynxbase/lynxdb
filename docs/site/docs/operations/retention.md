---
title: Retention and Data Lifecycle
description: Configure LynxDB data retention -- time partitions, whole-partition expiry and measuring storage use.
---

# Retention and Data Lifecycle

Every index keeps its events in time partitions, and LynxDB deletes whole partitions once they fall out of the retention period.

## Retention Policy

The global `retention` setting controls how long events are kept.

| Config Key | `retention` |
|---|---|
| **Env Var** | `LYNXDB_RETENTION` |
| **Default** | `7d` |
| **Applies** | At server start |

```yaml
retention: "30d"
```

```bash
# Through the environment
LYNXDB_RETENTION=30d lynxdb server

# In the config file, then restart the server
lynxdb config set retention 30d
sudo systemctl restart lynxdb
```

A duration is a Go duration (`6h`, `90m`) or a number of days (`7d`, `90d`, `365d`). `0` keeps every event.

## How Retention Works

Events are partitioned by their time. `storage.partition` sets the span of a partition (`24h` by default); partitions are aligned to the Unix epoch, so with the default each partition covers one UTC day.

1. A retention pass runs when the server starts and then once a minute.
2. It removes from every index the partitions that end at or before the cutoff, `now - retention`.
3. A removed partition's part files are deleted once no running query reads them.

This means:

- Deletion happens a whole partition at a time. With `retention: 7d` and daily partitions, the events of a day are deleted once that day ended more than 7 days ago, so the oldest events kept are up to 8 days old.
- A smaller `storage.partition` makes deletion finer, at the cost of more partitions for the store to keep apart.
- Retention applies to every index alike.
- Retention goes by the time of each event, not the time it arrived. Events whose own time is already older than the retention period, such as those of an old log file, are removed by the next pass, within about a minute of arriving. Set `retention: 0s` on a server that should keep them.

## Monitoring Retention

Check the oldest event and the storage in use:

```bash
lynxdb status
```

```text
  Oldest         2026-02-10T08:12:03Z
```

```bash
lynxdb status --format json | jq '{total_events, storage_bytes, oldest_event}'
```

The server logs every index from which a pass removes events, at `WARN`, with the number of events and the cutoff in UTC:

```text
time=2026-11-17T17:03:37.221+01:00 level=WARN msg="retention removed events older than the cutoff" index=main events=26 cutoff=2026-11-16T16:03:37Z
```

A pass that fails for an index logs `msg="retention failed"` with the index and the error, and the next pass tries again.

## Changing Retention

The database reads `retention` when it opens, so a change takes effect after a restart. A `lynxdb config reload` leaves it unapplied and logs it as a change that needs a restart.

```bash
# Keep more
lynxdb config set retention 90d
sudo systemctl restart lynxdb

# Keep less (the first pass after the restart deletes the expired partitions)
lynxdb config set retention 7d
sudo systemctl restart lynxdb
```

:::caution
Decreasing retention deletes every partition outside the new window at the first pass after the restart. This cannot be undone. Take a backup first if you may need the data.
:::

## Estimating Storage Needs

Compression depends on the data, so measure it on your own logs. Ingest a representative sample into a test index and compare its size with the size of the input:

```bash
lynxdb ingest sample.log --index sizing
lynxdb indexes
```

`BYTES` is the size of the index's part files. Multiply the bytes per day by the retention period in days, plus one partition, for the disk the data needs.

## Next Steps

- [Storage Settings](/docs/configuration/storage) -- `partition`, `flush_interval` and `time_fields`
- [Backup and Restore](/docs/operations/backup-restore) -- protect against data loss
- [Performance Tuning](/docs/operations/performance-tuning) -- tune ingest and queries

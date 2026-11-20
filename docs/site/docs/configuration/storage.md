---
title: Storage Settings
description: Configure the data directory, time partitions, flushing, event time and retention of the LynxDB columnar store.
---

# Storage Settings

The `storage` section sets defaults for new indexes and the flush interval for all stores. The top-level `data_dir` and `retention` control where the server stores events and when it drops old partitions.

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `data_dir` | `LYNXDB_DATA_DIR` | `$XDG_DATA_HOME/lynxdb` or `~/.local/share/lynxdb` | Root directory of the indexes. Empty keeps them in a temporary directory that is removed at shutdown. |
| `retention` | `LYNXDB_RETENTION` | `7d` | How long events are kept. `0` keeps them all. |
| `storage.partition` | `LYNXDB_STORAGE_PARTITION` | `24h` (shown as `1d`) | Default partition width for a new index. `0` means `24h`. |
| `storage.flush_interval` | `LYNXDB_STORAGE_FLUSH_INTERVAL` | `0s` | Age at which buffered input becomes eligible for a timed flush. `0` means one second. |
| `storage.time_fields` | `LYNXDB_STORAGE_TIME_FIELDS` | `[_time, timestamp, "@timestamp", time, ts]` | Default timestamp field paths for new indexes, tried in this order. Use a comma-separated list in the environment variable. |
| `storage.sort_key` | `LYNXDB_STORAGE_SORT_KEY` | `[]` | Default sort key for new indexes. An empty key orders rows by event time. Use a comma-separated list in the environment variable. See [Sort key](#sort-key). |

Changes to these server settings take effect on restart. Existing indexes reopen with their saved sort key, time fields and partition width.

## Sort key

An index's sort key lists the field paths that order the rows within a run. The leading fields are available for key-range pruning and, for eligible plans, ordered aggregation. With an empty key, the store orders rows by event time. See [Sort key pruning](../architecture/indexing.md#sort-key-pruning) for query examples.

## Indexes

An index is a store under `data_dir/indexes/<index>/`. Its `settings.json` records `sort_key`, `time_fields` and `partition`. Create the index with [`PUT /api/v1/indexes/{index}`](../api/ingest.md#put-indexesindex) to set those values explicitly. The first ingest to a new index creates it with the server defaults. Use `GET /api/v1/indexes/{index}` to see the resolved settings. To use a different configuration for existing data, create another index with the desired settings and ingest the data into it.

An index name holds ASCII letters, digits, dots, hyphens and underscores. It must have no leading dot and fit within 255 bytes. Native ingest returns `400 VALIDATION_ERROR` for an invalid name.

## Writing events

Ingested events wait in memory, then the store writes them as a sorted run. The flusher checks the age of buffered input against `storage.flush_interval`. Full buffers, a sync or graceful shutdown can trigger a flush sooner. The manifest commit makes a run durable, then the store publishes it for queries.

The server syncs before running an HTTP query and attempts to sync before reporting storage stats or the field catalog. Normal ingest can acknowledge rows that are still buffered. See [Acknowledgements and visibility](../architecture/storage.md#acknowledgements-and-visibility).

The store merges runs in the background, so that a query meets few of them. A quiet, closed partition can be merged into one run. See [Merges and snapshots](../architecture/storage.md#merges).

## Event time

The store tries timestamp candidates from the index's `time_fields` in order. A usable value is one of the following.

- a number: seconds, milliseconds, microseconds or nanoseconds since the Unix epoch, told apart by its size;
- text: a date, `T` or a space, a time of day with or without a fraction of a second, and a zone as `Z` or an offset such as `+02:00`; without a zone the time is in UTC. Text that holds only digits is read as an epoch number.

A field that holds something else is skipped, and the next one is tried. An event without any of the fields gets the time of its arrival. The time is returned as `_time`, and the field it came from keeps its value:

```bash
lynxdb query --file backend_server.log 'source=main | head 1 | fields _time, timestamp, service'
```

```json
{"_time":"2026-02-10T08:12:03.045Z","service":"user-service","timestamp":"2026-02-10T08:12:03.045Z"}
```

`time_fields` are paths: a member of an object is named like `http.request_time`.

## Partitions

The index's saved `partition` width divides event time into intervals aligned to the Unix epoch. With the 24-hour default, one partition is one UTC day. Merges operate within a partition and retention drops whole partitions. Time-bounded scans use the event-time bounds of parts and blocks.

A smaller width lets retention drop data in smaller steps and can produce more runs. Choose the width when creating the index.

## Retention

At startup and once a minute, the server drops committed partitions whose end is at or before `now - retention`. An event that arrives late follows the same rule as the rest of its partition. With the defaults, a daily partition becomes eligible seven days after its end. `retention: 0s` keeps all partitions.

Each removal is logged at `WARN`, with the index, the number of events and the cutoff:

```text
level=WARN msg="retention removed events older than the cutoff" index=main events=8 cutoff=2026-11-10T16:03:47Z
```

```yaml
retention: "30d"
storage:
  partition: "24h"
```

See [Retention](../operations/retention.md) for planning disk use.

## Complete Example

```yaml
data_dir: "/var/lib/lynxdb"
retention: "30d"

storage:
  partition: "24h"
  flush_interval: "1s"
  time_fields: ["_time", "timestamp", "@timestamp", "time", "ts", "event.created"]
```

## Related

- [Ingest Settings](./ingest.md)
- [Server Settings](./server.md)
- [Performance Tuning](../operations/performance-tuning.md)

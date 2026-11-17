---
title: Storage Settings
description: Configure the data directory, time partitions, flushing, event time and retention of the LynxDB columnar store.
---

# Storage Settings

The `storage` section, together with the top-level `data_dir` and `retention`, controls how the server keeps events.

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `data_dir` | `LYNXDB_DATA_DIR` | `$XDG_DATA_HOME/lynxdb` or `~/.local/share/lynxdb` | Root directory of the indexes. Empty keeps them in a temporary directory that is removed at shutdown. |
| `retention` | `LYNXDB_RETENTION` | `7d` | How long events are kept. `0` keeps them all. |
| `storage.partition` | `LYNXDB_STORAGE_PARTITION` | `24h` (shown as `1d`) | Span of event time whose events are kept apart. `0` means `24h`. |
| `storage.flush_interval` | `LYNXDB_STORAGE_FLUSH_INTERVAL` | `0s` | Longest time ingested events wait in memory before they are written. `0` means one second. |
| `storage.time_fields` | `LYNXDB_STORAGE_TIME_FIELDS` | `[_time, timestamp, "@timestamp", time, ts]` | Fields whose value becomes the event time, tried in this order. In the environment variable, a comma-separated list. |
| `storage.sort_key` | `LYNXDB_STORAGE_SORT_KEY` | `[]` | Fields that order the rows of each part, in every index. Empty keeps the rows in the order of time. In the environment variable, a comma-separated list. See [Sort key](#sort-key). |

None of these settings change on reload; they take effect at the next start.

## Sort key

`storage.sort_key` lists field paths, such as `[kind, commit.collection, did]`, that order the rows inside each part the store writes. Queries that filter or group by the leading fields of the key read fewer blocks: with a key that starts with the fields a query groups by, rows of one group sit next to each other. Without a key, rows are kept in the order of their time, which suits queries over time ranges.

The key applies to the parts written after it is set. Parts written before keep their order, and the store reads both, so changing the key needs no migration. The setting holds for every index; there is no key per index.

## Indexes

Every index has a store of its own: a directory under `data_dir/indexes` named after the index. An index comes into being with its first events, and its name holds letters, digits, dots, dashes and underscores, does not start with a dot, and is at most 255 characters long. Ingest that names another index is rejected with `400 VALIDATION_ERROR`.

A store keeps its events in immutable column files, called parts, grouped into sorted runs. `lynxdb indexes` and `GET /api/v1/stats` report the events, bytes and parts of each index.

## Writing events

Ingested events wait in memory, then the store writes them as a sorted run. A write happens when the oldest waiting event has waited `storage.flush_interval`, sooner when the buffers fill, on a sync, and at shutdown. A written run is durable on disk, and from then on queries see it.

The server syncs before it runs a query and before it answers `/api/v1/stats`, `/api/v1/status` and the field catalog, so a query sees every event ingested before it. Events that wait in memory when the process is killed are lost; a graceful shutdown (`SIGINT`, `SIGTERM`) writes them first.

The store merges runs in the background, so that a query meets few of them. A partition that ended an hour ago, or its own span ago when that is shorter, is merged into one run.

## Event time

The time of an event is the value of the first field in `storage.time_fields` that the event holds as a time:

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

The store sorts events into partitions by their time. A partition covers `storage.partition` of event time, counted from the Unix epoch, so with the default each partition is one UTC day. Runs never mix events of two partitions, which gives two benefits:

- a query with a time range reads only the partitions it meets;
- retention drops whole partitions.

Smaller partitions let retention drop data in smaller steps; larger ones keep fewer, larger runs. Choose the span before an index holds events and keep it: the store records each part's partition by number, and a server started with another span reads those numbers with that span, so retention can drop recent events.

## Retention

When it starts and once a minute after, the server drops, in every index, the partitions that ended more than `retention` ago. An event is kept for at least `retention` after its own time, and goes with its partition once the whole partition is older than that, so with the defaults events live for at most about 8 days. Events whose own time is already older than `retention` when they arrive are removed within about a minute. `retention: 0s` keeps everything.

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

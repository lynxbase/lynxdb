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
| `storage.part_dictionary` | `LYNXDB_STORAGE_PART_DICTIONARY` | `false` | Whether new indexes keep part dictionaries. See [Part dictionary](#part-dictionary). |

Changes to these server settings take effect on restart. Existing indexes reopen with their saved sort key, time fields, partition width and part dictionary setting.

## Sort key

An index's sort key lists the field paths that order the rows within a run. The leading fields are available for key-range pruning and, for eligible plans, ordered aggregation. With an empty key, the store orders rows by event time. See [Sort key pruning](../architecture/indexing.md#sort-key-pruning) for query examples.

## Part dictionary

An index created with `part_dictionary: true` keeps one dictionary per part for each string column whose values repeat across the blocks of that part, and the blocks store codes of it. The writer chooses the columns: fields outside the sort key whose values mostly share one length, such as account and content identifiers, where the dictionary and the codes take at least a quarter less than the per-block forms. Filters, groupings and distinct counts on such a column work on the codes. [Parts](../architecture/storage.md#parts) describes the format, and [Column metadata and exact counts](../architecture/indexing.md#column-metadata-and-exact-counts) how queries use it.

The setting is `false` by default. Set it for one index when you create it:

```bash
curl -s -X PUT localhost:3100/api/v1/indexes/bluesky \
  -H 'Content-Type: application/json' \
  -d '{"time_fields": ["time_us"], "partition": "1d", "part_dictionary": true}'
```

`storage.part_dictionary: true` makes it the default for indexes created afterwards. An index keeps the value it was created with. Its merges write parts the same way, and a `settings.json` that lacks the field belongs to an index without part dictionaries.

The setting makes the store smaller and speeds up hot aggregations on a string field of many values. The load takes longer and holds more memory. One measurement loaded 1 billion Bluesky events on an AWS `m6i.8xlarge` with a gp3 volume at 16,000 IOPS and ran the same queries on the same machine without the setting and with it. The answers were equal.

| Index design | `part_dictionary` | Size | Load, ingest and merge | Peak resident memory during load |
|---|---|---|---|---|
| `time_fields` only, daily partitions | `false` | 107.8 GB | 1001 to 1011 s | 47.1 GiB |
| | `true` | 99.9 GB | 1106 s | 59.2 GiB |
| Daily partitions, sort key `kind`, `commit.operation`, `commit.collection`, `did` | `false` | 93.3 GB | 609 s | 67 GiB |
| | `true` | 83.4 GB | 645 s | 68.6 GiB |
| One partition, the same sort key | `false` | 91.9 GB | 799 to 813 s | 75.6 to 79.3 GiB |
| | `true` | 79.5 GB | 909 s | 82.2 GiB |

Query times changed only in the first design, where the account field `did` is in no sort key and aggregations hash it. With the parts in the page cache, four aggregations by account ran 2.5 to 9 times faster and their peak memory fell from 13.5 to 6.9 GiB:

| Query | `false` | `true` |
|---|---|---|
| Events and distinct accounts by collection | 5.2 s | 2.06 s |
| Accounts whose posts span the longest time | 3.9 s | 1.53 s |
| Accounts with the most events | 10.2 s | 1.15 s |
| Distinct accounts | 4.4 s | 0.97 s |

Two of them were slower on a cold run. The query for the longest spans took 19.2 s instead of 12.3 s, although it read 6.4 GiB instead of 13.1 GiB, and the count of distinct accounts took 11.3 s instead of 9.2 to 9.9 s. In the two designs with the sort key, none of 30 queries ran faster or slower.

Set it for an index whose hot aggregations by such a field matter more than the load. With a sort key that includes the field, the setting made the store 11 to 13% smaller and the load 6 to 14% slower, and left the query times as they were.

## Indexes

An index is a store under `data_dir/indexes/<index>/`. Its `settings.json` records `sort_key`, `time_fields`, `partition` and `part_dictionary`. Create the index with [`PUT /api/v1/indexes/{index}`](../api/ingest.md#put-indexesindex) to set those values explicitly. The first ingest to a new index creates it with the server defaults. Use `GET /api/v1/indexes/{index}` to see the resolved settings. To use a different configuration for existing data, create another index with the desired settings and ingest the data into it.

An index name holds ASCII letters, digits, dots, hyphens and underscores. It must have no leading dot and fit within 255 bytes. Native ingest returns `400 VALIDATION_ERROR` for an invalid name.

## Writing events

Ingested events wait in memory, then the store writes them as a sorted run. The flusher checks the age of buffered input against `storage.flush_interval`. Full buffers, a sync or graceful shutdown can trigger a flush sooner. After the manifest commit, the disk parts replace the buffered rows for new queries.

HTTP queries can read buffered rows as soon as ingest acknowledges them. Storage stats count these rows and report the bytes in committed parts. See [Acknowledgements and visibility](../architecture/storage.md#acknowledgements-and-visibility).

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
  part_dictionary: false
```

## Related

- [Ingest Settings](./ingest.md)
- [Server Settings](./server.md)
- [Performance Tuning](../operations/performance-tuning.md)

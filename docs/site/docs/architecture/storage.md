---
sidebar_position: 2
title: Storage
description: How LynxDB keeps events -- one columnar store per index, time partitions, sorted runs merged in the background, immutable parts, flush and sync, recovery and retention.
---

# Storage

Every index has a columnar store of its own (`pkg/colstore`), kept in one directory under the data directory. `pkg/engine/columnar` opens the stores and `pkg/database` sets them up for a server. This page follows events from ingest to disk and back out.

## The Data Directory

```
<data_dir>/
  lynxdb.pid
  main/
    MANIFEST
    MANIFEST.end
    0000000000000000.part
    0000000000000001.part
  web/
    ...
```

| Path | Contents |
|------|----------|
| `<index>/` | The store of one index |
| `<index>/MANIFEST` | An append-only log of changes to the set of live parts |
| `<index>/MANIFEST.end` | Marks that record how far the manifest reached, so that a manifest cut short is told apart from a whole one |
| `<index>/*.part` | Immutable part files |
| `<index>/lost+found/` | Part files set aside at startup, see [Recovery](#recovery) |
| `lynxdb.pid` | The process ID of the running server |
| `auth/`, `tls/` | API keys and the self-signed certificate, when authentication or TLS is on |

An index comes into being with its first events; there is no step to create one. Its name holds letters, digits, dots, dashes and underscores, does not start with a dot and is at most 255 bytes long. Events for an invalid name are refused with `400 VALIDATION_ERROR`.

With an empty data directory (`--data-dir ""`), the stores live in a temporary directory that is deleted when the process exits. Pipe mode always works this way.

## From Events to Rows

A store takes NDJSON: one JSON object per line. The ingest paths turn what they receive into such lines first (see [Write Path](./overview.md#write-path)).

- **Columns by path.** Each line is parsed once, straight into typed columns. Members of nested objects become paths joined with dots, so `{"http": {"path": "/x"}}` gives the column `http.path`. A path holds values of several kinds side by side: integers, floats, strings, booleans and nulls each have a column of their own, and arrays are kept in a compact binary encoding. Members beyond the column budget of a block are kept in the `_rest` column.
- **Text lines.** A line that is not a JSON object is kept as an event holding the text in `_raw`. A JSON event has no `_raw`.
- **Event time.** `_time` comes from the first field of `storage.time_fields` that the event holds: by default `_time`, `timestamp`, `@timestamp`, `time`, `ts`. A number is read as seconds, milliseconds, microseconds or nanoseconds since the epoch, told apart by its size; text is read as a date, `T` or a space, a time of day and a zone (`Z` or an offset; UTC without one). An event without such a field gets the time it arrived. The field itself stays in the event.
- **Sparse rows.** A field an event lacks is absent from it, not null, and query results keep it that way.

```bash
printf '{"msg":"a","ts":1700000000}\nplain text line here\n{"msg":"b","http":{"path":"/x","status":200}}\n' |
  curl -s -X POST 'localhost:3100/api/v1/ingest?index=demo' --data-binary @-
curl -s localhost:3100/api/v1/query -d '{"q":"source=demo"}'
```

```json
{"data":{"type":"events","events":[
  {"_raw":"plain text line here","_time":"2026-11-17T15:42:37.334325Z"},
  {"_time":"2026-11-17T15:42:37.334325Z","http.path":"/x","http.status":200,"msg":"b"},
  {"_time":"2023-11-14T22:13:20Z","msg":"a","ts":1700000000}],
  "total":3,"has_more":false},
 "meta":{"took_ms":0.176,"query_id":"qry_5e863443609d2c18","rows":3}}
```

## Time Partitions

Rows are kept apart by the span of time they fall in, `storage.partition` long (24 hours by default). A partition holds the rows whose time, divided by the span, rounds down to its number, so with the default span the partitions are UTC days. Partitions decide three things:

- a query with a time range reads only the partitions it meets;
- merges never mix rows of two partitions;
- retention drops a partition whole.

## Writes: Buffers, Runs and Sync

`Append` parses the lines into in-memory buffers and returns. The buffers are written:

- once a row has waited for the flush interval (`storage.flush_interval`; `0s` means the store's default of one second),
- when the buffers hold their budget of input, or
- on a sync.

A write sorts the buffered rows by partition and time and writes them as one **run** per partition: a sorted sequence of rows, cut by key range into parts that are written side by side. The part files are made durable on disk, then one record in the manifest names them. From that record on, the rows are durable and visible to queries.

The server syncs at these points:

- before every query, so a query sees every event ingested before it;
- before it answers an Elasticsearch `_bulk` request, because shippers count a `200` as stored;
- for Splunk HEC acknowledgements;
- before `/api/v1/stats`, `/api/v1/status`, `/api/v1/indexes` and the field catalog answer.

Concurrent syncs share one write. A graceful shutdown writes what the buffers hold. Events that `POST /api/v1/ingest` accepted and that are still in the buffers when the process is killed are lost; at most one flush interval of them.

## Parts

A part is an immutable file of **blocks** of up to 16384 rows, followed by a footer that indexes them. Each block holds, for every path and kind present in it, a column chunk with one of these encodings:

| Encoding | Holds |
|----------|-------|
| Constant | One value for every row, kept in the block's column directory |
| Dictionary | A sorted dictionary and one code per present row |
| Flat | One value per present row |
| Time | The time of every row, in the coarsest unit that keeps it exact |
| Presence only | Which rows hold the path, for values such as null that need nothing more |

A sparse column records which rows hold it as one bit per row, a list of row numbers, or a list of the rows without it, whichever is smallest. Strings and arrays are compressed with zstd, or with LZ4 for long text where that costs at most half again the size, since LZ4 decodes several times faster. Identifier-like strings are packed at a few bits per character.

For every block the footer records its row count and the earliest and latest time of its rows, and the column directory records constant values and integer bounds. Every block also carries **token filters**: split-block Bloom filters over the case-folded words of its strings, one for each large column and one for the rest. A block is read only when its time range, its column directory and its token filters admit the query's filter. A part also remembers, per filter, the blocks a scan found to hold no matching row, and later scans of the same filter skip them.

Column chunks, filter pages and the footer carry checksums, which reads verify. Parts are read through memory mappings.

## Merges

Flushes leave many small runs behind. A background merger keeps the number that queries meet low:

- Runs that came from flushes are tier 0. Eight runs of one tier and one partition are merged into one run of the next tier, oldest first.
- A partition that has closed (its span ended an hour ago, or its own length ago when that is shorter) is merged into one run, and again once rows that arrived later make up an eighth of it.
- Merges give way to flushes while rows wait in the buffers and the backlog of runs due for merging is small, so that ingest does not wait for them.
- The memory of merges is bounded: a merge holds one decoded block per run it reads, and all merges together hold a fixed number of blocks.

A merge writes its run, then commits one manifest record that adds the merged parts and drops the parts they replace. Queries read a **snapshot**, a consistent set of parts taken when the query starts. A part that a merge replaced stays readable until the last snapshot that holds it is released; then its file is deleted.

## Retention

Once a minute, and when the server starts, the database drops from every index the partitions that end at or before the current time minus `retention`. A partition goes as a whole, so an event stays until the whole span it falls in has passed the retention period: with the defaults (`retention: 7d`, `storage.partition: 24h`) an event is dropped between seven and eight days after its time, plus up to a minute. `retention: 0` keeps everything. Events older than the retention period that arrive late are dropped by the next pass.

## Recovery

At startup a store reads its manifest and opens every part it names. Part files and temporary files that no record names are left behind by writes and merges that did not commit; they are deleted. When the marks in `MANIFEST.end` cannot vouch that the manifest is whole, such a part might hold rows a lost record named, so it is moved to `lost+found/` instead of being deleted.

A manifest shorter than its marks, or a part that the manifest names but the directory cannot provide, stops the store from opening: the server exits with an error that names the index, and every file is left as it was.

## Statistics

`GET /api/v1/stats` and `GET /api/v1/indexes` report, per index, the number of events, the bytes of its part files, the number of parts and the times of its first and last events, as the stores hold them after a sync.

## Related

- [Architecture Overview](./overview.md)
- [Query Engine](./query-engine.md) -- how queries read the stores
- [Storage configuration](../configuration/storage.md)
- [Retention](../operations/retention.md)

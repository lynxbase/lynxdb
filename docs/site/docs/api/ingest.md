---
sidebar_position: 2
title: Ingest
description: NDJSON event ingest, raw log text ingest, Splunk HEC and Elasticsearch bulk ingest, and the calls that create, describe and list indexes.
---

# Ingest API

| Endpoint | Use it for | Body |
|---|---|---|
| `POST /api/v1/ingest` | Structured events | NDJSON, one JSON object per line |
| `POST /api/v1/ingest/raw` | Log files and streams | Log text, one event per line |
| `POST /api/v1/ingest/hec` | Splunk HEC senders | One HEC event object per line |

Shippers that speak Elasticsearch, Splunk HEC or OTLP can also use the [compatibility endpoints](./compatibility.md). When authentication is on, the ingest endpoints need a key with `ingest` scope.

## How events are stored

Every index keeps its events in a columnar store of its own. An index comes into being with its first events, taking the server's `storage.sort_key`, `storage.time_fields` and `storage.partition` as its settings, or with [`PUT /indexes/{index}`](#put-indexesindex), which gives it settings of its own. The settings are written to the index's directory before it takes events, and the index keeps them: changing the `storage` settings affects only indexes created afterwards.

- **Time.** An event's time is the value of the first of the index's time fields that it holds; `storage.time_fields` gives `_time`, `timestamp`, `@timestamp`, `time` and `ts`. An event without one gets the time it arrived.
- **Text.** `POST /ingest` takes JSON objects only and counts any other line as failed. `POST /ingest/raw` keeps a line of text as an event holding it in `_raw`. A JSON event has no `_raw`.
- **Fields.** The members of a nested object are read as columns named by their path, such as `order.id`. A query result holds only the fields an event has: a field the event lacks is absent, not `null`.
- **Retention.** Events whose own time is older than `retention` (7 days by default) are removed, partition by partition, within about a minute of arriving or when the server starts. Each removal is logged at `WARN` with the index, the event count and the cutoff. `retention: 0s` keeps everything.
- **Visibility.** Ingested events are written within `storage.flush_interval` (one second by default). The server writes the pending events before it runs a query, so a query sees every event whose ingest request has been answered. The Elasticsearch bulk endpoints, and Splunk HEC requests that carry an acknowledgement channel, make the events durable before they answer.

Index names hold letters, digits, `.`, `-` and `_`, do not start with `.`, and are at most 255 bytes. Any other name answers `400 VALIDATION_ERROR`.

## POST /ingest

Takes NDJSON and appends it, line by line and unchanged, to one index: the `index` query parameter, else the `X-Index` header, else `main`.

```bash
curl -s -X POST 'localhost:3100/api/v1/ingest?index=demo' --data-binary $'{"_time":"2026-11-17T08:00:00Z","level":"info","msg":"user login","user_id":42}
{"timestamp":"2026-11-17T08:00:05Z","level":"error","msg":"payment failed","order":{"id":7,"total":19.5}}
plain text line
'
```

```json
{"data": {"accepted": 2, "failed": 1}}
```

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=demo | sort _time"}'
```

```json
{
  "data": {
    "type": "events",
    "events": [
      {"_time": "2026-11-17T08:00:00Z", "level": "info", "msg": "user login", "user_id": 42},
      {"_time": "2026-11-17T08:00:05Z", "level": "error", "msg": "payment failed", "order.id": 7, "order.total": 19.5, "timestamp": "2026-11-17T08:00:05Z"}
    ],
    "total": 2,
    "has_more": false
  },
  "meta": {"took_ms": 0.103, "query_id": "qry_f12636a8a2a0b0b4", "rows": 2}
}
```

`accepted` counts the lines stored as events; `failed` counts the lines that are not JSON objects, which are not stored. Blank lines are skipped. A body in which no line is a JSON object answers `400 INVALID_REQUEST`; send log text to [`POST /ingest/raw`](#post-ingestraw).

`POST /ingest` and `POST /ingest/raw` are bounded by `ingest.max_body_size` alone, as sent and after `gzip`, `zstd` or `snappy` decoding. A body over it stores nothing and answers `413`, naming the setting:

```json
{"error": {"type": "request_body_too_large", "reason": "compressed body exceeds limit 4096 set by ingest.max_body_size"}, "status": 413}
```

A body sent without `Content-Length`, and a compressed body, is appended as it is read. When it grows past the limit the reply is `413 INVALID_REQUEST`; when it is cut short or does not decode the reply is `400 INVALID_REQUEST`. Either message ends with the lines taken before that point:

```json
{"error": {"code": "INVALID_REQUEST", "message": "request body is cut short or does not decode; the 17371 lines before that were taken"}}
```

Sending such a body again repeats the lines already taken.

## POST /ingest/raw

Takes log text, one event per line. The index is the `X-Index` header, else the `index` query parameter, else `main`.

| Header | Default | Description |
|---|---|---|
| `X-Index` | `main` | Target index |
| `X-Source` | `http` | `source` of the text lines |
| `X-Source-Type` | `raw` | `sourcetype` of the text lines |
| `X-Host` | | `host` of the text lines |

Each header holds at most 256 characters; a longer one answers `400 VALIDATION_ERROR`.

- A line that is a JSON object (its first non-blank character is `{`) passes unchanged, without the labels.
- Any other line becomes an event with the line in `_raw`, the labels as `source`, `sourcetype` and `host`, and:
  - its timestamp as `_time`, found at the start of the line or after a space or a double quote within its first 100 characters: RFC 3339 with millisecond or finer precision or a numeric offset (`2026-11-17T08:00:00.123Z`, `2026-11-17T08:00:00+01:00`), `2026-11-17 08:00:00`, or the syslog form `Nov 17 08:00:00`. A line without one gets the time it arrived;
  - its `key=value` pairs as fields. Integers, decimals and `true`/`false` keep their type, so `status>=500` compares numbers.
- Blank lines are skipped. A line longer than 1 MiB ends the request.

```bash
curl -s -X POST localhost:3100/api/v1/ingest/raw \
  -H 'X-Index: rawdemo' \
  -H 'X-Source: api-gateway' \
  -H 'X-Host: gw-01' \
  --data-binary $'2026-11-17T08:00:00.000Z level=warn path=/api/users latency_ms=812 slow request\n{"level":"info","msg":"json passes unchanged"}\n'
```

```json
{"data": {"accepted": 2, "failed": 0}}
```

The two events read back as:

```json
{"_raw": "2026-11-17T08:00:00.000Z level=warn path=/api/users latency_ms=812 slow request", "_time": "2026-11-17T08:00:00Z", "host": "gw-01", "latency_ms": 812, "level": "warn", "path": "/api/users", "source": "api-gateway", "sourcetype": "raw"}
{"_time": "2026-11-17T15:46:41.906133Z", "level": "info", "msg": "json passes unchanged"}
```

Pipe command output the same way:

```bash
kubectl logs deploy/api --since=1h | \
  curl -s -X POST localhost:3100/api/v1/ingest/raw \
    -H 'X-Source: api-gateway' \
    --data-binary @-
```

When the body is cut off, by `ingest.max_body_size` or by a line over 1 MiB, after some events were stored, the response is `200` with `truncated: true` and a `warning`. When nothing was stored, it answers `400 INVALID_REQUEST`.

## POST /ingest/hec

Splunk HEC event ingest. Each line of the body is one HEC event object; lines that are not valid JSON are skipped and counted in `skipped_lines`.

```bash
curl -s -X POST localhost:3100/api/v1/ingest/hec \
  -d '{"event": "via api", "host": "web-02"}'
```

```json
{"code": 0, "text": "Success"}
```

This path authenticates like every other `/api/v1` endpoint and does not read `Authorization: Splunk` tokens. It accepts events whatever `ingest.splunk_hec.enabled` says. Shippers configured for Splunk usually point at `/services/collector`; see [Splunk HEC](./compatibility.md#splunk-hec) for those routes, acknowledgements and the mapping of HEC fields to events.

## GET /indexes

Lists the indexes with what they hold:

```bash
curl -s localhost:3100/api/v1/indexes
```

```json
{
  "data": {
    "indexes": [
      {"name": "backend", "events": 26, "bytes": 3978, "parts": 1, "merges_running": 0, "runs_to_merge": 0, "first_event": "2026-02-10T08:12:03.045Z", "last_event": "2026-02-10T10:45:00.5Z", "sort_key": [], "time_fields": ["_time", "timestamp", "@timestamp", "time", "ts"], "partition": "1d"},
      {"name": "main", "events": 1000, "bytes": 46638, "parts": 2, "merges_running": 0, "runs_to_merge": 2, "first_event": "2025-01-15T08:00:00.105Z", "last_event": "2026-11-17T15:43:40.839562Z", "sort_key": [], "time_fields": ["_time", "timestamp", "@timestamp", "time", "ts"], "partition": "1d"}
    ]
  }
}
```

| Field | Description |
|---|---|
| `name` | Index name |
| `events` | Events stored |
| `bytes` | Size of the index's files |
| `parts` | Columnar files the index holds |
| `merges_running` | Merges of the index's files under way |
| `runs_to_merge` | Sorted runs that background merging is still to merge, those of a past partition that waits for events to stop arriving included; the index is quiet when this and `merges_running` are both 0 |
| `first_event`, `last_event` | Times of the earliest and latest event; absent for an index without stored events |
| `sort_key`, `time_fields`, `partition` | The settings of the index, see [`PUT /indexes/{index}`](#put-indexesindex) |

Query an index with `source=<name>`:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=backend | stats count() by level"}'
```

## GET /indexes/\{index\}

Describes one index as an entry of [`GET /indexes`](#get-indexes) does:

```bash
curl -s localhost:3100/api/v1/indexes/bluesky
```

```json
{"data": {"name": "bluesky", "events": 2, "bytes": 889, "filter_bytes": 32, "parts": 1, "merges_running": 0, "runs_to_merge": 0, "first_event": "2024-11-21T16:25:49.000167Z", "last_event": "2024-11-21T16:25:49.00028Z", "sort_key": ["kind", "commit.operation", "commit.collection", "did"], "time_fields": ["time_us"], "partition": "1d"}}
```

An index that does not exist answers `404 NOT_FOUND`.

## PUT /indexes/\{index\}

Creates an index with settings of its own, before any event arrives. Declare as the sort key the fields that the queries of the index filter and group by, the most selective last, so that its files are written in that order:

```bash
curl -s -X PUT localhost:3100/api/v1/indexes/bluesky \
  -H 'Content-Type: application/json' \
  -d '{"sort_key": ["kind", "commit.operation", "commit.collection", "did"], "time_fields": ["time_us"], "partition": "1d"}'
```

```json
{"data": {"name": "bluesky", "events": 0, "bytes": 0, "filter_bytes": 0, "parts": 0, "merges_running": 0, "runs_to_merge": 0, "sort_key": ["kind", "commit.operation", "commit.collection", "did"], "time_fields": ["time_us"], "partition": "1d"}}
```

| Setting | Default | Value |
|---|---|---|
| `sort_key` | `storage.sort_key` | Up to 32 field paths whose values order the rows of the index, the first the most significant; `[]` orders them by time |
| `time_fields` | `storage.time_fields` | Up to 32 field paths whose value becomes the time of an event, tried in their order |
| `partition` | `storage.partition` | The span of time whose events are kept apart, from `1m` to `366d`, such as `1h` or `1d`; retention drops whole partitions |

Every setting is optional, and the body may be left out. A field path names a nested member by its path, such as `commit.collection`; it has no empty segment and no control character, and a list names it once.

The call answers `201` when it creates the index. The settings of an index do not change: on an index that exists, the call answers `200` with the index when the index has every setting the body gives, and `409 CONFLICT`, naming the first setting that differs, otherwise:

```json
{"error": {"code": "CONFLICT", "message": "columnar: the index exists with other settings: index bluesky has sort_key [kind commit.operation commit.collection did], not [did]", "suggestion": "The settings of an index do not change; create another index for another design."}}
```

It needs `ingest` or `admin` scope when authentication is on.

## POST /indexes/\{index\}/compact

Writes the events ingested so far, merges the files of every partition of the index into one sorted run, and answers when that is done. Background merging gets there too, but only after a partition has gone without new events for a while; call this at the end of a bulk load so that the queries that follow read merged files. The request takes as long as the merge; queries that run meanwhile read the files as they were until the merge commits. It needs `ingest` or `admin` scope when authentication is on.

```bash
curl -s -X POST localhost:3100/api/v1/indexes/main/compact
```

```json
{"data": {"index": "main", "parts_before": 6, "parts": 1, "bytes_before": 48210, "bytes": 46638, "seconds": 0.08}}
```

| Field | Description |
|---|---|
| `parts_before`, `bytes_before` | Files and their size once the pending events were written |
| `parts`, `bytes` | Files and their size after the merge |
| `seconds` | Time the request took, the write of the pending events included |

An index that does not exist answers `404 NOT_FOUND`; a server that is shutting down answers `503 SHUTTING_DOWN` and leaves the files as they were.

## Errors

| HTTP | Code | When |
|---|---|---|
| 400 | `VALIDATION_ERROR` | An invalid index name or index setting, or an `X-Index`, `X-Source`, `X-Source-Type` or `X-Host` header longer than 256 characters |
| 400 | `INVALID_JSON` | The body of `PUT /indexes/{index}` is not a JSON object of the settings |
| 400 | `INVALID_REQUEST` | The body could not be read |
| 400 | | The `Content-Encoding` is not `gzip`, `zstd` or `snappy`: `{"error": {"type": "invalid_request", ...}, "status": 400}` |
| 401 | `AUTH_REQUIRED`, `INVALID_TOKEN` | Missing or wrong credentials |
| 403 | `FORBIDDEN` | The key lacks `ingest` scope |
| 404 | `NOT_FOUND` | `GET /indexes/{index}` or `POST /indexes/{index}/compact` names an index that does not exist |
| 409 | `CONFLICT` | `PUT /indexes/{index}` asks for other settings than the index has |
| 413 | `INVALID_REQUEST` | The body of `POST /ingest`, sent without `Content-Length`, grows past `ingest.max_body_size` |
| 413 | | The body is over its size limit: `{"error": {"type": "request_body_too_large", ...}, "status": 413}`, with a `reason` that names the setting |
| 429 | `TOO_MANY_REQUESTS` | The per-IP rate limit (`http.rate_limit`) was hit |
| 503 | `SHUTTING_DOWN` | The server is stopping; retry after `Retry-After` |
| 500 | `INTERNAL_ERROR` | The events could not be stored |

## Related

- [`lynxdb ingest`](../cli/ingest.md): send files and pipes to `/api/v1/ingest/raw`
- [Compatibility endpoints](./compatibility.md): Elasticsearch, Splunk HEC, OTLP
- [Ingest data guide](../guides/ingest-data.md)
- [Ingest configuration](../configuration/ingest.md)

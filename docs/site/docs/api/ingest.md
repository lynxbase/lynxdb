---
sidebar_position: 2
title: Ingest
description: NDJSON event ingest, raw log text ingest, Splunk HEC and Elasticsearch bulk ingest, and the list of indexes.
---

# Ingest API

| Endpoint | Use it for | Body |
|---|---|---|
| `POST /api/v1/ingest` | Structured events | NDJSON, one JSON object per line |
| `POST /api/v1/ingest/raw` | Log files and streams | Log text, one event per line |
| `POST /api/v1/ingest/hec` | Splunk HEC senders | One HEC event object per line |
| `POST /api/v1/ingest/bulk` | Elasticsearch bulk producers | Elasticsearch `_bulk` NDJSON |

Shippers that speak Elasticsearch, Splunk HEC or OTLP can also use the [compatibility endpoints](./compatibility.md). When authentication is on, the ingest endpoints need a key with `ingest` scope.

## How events are stored

Every index keeps its events in a columnar store of its own. An index comes into being with its first events; there is no call to create one.

- **Time.** An event's time is the value of the first of `storage.time_fields` (`_time`, `timestamp`, `@timestamp`, `time`, `ts`) that it holds. An event without one gets the time it arrived.
- **Text.** A line that is not a JSON object is kept as an event holding the text in `_raw`. A JSON event has no `_raw`.
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
{"data": {"accepted": 3, "failed": 0}}
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
      {"_time": "2026-11-17T08:00:05Z", "level": "error", "msg": "payment failed", "order.id": 7, "order.total": 19.5, "timestamp": "2026-11-17T08:00:05Z"},
      {"_time": "2026-11-17T15:46:35.786981Z", "_raw": "plain text line"}
    ],
    "total": 3,
    "has_more": false
  },
  "meta": {"took_ms": 0.103, "query_id": "qry_f12636a8a2a0b0b4", "rows": 3}
}
```

`accepted` counts the lines that are not blank, less those the store rejected; `failed` counts the rejected ones. Blank lines are skipped.

`POST /ingest` and `POST /ingest/raw` are bounded by `ingest.max_body_size` alone, as sent and after `gzip`, `zstd` or `snappy` decoding. A body over it stores nothing and answers `413`, naming the setting:

```json
{"error": {"type": "request_body_too_large", "reason": "compressed body exceeds limit 4096 set by ingest.max_body_size"}, "status": 413}
```

A body sent without `Content-Length` that grows past the limit answers `413 INVALID_REQUEST` instead.

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

## POST /ingest/bulk

The Elasticsearch bulk handler under the native ingest path, with the same request and response as `POST /api/v1/es/_bulk`:

```bash
curl -s -X POST localhost:3100/api/v1/ingest/bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"index":{"_index":"logs"}}\n{"message":"hello from filebeat","@timestamp":"2026-11-17T09:00:00Z"}\n'
```

See [Elasticsearch](./compatibility.md#elasticsearch) for the format.

## GET /indexes

Lists the indexes with what they hold:

```bash
curl -s localhost:3100/api/v1/indexes
```

```json
{
  "data": {
    "indexes": [
      {"name": "backend", "events": 26, "bytes": 3978, "parts": 1, "first_event": "2026-02-10T08:12:03.045Z", "last_event": "2026-02-10T10:45:00.5Z"},
      {"name": "main", "events": 1000, "bytes": 46638, "parts": 2, "first_event": "2025-01-15T08:00:00.105Z", "last_event": "2026-11-17T15:43:40.839562Z"}
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
| `first_event`, `last_event` | Times of the earliest and latest event; absent for an index without stored events |

Query an index with `source=<name>`:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=backend | stats count() by level"}'
```

## Errors

| HTTP | Code | When |
|---|---|---|
| 400 | `VALIDATION_ERROR` | An invalid index name, or an `X-Index`, `X-Source`, `X-Source-Type` or `X-Host` header longer than 256 characters |
| 400 | `INVALID_REQUEST` | The body could not be read |
| 400 | | The `Content-Encoding` is not `gzip`, `zstd` or `snappy`: `{"error": {"type": "invalid_request", ...}, "status": 400}` |
| 401 | `AUTH_REQUIRED`, `INVALID_TOKEN` | Missing or wrong credentials |
| 403 | `FORBIDDEN` | The key lacks `ingest` scope |
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

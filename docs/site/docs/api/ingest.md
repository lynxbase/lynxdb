---
sidebar_position: 2
title: Ingest
description: Structured JSON event arrays, raw line ingest, Splunk HEC and Elasticsearch bulk ingest, and index management.
---

# Ingest API

| Endpoint | Use it for | Body |
|---|---|---|
| `POST /api/v1/ingest` | Structured events | JSON array of event objects |
| `POST /api/v1/ingest/raw` | Log files and streams | Newline-delimited text, one event per line |
| `POST /api/v1/ingest/hec` | Splunk HEC senders | One HEC event object per line |
| `POST /api/v1/ingest/bulk` | Elasticsearch bulk producers | Elasticsearch `_bulk` NDJSON |

Shippers that speak Elasticsearch, Splunk HEC or OTLP can also use the [compatibility endpoints](./compatibility.md). When authentication is on, the ingest endpoints need a key with `ingest` scope.

Every ingest path runs events through the ingest pipeline: it looks for an ISO 8601 or syslog-style timestamp in events that have no time, parses JSON events into fields, and extracts `key=value` pairs. Events whose time is still unknown get the time the server received them.

## POST /ingest

Takes a JSON array. A single object, NDJSON or plain text is rejected with `400 INVALID_JSON`.

| Field | Type | Description |
|---|---|---|
| `event` | string | Event text, stored in `_raw` (required) |
| `time` | number | Unix time in seconds; fractions are kept |
| `source` | string | Source label |
| `sourcetype` | string | Source type label |
| `host` | string | Host name |
| `index` | string | Target index; `main` when empty |
| `fields` | object | Extra fields. String, number and boolean values are kept; integral numbers become integers |

```bash
curl -s -X POST localhost:3100/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '[
    {
      "time": 1763370000,
      "event": "user login",
      "source": "auth-api",
      "sourcetype": "app",
      "host": "web-01",
      "fields": {"user_id": 42, "level": "info", "ip": "10.0.1.5"}
    }
  ]'
```

```json
{"data": {"accepted": 1, "failed": 0}}
```

Events are written in batches of `ingest.max_batch_size` (default 1000). If a later batch fails after earlier ones were stored, the response is still `200`, with `failed` counting the lost events and a `warning` explaining what happened. The same applies when the body turns out to be malformed after some events were accepted.

A body that is not an array:

```bash
curl -s -X POST localhost:3100/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{"event": "not wrapped in an array"}'
```

```json
{
  "error": {
    "code": "INVALID_JSON",
    "message": "invalid JSON: /api/v1/ingest expects a top-level JSON array of structured event objects",
    "suggestion": "Use POST /api/v1/ingest with a JSON array of objects containing event. Use /api/v1/ingest/raw for newline-delimited logs or NDJSON, and /api/v1/es/_bulk for Elasticsearch bulk payloads."
  }
}
```

## POST /ingest/raw

Each non-empty line of the body becomes one event, with the line in `_raw`. Lines longer than `ingest.max_line_bytes` (default 1 MiB) end the request.

| Header | Default | Description |
|---|---|---|
| `X-Source` | `http` | Source label, at most 256 characters |
| `X-Source-Type` | `raw` | Source type label, at most 256 characters |
| `X-Index` | `main` | Target index, at most 256 characters |

```bash
curl -s -X POST localhost:3100/api/v1/ingest/raw \
  -H 'Content-Type: text/plain' \
  -H 'X-Source: nginx' \
  -H 'X-Source-Type: combined' \
  --data-binary @/var/log/nginx/access.log
```

```json
{"data": {"accepted": 2, "failed": 0}}
```

Pipe command output the same way:

```bash
kubectl logs deploy/api --since=1h | \
  curl -s -X POST localhost:3100/api/v1/ingest/raw \
    -H 'X-Source: api-gateway' \
    --data-binary @-
```

When the body is cut off by `ingest.max_body_size` after some events were stored, the response is `200` with `truncated: true` and a `warning`. A body that cannot be read before any event is stored answers `400 INVALID_REQUEST`.

## POST /ingest/hec

Splunk HEC event ingest. Each line of the body is one HEC event object; lines that are not valid JSON are skipped and counted in `skipped_lines`.

```bash
curl -s -X POST localhost:3100/api/v1/ingest/hec \
  -d '{"event": "user login succeeded", "time": 1763370000, "host": "web-01", "source": "auth-service", "sourcetype": "json", "fields": {"level": "info"}}'
```

```json
{"code": 0, "text": "Success"}
```

This path authenticates like every other `/api/v1` endpoint and does not look at `Authorization: Splunk` tokens. Shippers configured for Splunk usually point at `/services/collector` instead; see [Splunk HEC](./compatibility.md#splunk-hec) for both paths and the field mapping.

## POST /ingest/bulk

An alias of the Elasticsearch bulk handler, with the same request and response format as `POST /api/v1/es/_bulk`:

```bash
curl -s -X POST localhost:3100/api/v1/ingest/bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"index":{"_index":"logs"}}\n{"message":"hello from filebeat","@timestamp":"2026-11-17T09:00:00Z"}\n'
```

See [Elasticsearch compatibility](./compatibility.md#elasticsearch) for the mapping of documents to events.

## Indexes

`GET /api/v1/indexes` lists the indexes:

```json
{"data": {"indexes": [{"name": "main", "retention_period": "2160h0m0s", "replication_factor": 1}]}}
```

`POST /api/v1/indexes` creates one. It needs a root key when authentication is on.

```bash
curl -s -X POST localhost:3100/api/v1/indexes \
  -H 'Content-Type: application/json' \
  -d '{"name": "audit", "retention_days": 365}'
```

```json
{"data": {"name": "audit", "status": "created"}}
```

| Field | Description |
|---|---|
| `name` | Index name (required). Use lowercase letters, digits, `-` and `_`, so the name works unquoted in `source=` |
| `retention_days` | Retention in days; 90 when omitted |

Ingest endpoints also create an index the first time an event names it. Query an index with `source=<name>`:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=audit | stats count() by host"}'
```

## Errors

| HTTP | Code | When |
|---|---|---|
| 400 | `INVALID_JSON` | The body of `POST /ingest` is not a JSON array of event objects |
| 400 | `INVALID_REQUEST` | The body could not be read |
| 400 | `VALIDATION_ERROR` | An `X-Source`, `X-Source-Type` or `X-Index` header is longer than 256 characters, or `POST /indexes` has no `name` |
| 401 | `AUTH_REQUIRED`, `INVALID_TOKEN` | Missing or wrong credentials |
| 403 | `FORBIDDEN` | The key lacks `ingest` scope |
| 413 | | The body is over the compressed or decompressed size limit |
| 429 | `TOO_MANY_REQUESTS` | The per-IP rate limit (`http.rate_limit`) was hit |
| 503 | `BACKPRESSURE` | Staging is full or compaction is behind; retry after `Retry-After` |
| 503 | `SHUTTING_DOWN` | The server is stopping |

## Related

- [`lynxdb ingest`](../cli/ingest.md): send files and pipes to `/api/v1/ingest/raw`
- [Compatibility endpoints](./compatibility.md): Elasticsearch, Splunk HEC, OTLP
- [Ingest data guide](../guides/ingest-data.md)
- [Ingest configuration](../configuration/ingest.md)

---
sidebar_position: 2
title: Ingest
description: POST /ingest, /ingest/bulk endpoints -- send JSON, NDJSON, or plain text log events into LynxDB.
---

# Ingest API

Send log events into LynxDB. Supports single events, JSON arrays, NDJSON streams, and plain text. No required schema -- send any JSON and fields are indexed automatically.

## POST /ingest

Primary ingest endpoint.

### Content Types

| Content-Type | Format | Description |
|---|---|---|
| `application/json` | Single object or array | One event or batch of events |
| `application/x-ndjson` | One JSON object per line | Streaming / high-throughput ingest |
| `text/plain` | Raw text lines | Each line becomes an event with `_raw` field |

### Headers

| Header | Required | Description | Default |
|---|---|---|---|
| `Content-Type` | No | Request body format | `application/json` |
| `X-Source` | No | Tag events with a source label (e.g., `nginx`) | -- |
| `X-Format` | No | Force log format parser: `json`, `syslog`, `clf`, `raw`, `auto` | `auto` |

### Behavior

- No required schema -- send any JSON object and all fields are indexed automatically.
- `_timestamp` is auto-assigned if absent. Recognized timestamp field aliases: `timestamp`, `@timestamp`, `time`, `ts`, `datetime`.
- `_source` can be set via the `X-Source` header or a `source`/`_source` field in the body.
- The server assigns a `_id` (ULID) to each event.
- Events larger than 1 MB are rejected with `413`.

### Single Event

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -d '{"message": "user login", "user_id": 42, "ip": "10.0.1.5"}'
```

**Response (200):**

```json
{
  "data": {
    "accepted": 1,
    "failed": 0
  }
}
```

### Batch of Events (JSON Array)

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '[
    {"message": "request started", "trace_id": "abc123"},
    {"message": "request completed", "trace_id": "abc123", "duration_ms": 45}
  ]'
```

**Response (200):**

```json
{
  "data": {
    "accepted": 2,
    "failed": 0
  }
}
```

### NDJSON Stream

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/x-ndjson" \
  -d '{"message": "event 1", "level": "info"}
{"message": "event 2", "level": "error"}'
```

### Plain Text

Each line becomes a separate event with the full line stored as `_raw`:

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: text/plain" \
  -H "X-Source: nginx" \
  -d '192.168.1.1 - - [14/Feb/2026:14:23:01 +0000] "GET /api/users HTTP/1.1" 200 1234
192.168.1.2 - - [14/Feb/2026:14:23:02 +0000] "POST /api/orders HTTP/1.1" 500 89'
```

### Piping from Files

```bash
# Ingest a log file
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: text/plain" \
  -H "X-Source: web-01" \
  --data-binary @/var/log/nginx/access.log

# Pipe from a command
kubectl logs deploy/api --since=1h | \
  curl -X POST localhost:3100/api/v1/ingest \
    -H "Content-Type: text/plain" \
    -H "X-Source: api-gateway" \
    --data-binary @-
```

### With Source Tagging

The `X-Source` header tags all events in the request, making them queryable with `source=nginx`:

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "X-Source: nginx" \
  -d '{"status": 200, "uri": "/api/users", "duration_ms": 12}'
```

### Partial Failure (207)

When some events in a batch are accepted and some fail:

```json
{
  "data": {
    "accepted": 2,
    "failed": 1,
    "errors": [
      {
        "index": 1,
        "code": "PARSE_ERROR",
        "message": "Invalid JSON at line 2"
      }
    ]
  }
}
```

### Error Responses

| Status | Code | Description |
|---|---|---|
| `401` | `AUTH_REQUIRED` | Authentication enabled but no token provided |
| `413` | `PAYLOAD_TOO_LARGE` | Event exceeds 1 MB limit |
| `429` | `RATE_LIMITED` | Ingest rate limit exceeded (check `Retry-After` header) |

---

## POST /ingest/bulk

Elasticsearch-compatible `_bulk` API for zero-config migration from Filebeat, Logstash, Vector, and Fluentd.

### Format

Standard Elasticsearch bulk format: alternating action/metadata lines and document lines in NDJSON:

```bash
curl -X POST localhost:3100/api/v1/ingest/bulk \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index": {"_index": "logs"}}
{"message": "hello from filebeat", "@timestamp": "2026-02-14T12:00:00Z"}
{"index": {"_index": "logs"}}
{"message": "another event"}'
```

### Mapping

- `_index` is accepted but mapped to `_source` tag (LynxDB is single-index by design).
- `_type` is ignored.
- The response mimics the Elasticsearch bulk response shape for client compatibility.

**Response (200):**

```json
{
  "took": 12,
  "errors": false,
  "items": [
    {
      "index": {
        "_id": "01JKNM3VXQP...",
        "status": 201
      }
    },
    {
      "index": {
        "_id": "01JKNM4ABCD...",
        "status": 201
      }
    }
  ]
}
```

### Error Responses

| Status | Code | Description |
|---|---|---|
| `401` | `AUTH_REQUIRED` | Authentication required |
| `429` | `RATE_LIMITED` | Rate limit exceeded |

---

## Timestamp Auto-Detection

LynxDB automatically detects timestamps from the following field names (in order of priority):

1. `_timestamp`
2. `timestamp`
3. `@timestamp`
4. `time`
5. `ts`
6. `datetime`

If no timestamp field is found, the server assigns the current time as `_timestamp`.

## Related

- **[`lynxdb ingest` CLI command](/docs/cli/ingest)** -- ingest from the command line without `curl`
- **[Compatibility endpoints](/docs/api/compatibility)** -- Elasticsearch `_bulk`, OTLP, and Splunk HEC
- **[Ingest Data guide](/docs/guides/ingest-data)** -- end-to-end ingestion walkthrough
- **[Configuration: Ingest](/docs/configuration/ingest)** -- batch size, max body size, rate limits

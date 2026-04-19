---
sidebar_position: 2
title: Ingest
description: Structured event arrays, raw line ingest, Splunk HEC, and Elasticsearch bulk ingest endpoints.
---

# Ingest API

LynxDB exposes multiple ingest endpoints. They are not interchangeable:

| Endpoint | Use it for | Request format |
|---|---|---|
| `POST /api/v1/ingest` | Structured event payloads | JSON top-level array |
| `POST /api/v1/ingest/raw` | Raw log lines | Newline-delimited text |
| `POST /api/v1/ingest/hec` | Splunk HEC senders | One HEC event per line |
| `POST /api/v1/ingest/bulk` | Elasticsearch bulk producers | Elasticsearch `_bulk` NDJSON alias |

If you are sending arbitrary JSON documents from Elasticsearch- or OTLP-style pipelines, use the compatibility endpoints instead of `POST /ingest`.

## POST /ingest

Primary structured ingest endpoint.

`POST /ingest` accepts a JSON array of event payloads. It does not accept:

- a single JSON object
- NDJSON
- plain text log lines

### Request Schema

Each array element maps to this payload shape:

| Field | Type | Required | Description |
|---|---|---|---|
| `event` | string | Yes | Raw event text stored in `_raw` |
| `time` | number | No | Unix timestamp in seconds; fractional seconds are accepted |
| `source` | string | No | Event source label |
| `sourcetype` | string | No | Event format/source type label |
| `host` | string | No | Host label |
| `index` | string | No | Target index name |
| `fields` | object | No | Additional typed fields copied onto the event |

`fields` currently preserves string, numeric, and boolean values.

### Headers

| Header | Required | Description | Default |
|---|---|---|---|
| `Content-Type` | No | Must be JSON | `application/json` |

### Behavior

- The request body must start with `[` and end with `]`.
- `time` is optional. If omitted, LynxDB assigns the server receive time.
- `event` becomes the `_raw` field.
- `fields` values are added as queryable fields on the event.

### Structured Batch Example

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '[
    {
      "time": 1760000000,
      "event": "user login",
      "source": "auth-api",
      "sourcetype": "app",
      "host": "web-01",
      "fields": {
        "user_id": 42,
        "level": "info",
        "ip": "10.0.1.5"
      }
    }
  ]'
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

### Multiple Events

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '[
    {
      "event": "request started",
      "source": "api",
      "fields": {"trace_id": "abc123", "level": "info"}
    },
    {
      "event": "request completed",
      "source": "api",
      "fields": {"trace_id": "abc123", "duration_ms": 45, "level": "info"}
    }
  ]'
```

### Common Validation Error

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"event":"not wrapped in an array"}'
```

This fails because `POST /ingest` requires a top-level JSON array.

## POST /ingest/raw

Use `POST /ingest/raw` for raw log files, stdout streams, and other newline-delimited text.

Each non-empty line becomes a separate event. The line is stored in `_raw` and then passed through the configured ingest pipeline.

### Headers

| Header | Required | Description | Default |
|---|---|---|---|
| `Content-Type` | No | Raw body type | `text/plain` |
| `X-Source` | No | Source label | `http` |
| `X-Source-Type` | No | Source type label | `raw` |
| `X-Index` | No | Target index | `main` |

### Raw Text Example

```bash
curl -X POST localhost:3100/api/v1/ingest/raw \
  -H "Content-Type: text/plain" \
  -H "X-Source: nginx" \
  -H "X-Source-Type: combined" \
  -d '192.168.1.1 - - [14/Feb/2026:14:23:01 +0000] "GET /api/users HTTP/1.1" 200 1234
192.168.1.2 - - [14/Feb/2026:14:23:02 +0000] "POST /api/orders HTTP/1.1" 500 89'
```

### Piping from Files or Commands

```bash
# Send a log file
curl -X POST localhost:3100/api/v1/ingest/raw \
  -H "Content-Type: text/plain" \
  -H "X-Source: web-01" \
  --data-binary @/var/log/nginx/access.log

# Pipe from a command
kubectl logs deploy/api --since=1h | \
  curl -X POST localhost:3100/api/v1/ingest/raw \
    -H "Content-Type: text/plain" \
    -H "X-Source: api-gateway" \
    --data-binary @-
```

If the request body is truncated by `ingest.max_body_size`, the response still returns `200` with `truncated: true` and a warning message.

## POST /ingest/hec

Splunk HEC-compatible ingest path. The request body is read line by line, and each line is parsed as a Splunk HEC event object.

```bash
curl -X POST localhost:3100/api/v1/ingest/hec \
  -d '{"event":"user login","source":"auth-api","sourcetype":"json","fields":{"level":"info"}}'
```

See [Compatibility](/docs/api/compatibility) for migration-oriented examples.

## POST /ingest/bulk

Elasticsearch bulk ingest alias. This is the same handler as the Elasticsearch compatibility endpoints.

```bash
curl -X POST localhost:3100/api/v1/ingest/bulk \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index": {"_index": "logs"}}
{"message": "hello from filebeat", "@timestamp": "2026-02-14T12:00:00Z"}'
```

For Filebeat, Logstash, Vector, Fluentd, and other Elasticsearch-compatible clients, prefer the `/api/v1/es` compatibility base documented in [Compatibility](/docs/api/compatibility). `/api/v1/ingest/bulk` remains available as an alias.

## Error Responses

| Status | Code | Description |
|---|---|---|
| `401` | `AUTH_REQUIRED` | Authentication enabled but no token provided |
| `400` | `INVALID_JSON` / `INVALID_REQUEST` | Malformed JSON array or unreadable request body |
| `404` | `NOT_FOUND` | Wrong endpoint path |
| `413` | `PAYLOAD_TOO_LARGE` | Request exceeds configured body limit |
| `429` | `RATE_LIMITED` | Ingest rate limit exceeded |
| `503` | `BACKPRESSURE` / `SHUTTING_DOWN` | Server is under ingest pressure or shutting down |

## Related

- **[`lynxdb ingest` CLI command](/docs/cli/ingest)** -- send raw lines to `/api/v1/ingest/raw`
- **[Compatibility endpoints](/docs/api/compatibility)** -- Elasticsearch `_bulk`, OTLP, and Splunk HEC
- **[Ingest Data guide](/docs/guides/ingest-data)** -- end-to-end ingestion walkthrough
- **[Configuration: Ingest](/docs/configuration/ingest)** -- batch size, max body size, rate limits

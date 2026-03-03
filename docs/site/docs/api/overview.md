---
sidebar_position: 1
title: API Overview
description: LynxDB REST API design principles, authentication, response envelope, error format, and execution modes.
---

# REST API Overview

LynxDB exposes a clean, streaming-first HTTP API at `http://localhost:3100/api/v1`. The same API powers the Web UI, CLI (in server mode), and any programmatic integration.

:::tip Base URL
All endpoints described in this section are relative to `/api/v1`. For example, `POST /ingest` means `POST http://localhost:3100/api/v1/ingest`.
:::

## Design Principles

### 1. One URL -- One Resource

Each endpoint does exactly one thing:

| Endpoint | Purpose |
|---|---|
| `POST /ingest` | Ingest events |
| `POST /query` | Execute SPL2 query |
| `GET /fields` | List fields |
| `GET /health` | Health check |

No overloaded endpoints, no action parameters. The HTTP method and path tell you everything.

### 2. Streaming-First

Large-data endpoints support NDJSON (newline-delimited JSON) via `Accept: application/x-ndjson`. Real-time endpoints use Server-Sent Events (SSE). This means you can `curl | jq` any result set, pipe exports to files, and build reactive UIs with native `EventSource`.

### 3. Errors Are UX

Every error response carries machine-readable `code`, human-readable `message`, and optionally a `suggestion` and `docs_url`:

```json
{
  "error": {
    "code": "INVALID_QUERY",
    "message": "Unknown field 'stauts'.",
    "suggestion": "status",
    "docs_url": "https://lynxdb.io/docs/spl2/overview"
  }
}
```

Error codes use `SCREAMING_SNAKE_CASE`. The `suggestion` field powers "did you mean?" in the CLI and Web UI.

### 4. Zero Required Headers

`Content-Type` defaults to `application/json`. You do not need to set any headers for basic usage -- `curl` just works:

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -d '{"message": "hello", "level": "info"}'
```

### 5. Three Execution Modes

The `POST /query` endpoint supports three modes via the `wait` parameter:

| `wait` value | Mode | Behavior |
|---|---|---|
| `null` (default) | **Sync** | Block until complete or server timeout (30s). Returns `200` with results. |
| `0` | **Async** | Return `202` immediately with a job handle. Poll or subscribe to SSE. |
| `N` (seconds) | **Hybrid** | Wait up to N seconds. `200` if done in time, `202` with job handle otherwise. |

Hybrid mode (`wait: 5`) is ideal for Web UI -- fast queries feel instant, slow queries degrade gracefully to async with progress tracking.

See [Query API](/docs/api/query) for full details.

## Authentication

Authentication is **off by default**. When enabled via `lynxdb server --auth`, all requests require a Bearer token:

```bash
curl -H "Authorization: Bearer <token>" \
  localhost:3100/api/v1/query -d '{"q": "level=error"}'
```

Unauthenticated requests return `401`:

```json
{
  "error": {
    "code": "AUTH_REQUIRED",
    "message": "Authentication is enabled. Provide a Bearer token.",
    "docs_url": "https://lynxdb.io/docs/deployment/tls-auth"
  }
}
```

See [TLS & Authentication](/docs/deployment/tls-auth) for setup instructions.

## Response Envelope

All JSON responses follow a consistent envelope:

### Success

```json
{
  "data": { ... },
  "meta": {
    "took_ms": 89,
    "scanned": 12400000,
    "query_id": "qry_7f3a..."
  }
}
```

- `data` -- the resource or result
- `meta` -- timing, scan stats, query ID (present on query endpoints)

### Error

```json
{
  "error": {
    "code": "INVALID_QUERY",
    "message": "Unknown command 'staats'.",
    "suggestion": "stats"
  }
}
```

- `error.code` -- machine-readable `SCREAMING_SNAKE_CASE` code
- `error.message` -- human-readable description
- `error.suggestion` -- optional "did you mean?" hint
- `error.docs_url` -- optional link to relevant documentation

No `"status": "ok"` wrapper. HTTP status codes carry success/failure.

## Common HTTP Status Codes

| Code | Meaning |
|---|---|
| `200` | Success |
| `201` | Resource created |
| `202` | Accepted (async job started) |
| `204` | Deleted (no content) |
| `207` | Partial success (some events failed) |
| `400` | Bad request (invalid query, malformed JSON) |
| `401` | Authentication required |
| `404` | Resource not found |
| `408` | Query timeout |
| `410` | Job results expired |
| `413` | Payload too large |
| `422` | Validation error |
| `429` | Rate limited (check `Retry-After` header) |
| `503` | Server unhealthy |

## Common Headers

### Request Headers

| Header | Description | Default |
|---|---|---|
| `Content-Type` | Request body format | `application/json` |
| `Accept` | Response format (`application/json` or `application/x-ndjson`) | `application/json` |
| `Authorization` | `Bearer <token>` (when auth enabled) | -- |
| `X-Source` | Tag ingested events with a source label | -- |
| `X-Format` | Force log format parser: `json`, `syslog`, `clf`, `raw`, `auto` | `auto` |

### Response Headers

| Header | Description |
|---|---|
| `Content-Type` | `application/json`, `application/x-ndjson`, or `text/event-stream` |
| `Retry-After` | Seconds to wait (on `429` responses) |
| `Transfer-Encoding` | `chunked` (on streaming responses) |

## Endpoint Summary

| Method | Endpoint | Description | Docs |
|---|---|---|---|
| `POST` | `/ingest` | Ingest events (JSON, NDJSON, text) | [Ingest](/docs/api/ingest) |
| `POST` | `/ingest/bulk` | ES-compatible bulk ingest | [Ingest](/docs/api/ingest) |
| `POST` | `/query` | Execute SPL2 query | [Query](/docs/api/query) |
| `GET` | `/query` | Execute query (GET convenience) | [Query](/docs/api/query) |
| `POST` | `/query/stream` | NDJSON streaming export | [Query](/docs/api/query) |
| `GET` | `/query/explain` | Parse and explain query | [Query](/docs/api/query) |
| `GET` | `/query/jobs` | List query jobs | [Query](/docs/api/query) |
| `GET` | `/query/jobs/{id}` | Get job status and results | [Query](/docs/api/query) |
| `GET` | `/query/jobs/{id}/stream` | SSE job progress stream | [Query](/docs/api/query) |
| `DELETE` | `/query/jobs/{id}` | Cancel a running job | [Query](/docs/api/query) |
| `GET` | `/tail` | SSE live tail | [Live Tail & Histogram](/docs/api/tail-histogram) |
| `GET` | `/histogram` | Time-bucketed event counts | [Live Tail & Histogram](/docs/api/tail-histogram) |
| `GET` | `/fields` | Field catalog | [Fields & Sources](/docs/api/fields) |
| `GET` | `/fields/{name}/values` | Top values for a field | [Fields & Sources](/docs/api/fields) |
| `GET` | `/sources` | Log sources with stats | [Fields & Sources](/docs/api/fields) |
| `GET/POST` | `/queries` | List/create saved queries | [Saved Queries](/docs/api/saved-queries) |
| `PUT/DELETE` | `/queries/{id}` | Update/delete saved query | [Saved Queries](/docs/api/saved-queries) |
| `GET/POST` | `/alerts` | List/create alerts | [Alerts](/docs/api/alerts) |
| `PUT/DELETE` | `/alerts/{id}` | Update/delete alert | [Alerts](/docs/api/alerts) |
| `POST` | `/alerts/{id}/test` | Test alert (dry run) | [Alerts](/docs/api/alerts) |
| `POST` | `/alerts/{id}/test-channels` | Test notification channels | [Alerts](/docs/api/alerts) |
| `GET/POST` | `/dashboards` | List/create dashboards | [Dashboards](/docs/api/dashboards) |
| `GET/PUT/DELETE` | `/dashboards/{id}` | Get/update/delete dashboard | [Dashboards](/docs/api/dashboards) |
| `GET/POST` | `/views` | List/create materialized views | [Views](/docs/api/views) |
| `GET/PATCH/DELETE` | `/views/{name}` | Get/update/delete view | [Views](/docs/api/views) |
| `GET` | `/views/{name}/backfill` | Backfill progress | [Views](/docs/api/views) |
| `GET` | `/status` | Server status and metrics | [Server](/docs/api/server) |
| `GET` | `/health` | Health check | [Server](/docs/api/server) |
| `GET` | `/config` | Runtime configuration | [Server](/docs/api/server) |
| `PATCH` | `/config` | Update runtime configuration | [Server](/docs/api/server) |
| `POST` | `/es/_bulk` | Elasticsearch bulk API | [Compatibility](/docs/api/compatibility) |
| `POST` | `/es/{index}/_doc` | ES single-doc ingest | [Compatibility](/docs/api/compatibility) |
| `GET` | `/es/` | ES cluster info (handshake) | [Compatibility](/docs/api/compatibility) |
| `POST` | `/otlp/v1/logs` | OpenTelemetry OTLP logs | [Compatibility](/docs/api/compatibility) |

## Related

- **[CLI Reference](/docs/cli/overview)** -- the `lynxdb query` and `lynxdb ingest` commands use this API under the hood
- **[Configuration](/docs/configuration/overview)** -- server listen address, auth, rate limits
- **[Quick Start](/docs/getting-started/quickstart)** -- first API call walkthrough

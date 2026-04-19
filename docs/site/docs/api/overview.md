---
sidebar_position: 1
title: API Overview
description: LynxDB HTTP API entry points, authentication rules, response envelopes, and the core endpoint map.
---

# REST API Overview

Most HTTP endpoints live under `http://localhost:3100/api/v1`. The main exceptions are:

- `GET /health` for health checks
- `GET /metrics` for Prometheus scraping

The same API powers the Web UI, the server-mode CLI, and external integrations.

:::tip Base path
Unless a path in this page starts with `/health` or `/metrics`, it is relative to `/api/v1`.
:::

## Response Envelope

Successful JSON responses use a `data` envelope and optionally a `meta` object:

```json
{
  "data": {},
  "meta": {
    "took_ms": 12
  }
}
```

Errors use an `error` envelope:

```json
{
  "error": {
    "code": "INVALID_QUERY",
    "message": "Unknown command 'staats'.",
    "suggestion": "stats"
  }
}
```

## Authentication and Scopes

Authentication is off by default.

When enabled, the middleware protects all `/api/...` routes except for:

- `GET /health`
- `GET /metrics`
- non-API Web UI and static asset paths

Accepted `Authorization` schemes:

- `Bearer <token>`
- `ApiKey <base64(id:secret)>`
- `Basic <base64(user:token)>`

For browser SSE clients that cannot set headers, LynxDB also accepts `_token` as a query parameter and then redacts it from the request URL before downstream handling.

Some endpoints add scope requirements on top of authentication:

- query endpoints require query scope
- ingest endpoints require ingest scope
- `GET /api/v1/config` requires admin scope
- `PATCH /api/v1/config` requires a root key
- auth key management endpoints require a root key

See [TLS & Authentication](/docs/deployment/tls-auth) for setup.

## Common HTTP Status Codes

| Code | Meaning |
|---|---|
| `200` | Success |
| `201` | Resource created |
| `202` | Accepted (async job started) |
| `204` | Deleted (no content) |
| `400` | Bad request |
| `401` | Authentication required |
| `403` | Authenticated but insufficient scope |
| `404` | Resource not found |
| `413` | Payload too large |
| `422` | Validation error |
| `429` | Rate limited |
| `503` | Backpressure or shutting down |

## Common Headers

### Request Headers

| Header | Description | Default |
|---|---|---|
| `Content-Type` | Request body format | `application/json` |
| `Accept` | Response format (`application/json` or `application/x-ndjson`) | `application/json` |
| `Authorization` | `Bearer`, `ApiKey`, or `Basic` auth | -- |
| `X-Source` | Source label for raw ingest | -- |
| `X-Source-Type` | Source type label for raw ingest | -- |
| `X-Index` | Target index for raw ingest | `main` |

### Response Headers

| Header | Description |
|---|---|
| `Content-Type` | JSON, NDJSON, or SSE |
| `Retry-After` | Retry hint on rate limiting or backpressure |
| `X-Request-ID` | Request correlation ID |
| `X-Query-ID` | Query correlation ID |

## Query Execution Modes

`POST /api/v1/query` supports three execution modes through the `wait` field:

| `wait` value | Mode | Behavior |
|---|---|---|
| omitted / `null` | Sync window | Wait until completion. If the query misses `query.sync_timeout` (default `30s`), LynxDB detaches it and returns `202 Accepted` with a job handle |
| `0` | Async | Return a job handle immediately |
| positive number | Hybrid | Wait up to N seconds, then fall back to a job handle |

`POST /api/v1/query` always returns the JSON response envelope. For NDJSON export, use `POST /api/v1/query/stream`.

See [Query API](/docs/api/query) for the exact request and response formats.

## Endpoint Summary

| Method | Endpoint | Description | Docs |
|---|---|---|---|
| `GET` | `/health` | Health check outside `/api/v1` | [Server](/docs/api/server) |
| `GET` | `/metrics` | Prometheus metrics outside `/api/v1` | [Server](/docs/api/server) |
| `POST` | `/ingest` | Ingest JSON event arrays | [Ingest](/docs/api/ingest) |
| `POST` | `/ingest/raw` | Ingest newline-delimited raw text | [Ingest](/docs/api/ingest) |
| `POST` | `/ingest/hec` | Splunk HEC-compatible ingest | [Ingest](/docs/api/ingest) |
| `POST` | `/ingest/bulk` | Elasticsearch bulk ingest alias (prefer `/es/_bulk`) | [Ingest](/docs/api/ingest) |
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
| `GET` | `/fields` | Field catalog | [Fields](/docs/api/fields) |
| `GET` | `/fields/{name}/values` | Top values for a field | [Fields](/docs/api/fields) |
| `GET` | `/sources` | Source catalog | [Fields](/docs/api/fields) |
| `GET/POST` | `/queries` | List/create saved queries | [Saved Queries](/docs/api/saved-queries) |
| `PUT/DELETE` | `/queries/{id}` | Update/delete saved query | [Saved Queries](/docs/api/saved-queries) |
| `GET/POST` | `/alerts` | List/create alerts | [Alerts](/docs/api/alerts) |
| `PUT/PATCH/DELETE` | `/alerts/{id}` | Update/delete alert | [Alerts](/docs/api/alerts) |
| `POST` | `/alerts/{id}/test` | Test alert (dry run) | [Alerts](/docs/api/alerts) |
| `POST` | `/alerts/{id}/test-channels` | Test notification channels | [Alerts](/docs/api/alerts) |
| `GET/POST` | `/dashboards` | List/create dashboards | [Dashboards](/docs/api/dashboards) |
| `GET/PUT/DELETE` | `/dashboards/{id}` | Get/update/delete dashboard | [Dashboards](/docs/api/dashboards) |
| `GET/POST` | `/views` | List/create materialized views | [Views](/docs/api/views) |
| `GET/PATCH/DELETE` | `/views/{name}` | Get/update/delete view | [Views](/docs/api/views) |
| `GET/POST` | `/views/{name}/backfill` | Inspect or start backfill | [Views](/docs/api/views) |
| `GET` | `/status` | Operational status snapshot | [Server](/docs/api/server) |
| `GET` | `/stats` | Storage and ingest statistics | [Server](/docs/api/server) |
| `GET` | `/metrics` | JSON storage metrics | [Server](/docs/api/server) |
| `GET` | `/config` | Runtime configuration | [Server](/docs/api/server) |
| `PATCH` | `/config` | Update runtime configuration snapshot | [Server](/docs/api/server) |
| `GET` | `/cluster/status` | Cluster status summary | [Server](/docs/api/server) |
| `GET` | `/compaction/history` | Recent compaction manifests | [Server](/docs/api/server) |
| `GET` | `/cache/stats` | Query cache statistics | [Server](/docs/api/server) |
| `DELETE` | `/cache` | Clear query cache | [Server](/docs/api/server) |
| `GET/POST/DELETE` | `/auth/...` | Auth key management, only when auth is enabled | [Server](/docs/api/server) |
| `POST` | `/es/_bulk` | Preferred Elasticsearch bulk API | [Compatibility](/docs/api/compatibility) |
| `POST` | `/es/{index}/_doc` | ES single-doc ingest | [Compatibility](/docs/api/compatibility) |
| `GET` | `/es/` | ES cluster info handshake | [Compatibility](/docs/api/compatibility) |
| `POST` | `/otlp/v1/logs` | OpenTelemetry OTLP logs | [Compatibility](/docs/api/compatibility) |

## Related

- [Query API](/docs/api/query)
- [Server API](/docs/api/server)
- [CLI Overview](/docs/cli/overview)
- [Configuration Overview](/docs/configuration/overview)

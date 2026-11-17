---
sidebar_position: 1
title: API Overview
description: LynxDB HTTP API base paths, authentication and scopes, response and error envelopes, common headers, size limits and the full endpoint map.
---

# REST API Overview

The HTTP API listens on `http://localhost:3100` by default. Most endpoints live under `/api/v1`; the exceptions are:

- `GET /health` and `GET /metrics`
- the Elasticsearch compatibility routes at the root (`/`, `/_bulk`, `/{index}/_bulk`, ...)
- the Splunk HEC routes under `/services/collector`

The CLI in server mode and every integration use this API. Queries are written in [PPL](../ppl/overview.md).

## Response envelope

Successful JSON responses wrap the payload in `data`, with a `meta` object when there is something to report:

```json
{
  "data": {"type": "aggregate", "columns": ["n", "level"], "rows": [[5, "ERROR"], [15, "INFO"], [6, "WARN"]], "total_rows": 3, "has_more": false},
  "meta": {"took_ms": 0.232, "query_id": "qry_c691abc2c45c5cfa", "rows": 3}
}
```

| `meta` field | Description |
|---|---|
| `took_ms` | Time the request or the query took, in milliseconds |
| `query_id` | ID of the query job, also sent as the `X-Query-ID` header |
| `rows` | Number of rows the query produced |

Errors use an `error` envelope with a machine-readable `code`, a `message` and, when there is one, a `suggestion`. Query compile errors add `position` and `expected`:

```json
{
  "error": {
    "code": "SYNTAX_ERROR",
    "message": "syntax error at line 1, column 29: expected an expression",
    "position": {"line": 1, "column": 29},
    "expected": ["expression"]
  }
}
```

Compatibility endpoints answer in the format of the protocol they implement (Elasticsearch, Splunk HEC, OTLP); their pages say so.

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `SYNTAX_ERROR`, `SEMANTIC_ERROR`, `TYPE_ERROR` | 400 | The query does not compile. See [Query errors](./query.md#errors) |
| `PLANNING_ERROR` | 400 | The query parsed but no plan could be built for it |
| `UNSUPPORTED_COMMAND`, `UNSUPPORTED_FUNCTION`, `UNSUPPORTED_FEATURE` | 422 | The query uses PPL that LynxDB does not run |
| `EXECUTION_ERROR` | 500 | The query failed while running |
| `QUERY_TOO_LARGE` | 400 | The query text is longer than `query.max_query_length` |
| `TOO_MANY_REQUESTS` | 429 | `query.max_concurrent` queries are running, or the per-IP rate limit was hit; see `Retry-After` |
| `VALIDATION_ERROR` | 400 | A required parameter is missing or a value is invalid |
| `INVALID_JSON` | 400 | The body is not valid JSON |
| `INVALID_REQUEST` | 400, 413, 415 | The body could not be read, is too large, or has an unsupported media type |
| `AUTH_REQUIRED`, `INVALID_TOKEN` | 401 | No credentials, or credentials that do not match a key |
| `FORBIDDEN` | 403 | The key lacks the scope the endpoint needs |
| `NOT_FOUND` | 404 | No such job or key, or authentication is off on an `/auth` route |
| `LAST_ROOT_KEY` | 409 | Revoking the only root key; rotate it instead |
| `SHUTTING_DOWN` | 503 | The server is stopping; see `Retry-After` |
| `INTERNAL_ERROR` | 500 | An unexpected server failure |

## Authentication and scopes

Authentication is off by default (`lynxdb server --auth` turns it on). When it is on, every route under `/api/` needs credentials. `GET /health`, `GET /metrics` and the routes outside `/api/` are not checked by the API key middleware.

Accepted `Authorization` schemes:

- `Bearer <token>`
- `ApiKey <base64(id:token)>`, the format Filebeat and other Elasticsearch clients send
- `Basic <base64(user:token)>`; the user name is ignored

Clients that cannot set headers, such as a browser `EventSource` following a job stream, can pass the token as the `_token` query parameter. The server removes it from the URL before the request is logged.

Keys have one of four scopes: `ingest`, `query`, `admin`, `full`. Root keys and `full` keys pass every check.

| Endpoints | Required |
|---|---|
| `POST /ingest`, `/ingest/raw`, `/ingest/hec`, `/ingest/bulk`, `/es/_bulk`, `/es/_data_stream/{name}/_bulk`, `/es/{index}/_doc` | `ingest` |
| `POST` and `GET /query`, `POST /query/stream`, `GET /query/jobs`, `GET /query/jobs/{id}`, `GET /query/jobs/{id}/stream` | `query` |
| `DELETE /query/jobs/{id}`, `GET /config` | `admin` |
| `PATCH /config`, `/auth/...` | a root key |
| Everything else under `/api/` | any valid key |

The Elasticsearch bulk routes at the root (`/_bulk`, `/{index}/_bulk`, `/_data_stream/{name}/_bulk`) also check for an `ingest` key, but the middleware does not read credentials outside `/api/`, so with authentication on they answer `401`. Point shippers at `/api/v1/es` instead. The Splunk HEC routes under `/services/collector` check their own `Authorization: Splunk <token>` header.

See [TLS & Authentication](../deployment/tls-auth.md) for setup and [`lynxdb auth`](../cli/auth.md) for key management from the CLI.

## Headers

### Request

| Header | Description |
|---|---|
| `Content-Type` | Body format; JSON endpoints read `application/json` |
| `Content-Encoding` | `gzip`, `zstd` or `snappy` bodies are decoded on every route; any other encoding answers `400` |
| `Authorization` | `Bearer`, `ApiKey` or `Basic` credentials |
| `X-Request-ID` | Correlation ID; echoed back, generated when absent |
| `X-Index` | Target index of [`POST /ingest`](./ingest.md#post-ingest) and [`POST /ingest/raw`](./ingest.md#post-ingestraw) |
| `X-Source`, `X-Source-Type`, `X-Host` | Labels of the text lines of [`POST /ingest/raw`](./ingest.md#post-ingestraw) |

### Response

| Header | Description |
|---|---|
| `X-Request-ID` | Correlation ID of the request |
| `X-Query-ID` | ID of the query job, on query and job responses |
| `Retry-After` | Seconds to wait, on `429` and `503` |
| `ETag`, `Cache-Control` | On `GET /api/v1/catalog` |

## Request size limits

| Setting | Default | Applies to |
|---|---|---|
| `ingest.max_body_size` | 100 MB | Every request body as sent; for `POST /ingest` and `POST /ingest/raw` also after `Content-Encoding` decoding |
| `ingest.limits.max_compressed_body_bytes` | 32 MB | Body as sent, on every route other than `POST /ingest` and `POST /ingest/raw` |
| `ingest.limits.max_decompressed_body_bytes` | 256 MB | Body after `Content-Encoding` decoding, on the same routes |
| `ingest.max_line_bytes` | 1 MiB | One line of an Elasticsearch bulk or Splunk HEC body |
| `query.max_query_length` | 1 MiB | Query text |

A body over the compressed or decompressed limit, or with an encoding the server cannot decode, answers in the Elasticsearch error format:

```json
{"error": {"type": "invalid_request", "reason": "unsupported content encoding \"br\""}, "status": 400}
```

The `type` is `request_body_too_large` with status `413` for a body over a limit, and the `reason` names the setting that applies:

```json
{"error": {"type": "request_body_too_large", "reason": "compressed body exceeds limit 2048 set by ingest.limits.max_compressed_body_bytes"}, "status": 413}
```

## Endpoint map

Paths below are relative to `/api/v1` unless they start with `/health`, `/metrics`, `/_`, `/{index}` or `/services`.

| Method | Path | Description | Page |
|---|---|---|---|
| `POST` | `/query` | Run a PPL query: sync, async or hybrid | [Query](./query.md) |
| `GET` | `/query` | Run a PPL query from query-string parameters | [Query](./query.md#get-query) |
| `POST` | `/query/stream` | Run a query and stream NDJSON rows | [Query](./query.md#post-querystream) |
| `GET` | `/query/explain` | Compile a query and return its plans | [Query](./query.md#get-queryexplain) |
| `GET` | `/catalog` | PPL commands and functions with support levels | [Query](./query.md#get-catalog) |
| `GET` | `/query/jobs` | List jobs | [Query](./query.md#get-queryjobs) |
| `GET` | `/query/jobs/{id}` | Job status and result | [Query](./query.md#get-queryjobsid) |
| `DELETE` | `/query/jobs/{id}` | Cancel a job | [Query](./query.md#delete-queryjobsid) |
| `GET` | `/query/jobs/{id}/stream` | Job state and result over SSE | [Query](./query.md#get-queryjobsidstream) |
| `GET` | `/histogram` | Event counts per time bucket | [Histogram](./histogram.md) |
| `GET` | `/fields` | Field catalog | [Fields & Sources](./fields.md) |
| `GET` | `/fields/{name}/values` | Top values of a field | [Fields & Sources](./fields.md#get-fieldsnamevalues) |
| `GET` | `/sources` | Indexes with event counts | [Fields & Sources](./fields.md#get-sources) |
| `POST` | `/ingest` | Ingest NDJSON events | [Ingest](./ingest.md) |
| `POST` | `/ingest/raw` | Ingest log text | [Ingest](./ingest.md#post-ingestraw) |
| `POST` | `/ingest/hec` | Ingest Splunk HEC events | [Ingest](./ingest.md#post-ingesthec) |
| `POST` | `/ingest/bulk` | Ingest an Elasticsearch bulk body | [Ingest](./ingest.md#post-ingestbulk) |
| `GET` | `/indexes` | List indexes | [Ingest](./ingest.md#get-indexes) |
| `POST` | `/es/_bulk`, `/_bulk`, `/{index}/_bulk` | Elasticsearch bulk ingest | [Compatibility](./compatibility.md) |
| `POST` | `/es/{index}/_doc` | Elasticsearch single-document ingest | [Compatibility](./compatibility.md#post-apiv1esindex_doc) |
| `GET` | `/`, `/es`, `/es/` | Elasticsearch handshake | [Compatibility](./compatibility.md#handshake-and-setup-probes) |
| `POST` | `/services/collector`, `/services/collector/event`, `/services/collector/raw` | Splunk HEC ingest | [Compatibility](./compatibility.md#splunk-hec) |
| `POST` | `/services/collector/ack` | Splunk HEC acknowledgements | [Compatibility](./compatibility.md#splunk-hec) |
| `GET` | `/services/collector/health` | Splunk HEC health | [Compatibility](./compatibility.md#splunk-hec) |
| `POST` | `/otlp/v1/logs` | OTLP/HTTP JSON logs | [Compatibility](./compatibility.md#post-apiv1otlpv1logs) |
| `GET` | `/health` | Health check | [Server](./server.md) |
| `GET` | `/metrics` | Prometheus metrics | [Server](./server.md#get-metrics) |
| `GET` | `/status` | Server snapshot | [Server](./server.md#get-apiv1status) |
| `GET` | `/stats` | Indexes, storage and job counters | [Server](./server.md#get-apiv1stats) |
| `GET` | `/config` | Runtime configuration | [Server](./server.md#get-apiv1config) |
| `PATCH` | `/config` | Change runtime configuration | [Server](./server.md#patch-apiv1config) |
| `POST`, `GET` | `/auth/keys` | Create or list API keys | [Server](./server.md#api-keys) |
| `DELETE` | `/auth/keys/{id}` | Revoke an API key | [Server](./server.md#api-keys) |
| `POST` | `/auth/rotate-root` | Rotate the root key | [Server](./server.md#api-keys) |
| `GET` | `/debug/pprof/...` | Go runtime profiles | [Server](./server.md#profiling) |

The OpenAPI description of these endpoints is at [`/api/swagger.yaml`](pathname:///api/swagger.yaml).

## Related

- [Query API](./query.md)
- [Ingest API](./ingest.md)
- [CLI overview](../cli/overview.md)
- [Configuration overview](../configuration/overview.md)

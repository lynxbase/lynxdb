---
sidebar_position: 1
title: API Overview
description: LynxDB HTTP API base paths, authentication and scopes, response and error envelopes, common headers, and the full endpoint map.
---

# REST API Overview

The HTTP API listens on `http://localhost:3100` by default. Most endpoints live under `/api/v1`; the exceptions are:

- `GET /health` and `GET /metrics`
- the Elasticsearch compatibility routes at the root (`/`, `/_bulk`, `/{index}/_bulk`, ...)
- the Splunk HEC routes under `/services/collector`

The CLI in server mode and every integration use this API. Queries are written in [PPL](../ppl/overview.md).

## Response envelope

Successful JSON responses wrap the payload in `data`, with an optional `meta` object:

```json
{
  "data": {"columns": ["n"], "rows": [[1000]], "total_rows": 1, "has_more": false, "type": "aggregate"},
  "meta": {"took_ms": 1, "scanned": 1000, "query_id": "qry_78ad6ed9aaaef105"}
}
```

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

Compatibility endpoints answer in the format of the protocol they implement (Elasticsearch, Splunk HEC, OTLP), and a few endpoints answer with a bare `{"error": "..."}` object; their pages say so.

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `SYNTAX_ERROR`, `SEMANTIC_ERROR`, `TYPE_ERROR`, `PLANNING_ERROR` | 400 | The query does not compile. See [Query errors](./query.md#errors) |
| `UNSUPPORTED_COMMAND`, `UNSUPPORTED_FUNCTION`, `UNSUPPORTED_FEATURE` | 422 | The query uses PPL that LynxDB does not run |
| `EXECUTION_ERROR` | 500 | The query failed while running |
| `QUERY_TOO_LARGE`, `QUERY_MEMORY_EXCEEDED` | 400 | The query is too long, or used more memory than its budget |
| `QUERY_POOL_EXHAUSTED` | 503 | The server-wide query memory pool is full |
| `TOO_MANY_REQUESTS` | 429 | Concurrency or rate limit reached; see `Retry-After` |
| `VALIDATION_ERROR` | 400 | A required parameter is missing or a value is invalid |
| `INVALID_JSON`, `INVALID_REQUEST` | 400 | The body is not valid JSON, or could not be read |
| `AUTH_REQUIRED`, `INVALID_TOKEN` | 401 | No credentials, or credentials that do not match a key |
| `FORBIDDEN` | 403 | The key lacks the scope the endpoint needs |
| `NOT_FOUND` | 404 | No such job, key or resource |
| `LAST_ROOT_KEY` | 409 | Revoking the only root key; rotate it instead |
| `BACKPRESSURE`, `SHUTTING_DOWN` | 503 | Ingest is saturated or the server is stopping; see `Retry-After` |
| `INTERNAL_ERROR` | 500 | An unexpected server failure |

## Authentication and scopes

Authentication is off by default. When it is on, every `/api/...` route needs credentials. `GET /health`, `GET /metrics` and routes outside `/api/` are not checked by the API key middleware.

Accepted `Authorization` schemes:

- `Bearer <token>`
- `ApiKey <base64(id:secret)>`, the format Filebeat and other Elasticsearch clients send
- `Basic <base64(user:token)>`; the user name is ignored

Clients that cannot set headers, such as a browser `EventSource` for live tail, can pass the token as the `_token` query parameter. The server removes it from the URL before logging.

Keys have one of four scopes: `ingest`, `query`, `admin`, `full`. Root keys and `full` keys pass every check.

| Endpoints | Required |
|---|---|
| Ingest, Elasticsearch bulk and single-document ingest | `ingest` |
| `POST`/`GET /query`, `/query/stream`, `GET /query/jobs...` | `query` |
| `DELETE /query/jobs/{id}`, `DELETE /cache`, `GET /config` | `admin` |
| `PATCH /config`, `POST /indexes`, `/auth/...` | a root key |
| Everything else under `/api/v1` | any valid key |

See [TLS & Authentication](../deployment/tls-auth.md) for setup and [`lynxdb auth`](../cli/auth.md) for key management from the CLI.

## Headers

### Request

| Header | Description |
|---|---|
| `Content-Type` | Body format; JSON endpoints accept `application/json` |
| `Content-Encoding` | `gzip`, `zstd` or `snappy` bodies are decoded on every route |
| `Authorization` | `Bearer`, `ApiKey` or `Basic` credentials |
| `X-Request-ID` | Correlation ID; echoed back, generated when absent |
| `X-Source`, `X-Source-Type`, `X-Index` | Labels for [`POST /ingest/raw`](./ingest.md#post-ingestraw) |

### Response

| Header | Description |
|---|---|
| `X-Request-ID` | Correlation ID of the request |
| `X-Query-ID` | ID of the query job, on query responses |
| `Retry-After` | Seconds to wait, on `429` and `503` |
| `ETag`, `Cache-Control` | On `GET /api/v1/catalog` |

## Request size limits

| Setting | Default | Applies to |
|---|---|---|
| `ingest.max_body_size` | 100 MB | Every request body |
| `ingest.limits.max_compressed_body_bytes` | 32 MB | Body as sent |
| `ingest.limits.max_decompressed_body_bytes` | 256 MB | Body after `Content-Encoding` decoding |
| `ingest.max_line_bytes` | 1 MiB | One line of a line-oriented ingest body |
| `query.max_query_length` | 1 MiB | Query text |

Bodies over the compressed or decompressed limit get `413` with `{"error": {"type": "request_body_too_large", "reason": "..."}, "status": 413}`.

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
| `GET` | `/query/jobs/{id}/stream` | Job progress over SSE | [Query](./query.md#get-queryjobsidstream) |
| `GET` | `/tail` | Live tail over SSE | [Live Tail & Histogram](./tail-histogram.md) |
| `GET` | `/histogram` | Event counts per time bucket | [Live Tail & Histogram](./tail-histogram.md#get-histogram) |
| `GET` | `/fields` | Field catalog | [Fields & Sources](./fields.md) |
| `GET` | `/fields/{name}/values` | Top values of a field | [Fields & Sources](./fields.md#get-fieldsnamevalues) |
| `GET` | `/sources` | Sources with event counts | [Fields & Sources](./fields.md#get-sources) |
| `POST` | `/ingest` | Ingest a JSON array of events | [Ingest](./ingest.md) |
| `POST` | `/ingest/raw` | Ingest newline-delimited text | [Ingest](./ingest.md#post-ingestraw) |
| `POST` | `/ingest/hec` | Ingest Splunk HEC events | [Ingest](./ingest.md#post-ingesthec) |
| `POST` | `/ingest/bulk` | Ingest an Elasticsearch bulk body | [Ingest](./ingest.md#post-ingestbulk) |
| `GET` | `/indexes` | List indexes | [Ingest](./ingest.md#indexes) |
| `POST` | `/indexes` | Create an index | [Ingest](./ingest.md#indexes) |
| `POST` | `/es/_bulk`, `/_bulk`, `/{index}/_bulk` | Elasticsearch bulk ingest | [Compatibility](./compatibility.md) |
| `POST` | `/es/{index}/_doc` | Elasticsearch single-document ingest | [Compatibility](./compatibility.md#post-apiv1esindex_doc) |
| `GET` | `/`, `/es/` | Elasticsearch handshake | [Compatibility](./compatibility.md#handshake-and-setup-probes) |
| `POST` | `/services/collector`, `/services/collector/event`, `/services/collector/raw` | Splunk HEC ingest | [Compatibility](./compatibility.md#splunk-hec) |
| `POST` | `/services/collector/ack` | Splunk HEC acknowledgements | [Compatibility](./compatibility.md#splunk-hec) |
| `GET` | `/services/collector/health` | Splunk HEC health | [Compatibility](./compatibility.md#splunk-hec) |
| `POST` | `/otlp/v1/logs` | OTLP/HTTP JSON logs | [Compatibility](./compatibility.md#opentelemetry-otlp) |
| `GET` | `/health` | Health check | [Server](./server.md) |
| `GET` | `/metrics` | Prometheus metrics | [Server](./server.md#get-metrics) |
| `GET` | `/status` | Operational snapshot | [Server](./server.md#get-apiv1status) |
| `GET` | `/stats` | Storage and event counters | [Server](./server.md#get-apiv1stats) |
| `GET` | `/metrics` (under `/api/v1`) | Storage metrics as JSON | [Server](./server.md#get-apiv1metrics) |
| `GET` | `/compaction/history` | Recent compactions | [Server](./server.md#get-apiv1compactionhistory) |
| `GET` | `/cache/stats` | Query cache statistics | [Server](./server.md#cache) |
| `DELETE` | `/cache` | Clear the query cache | [Server](./server.md#cache) |
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

---
sidebar_position: 10
title: Server
description: Health checks, Prometheus metrics, status and stats snapshots, runtime configuration, API keys and profiling endpoints.
---

# Server API

## GET /health

The health check lives at `/health`, outside `/api/v1`. It never requires authentication and is not rate limited.

```bash
curl -s localhost:3100/health
```

```json
{"data": {"status": "healthy", "version": "dev"}}
```

A server that answers is healthy; `status` is always `healthy`. `version` is the version of the binary, `dev` for a build from source.

## GET /metrics

Prometheus text metrics, outside `/api/v1` and without authentication. Besides the Go runtime and process metrics, LynxDB exports:

| Metric | Labels | Description |
|---|---|---|
| `lynxdb_query_duration_seconds` | `result_type` | Histogram of query durations |
| `lynxdb_query_errors_total` | `type` | Failed queries by error code |
| `lynxdb_ingest_events_total` | | Events ingested |
| `lynxdb_ingest_events_expired_total` | | Events refused for being older than the retention |
| `lynxdb_ingest_batches_total` | | Ingest batches |
| `lynxdb_ingest_bytes_total` | | Bytes of NDJSON ingested |
| `lynxdb_ingest_errors_total` | | Failed ingest batches |
| `lynxdb_ingest_es_bulk_requests_total` | `result` | Elasticsearch bulk requests: `success`, `partial`, `error` |
| `lynxdb_ingest_es_bulk_items_total` | `action`, `result` | Elasticsearch bulk items: `ok`, `rejected` |
| `lynxdb_ingest_es_bulk_duration_seconds` | | Histogram of bulk request times |
| `lynxdb_ingest_es_handshake_total` | `kind` | Elasticsearch handshake and setup probes |
| `lynxdb_ingest_otlp_requests_total` | `signal`, `encoding`, `result` | OTLP requests |
| `lynxdb_ingest_otlp_records_total` | `signal`, `result` | OTLP log records accepted or dropped |
| `lynxdb_ingest_otlp_request_bytes` | | Histogram of decoded OTLP body sizes |
| `lynxdb_ingest_dropped_records_total` | `source`, `reason` | Records dropped at ingest |
| `lynxdb_ingest_decompression_rejected_total` | `stage`, `encoding` | Bodies rejected by the compressed or decompressed size limit |
| `lynxdb_ingest_listener_up` | `listener` | `1` while the `es`, `otlp_http` or `otlp_grpc` listener is bound |
| `lynxdb_syslog_messages_received_total`, `lynxdb_syslog_messages_dropped_total`, `lynxdb_syslog_parse_errors_total`, `lynxdb_syslog_active_connections` | | Syslog receiver, when it runs |

See [Monitoring](../operations/monitoring.md).

## GET /api/v1/status

A compact snapshot of the server:

```bash
curl -s localhost:3100/api/v1/status
```

```json
{
  "data": {
    "version": "dev",
    "uptime_seconds": 137,
    "health": "healthy",
    "storage": {"used_bytes": 53424},
    "events": {"total": 1060},
    "queries": {"active": 0},
    "retention": {"oldest_event": "2025-01-15T08:00:00Z"}
  }
}
```

| Field | Description |
|---|---|
| `storage.used_bytes` | Size of the files of every index |
| `events.total` | Events stored |
| `queries.active` | Jobs running |
| `retention.policy` | The `retention` setting, when events expire |
| `retention.oldest_event` | Time of the oldest event, when there is one |

## GET /api/v1/stats

The indexes, their totals and the jobs, as used by `lynxdb status`:

```bash
curl -s localhost:3100/api/v1/stats
```

```json
{
  "data": {
    "uptime_seconds": 137,
    "storage_bytes": 53424,
    "filter_bytes": 9216,
    "total_events": 1060,
    "index_count": 3,
    "part_count": 4,
    "merges_running": 0,
    "runs_to_merge": 0,
    "oldest_event": "2025-01-15T08:00:00Z",
    "newest_event": "2026-11-17T15:43:40Z",
    "indexes": [
      {"name": "backend", "events": 26, "bytes": 3978, "parts": 1, "merges_running": 0, "runs_to_merge": 0, "first_event": "2026-02-10T08:12:03.045Z", "last_event": "2026-02-10T10:45:00.5Z", "sort_key": [], "time_fields": ["_time", "timestamp", "@timestamp", "time", "ts"], "partition": "1d"},
      {"name": "main", "events": 1000, "bytes": 46638, "parts": 2, "merges_running": 0, "runs_to_merge": 0, "first_event": "2025-01-15T08:00:00.105Z", "last_event": "2026-11-17T15:43:40.839562Z", "sort_key": [], "time_fields": ["_time", "timestamp", "@timestamp", "time", "ts"], "partition": "1d"},
      {"name": "nginx", "events": 34, "bytes": 2808, "parts": 1, "merges_running": 0, "runs_to_merge": 0, "first_event": "2026-11-17T15:43:40.867817Z", "last_event": "2026-11-17T15:43:40.867817Z", "sort_key": [], "time_fields": ["_time", "timestamp", "@timestamp", "time", "ts"], "partition": "1d"}
    ],
    "jobs": {"running": 0, "done": 8, "error": 0, "canceled": 0}
  }
}
```

| Field | Description |
|---|---|
| `storage_bytes`, `total_events`, `part_count` | Totals over the indexes |
| `merges_running`, `runs_to_merge` | Merges under way and sorted runs still to be merged, over the indexes; the store is quiet when both are 0. [`POST /api/v1/indexes/{index}/compact`](./ingest.md#post-indexesindexcompact) merges an index at once |
| `filter_bytes` | The part of `storage_bytes` that the token filters take; each index entry has it too |
| `oldest_event`, `newest_event` | Times of the oldest and newest event, in whole seconds; absent without events |
| `indexes` | One entry per index, as in [`GET /api/v1/indexes`](./ingest.md#get-indexes) |
| `jobs` | Jobs the server keeps, by state |

Like a query, `status` and `stats` first write the events ingested so far, so the counts include them.

## GET /api/v1/config

Returns the running configuration. Needs `admin` scope when authentication is on.

```bash
curl -s localhost:3100/api/v1/config
```

The top-level keys of `data` are `listen`, `data_dir`, `retention`, `log_level`, `storage`, `query`, `ingest`, `http`, `syslog`, `tls`, `auth` and `profiles`. Durations in `query` (`sync_timeout`, `job_ttl`) and `http` are integer nanoseconds; the others are text such as `30d` or `5m0s`. See [Configuration](../configuration/overview.md) for each section.

## PATCH /api/v1/config

Merges a JSON object into the running configuration, validates the result and applies it the same way a `SIGHUP` reload does. Needs a root key when authentication is on.

Accepted top-level keys: `listen`, `data_dir`, `retention`, `log_level`, `storage`, `query`, `ingest`, `http`, `syslog`, `tls`, `auth`. Nested objects merge member by member. Any other key, nested ones included, an empty patch, a value of the wrong type, or a config that fails validation answers `400 VALIDATION_ERROR`.

```bash
curl -s -X PATCH localhost:3100/api/v1/config \
  -H 'Content-Type: application/json' \
  -d '{"log_level": "debug", "query": {"default_result_limit": 500}}'
```

The response holds the resulting configuration in `data.config`.

A patch may change only the settings that take effect at once: `log_level`, `query.sync_timeout`, `query.default_result_limit`, `query.max_result_limit`, `query.max_query_length`, `ingest.max_batch_size`, `ingest.max_line_bytes`, `ingest.es_compat.enabled`, `ingest.splunk_hec.enabled`, `http.shutdown_timeout`, and the syslog `index`, `sourcetype`, `default_timezone`, `default_hostname`, `batch_size` and `batch_timeout`. A patch that changes any other setting answers `400 VALIDATION_ERROR` naming it and changes nothing, so `GET /api/v1/config` reports only settings in effect:

```json
{"error": {"code": "VALIDATION_ERROR", "message": "these settings take effect only at startup: listen, retention", "suggestion": "Change them in the config file and restart the server"}}
```

The change lives only in the running process. Edit the config file as well to keep it after a restart.

## API keys

These endpoints exist only when authentication is on; otherwise they answer `404 NOT_FOUND`. They all need a root key; any other key answers `403 FORBIDDEN`.

### POST /api/v1/auth/keys

```bash
curl -s -X POST localhost:3100/api/v1/auth/keys \
  -H 'Authorization: Bearer <root-token>' \
  -H 'Content-Type: application/json' \
  -d '{"name": "filebeat", "scope": "ingest", "expires_in": "90d", "description": "edge shipper"}'
```

| Field | Description |
|---|---|
| `name` | Key name (required) |
| `scope` | `ingest`, `query`, `admin` or `full` (default `full`) |
| `expires_in` | A Go duration (`720h`) or a count of days, weeks or years (`30d`, `4w`, `1y`); `never` or empty for no expiry |
| `description` | Free text |

The response (`201`) holds the key and its plaintext token, which is shown only this once:

```json
{
  "data": {
    "id": "ak_EQcWtLGOrPfq",
    "name": "filebeat",
    "prefix": "lynx_ak_EQcWtLGOrPfq",
    "scope": "ingest",
    "description": "edge shipper",
    "expires_at": "2027-02-15T16:47:01.300723+01:00",
    "is_root": false,
    "created_at": "2026-11-17T15:47:01.326646Z",
    "last_used_at": "0001-01-01T00:00:00Z",
    "token": "lynx_ak_EQcWtLGOrPfq_R0CsTy4Qwb8DwkNhFdnXsiVpzz63FfkC",
    "api_key": "ak_EQcWtLGOrPfq:lynx_ak_EQcWtLGOrPfq_R0CsTy4Qwb8DwkNhFdnXsiVpzz63FfkC"
  }
}
```

`api_key` is `id:token`, the value to base64-encode for the `ApiKey` scheme. A key without expiry has `expires_at` `0001-01-01T00:00:00Z`, and a key never used has that `last_used_at`. A missing `name`, an unknown `scope` or an unparsable `expires_in` answers `400 VALIDATION_ERROR`.

### GET /api/v1/auth/keys

Lists keys as `{"data": {"keys": [...]}}` with the same fields minus `token` and `api_key`.

### DELETE /api/v1/auth/keys/\{id\}

Revokes a key and answers `204`. An unknown ID answers `404 NOT_FOUND`; revoking the last root key answers `409 LAST_ROOT_KEY`.

### POST /api/v1/auth/rotate-root

Replaces the root key used to call it:

```json
{
  "data": {
    "id": "rk_9EB9RHIWv61H",
    "name": "root",
    "prefix": "lynx_rk_9EB9RHIWv61H",
    "token": "lynx_rk_9EB9RHIWv61H_wR4y9ojfljoQoaF1LlqsajAIxNKu8iS2",
    "is_root": true,
    "created_at": "2026-11-17T15:47:21.381866Z",
    "revoked_key_id": "rk_kZCEAKYUaACk"
  }
}
```

The token of the replaced key stops working at once.

See [`lynxdb auth`](../cli/auth.md) for the CLI and [TLS & Authentication](../deployment/tls-auth.md) for setup.

## Profiling

The Go runtime profiles are served under `/api/v1/debug/pprof/`: the index, `profile`, `trace`, `heap`, `allocs`, `goroutine`, `mutex`, `block`, `threadcreate`, `cmdline` and `symbol`. They follow API authentication like other `/api/v1` routes; any valid key can read them.

```bash
go tool pprof http://localhost:3100/api/v1/debug/pprof/profile?seconds=30
```

`mutex` and `block` profiles hold samples only when the server runs with `--profile-runtime`.

## Related

- [API overview](./overview.md)
- [Configuration overview](../configuration/overview.md)
- [`lynxdb config`](../cli/config-cmd.md)
- [Monitoring](../operations/monitoring.md)

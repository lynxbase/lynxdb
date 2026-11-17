---
sidebar_position: 10
title: Server
description: Health checks, Prometheus and JSON metrics, status snapshots, cache, compaction history, cluster status, runtime configuration, API keys and profiling endpoints.
---

# Server API

## GET /health

The health check lives at `/health`, outside `/api/v1`. It never requires authentication and is not rate limited.

```bash
curl -s localhost:3100/health
```

```json
{"data": {"status": "healthy", "degraded": false, "version": "0.4.0"}}
```

`status` is `degraded` and `degraded` is `true` when a persistent store fell back to memory.

## GET /metrics

Prometheus text metrics, outside `/api/v1` and without authentication. Metric names start with `lynxdb_`: ingest counters and staging gauges, query duration, scan and memory histograms, segment skip counters, cache, compaction, spill and memory-governor metrics. See [Monitoring](../operations/monitoring.md).

## GET /api/v1/status

A compact snapshot of the server:

```json
{
  "data": {
    "version": "0.4.0",
    "uptime_seconds": 485,
    "health": "healthy",
    "storage": {"used_bytes": 246702},
    "events": {"total": 1007, "today": 116},
    "queries": {"active": 0},
    "tail": {"active_sessions": 0, "subscriber_count": 0, "total_dropped_events": 0},
    "retention": {"oldest_event": "2025-01-15T08:00:00Z"},
    "memory": {
      "governor": {
        "allocated_bytes": 1110668,
        "peak_bytes": 1508024,
        "limit_bytes": 10307921510,
        "by_class": {
          "page-cache": {"allocated_bytes": 1110668, "peak_bytes": 1110668, "peak_60s_bytes": 1110668, "peak_24h_bytes": 1110668, "limit_bytes": 0}
        }
      }
    }
  }
}
```

`retention` appears when the oldest event is known. `memory.governor` and `memory.buffer_manager` appear when those components are active.

## GET /api/v1/stats

Storage and event counters, as used by `lynxdb status`:

```json
{
  "data": {
    "uptime_seconds": 485,
    "storage_bytes": 246702,
    "total_events": 1007,
    "events_today": 116,
    "index_count": 2,
    "segment_count": 7,
    "buffered_events": 0,
    "sources": [],
    "oldest_event": "2025-01-15T08:00:00Z"
  }
}
```

## GET /api/v1/metrics

Storage metrics as JSON, separate from the Prometheus endpoint. The top-level keys are `uptime_seconds`, `ingest`, `flush`, `segment`, `compaction`, `compaction_levels`, `tiering`, `cache`, `query`, `pruning` and `system_write_amplification`, plus `adaptive_compaction` when adaptive compaction is on.

## GET /api/v1/cluster/status

```json
{
  "data": {
    "status": "healthy",
    "node_count": 1,
    "index_count": 2,
    "segment_count": 7,
    "buffered_size": 0,
    "buffered_events": 0,
    "data_dir": "/var/lib/lynxdb"
  }
}
```

## GET /api/v1/compaction/history

Recorded compactions.

| Parameter | Description |
|---|---|
| `since` | Only compactions at or after this RFC 3339 time. Any other format answers `400 INVALID_REQUEST` |

```json
{"data": {"compactions": [], "count": 0}}
```

## Cache

`GET /api/v1/cache/stats` returns the query result cache counters:

```json
{"data": {"hits": 2, "misses": 26, "hit_rate": 0.0714, "entries": 25, "size_bytes": 1110668, "evictions": 0}}
```

`DELETE /api/v1/cache` empties the cache and answers `{"data": {"status": "cleared"}}`. It needs `admin` scope when authentication is on.

Queries sent with a `from` or `to` request field skip the result cache.

## GET /api/v1/config

Returns the running configuration. Needs `admin` scope when authentication is on.

```bash
curl -s localhost:3100/api/v1/config
```

The top-level keys of `data` are `listen`, `data_dir`, `retention`, `log_level`, `storage`, `query`, `ingest`, `http`, `syslog`, `tail`, `tls`, `auth`, `server`, `buffer_manager`, `cluster` and `profiles`. See [Configuration](../configuration/overview.md) for each section.

## PATCH /api/v1/config

Merges a JSON patch into the running configuration, validates it and applies it the same way a `SIGHUP` reload does. Needs a root key when authentication is on.

Accepted top-level keys: `retention`, `log_level`, `listen`, `query`, `ingest`, `storage`, `http`, `syslog`. Any other key, an empty patch, or a config that fails validation answers `400 VALIDATION_ERROR`.

```bash
curl -s -X PATCH localhost:3100/api/v1/config \
  -H 'Content-Type: application/json' \
  -d '{"log_level": "debug", "query": {"max_query_runtime": "10m"}}'
```

The response holds the resulting configuration in `data.config` and, when some changed settings take effect only after a restart (such as `listen`), lists them in `data.restart_required`.

The change lives only in the running process. Edit the config file as well to keep it after a restart.

## API keys

These endpoints exist only when authentication is on; otherwise they answer `404 NOT_FOUND`. They all need a root key.

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

The response (`201`) holds `id`, `name`, `prefix`, `scope`, `description`, `expires_at`, `is_root`, `created_at`, the plaintext `token`, and `api_key` (`id:token`, for the `ApiKey` scheme). The token is shown only once.

### GET /api/v1/auth/keys

Lists keys as `{"data": {"keys": [...]}}` with the same fields minus `token` and `api_key`, plus `last_used_at`.

### DELETE /api/v1/auth/keys/\{id\}

Revokes a key and answers `204`. An unknown ID answers `404 NOT_FOUND`; revoking the last root key answers `409 LAST_ROOT_KEY`.

### POST /api/v1/auth/rotate-root

Replaces the root key used to call it. The response holds the replacement key's `id`, `name`, `prefix`, `token`, `is_root` and `created_at`, and `revoked_key_id`.

See [`lynxdb auth`](../cli/auth.md) for the CLI and [TLS & Authentication](../deployment/tls-auth.md) for setup.

## Profiling

The Go runtime profiles are served under `/api/v1/debug/pprof/`: `profile`, `trace`, `heap`, `allocs`, `goroutine`, `mutex`, `block`, `threadcreate`, `cmdline` and `symbol`. They follow API authentication like other `/api/v1` routes.

```bash
go tool pprof http://localhost:3100/api/v1/debug/pprof/profile?seconds=30
```

## Related

- [API overview](./overview.md)
- [Configuration overview](../configuration/overview.md)
- [`lynxdb config`](../cli/config-cmd.md)
- [Monitoring](../operations/monitoring.md)

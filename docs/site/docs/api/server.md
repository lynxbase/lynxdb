---
sidebar_position: 10
title: Server
description: Health checks, Prometheus metrics, status snapshots, cache endpoints, cluster status, compaction history, and runtime config endpoints.
---

# Server API

These endpoints expose the server's operational state. Most of them live under `/api/v1`. The main exceptions are the health check and Prometheus endpoint.

## GET /health

Health checks are served at `/health`, not `/api/v1/health`.

This endpoint is always reachable without authentication, even when API auth is enabled.

```bash
curl -s localhost:3100/health | jq .
```

**Response:**

```json
{
  "data": {
    "status": "healthy",
    "degraded": false,
    "version": "0.4.0"
  }
}
```

`status` is `healthy` unless the server has fallen back to in-memory state for a persistent subsystem, in which case it becomes `degraded`.

## GET /metrics

Prometheus metrics are served at `/metrics`.

This endpoint is also unauthenticated when auth is enabled.

## GET /api/v1/status

Returns a compact operational snapshot assembled from engine stats, cluster status, materialized views, tail state, and optional memory-governance information.

```bash
curl -s localhost:3100/api/v1/status | jq .
```

**Response shape:**

```json
{
  "data": {
    "version": "0.4.0",
    "uptime_seconds": 1234567,
    "health": "healthy",
    "storage": {
      "used_bytes": 13300000000
    },
    "events": {
      "total": 847000000,
      "today": 1200000
    },
    "queries": {
      "active": 12
    },
    "views": {
      "total": 3,
      "active": 2
    },
    "tail": {
      "active_sessions": 1,
      "subscriber_count": 1,
      "total_dropped_events": 0
    }
  }
}
```

Additional sections such as `retention` and `memory` are present only when the server has that information available.

## GET /api/v1/stats

Returns storage-oriented counters used by the CLI `status` and `doctor` commands.

```bash
curl -s localhost:3100/api/v1/stats | jq .
```

Important fields:

- `uptime_seconds`
- `storage_bytes`
- `total_events`
- `events_today`
- `index_count`
- `segment_count`
- `buffered_events`
- `sources`
- `oldest_event` when available

## GET /api/v1/metrics

Returns JSON storage metrics, distinct from the Prometheus text endpoint at `/metrics`.

```bash
curl -s localhost:3100/api/v1/metrics | jq .
```

If adaptive compaction is enabled, the response may also include an `adaptive_compaction` section.

## Cache Endpoints

### GET /api/v1/cache/stats

Returns:

- `hits`
- `misses`
- `hit_rate`
- `entries`
- `size_bytes`
- `evictions`

### DELETE /api/v1/cache

Clears the query cache.

This endpoint requires admin scope when auth is enabled.

## GET /api/v1/compaction/history

Returns recorded compaction manifests.

```bash
curl -s 'localhost:3100/api/v1/compaction/history?since=2026-03-19T00:00:00Z' | jq .
```

Query parameters:

| Parameter | Type | Description |
|---|---|---|
| `since` | RFC3339 timestamp | Return compactions at or after the given time |

## GET /api/v1/cluster/status

Returns a small cluster status summary:

- `status`
- `node_count`
- `index_count`
- `segment_count`
- `buffered_size`
- `buffered_events`
- `data_dir`

## GET /api/v1/config

Returns the server's runtime config snapshot.

This endpoint requires admin scope when auth is enabled.

```bash
curl -s localhost:3100/api/v1/config | jq .
```

**Response:**

```json
{
  "data": {
    "listen": "localhost:3100",
    "data_dir": "/var/lib/lynxdb",
    "retention": "7d",
    "log_level": "info",
    "storage": {},
    "query": {},
    "ingest": {},
    "http": {},
    "tail": {},
    "tls": {},
    "auth": {},
    "views": {},
    "server": {},
    "buffer_manager": {},
    "cluster": {},
    "profiles": {}
  }
}
```

## PATCH /api/v1/config

Applies a JSON merge-style patch to the in-memory runtime config snapshot and returns the updated config.

This endpoint requires a root key when auth is enabled.

It accepts top-level keys only:

- `retention`
- `log_level`
- `listen`
- `query`
- `ingest`
- `storage`
- `http`

Unknown top-level keys are rejected.

```bash
curl -X PATCH localhost:3100/api/v1/config \
  -d '{
    "retention": "30d",
    "query": {
      "max_query_runtime": "10m"
    }
  }'
```

**Response:**

```json
{
  "data": {
    "config": {
      "retention": "30d"
    },
    "restart_required": []
  }
}
```

### Important caveat

`PATCH /api/v1/config` does not persist changes to the YAML config file.

Unlike editing the YAML config file, this endpoint applies changes only to the running process. It validates the updated config and uses the same in-memory reload path as `SIGHUP`, but the changes are lost on restart unless you also update the config file on disk.

## Auth Key Management Endpoints

These endpoints are available only when auth is enabled. They all require a root key. When auth is disabled, they return `404`.

### POST /api/v1/auth/keys

Create a new API key.

```bash
curl -X POST localhost:3100/api/v1/auth/keys \
  -H 'Authorization: Bearer <root-token>' \
  -d '{
    "name": "filebeat",
    "scope": "ingest",
    "expires_in": "90d",
    "description": "edge shipper"
  }'
```

Request fields:

- `name` required
- `scope` optional, one of `ingest`, `query`, `admin`, `full`
- `expires_in` optional, such as `30d`, `1y`, or `never`
- `description` optional

### GET /api/v1/auth/keys

List keys without returning their secret token values.

```bash
curl -s localhost:3100/api/v1/auth/keys \
  -H 'Authorization: Bearer <root-token>' | jq .
```

### `DELETE /api/v1/auth/keys/{id}`

Revoke a key by ID.

```bash
curl -X DELETE localhost:3100/api/v1/auth/keys/key_002 \
  -H 'Authorization: Bearer <root-token>'
```

### POST /api/v1/auth/rotate-root

Rotate the calling root key. The response returns the new root token once and identifies the revoked key ID.

```bash
curl -X POST localhost:3100/api/v1/auth/rotate-root \
  -H 'Authorization: Bearer <root-token>'
```

All authenticated requests use `Authorization: Bearer <token>`. For compatibility clients, LynxDB also accepts `ApiKey <base64(id:secret)>` and `Basic <base64(user:token)>`.

## Monitoring with the CLI

The CLI wraps these endpoints:

```bash
lynxdb health
lynxdb status
lynxdb cache stats
lynxdb indexes
lynxdb doctor
```

## Related

- [API Overview](/docs/api/overview)
- [Configuration Overview](/docs/configuration/overview)
- [config & doctor](/docs/cli/config-cmd)

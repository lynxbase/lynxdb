---
sidebar_position: 10
title: Server
description: GET /status, /health, /config -- server metrics, health checks, and runtime configuration management.
---

# Server API

Server status, health checks, and runtime configuration management. These endpoints are used for monitoring, load balancer integration, and operational management.

## GET /status

Full server status including version, uptime, storage metrics, event statistics, query workload, materialized view state, and retention info.

```bash
curl -s localhost:3100/api/v1/status | jq .
```

**Response (200):**

```json
{
  "data": {
    "version": "0.4.0",
    "uptime_seconds": 1234567,
    "storage": {
      "used_bytes": 13300000000,
      "total_bytes": 53700000000,
      "usage_percent": 24.8
    },
    "events": {
      "total": 847000000,
      "today": 1200000,
      "ingest_rate": 2340.5
    },
    "queries": {
      "active": 12,
      "avg_duration_ms": 45,
      "jobs": {
        "running": 2,
        "queued": 0,
        "max_concurrent": 10
      }
    },
    "views": {
      "total": 3,
      "active": 2,
      "backfilling": 1,
      "storage_bytes": 2267789702
    },
    "retention": {
      "policy": "7d",
      "oldest_event": "2026-02-07T00:00:01Z"
    },
    "health": "healthy"
  }
}
```

### Response Fields

#### Top Level

| Field | Type | Description |
|---|---|---|
| `version` | string | LynxDB version |
| `uptime_seconds` | integer | Seconds since server start |
| `health` | string | Overall health: `healthy`, `degraded`, `unhealthy` |

#### Storage

| Field | Type | Description |
|---|---|---|
| `storage.used_bytes` | integer | Disk space used |
| `storage.total_bytes` | integer | Total available disk space |
| `storage.usage_percent` | number | Disk usage percentage |

#### Events

| Field | Type | Description |
|---|---|---|
| `events.total` | integer | Total events stored |
| `events.today` | integer | Events ingested today |
| `events.ingest_rate` | number | Current ingest rate (events per second) |

#### Queries

| Field | Type | Description |
|---|---|---|
| `queries.active` | integer | Currently executing queries |
| `queries.avg_duration_ms` | number | Average query duration |
| `queries.jobs.running` | integer | Async jobs currently running |
| `queries.jobs.queued` | integer | Jobs waiting for a slot |
| `queries.jobs.max_concurrent` | integer | Max concurrent jobs allowed |

#### Materialized Views

| Field | Type | Description |
|---|---|---|
| `views.total` | integer | Total materialized views |
| `views.active` | integer | Views actively serving queries |
| `views.backfilling` | integer | Views currently backfilling |
| `views.storage_bytes` | integer | Total storage used by all views |

#### Retention

| Field | Type | Description |
|---|---|---|
| `retention.policy` | string | Configured retention period |
| `retention.oldest_event` | string | Timestamp of oldest event in storage |

---

## GET /health

Minimal health check for load balancers. Returns `200` when healthy, `503` when not. No envelope -- trivially parseable.

```bash
curl -s localhost:3100/api/v1/health
```

**Response -- healthy (200):**

```json
{"status": "ok"}
```

**Response -- unhealthy (503):**

```json
{"status": "unhealthy"}
```

### Load Balancer Configuration

```nginx
# nginx
location /health {
    proxy_pass http://lynxdb:3100/api/v1/health;
}
```

```yaml
# Kubernetes liveness probe
livenessProbe:
  httpGet:
    path: /api/v1/health
    port: 3100
  initialDelaySeconds: 5
  periodSeconds: 10
```

```yaml
# AWS ALB target group health check
health_check:
  path: /api/v1/health
  interval: 10
  healthy_threshold: 2
  unhealthy_threshold: 3
```

:::tip
The health endpoint is also available at `/health` (without the `/api/v1` prefix) for convenience.
:::

---

## GET /config

Get the current runtime configuration.

```bash
curl -s localhost:3100/api/v1/config | jq .
```

**Response (200):**

```json
{
  "data": {
    "listen": "localhost:3100",
    "data_dir": "~/.lynxdb/data",
    "retention": "7d",
    "auth_enabled": false,
    "otlp_enabled": false,
    "syslog_enabled": false,
    "max_query_memory_mb": 512
  }
}
```

### Configuration Fields

| Field | Type | Description |
|---|---|---|
| `listen` | string | Server listen address |
| `data_dir` | string | Data storage directory |
| `retention` | string | Event retention period |
| `auth_enabled` | boolean | Whether authentication is enabled |
| `otlp_enabled` | boolean | Whether OTLP receiver is enabled |
| `syslog_enabled` | boolean | Whether syslog receiver is enabled |
| `max_query_memory_mb` | integer | Maximum memory per query |

---

## PATCH /config

Update runtime configuration. Only runtime-adjustable fields can be changed without a restart.

```bash
curl -X PATCH localhost:3100/api/v1/config \
  -d '{
    "retention": "30d",
    "max_query_memory_mb": 1024
  }'
```

**Response (200):**

```json
{
  "data": {
    "listen": "localhost:3100",
    "data_dir": "~/.lynxdb/data",
    "retention": "30d",
    "auth_enabled": false,
    "otlp_enabled": false,
    "syslog_enabled": false,
    "max_query_memory_mb": 1024
  },
  "meta": {
    "restart_required": []
  }
}
```

### Hot-Reloadable Settings

These settings take effect immediately without a server restart:

- `retention` -- event retention period
- `max_query_memory_mb` -- maximum memory per query
- `log_level` -- server log verbosity

### Settings Requiring Restart

If you change a setting that requires a restart, the response `meta.restart_required` lists them:

```json
{
  "meta": {
    "restart_required": ["listen", "auth_enabled"]
  }
}
```

---

## Monitoring with the CLI

The same information is available from the command line:

```bash
# Server status
lynxdb status

# Health check
lynxdb health

# View current config
lynxdb config

# Hot-reload config file
lynxdb config reload
```

See the [CLI Reference](/docs/cli/server) for details.

## Related

- **[`lynxdb server` CLI command](/docs/cli/server)** -- starting and managing the server
- **[`lynxdb config` CLI command](/docs/cli/config-cmd)** -- configuration management
- **[Configuration Reference](/docs/configuration/overview)** -- all configuration options
- **[Monitoring guide](/docs/operations/monitoring)** -- setting up alerts on server metrics
- **[Deployment](/docs/deployment/single-node)** -- production deployment guides

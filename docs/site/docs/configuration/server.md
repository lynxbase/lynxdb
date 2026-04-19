---
title: Server Settings
description: Configure the LynxDB server process, including listen address, data directory, UI, TLS, auth, HTTP limits, and query spill settings.
---

# Server Settings

This page covers the top-level settings that control the `lynxdb server` process and the flags that override them.

## Listen Address

The address and port the HTTP server binds to.

| Config Key | `listen` |
|---|---|
| **CLI Flag** | `--addr` |
| **Env Var** | `LYNXDB_LISTEN` |
| **Default** | `localhost:3100` |

```yaml
listen: "0.0.0.0:3100"
```

```bash
lynxdb server --addr 0.0.0.0:8080
LYNXDB_LISTEN=0.0.0.0:3100 lynxdb server
```

`listen` requires a restart.

## Data Directory

The root directory for persistent LynxDB state.

| Config Key | `data_dir` |
|---|---|
| **CLI Flag** | `--data-dir` |
| **Env Var** | `LYNXDB_DATA_DIR` |
| **Default** | `$XDG_DATA_HOME/lynxdb` or `~/.local/share/lynxdb` |

```yaml
data_dir: "/var/lib/lynxdb"
```

```bash
lynxdb server --data-dir /data/lynxdb
lynxdb server --data-dir ""
```

When `data_dir` is an empty string, LynxDB runs in-memory only. Data is lost on shutdown.

## Retention

How long data is kept before automatic deletion.

| Config Key | `retention` |
|---|---|
| **CLI Flag** | none |
| **Env Var** | `LYNXDB_RETENTION` |
| **Default** | `7d` |

```yaml
retention: "30d"
```

```bash
LYNXDB_RETENTION=90d lynxdb server
```

Accepted duration formats include `6h`, `7d`, and `4w`.

## Log Level

Controls server log verbosity.

| Config Key | `log_level` |
|---|---|
| **CLI Flag** | `--log-level` |
| **Env Var** | `LYNXDB_LOG_LEVEL` |
| **Default** | `info` |

Valid values: `debug`, `info`, `warn`, `error`.

## Embedded Web UI

LynxDB serves the embedded Web UI by default when the build includes it.

| Config Key | `no_ui` |
|---|---|
| **CLI Flag** | `--no-ui` |
| **Default** | `false` |

```bash
# Disable the embedded UI
lynxdb server --no-ui

# Start the server and open the UI in a browser
lynxdb server --ui
```

## TLS

Enable HTTPS for the server. If TLS is enabled without an explicit certificate and key, LynxDB generates a self-signed certificate at startup.

| Config Key | `tls.enabled`, `tls.cert_file`, `tls.key_file` |
|---|---|
| **CLI Flags** | `--tls`, `--tls-cert`, `--tls-key` |
| **Env Vars** | `LYNXDB_TLS_ENABLED`, `LYNXDB_TLS_CERT_FILE`, `LYNXDB_TLS_KEY_FILE` |

```bash
# Auto-generate a self-signed certificate
lynxdb server --tls

# Use existing certificate files
lynxdb server --tls-cert /etc/ssl/lynxdb.crt --tls-key /etc/ssl/lynxdb.key
```

When connecting to a server with a self-signed certificate, the CLI performs trust-on-first-use during `lynxdb login`.

## Authentication

Enable API key authentication for API routes.

| Config Key | `auth.enabled` |
|---|---|
| **CLI Flag** | `--auth` |
| **Env Var** | `LYNXDB_AUTH_ENABLED` |

```bash
lynxdb server --auth
```

On first startup with auth enabled, LynxDB creates a root key if none exist and prints it once.

Manage keys with the CLI:

```bash
lynxdb auth create --name ci-pipeline --scope ingest
lynxdb auth list
lynxdb auth revoke <id>
lynxdb auth rotate-root
lynxdb auth status
```

Supported scopes are `ingest`, `query`, `admin`, and `full`.

## HTTP Settings

```yaml
http:
  idle_timeout: "120s"
  shutdown_timeout: "30s"
  alert_shutdown_timeout: "10s"
  read_header_timeout: "10s"
  rate_limit: 1000
```

| Config Key | Env Var | Default | Description |
|---|---|---|---|
| `http.idle_timeout` | `LYNXDB_HTTP_IDLE_TIMEOUT` | `120s` | Idle keep-alive timeout |
| `http.shutdown_timeout` | `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown deadline |
| `http.alert_shutdown_timeout` | `LYNXDB_HTTP_ALERT_SHUTDOWN_TIMEOUT` | `10s` | Alert-manager shutdown deadline |
| `http.read_header_timeout` | `LYNXDB_HTTP_READ_HEADER_TIMEOUT` | `10s` | Header read deadline |
| `http.rate_limit` | `LYNXDB_HTTP_RATE_LIMIT` | `1000` | Per-IP request rate limit in requests per second (`0` disables it) |

## Query Memory and Spill Settings

These settings live under `query.*`, but they are often tuned as server-level operational controls.

| Setting | CLI Flag | Default | Description |
|---|---|---|---|
| `query.global_query_pool_bytes` | `--max-query-pool` | `0` | Combined query memory pool (`0` = auto, currently 25% of system RAM) |
| `query.spill_dir` | `--spill-dir` | empty | Directory for spill files (`empty` = `os.TempDir()`) |
| `query.max_query_memory_bytes` | none | `1gb` | Per-query memory budget |
| `query.max_temp_dir_size_bytes` | none | `10gb` | Spill disk quota across concurrent queries |

```bash
lynxdb server --max-query-pool 4gb --spill-dir /var/lib/lynxdb/spill
```

## Complete Example

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"
no_ui: false

tls:
  enabled: true
  cert_file: "/etc/lynxdb/tls/server.crt"
  key_file: "/etc/lynxdb/tls/server.key"

auth:
  enabled: true

http:
  idle_timeout: "120s"
  shutdown_timeout: "30s"

query:
  max_query_memory_bytes: "1gb"
  global_query_pool_bytes: "4gb"
  spill_dir: "/var/lib/lynxdb/spill"
```

```bash
lynxdb server \
  --addr 0.0.0.0:3100 \
  --data-dir /var/lib/lynxdb \
  --auth \
  --tls-cert /etc/lynxdb/tls/server.crt \
  --tls-key /etc/lynxdb/tls/server.key \
  --max-query-pool 4gb \
  --spill-dir /var/lib/lynxdb/spill
```

## Related

- [Configuration Overview](/docs/configuration/overview)
- [Query Settings](/docs/configuration/query)
- [Deployment: TLS & Auth](/docs/deployment/tls-auth)
- [Server API](/docs/api/server)

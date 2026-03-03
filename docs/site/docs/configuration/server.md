---
title: Server Settings
description: Configure LynxDB server listen address, data directory, retention, log level, TLS, and authentication.
---

# Server Settings

Top-level server settings control the listen address, persistent storage location, data retention, logging, TLS, and authentication.

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
# Bind to all interfaces on port 8080
lynxdb server --addr 0.0.0.0:8080

# Via environment variable
LYNXDB_LISTEN=0.0.0.0:3100 lynxdb server
```

:::note
Changing `listen` requires a server restart. It is not hot-reloadable.
:::

## Data Directory

The root directory for all persistent data (WAL, segments, indexes, metadata).

| Config Key | `data_dir` |
|---|---|
| **CLI Flag** | `--data-dir` |
| **Env Var** | `LYNXDB_DATA_DIR` |
| **Default** | `~/.local/share/lynxdb` |

```yaml
data_dir: "/var/lib/lynxdb"
```

```bash
# Custom data directory
lynxdb server --data-dir /data/lynxdb

# In-memory mode (no persistence)
lynxdb server --data-dir ""
```

When `data_dir` is set to an empty string, LynxDB runs entirely in memory. All data is lost on shutdown. This is useful for development and testing.

The default data directory follows the XDG Base Directory specification:
- `$XDG_DATA_HOME/lynxdb` if `XDG_DATA_HOME` is set
- `~/.local/share/lynxdb` otherwise
- `.lynxdb/data` as a last resort

:::note
Changing `data_dir` requires a server restart. It is not hot-reloadable.
:::

## Retention

How long data is kept before automatic deletion.

| Config Key | `retention` |
|---|---|
| **CLI Flag** | (config file / env only) |
| **Env Var** | `LYNXDB_RETENTION` |
| **Default** | `7d` |
| **Hot-Reloadable** | Yes |

```yaml
retention: "30d"
```

```bash
LYNXDB_RETENTION=90d lynxdb server
```

Accepted duration formats: `7d` (days), `4w` (weeks), `6h` (hours). Segments older than the retention period are deleted during compaction.

See [Retention Policies](/docs/operations/retention) for more details on lifecycle management.

## Log Level

Controls the verbosity of server logging.

| Config Key | `log_level` |
|---|---|
| **CLI Flag** | `--log-level` |
| **Env Var** | `LYNXDB_LOG_LEVEL` |
| **Default** | `info` |
| **Hot-Reloadable** | Yes |

```yaml
log_level: "info"
```

Valid values: `debug`, `info`, `warn`, `error`.

```bash
# Start with debug logging
lynxdb server --log-level debug

# Change at runtime (no restart)
lynxdb config set log_level debug
lynxdb config reload
```

## TLS

Enable HTTPS for the server. When `--tls` is passed without certificate paths, LynxDB auto-generates a self-signed certificate.

| Config Key | (CLI flags only) |
|---|---|
| **CLI Flags** | `--tls`, `--tls-cert`, `--tls-key` |

```bash
# Auto-generate self-signed certificate
lynxdb server --tls

# Use your own certificates
lynxdb server --tls-cert /etc/ssl/lynxdb.crt --tls-key /etc/ssl/lynxdb.key
```

When connecting to a server with a self-signed certificate, the CLI implements Trust-On-First-Use (TOFU):

```bash
# First connection shows certificate fingerprint and asks for confirmation
lynxdb login --server https://localhost:3100
```

See [TLS and Authentication Setup](/docs/deployment/tls-auth) for a complete guide.

## Authentication

Enable API key authentication for all endpoints.

| Config Key | (CLI flag only) |
|---|---|
| **CLI Flag** | `--auth` |

```bash
lynxdb server --auth
```

When `--auth` is enabled and no API keys exist, LynxDB generates a root key and displays it once at startup:

```
Auth enabled -- no API keys exist. Generated root key:

  lxk_a1b2c3d4e5f6...

Save this key now. It will NOT be shown again.
```

Use this key to authenticate:

```bash
# Interactive login (prompts for key)
lynxdb login

# Non-interactive
lynxdb login --token lxk_a1b2c3d4e5f6...

# Or via environment variable
export LYNXDB_TOKEN=lxk_a1b2c3d4e5f6...
```

Manage API keys:

```bash
# Create a new key
lynxdb auth create-key --name ci-pipeline

# List all keys
lynxdb auth list-keys

# Revoke a key
lynxdb auth revoke-key <id>

# Rotate the root key
lynxdb auth rotate-root
```

See [TLS and Authentication Setup](/docs/deployment/tls-auth) for production authentication setup.

## HTTP Settings

Fine-tune the HTTP server behavior.

```yaml
http:
  idle_timeout: "2m"          # Keep-alive idle timeout
  shutdown_timeout: "30s"     # Graceful shutdown deadline
```

| Config Key | Env Var | Default | Description |
|---|---|---|---|
| `http.idle_timeout` | `LYNXDB_HTTP_IDLE_TIMEOUT` | `2m` | How long to keep idle connections open |
| `http.shutdown_timeout` | `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `30s` | Max time to wait for in-flight requests during shutdown |

## Memory Pool

Control the global memory pool used for query execution.

| CLI Flag | Default | Description |
|---|---|---|
| `--max-query-pool` | (unlimited) | Global query memory pool size (e.g., `2gb`, `4gb`) |
| `--spill-dir` | OS temp dir | Directory for temporary spill files when memory is exceeded |

```bash
lynxdb server --max-query-pool 4gb --spill-dir /data/lynxdb/tmp
```

## Complete Example

```yaml
# /etc/lynxdb/config.yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

http:
  idle_timeout: "2m"
  shutdown_timeout: "30s"
```

```bash
lynxdb server \
  --addr 0.0.0.0:3100 \
  --data-dir /var/lib/lynxdb \
  --log-level info \
  --tls \
  --auth
```

## Next Steps

- [Storage Settings](/docs/configuration/storage) -- compression, WAL, compaction
- [Query Settings](/docs/configuration/query) -- concurrency limits, timeouts
- [TLS and Authentication Setup](/docs/deployment/tls-auth) -- production security
- [Single Node Deployment](/docs/deployment/single-node) -- systemd service setup

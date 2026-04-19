---
sidebar_position: 5
title: server
description: Start the LynxDB server -- flags, startup behavior, TLS, authentication, signals, and systemd integration.
---

# server

Start the LynxDB server.

```
lynxdb server [flags]
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | (from config) | Listen address (overrides config) |
| `--data-dir` | (from config) | Root directory for data storage |
| `--s3-bucket` | | S3 bucket for warm/cold storage |
| `--s3-region` | | AWS region |
| `--s3-prefix` | | Key prefix in S3 |
| `--compaction-interval` | | Compaction check interval |
| `--tiering-interval` | | Tier evaluation interval |
| `--cache-max-mb` | | Max cache size (e.g., `1gb`, `512mb`) |
| `--log-level` | | Log level: debug, info, warn, error |
| `--auth` | `false` | Enable API key authentication |
| `--tls` | `false` | Enable TLS (auto-generates self-signed cert if no `--tls-cert`) |
| `--tls-cert` | | Path to TLS certificate PEM file |
| `--tls-key` | | Path to TLS private key PEM file |
| `--max-query-pool` | | Global query memory pool (e.g., `2gb`, `4gb`) |
| `--spill-dir` | | Directory for temporary spill files (default: OS temp dir) |
| `--no-ui` | `false` | Disable the embedded Web UI |
| `--ui` | `false` | Open the embedded Web UI in a browser after startup |
| `--profile-runtime` | `false` | Enable Go mutex and block profiling |
| `--cluster.enabled` | `false` | Enable cluster mode |
| `--cluster.node-id` | | Unique node identifier in cluster mode |
| `--cluster.roles` | | Comma-separated node roles: `meta`, `ingest`, `query` |
| `--cluster.seeds` | | Comma-separated seed node addresses (`host:port`) |
| `--cluster.grpc-port` | | gRPC port for inter-node communication |

## Examples

```bash
# Start with defaults (localhost:3100)
lynxdb server

# Start with persistent storage and custom address
lynxdb server --addr 0.0.0.0:8080 --data-dir /var/lib/lynxdb

# In-memory mode (no persistence)
lynxdb server --data-dir ""

# With S3 tiering
lynxdb server --s3-bucket my-logs --s3-region eu-west-1

# With TLS and auth
lynxdb server --tls --auth --data-dir /var/lib/lynxdb

# With your own certificates
lynxdb server --tls-cert /etc/ssl/lynxdb.crt --tls-key /etc/ssl/lynxdb.key
```

## Startup Output

```
  Config:  /home/user/.config/lynxdb/config.yaml
  Overrides: --addr, --log-level
  Data:    /var/lib/lynxdb
  Listen:  0.0.0.0:8080

time=2026-01-15T10:00:00.000Z level=INFO msg="starting LynxDB" version=0.1.0 addr=0.0.0.0:8080
```

## Authentication

When `--auth` is enabled and no keys exist, a root key is generated and displayed once:

```
  Auth enabled — no API keys exist. Generated root key:

    lxk_a1b2c3d4e5f6...

  Save this key now. It will NOT be shown again.
```

Use the root key to create additional keys with `lynxdb auth create --name <name>`. See the [config command](/docs/cli/config-cmd) for managing connection profiles that store tokens.

Use a persistent `--data-dir` when enabling auth. LynxDB only bootstraps the API key store when the server has a data directory to store keys in.

## Signals

| Signal | Action |
|--------|--------|
| `SIGINT` | Graceful shutdown (finish in-flight requests, flush, exit) |
| `SIGTERM` | Graceful shutdown (same as SIGINT) |
| `SIGHUP` | Hot-reload configuration from file |

Graceful shutdown ensures all in-flight queries complete and any pending part flushes are finished before the process exits.

### Hot-Reloadable Settings

These settings are applied to new work when you send `SIGHUP` or run `lynxdb config reload`:

- `log_level`
- `retention`
- `query.*` execution limits such as `max_concurrent`, `default_result_limit`, `max_result_limit`, `max_query_runtime`, `max_query_length`, and profiling preview settings
- `storage.compaction_rate_limit_mb`

Restart the server after changing startup-time settings such as:

- `listen`
- `data_dir`
- TLS certificate and key settings
- authentication enablement
- UI enablement
- compaction worker counts and similar storage scheduler settings

## systemd Integration

### Recommended: `lynxdb install`

The easiest way to create a production systemd service. It generates a hardened unit file with security settings (`ProtectSystem=strict`, `NoNewPrivileges=true`, etc.), creates a dedicated user, sets file descriptor limits, and grants `CAP_NET_BIND_SERVICE`:

```bash
sudo lynxdb install
```

See the full [`install` reference](/docs/cli/install) for all flags.

### Managing the service

```bash
# Enable and start
sudo systemctl enable lynxdb
sudo systemctl start lynxdb

# Hot-reload config
sudo systemctl reload lynxdb

# Check status
sudo systemctl status lynxdb
journalctl -u lynxdb -f
```

### Manual unit file

If you need full control over the service unit, create `/etc/systemd/system/lynxdb.service` yourself:

```ini
[Unit]
Description=LynxDB Log Analytics Server
After=network.target

[Service]
Type=simple
User=lynxdb
Group=lynxdb
ExecStart=/usr/local/bin/lynxdb server --config /etc/lynxdb/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

## Related Commands

| Command | Description |
|---------|-------------|
| `lynxdb status` | Show detailed server status (alias: `st`) |
| `lynxdb health` | Quick health check |
| `lynxdb indexes` | List all indexes |
| `lynxdb cache stats` | Show cache statistics |
| `lynxdb cache clear` | Clear the query cache |

### status

```bash
lynxdb status
```

```
  LynxDB v0.1.0 — uptime 2d 5h 30m — healthy

  Storage:     1.2 GB
  Events:      3,456,789 total    123,456 today
  Segments:    42    Memtable: 8200 events
  Sources:     nginx (45%), api-gateway (30%), postgres (25%)
  Oldest:      2025-01-08T10:30:00Z
  Indexes:     3
```

### health

```bash
lynxdb health
lynxdb health --format json
```

## See Also

- [config](/docs/cli/config-cmd) for configuration management and profiles
- [bench & demo](/docs/cli/bench-demo) for testing and demonstration modes
- [CLI Overview](/docs/cli/overview) for global flags and modes of operation

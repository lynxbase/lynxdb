---
sidebar_position: 6
title: server
description: Start the LynxDB server - flags, storage, TLS, authentication, receivers, signals and systemd.
---

# server

Start the LynxDB server.

```
lynxdb server [flags]
```

The server loads its config file (see [config](./config-cmd.md) for the search order), applies `LYNXDB_*` environment variables, then the flags below. With no config at all it listens on `localhost:3100` and stores data under `~/.local/share/lynxdb` (`$XDG_DATA_HOME/lynxdb` when that is set).

## Flags

### Storage and runtime

| Flag | Description |
|------|-------------|
| `--addr` | Listen address, for example `0.0.0.0:3100` |
| `--data-dir` | Data directory; `""` keeps everything in memory |
| `--compaction-interval` | How often compaction checks for work, for example `30s` |
| `--cache-max-mb` | Query result cache size, for example `512mb`, `1gb` |
| `--max-query-pool` | Memory pool shared by all queries, for example `4gb` |
| `--spill-dir` | Directory for spill files (default: the OS temp directory) |
| `--log-level` | `debug`, `info`, `warn` or `error` |
| `--profile-runtime` | Turn on Go mutex and block profiling (about 2-5% overhead) |

### Security

| Flag | Description |
|------|-------------|
| `--auth` | Require API keys |
| `--tls` | Serve HTTPS; without `--tls-cert`, generate a self-signed certificate in `<data-dir>/tls/` |
| `--tls-cert` | Certificate PEM file (with `--tls-key`; either one turns TLS on) |
| `--tls-key` | Private key PEM file |

### Receivers

| Flag | Default | Description |
|------|---------|-------------|
| `--syslog` | | Receive syslog over UDP and TCP on this address (port `5514` when the address has none) |
| `--syslog-udp` | | Receive syslog over UDP only |
| `--syslog-tcp` | | Receive syslog over TCP only (port `6514` with `--syslog-tls`) |
| `--syslog-tls` | `false` | Wrap TCP syslog in the server's TLS |
| `--syslog-parser` | | `auto`, `rfc5424`, `rfc3164` or `raw` |
| `--syslog-index` | | Index for syslog events |
| `--otlp-http-listen` | `0.0.0.0:4318` | OTLP/HTTP logs listener address; empty disables |
| `--otlp-grpc-listen` | | OTLP/gRPC logs listener address; empty disables |
| `--otlp-grpc-max-recv-bytes` | | Largest OTLP/gRPC message |
| `--ingest-es-enabled` | `true` | Serve the Elasticsearch-compatible `_bulk` endpoint |
| `--ingest-es-version` | | Elasticsearch version reported to clients that check it |
| `--ingest-max-compressed-body-bytes` | | Largest compressed request body from a log shipper |
| `--ingest-max-decompressed-body-bytes` | | Largest request body after decompression |
| `--ingest-staging-enabled` | `true` | Buffer shipper requests in the server-side staging area |
| `--ingest-staging-max-bytes` | | Staging buffer size limit |
| `--ingest-staging-max-age` | | Longest time an entry stays in the staging buffer, for example `5s` |

The HTTP ingest endpoints, including Splunk HEC (`/services/collector`), are always served on the main listener. See [Ingest settings](../configuration/ingest.md) and [Syslog](../configuration/syslog.md).

Size flags take values such as `512mb` or `2gb`; interval flags take durations such as `30s`, `5m` or `1d`.

## Examples

```bash
# Defaults: localhost:3100, data under ~/.local/share/lynxdb
lynxdb server

# Listen on all interfaces with a fixed data directory
lynxdb server --addr 0.0.0.0:3100 --data-dir /var/lib/lynxdb

# In memory, nothing written to disk
lynxdb server --data-dir ""

# HTTPS with a self-signed certificate and API keys
lynxdb server --data-dir /var/lib/lynxdb --tls --auth

# Your own certificate
lynxdb server --tls-cert /etc/ssl/lynxdb.crt --tls-key /etc/ssl/lynxdb.key

# Syslog on port 514, UDP and TCP
lynxdb server --syslog :514

# Syslog over TLS
lynxdb server --tls --syslog-tcp :6514 --syslog-tls

# OTLP receivers
lynxdb server --otlp-http-listen :4318 --otlp-grpc-listen :4317
```

## Startup output

The server prints where its settings came from, then logs to stdout:

```
  Config     /etc/lynxdb/config.yaml
  Overrides  LYNXDB_LOG_LEVEL, --addr
  Data       /var/lib/lynxdb
  Listen     0.0.0.0:3100

time=2026-01-15T10:00:00.000Z level=INFO msg="starting LynxDB" version=1.0.0 addr=0.0.0.0:3100 config=/etc/lynxdb/config.yaml
```

With TLS on, it also prints the certificate path and its SHA-256 fingerprint. Clients pin that fingerprint on first [`lynxdb login`](./auth.md#login).

It writes its PID to `<data-dir>/lynxdb.pid` (or `$XDG_RUNTIME_DIR/lynxdb.pid` in memory mode) so that `lynxdb config reload` can find it.

## Authentication

With `--auth` (or `auth.enabled: true`) the server keeps API keys in `<data-dir>/auth/`, so authentication needs a data directory. When no key exists yet, it creates a root key and prints it once on stderr:

```
  ⚠  Auth enabled — no API keys exist. Generated root key:

    lynx_rk_7fA3mNpQ9xKvL2wB8cD4eF6gH1jR5tY

  Save this key now. It will NOT be shown again.
```

Log in with it and create narrower keys:

```bash
lynxdb login --token lynx_rk_7fA3mNpQ9xKvL2wB8cD4eF6gH1jR5tY
lynxdb auth create --name shipper --scope ingest
```

See [auth](./auth.md) and [TLS & Authentication](../deployment/tls-auth.md).

## Signals

| Signal | Action |
|--------|--------|
| `SIGINT`, `SIGTERM` | Graceful shutdown: stop accepting requests, finish in-flight work, flush buffered events, exit |
| `SIGHUP` | Reload the config file |

`lynxdb config reload` sends `SIGHUP` to the PID in the PID file.

### What a reload changes

A reload applies these settings to new work without a restart:

- `log_level`, `retention`
- `query.*` and `tail.*`, except `query.global_query_pool_bytes`, `query.spill_dir` and `query.max_temp_dir_size_bytes`
- `storage.compaction_rate_limit_mb`
- `ingest.mode`, `ingest.max_batch_size`, `ingest.max_line_bytes`
- `ingest.es_compat.*`, `ingest.limits.*`, `ingest.staging.*`, `ingest.splunk_hec.enabled`, `ingest.splunk_hec.require_token`
- `http.shutdown_timeout`
- `syslog.index`, `syslog.sourcetype`, `syslog.default_timezone`, `syslog.default_hostname`, `syslog.batch_size`, `syslog.batch_timeout`

Any other change, such as `listen`, `data_dir`, TLS, auth, OTLP listeners, `ingest.max_body_size` or storage scheduler settings, needs a restart; the server logs the fields it left unapplied.

## systemd

```ini
[Unit]
Description=LynxDB log analytics server
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

```bash
sudo systemctl enable --now lynxdb
sudo systemctl reload lynxdb      # SIGHUP: reload the config
journalctl -u lynxdb -f
```

## See also

- [Server settings](../configuration/server.md)
- [status, health and other inspection commands](./status.md)
- [config](./config-cmd.md)
- [Single-node deployment](../deployment/single-node.md)

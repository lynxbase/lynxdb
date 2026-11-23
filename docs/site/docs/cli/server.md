---
sidebar_position: 6
title: server
description: Start the LynxDB server - flags, data directory, TLS, authentication, receivers, signals and systemd.
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
| `--data-dir` | Data directory; `""` keeps the indexes in a temporary directory that is removed at shutdown |
| `--log-level` | `debug`, `info`, `warn` or `error` |
| `--profile-runtime` | Turn on Go mutex and block profiling for the pprof endpoints (about 2-5% overhead) |

### Security

| Flag | Description |
|------|-------------|
| `--auth` | Require API keys (needs a data directory) |
| `--tls` | Serve HTTPS; without `--tls-cert`, use a self-signed certificate kept in `<data-dir>/tls/` |
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
| `--otlp-http-listen` | `localhost:4318` | OTLP/HTTP logs listener address; empty disables |
| `--otlp-grpc-listen` | | OTLP/gRPC logs listener address; empty disables |
| `--otlp-grpc-max-recv-bytes` | | Largest OTLP/gRPC message |
| `--ingest-es-enabled` | `true` | Serve the Elasticsearch-compatible `_bulk` endpoint |
| `--ingest-es-version` | | Elasticsearch version reported to clients that check it |
| `--ingest-max-compressed-body-bytes` | | Largest request body on the wire |
| `--ingest-max-decompressed-body-bytes` | | Largest request body after decompression |

The HTTP ingest endpoints, including Splunk HEC (`/services/collector`), are always served on the main listener. See [Ingest settings](../configuration/ingest.md) and [Syslog](../configuration/syslog.md).

Size flags take values such as `512mb` or `2gb`.

## Examples

```bash
# Defaults: localhost:3100, data under ~/.local/share/lynxdb
lynxdb server

# Listen on all interfaces with a fixed data directory
lynxdb server --addr 0.0.0.0:3100 --data-dir /var/lib/lynxdb

# Indexes in a temporary directory, removed at shutdown
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

## Data directory

The data directory holds `indexes/`, with one directory per index, each with the store that keeps that index's events. An index comes into being with its first events. Next to `indexes/` are `auth/` (API keys, with `--auth`), `tls/` (the self-signed certificate, with `--tls`) and the PID file `lynxdb.pid`. [Storage settings](../configuration/storage.md) describe how events are written, partitioned by time and dropped by retention.

With `--data-dir ""` the server keeps its indexes in a temporary directory and removes it at shutdown. The startup banner shows `Data (in-memory)`. Features that keep files next to the data are not available: the server refuses to start with `--auth`, or with `--tls` and no certificate. The PID file goes to `$XDG_RUNTIME_DIR`, or the system temp directory.

## Startup output

The server prints where its settings came from, then logs to stdout:

```
  Config  /etc/lynxdb/config.yaml
  Overrides LYNXDB_LOG_LEVEL, --addr
  Data    /var/lib/lynxdb
  Listen  0.0.0.0:3100

time=2026-11-17T10:00:00.000Z level=INFO msg="starting LynxDB" version=1.0.0 addr=0.0.0.0:3100 config=/etc/lynxdb/config.yaml
```

With TLS on, it also prints the certificate path and its SHA-256 fingerprint. Clients pin that fingerprint on first [`lynxdb login`](./auth.md#login).

It writes its PID to `<data-dir>/lynxdb.pid` so that `lynxdb config reload` can find it.

## Authentication

With `--auth` (or `auth.enabled: true`) the server keeps API keys in `<data-dir>/auth/`. When no key exists yet, it creates a root key and prints it once on stderr:

```
  ⚠  Auth enabled — no API keys exist. Generated root key:

    lynx_rk_7fA3mNpQ9xKv_d6GncfBAepfJBd0Kh8oOOL8dKLzdocJ2

  Save this key now. It will NOT be shown again.
  Use it to authenticate:  lynxdb login
  Or generate new keys:    lynxdb auth create
```

Log in with it and create narrower keys:

```bash
lynxdb login --token lynx_rk_7fA3mNpQ9xKv_d6GncfBAepfJBd0Kh8oOOL8dKLzdocJ2
lynxdb auth create --name shipper --scope ingest
```

See [auth](./auth.md) and [TLS & Authentication](../deployment/tls-auth.md).

## Signals

| Signal | Action |
|--------|--------|
| `SIGINT`, `SIGTERM` | Graceful shutdown: stop the receivers, finish requests in flight within `http.shutdown_timeout`, cancel running query jobs, write the events still in memory, exit |
| `SIGHUP` | Reload the config file |

`lynxdb config reload` sends `SIGHUP` to the PID in the PID file.

### What a reload changes

A reload reads the config file and the environment again; it does not re-apply the flags the server started with. It applies these settings without a restart:

- `log_level`
- `query.sync_timeout`, `query.default_result_limit`, `query.max_result_limit`, `query.max_query_length`
- `ingest.mode`, `ingest.max_batch_size`, `ingest.max_line_bytes`
- `ingest.es_compat.enabled`, `ingest.splunk_hec.enabled`
- `http.shutdown_timeout`
- `syslog.index`, `syslog.sourcetype`, `syslog.default_timezone`, `syslog.default_hostname`, `syslog.batch_size`, `syslog.batch_timeout`

Any other change, such as `listen`, `data_dir`, `retention`, `storage.*`, `query.max_concurrent`, `query.job_ttl`, TLS, auth, the OTLP listeners or the body limits, needs a restart; the server logs the fields it left unapplied.

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

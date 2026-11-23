---
title: Server Settings
description: Top-level server settings, TLS, auth and HTTP settings, and every lynxdb server flag.
---

# Server Settings

## Top-Level Keys

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `listen` | `--addr` | `LYNXDB_LISTEN` | `localhost:3100` | HTTP listen address (`host:port`). Use `0.0.0.0:3100` to accept remote and container traffic. |
| `data_dir` | `--data-dir` | `LYNXDB_DATA_DIR` | `$XDG_DATA_HOME/lynxdb` or `~/.local/share/lynxdb` | Root directory of the indexes, the API keys, the self-signed certificate and the PID file. Empty keeps the indexes in a temporary directory that is removed at shutdown. |
| `retention` | none | `LYNXDB_RETENTION` | `7d` | Events older than this are dropped, a whole time partition at a time. `0` keeps everything. See [Storage Settings](./storage.md#retention). |
| `log_level` | `--log-level` | `LYNXDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. Reloadable. |

```bash
lynxdb server --addr 0.0.0.0:3100 --data-dir /var/lib/lynxdb
LYNXDB_RETENTION=30d lynxdb server
```

### Empty Data Directory

```bash
lynxdb server --data-dir ""
```

With an empty `data_dir` the server keeps its indexes in a temporary directory and removes it at shutdown, so nothing outlives the process. The features that keep files next to the data are unavailable: with `--auth` (or `auth.enabled: true`) or with `--tls` and no certificate, the server refuses to start. The PID file goes to `$XDG_RUNTIME_DIR` or the system temp directory.

## TLS

| Key | Flag | Env var | Default |
|-----|------|---------|---------|
| `tls.enabled` | `--tls` | `LYNXDB_TLS_ENABLED` | `false` |
| `tls.cert_file` | `--tls-cert` | `LYNXDB_TLS_CERT_FILE` | empty |
| `tls.key_file` | `--tls-key` | `LYNXDB_TLS_KEY_FILE` | empty |

The certificate and key must be set together. The `--tls-cert` and `--tls-key` flags turn TLS on by themselves; in the config file, set `tls.enabled: true` with them. With TLS on and no certificate, the server generates a self-signed certificate under `<data_dir>/tls/` and prints its SHA-256 fingerprint at startup. The minimum protocol version is TLS 1.2. Certificate changes need a restart.

```bash
lynxdb server --tls
lynxdb server --tls-cert /etc/lynxdb/tls/server.crt --tls-key /etc/lynxdb/tls/server.key
```

## Authentication

| Key | Flag | Env var | Default |
|-----|------|---------|---------|
| `auth.enabled` | `--auth` | `LYNXDB_AUTH_ENABLED` | `false` |

With auth on, API routes under `/api/` require an API key. Keys are stored in `<data_dir>/auth/`, so auth needs a `data_dir`: with an empty one the server refuses to start. `GET /api/v1/config` reports `auth.enabled: true` whether auth came from the flag, the environment or the file. On the first start with no keys, the server generates a root key and prints it once to stderr. Manage keys with `lynxdb auth create`, `list`, `revoke` and `rotate-root`, which take the root key; scopes are `ingest`, `query`, `admin` and `full`.

See [TLS and Authentication](../deployment/tls-auth.md) for the request schemes, the routes that stay open, and the client setup.

## HTTP

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `http.idle_timeout` | `LYNXDB_HTTP_IDLE_TIMEOUT` | `120s` | Keep-alive idle timeout. Minimum `1s`. |
| `http.shutdown_timeout` | `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown deadline for requests in flight. Minimum `1s`. Reloadable. |
| `http.read_header_timeout` | none | `10s` | Deadline for reading request headers. |
| `http.rate_limit` | `LYNXDB_HTTP_RATE_LIMIT` | `0` | Requests per second per client IP, with bursts of twice that. `0` disables the limiter. |

## Server Flags

Every flag of `lynxdb server`, with the config key it sets:

| Flag | Config key |
|------|-----------|
| `--config <path>` | config file path (global flag) |
| `--addr` | `listen` |
| `--data-dir` | `data_dir` |
| `--log-level` | `log_level` |
| `--tls` | `tls.enabled` |
| `--tls-cert`, `--tls-key` | `tls.cert_file`, `tls.key_file` |
| `--auth` | `auth.enabled` |
| `--syslog <addr>` | `syslog.udp` and `syslog.tcp` (port `5514` when omitted) |
| `--syslog-udp`, `--syslog-tcp` | `syslog.udp`, `syslog.tcp` |
| `--syslog-tls` | `syslog.tls` (TCP port `6514` when omitted) |
| `--syslog-parser`, `--syslog-index` | `syslog.parser`, `syslog.index` |
| `--ingest-es-enabled` | `ingest.es_compat.enabled` |
| `--ingest-es-version` | `ingest.es_compat.advertised_version` |
| `--otlp-http-listen`, `--otlp-grpc-listen` | `ingest.otlp.http_listen`, `ingest.otlp.grpc_listen` |
| `--otlp-grpc-max-recv-bytes` | `ingest.otlp.grpc_max_recv_bytes` |
| `--ingest-max-compressed-body-bytes` | `ingest.limits.max_compressed_body_bytes` |
| `--ingest-max-decompressed-body-bytes` | `ingest.limits.max_decompressed_body_bytes` |
| `--profile-runtime` | none: turns on mutex and block profiling for the pprof endpoints under `/api/v1/debug/pprof/` (2 to 5% overhead) |

The startup banner lists the config file in use and every environment variable and flag that overrode it.

## Complete Example

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

tls:
  enabled: true
  cert_file: "/etc/lynxdb/tls/server.crt"
  key_file: "/etc/lynxdb/tls/server.key"

auth:
  enabled: true

http:
  idle_timeout: "120s"
  shutdown_timeout: "30s"
  rate_limit: 200
```

## Related

- [Configuration Overview](./overview.md)
- [Storage Settings](./storage.md)
- [Query Settings](./query.md)
- [TLS and Authentication](../deployment/tls-auth.md)
- [Server API](../api/server.md)

---
title: Server Settings
description: Top-level server settings, TLS, auth, HTTP, live tail and memory pool settings, and every lynxdb server flag.
---

# Server Settings

This page covers the top-level keys of the server config file, the `tls`, `auth`, `http`, `tail`, `server` and `buffer_manager` sections, and the flags of `lynxdb server`.

## Top-Level Keys

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `listen` | `--addr` | `LYNXDB_LISTEN` | `localhost:3100` | HTTP listen address (`host:port`). Use `0.0.0.0:3100` to accept remote and container traffic. |
| `data_dir` | `--data-dir` | `LYNXDB_DATA_DIR` | `$XDG_DATA_HOME/lynxdb` or `~/.local/share/lynxdb` | Root directory for parts, auth keys, the self-signed certificate and the PID file. An empty value runs in memory. |
| `retention` | none | `LYNXDB_RETENTION` | `7d` | Events older than this are deleted. Reloadable. |
| `log_level` | `--log-level` | `LYNXDB_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. Reloadable. |

```bash
lynxdb server --addr 0.0.0.0:3100 --data-dir /var/lib/lynxdb
LYNXDB_RETENTION=30d lynxdb server
```

### In-Memory Mode

```bash
lynxdb server --data-dir ""
```

With an empty `data_dir` nothing is persisted, and the features that store files are unavailable: API key authentication (the server runs without auth even when `--auth` is set), the self-signed certificate, compaction and tiering. The PID file goes to `$XDG_RUNTIME_DIR` or the system temp directory.

## TLS

| Key | Flag | Env var | Default |
|-----|------|---------|---------|
| `tls.enabled` | `--tls` | `LYNXDB_TLS_ENABLED` | `false` |
| `tls.cert_file` | `--tls-cert` | `LYNXDB_TLS_CERT_FILE` | empty |
| `tls.key_file` | `--tls-key` | `LYNXDB_TLS_KEY_FILE` | empty |

Setting a certificate and key turns TLS on; the two must be set together. With TLS on and no certificate, the server generates a self-signed certificate under `<data_dir>/tls/` and prints its SHA-256 fingerprint at startup. The minimum protocol version is TLS 1.2. Certificate changes need a restart.

```bash
lynxdb server --tls
lynxdb server --tls-cert /etc/lynxdb/tls/server.crt --tls-key /etc/lynxdb/tls/server.key
```

## Authentication

| Key | Flag | Env var | Default |
|-----|------|---------|---------|
| `auth.enabled` | `--auth` | `LYNXDB_AUTH_ENABLED` | `false` |

With auth on, API routes under `/api/` require an API key. Keys are stored in `<data_dir>/auth/`. On the first start with no keys, the server generates a root key and prints it once to stderr. Manage keys with `lynxdb auth create`, `list`, `revoke` and `rotate-root`; scopes are `ingest`, `query`, `admin` and `full`.

See [TLS and Authentication](../deployment/tls-auth.md) for the request schemes, the routes that stay open, and the client setup.

## HTTP

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `http.idle_timeout` | `LYNXDB_HTTP_IDLE_TIMEOUT` | `120s` | Keep-alive idle timeout. Minimum `1s`. |
| `http.shutdown_timeout` | `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown deadline. Minimum `1s`. Reloadable. |
| `http.read_header_timeout` | none | `10s` | Deadline for reading request headers. |
| `http.rate_limit` | `LYNXDB_HTTP_RATE_LIMIT` | `0` | Requests per second per client IP. `0` disables the limiter. |

## Live Tail

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `tail.max_concurrent_sessions` | `LYNXDB_TAIL_MAX_CONCURRENT_SESSIONS` | `100` | Open `GET /api/v1/tail` sessions. Further sessions get `429` with `Retry-After: 5`. `0` means unlimited. |
| `tail.max_session_duration` | `LYNXDB_TAIL_MAX_SESSION_DURATION` | `24h` | Maximum lifetime of one tail session. `0` means unlimited. |

Both are reloadable.

## Memory Pool

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `server.total_memory_pool_bytes` | `LYNXDB_SERVER_TOTAL_MEMORY_POOL_BYTES` | `0` | Memory budget shared by query execution and the query cache. `0` sizes it at startup to 40% of system RAM (on Linux, of the cgroup memory limit when one is set), clamped to 512 MB through 128 GB. Under pressure the server evicts cache entries first. |
| `server.cache_reserve_percent` | `LYNXDB_SERVER_CACHE_RESERVE_PERCENT` | `20` | Validated (0 to 50) but not read by the server. |

Per-query limits and spill settings live under `query.*`; see [Query Settings](./query.md#memory-and-spill).

## Buffer Manager

The buffer manager is an optional page pool for the segment cache. It is off by default.

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `buffer_manager.enabled` | `LYNXDB_BUFFER_MANAGER_ENABLED` | `false` | Start the buffer manager. |
| `buffer_manager.max_memory_bytes` | `LYNXDB_BUFFER_MANAGER_MAX_MEMORY_BYTES` | `0` | Pool size. `0` sizes it automatically. |
| `buffer_manager.page_size` | `LYNXDB_BUFFER_MANAGER_PAGE_SIZE` | `65536` | Page size in bytes: a power of two, at least 4096. |
| `buffer_manager.enable_off_heap` | `LYNXDB_BUFFER_MANAGER_ENABLE_OFF_HEAP` | `true` | Allocate pages with mmap instead of the Go heap. |
| `buffer_manager.cache_target_percent` | `LYNXDB_BUFFER_MANAGER_CACHE_TARGET_PERCENT` | `60` | Validated, not read by the server. |
| `buffer_manager.query_target_percent` | `LYNXDB_BUFFER_MANAGER_QUERY_TARGET_PERCENT` | `30` | Validated, not read by the server. |
| `buffer_manager.batcher_target_percent` | `LYNXDB_BUFFER_MANAGER_BATCHER_TARGET_PERCENT` | `10` | Validated, not read by the server. |
| `buffer_manager.max_pinned_pages_per_query` | `LYNXDB_BUFFER_MANAGER_MAX_PINNED_PAGES_PER_QUERY` | `1024` | Validated, not read by the server. |

The three target percentages must not add up to more than 100.

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
| `--s3-bucket`, `--s3-region`, `--s3-prefix` | `storage.s3_bucket`, `storage.s3_region`, `storage.s3_prefix` |
| `--compaction-interval` | `storage.compaction_interval` |
| `--tiering-interval` | `storage.tiering_interval` |
| `--cache-max-mb` | `storage.cache_max_bytes` (takes a byte size such as `512mb`) |
| `--max-query-pool` | `query.global_query_pool_bytes` |
| `--spill-dir` | `query.spill_dir` |
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
| `--ingest-staging-enabled` | `ingest.staging.enabled` |
| `--ingest-staging-max-bytes`, `--ingest-staging-max-age` | `ingest.staging.max_bytes`, `ingest.staging.max_age` |
| `--cluster.enabled`, `--cluster.node-id`, `--cluster.roles`, `--cluster.seeds`, `--cluster.grpc-port` | `cluster.enabled`, `cluster.node_id`, `cluster.roles`, `cluster.seeds`, `cluster.grpc_port` |
| `--profile-runtime` | none: turns on mutex and block profiling for the pprof endpoints under `/api/v1/debug/pprof/` (2 to 5% overhead) |

The startup banner lists the config file in use and every environment variable and flag that overrode it.

## Complete Example

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

tls:
  cert_file: "/etc/lynxdb/tls/server.crt"
  key_file: "/etc/lynxdb/tls/server.key"

auth:
  enabled: true

http:
  idle_timeout: "120s"
  shutdown_timeout: "30s"
  rate_limit: 200

tail:
  max_concurrent_sessions: 100
  max_session_duration: "8h"

server:
  total_memory_pool_bytes: "8gb"
```

## Related

- [Configuration Overview](./overview.md)
- [Query Settings](./query.md)
- [TLS and Authentication](../deployment/tls-auth.md)
- [Server API](../api/server.md)

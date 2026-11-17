---
title: Environment Variables
description: Every LYNXDB_* environment variable the server and the CLI read, with the config key each one sets.
---

# Environment Variables

Server environment variables override the config file and are overridden by CLI flags. A variable set to an empty string is ignored.

Only the variables on this page are read. Config keys without a variable (marked in the [Keys Without a Variable](#keys-without-a-variable) section) can be set only in the config file or, where one exists, with a server flag. `lynxdb config` lists the effective value and source of every key.

## Client Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LYNXDB_SERVER` | `http://localhost:3100` | Server URL for CLI commands (`--server`). |
| `LYNXDB_TOKEN` | empty | API key (`--token`). Takes precedence over saved credentials. |
| `LYNXDB_PROFILE` | empty | Connection profile (`--profile`). |
| `LYNXDB_TLS_SKIP_VERIFY` | `false` | Skip TLS verification when `true`, `1` or `yes` (`--tls-skip-verify`). |
| `LYNXDB_CONFIG` | empty | Config file path, overriding the search order. Used by the server, the `config` command and profile lookups. |
| `NO_COLOR` | empty | Any value turns off colored output. |
| `EDITOR`, `VISUAL` | | Editor for `lynxdb config edit`. |

```bash
export LYNXDB_SERVER=https://lynxdb.example.com
export LYNXDB_TOKEN=lynx_ak_a1b2c3d4e5f6...
lynxdb query 'source=main level=ERROR | stats count() by host'
```

## Server Variables

### Top Level

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_LISTEN` | `listen` | `localhost:3100` |
| `LYNXDB_DATA_DIR` | `data_dir` | `~/.local/share/lynxdb` |
| `LYNXDB_RETENTION` | `retention` | `7d` |
| `LYNXDB_LOG_LEVEL` | `log_level` | `info` |

### Storage

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_STORAGE_PARTITION` | `storage.partition` | `24h` |
| `LYNXDB_STORAGE_FLUSH_INTERVAL` | `storage.flush_interval` | `0s` (one second) |
| `LYNXDB_STORAGE_TIME_FIELDS` | `storage.time_fields` | `_time,timestamp,@timestamp,time,ts` |
| `LYNXDB_STORAGE_SORT_KEY` | `storage.sort_key` | empty |

### Query

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_QUERY_SYNC_TIMEOUT` | `query.sync_timeout` | `30s` |
| `LYNXDB_QUERY_JOB_TTL` | `query.job_ttl` | `5m` |
| `LYNXDB_QUERY_MAX_CONCURRENT` | `query.max_concurrent` | `32` |
| `LYNXDB_QUERY_DEFAULT_RESULT_LIMIT` | `query.default_result_limit` | `1000` |
| `LYNXDB_QUERY_MAX_RESULT_LIMIT` | `query.max_result_limit` | `50000` |

### Ingest

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_INGEST_MAX_BODY_SIZE` | `ingest.max_body_size` | `100mb` |
| `LYNXDB_INGEST_MAX_BATCH_SIZE` | `ingest.max_batch_size` | `1000` |
| `LYNXDB_INGEST_MAX_LINE_BYTES` | `ingest.max_line_bytes` | `1048576` |
| `LYNXDB_INGEST_MODE` | `ingest.mode` | empty (`full`) |
| `LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES` | `ingest.limits.max_compressed_body_bytes` | `32mb` |
| `LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES` | `ingest.limits.max_decompressed_body_bytes` | `256mb` |
| `LYNXDB_INGEST_ES_COMPAT_ENABLED` | `ingest.es_compat.enabled` | `true` |
| `LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION` | `ingest.es_compat.advertised_version` | `8.15.0` |
| `LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME` | `ingest.es_compat.cluster_name` | `lynxdb` |
| `LYNXDB_INGEST_OTLP_HTTP_LISTEN` | `ingest.otlp.http_listen` | `0.0.0.0:4318` |
| `LYNXDB_INGEST_OTLP_GRPC_LISTEN` | `ingest.otlp.grpc_listen` | empty |
| `LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES` | `ingest.otlp.grpc_max_recv_bytes` | `16mb` |
| `LYNXDB_INGEST_SPLUNK_HEC_ENABLED` | `ingest.splunk_hec.enabled` | `true` |
| `LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN` | `ingest.splunk_hec.require_token` | `false` |

### HTTP, TLS, Auth

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_HTTP_IDLE_TIMEOUT` | `http.idle_timeout` | `120s` |
| `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `http.shutdown_timeout` | `30s` |
| `LYNXDB_HTTP_RATE_LIMIT` | `http.rate_limit` | `0` |
| `LYNXDB_TLS_ENABLED` | `tls.enabled` | `false` |
| `LYNXDB_TLS_CERT_FILE` | `tls.cert_file` | empty |
| `LYNXDB_TLS_KEY_FILE` | `tls.key_file` | empty |
| `LYNXDB_AUTH_ENABLED` | `auth.enabled` | `false` |

### Syslog

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_SYSLOG_UDP` | `syslog.udp` | empty |
| `LYNXDB_SYSLOG_TCP` | `syslog.tcp` | empty |
| `LYNXDB_SYSLOG_TLS` | `syslog.tls` | `false` |
| `LYNXDB_SYSLOG_PARSER` | `syslog.parser` | `auto` |
| `LYNXDB_SYSLOG_FRAMING` | `syslog.framing` | `auto` |
| `LYNXDB_SYSLOG_TRAILER` | `syslog.trailer` | `auto` |
| `LYNXDB_SYSLOG_DEFAULT_TIMEZONE` | `syslog.default_timezone` | `Local` |
| `LYNXDB_SYSLOG_DEFAULT_HOSTNAME` | `syslog.default_hostname` | empty |
| `LYNXDB_SYSLOG_INDEX` | `syslog.index` | `main` |
| `LYNXDB_SYSLOG_SOURCETYPE` | `syslog.sourcetype` | `syslog` |
| `LYNXDB_SYSLOG_USE_PEER_AS_SOURCE` | `syslog.use_peer_as_source` | `true` |
| `LYNXDB_SYSLOG_MAX_MESSAGE_BYTES` | `syslog.max_message_bytes` | `65536` |
| `LYNXDB_SYSLOG_UDP_READ_BUFFER` | `syslog.udp_read_buffer` | `2mb` |
| `LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT` | `syslog.tcp_idle_timeout` | `5m` |
| `LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS` | `syslog.tcp_max_connections` | `1000` |
| `LYNXDB_SYSLOG_BATCH_SIZE` | `syslog.batch_size` | `1000` |
| `LYNXDB_SYSLOG_BATCH_TIMEOUT` | `syslog.batch_timeout` | `200ms` |

## Keys Without a Variable

These keys are read from the config file only:

- `query.max_query_length`
- `http.read_header_timeout`
- `profiles.*`

## Install Script Variables

`https://lynxdb.org/install.sh` reads these variables; the `lynxdb` binary does not:

| Variable | Default | Description |
|----------|---------|-------------|
| `LYNXDB_VERSION` | latest | Version to install |
| `LYNXDB_CHANNEL` | `stable` | `stable` or `nightly` |
| `LYNXDB_ALLOW_PRERELEASE` | empty | Required for nightly and other prerelease versions |
| `LYNXDB_INSTALL_DIR` | auto-detected | Installation directory |
| `LYNXDB_BASE_URL` | `https://dl.lynxdb.org` | Download base URL |
| `LYNXDB_NO_MODIFY_PATH` | empty | Do not edit shell profiles to add the directory to `PATH` |
| `LYNXDB_VERBOSE`, `LYNXDB_QUIET` | empty | More or less output |
| `LYNXDB_FORCE` | empty | Skip confirmation prompts |

## Examples

systemd drop-in:

```ini
# /etc/systemd/system/lynxdb.service.d/env.conf
[Service]
Environment=LYNXDB_LISTEN=0.0.0.0:3100
Environment=LYNXDB_DATA_DIR=/var/lib/lynxdb
Environment=LYNXDB_RETENTION=30d
Environment=LYNXDB_AUTH_ENABLED=true
```

Kubernetes container:

```yaml
env:
  - name: LYNXDB_LISTEN
    value: "0.0.0.0:3100"
  - name: LYNXDB_DATA_DIR
    value: "/data"
```

## Related

- [Configuration Overview](./overview.md)
- [Docker Deployment](../deployment/docker.md)
- [Kubernetes Deployment](../deployment/kubernetes.md)

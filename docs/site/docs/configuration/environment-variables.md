---
title: Environment Variables
description: Complete reference of all LYNXDB_* environment variables for configuring LynxDB server and client.
---

# Environment Variables

Every LynxDB config key can be overridden with an environment variable using the `LYNXDB_` prefix. Environment variables take precedence over the config file but are overridden by CLI flags.

Precedence: CLI flags > environment variables > config file > defaults.

## Client Variables

These control CLI client behavior when connecting to a LynxDB server.

| Variable | Default | Description |
|----------|---------|-------------|
| `LYNXDB_SERVER` | `http://localhost:3100` | Default server address |
| `LYNXDB_TOKEN` | | API key for authentication |
| `LYNXDB_PROFILE` | | Default connection profile name |
| `LYNXDB_TLS_SKIP_VERIFY` | `false` | Skip TLS certificate verification (`true`/`1`/`yes`) |
| `LYNXDB_CONFIG` | | Path to config file (overrides search order) |
| `NO_COLOR` | | Disable colored output (any non-empty value) |

```bash
# Connect to a remote server with authentication
export LYNXDB_SERVER=https://lynxdb.company.com
export LYNXDB_TOKEN=lxk_a1b2c3d4e5f6...
lynxdb query 'level=error | stats count'
```

## Server Variables

Top-level server settings.

| Variable | Config Key | Default | Description |
|----------|-----------|---------|-------------|
| `LYNXDB_LISTEN` | `listen` | `localhost:3100` | Listen address |
| `LYNXDB_DATA_DIR` | `data_dir` | `~/.local/share/lynxdb` | Data directory |
| `LYNXDB_RETENTION` | `retention` | `7d` | Data retention period |
| `LYNXDB_LOG_LEVEL` | `log_level` | `info` | Log level (debug/info/warn/error) |

```bash
LYNXDB_LISTEN=0.0.0.0:3100 \
LYNXDB_DATA_DIR=/var/lib/lynxdb \
LYNXDB_RETENTION=30d \
LYNXDB_LOG_LEVEL=info \
lynxdb server
```

## Storage Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_STORAGE_COMPRESSION` | `storage.compression` | `lz4` |
| `LYNXDB_STORAGE_ROW_GROUP_SIZE` | `storage.row_group_size` | `65536` |
| `LYNXDB_STORAGE_FLUSH_THRESHOLD` | `storage.flush_threshold` | `512mb` |
| `LYNXDB_STORAGE_MEMTABLE_SHARDS` | `storage.memtable_shards` | `0` (auto) |
| `LYNXDB_STORAGE_MAX_IMMUTABLE` | `storage.max_immutable` | `2` |

### WAL Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_STORAGE_WAL_SYNC_MODE` | `storage.wal_sync_mode` | `write` |
| `LYNXDB_STORAGE_WAL_SYNC_INTERVAL` | `storage.wal_sync_interval` | `100ms` |
| `LYNXDB_STORAGE_WAL_SYNC_BYTES` | `storage.wal_sync_bytes` | `0` |
| `LYNXDB_STORAGE_WAL_MAX_SEGMENT_SIZE` | `storage.wal_max_segment_size` | `256mb` |

### Compaction Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_STORAGE_COMPACTION_INTERVAL` | `storage.compaction_interval` | `30s` |
| `LYNXDB_STORAGE_COMPACTION_WORKERS` | `storage.compaction_workers` | `2` |
| `LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB` | `storage.compaction_rate_limit_mb` | `0` |
| `LYNXDB_STORAGE_L0_THRESHOLD` | `storage.l0_threshold` | `4` |
| `LYNXDB_STORAGE_L1_THRESHOLD` | `storage.l1_threshold` | `10` |
| `LYNXDB_STORAGE_L2_TARGET_SIZE` | `storage.l2_target_size` | `1gb` |

### S3 Tiering Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_STORAGE_S3_BUCKET` | `storage.s3_bucket` | `""` |
| `LYNXDB_STORAGE_S3_REGION` | `storage.s3_region` | `us-east-1` |
| `LYNXDB_STORAGE_S3_PREFIX` | `storage.s3_prefix` | `""` |
| `LYNXDB_STORAGE_S3_ENDPOINT` | `storage.s3_endpoint` | `""` |
| `LYNXDB_STORAGE_S3_FORCE_PATH_STYLE` | `storage.s3_force_path_style` | `false` |
| `LYNXDB_STORAGE_TIERING_INTERVAL` | `storage.tiering_interval` | `5m` |
| `LYNXDB_STORAGE_TIERING_PARALLELISM` | `storage.tiering_parallelism` | `2` |
| `LYNXDB_STORAGE_SEGMENT_CACHE_SIZE` | `storage.segment_cache_size` | `1gb` |

### Cache Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_STORAGE_CACHE_MAX_BYTES` | `storage.cache_max_bytes` | `1gb` |
| `LYNXDB_STORAGE_CACHE_TTL` | `storage.cache_ttl` | `5m` |

## Query Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_QUERY_SYNC_TIMEOUT` | `query.sync_timeout` | `30s` |
| `LYNXDB_QUERY_MAX_QUERY_RUNTIME` | `query.max_query_runtime` | `5m` |
| `LYNXDB_QUERY_MAX_CONCURRENT` | `query.max_concurrent` | `10` |
| `LYNXDB_QUERY_DEFAULT_RESULT_LIMIT` | `query.default_result_limit` | `1000` |
| `LYNXDB_QUERY_MAX_RESULT_LIMIT` | `query.max_result_limit` | `50000` |
| `LYNXDB_QUERY_JOB_TTL` | `query.job_ttl` | `10m` |
| `LYNXDB_QUERY_JOB_GC_INTERVAL` | `query.job_gc_interval` | `1m` |

## Ingest Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_INGEST_MAX_BODY_SIZE` | `ingest.max_body_size` | `10mb` |
| `LYNXDB_INGEST_MAX_BATCH_SIZE` | `ingest.max_batch_size` | `1000` |

## HTTP Variables

| Variable | Config Key | Default |
|----------|-----------|---------|
| `LYNXDB_HTTP_IDLE_TIMEOUT` | `http.idle_timeout` | `2m` |
| `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `http.shutdown_timeout` | `30s` |

## AWS Credentials

LynxDB uses the standard AWS SDK credential chain for S3 access. These are standard AWS variables, not LynxDB-specific:

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_SESSION_TOKEN` | AWS session token (for temporary credentials) |
| `AWS_REGION` | Default AWS region (fallback for `LYNXDB_STORAGE_S3_REGION`) |
| `AWS_PROFILE` | AWS profile name from `~/.aws/credentials` |

## Install Script Variables

These are used by the `install.sh` script, not by the LynxDB binary itself:

| Variable | Default | Description |
|----------|---------|-------------|
| `LYNXDB_VERSION` | latest | Specific version to install |
| `LYNXDB_INSTALL_DIR` | auto-detect | Installation directory |
| `LYNXDB_BASE_URL` | `https://dl.lynxdb.org` | CDN base URL |
| `LYNXDB_NO_MODIFY_PATH` | | Skip PATH modification |
| `LYNXDB_VERBOSE` | | Verbose output |
| `LYNXDB_FORCE` | | Skip confirmation prompts |

## Usage in Docker

```dockerfile
FROM ghcr.io/lynxbase/lynxdb:latest

ENV LYNXDB_LISTEN=0.0.0.0:3100
ENV LYNXDB_DATA_DIR=/data
ENV LYNXDB_RETENTION=30d
ENV LYNXDB_LOG_LEVEL=info
ENV LYNXDB_STORAGE_COMPRESSION=lz4
ENV LYNXDB_STORAGE_S3_BUCKET=my-logs
ENV LYNXDB_STORAGE_S3_REGION=us-east-1

CMD ["server"]
```

## Usage in systemd

```ini
# /etc/systemd/system/lynxdb.service
[Service]
Environment=LYNXDB_LISTEN=0.0.0.0:3100
Environment=LYNXDB_DATA_DIR=/var/lib/lynxdb
Environment=LYNXDB_RETENTION=30d
Environment=LYNXDB_LOG_LEVEL=info
ExecStart=/usr/local/bin/lynxdb server
```

## Usage in Kubernetes

```yaml
env:
  - name: LYNXDB_LISTEN
    value: "0.0.0.0:3100"
  - name: LYNXDB_DATA_DIR
    value: "/data"
  - name: LYNXDB_STORAGE_S3_BUCKET
    valueFrom:
      configMapKeyRef:
        name: lynxdb-config
        key: s3-bucket
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: lynxdb-s3-credentials
        key: access-key-id
```

## Next Steps

- [Configuration Overview](/docs/configuration/overview) -- config cascade and file locations
- [Docker Deployment](/docs/deployment/docker) -- Docker and Docker Compose setup
- [Kubernetes Deployment](/docs/deployment/kubernetes) -- Kubernetes manifests

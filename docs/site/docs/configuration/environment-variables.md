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
| `LYNXDB_CONFIG` | empty | Config file path, overriding the search order. Used by the server and the `config` command. |
| `NO_COLOR` | empty | Any value turns off colored output. |
| `EDITOR`, `VISUAL` | | Editor for `lynxdb config edit`. |

```bash
export LYNXDB_SERVER=https://lynxdb.example.com
export LYNXDB_TOKEN=lxk_a1b2c3d4e5f6...
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
| `LYNXDB_STORAGE_COMPRESSION` | `storage.compression` | `lz4` |
| `LYNXDB_STORAGE_ROW_GROUP_SIZE` | `storage.row_group_size` | `65536` |
| `LYNXDB_STORAGE_FLUSH_THRESHOLD` | `storage.flush_threshold` | `512mb` |
| `LYNXDB_STORAGE_FLUSH_IDLE_TIMEOUT` | `storage.flush_idle_timeout` | `200ms` |
| `LYNXDB_STORAGE_MAX_COLUMNS_PER_PART` | `storage.max_columns_per_part` | `256` |
| `LYNXDB_STORAGE_PARTITION_BY` | `storage.partition_by` | `daily` |
| `LYNXDB_STORAGE_COMPACTION_INTERVAL` | `storage.compaction_interval` | `30s` |
| `LYNXDB_STORAGE_COMPACTION_WORKERS` | `storage.compaction_workers` | `2` |
| `LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB` | `storage.compaction_rate_limit_mb` | `100` |
| `LYNXDB_STORAGE_COMPACTION_DISABLE_BSI_ON_OUTPUT` | `storage.compaction_disable_bsi_on_output` | `false` |
| `LYNXDB_STORAGE_L0_THRESHOLD` | `storage.l0_threshold` | `4` |
| `LYNXDB_STORAGE_L1_THRESHOLD` | `storage.l1_threshold` | `4` |
| `LYNXDB_STORAGE_L2_TARGET_SIZE` | `storage.l2_target_size` | `1gb` |
| `LYNXDB_STORAGE_S3_BUCKET` | `storage.s3_bucket` | empty |
| `LYNXDB_STORAGE_S3_REGION` | `storage.s3_region` | `us-east-1` |
| `LYNXDB_STORAGE_S3_PREFIX` | `storage.s3_prefix` | `lynxdb/` |
| `LYNXDB_STORAGE_S3_ENDPOINT` | `storage.s3_endpoint` | empty |
| `LYNXDB_STORAGE_S3_FORCE_PATH_STYLE` | `storage.s3_force_path_style` | `false` |
| `LYNXDB_STORAGE_TIERING_INTERVAL` | `storage.tiering_interval` | `5m` |
| `LYNXDB_STORAGE_TIERING_PARALLELISM` | `storage.tiering_parallelism` | `3` |
| `LYNXDB_STORAGE_SEGMENT_CACHE_SIZE` | `storage.segment_cache_size` | `10gb` |
| `LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT` | `storage.remote_fetch_timeout` | `30s` |
| `LYNXDB_STORAGE_CACHE_MAX_BYTES` | `storage.cache_max_bytes` | `1gb` |
| `LYNXDB_STORAGE_CACHE_TTL` | `storage.cache_ttl` | `5m` |

### Query

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_QUERY_SYNC_TIMEOUT` | `query.sync_timeout` | `30s` |
| `LYNXDB_QUERY_MAX_QUERY_RUNTIME` | `query.max_query_runtime` | `5m` |
| `LYNXDB_QUERY_MAX_CONCURRENT` | `query.max_concurrent` | `32` |
| `LYNXDB_QUERY_DEFAULT_RESULT_LIMIT` | `query.default_result_limit` | `1000` |
| `LYNXDB_QUERY_MAX_RESULT_LIMIT` | `query.max_result_limit` | `50000` |
| `LYNXDB_QUERY_MAX_QUERY_MEMORY_BYTES` | `query.max_query_memory_bytes` | `1gb` |
| `LYNXDB_QUERY_GLOBAL_QUERY_POOL_BYTES` | `query.global_query_pool_bytes` | `0` |
| `LYNXDB_QUERY_SPILL_DIR` | `query.spill_dir` | system temp directory |
| `LYNXDB_QUERY_MAX_TEMP_DIR_SIZE_BYTES` | `query.max_temp_dir_size_bytes` | `10gb` |
| `LYNXDB_QUERY_JOB_TTL` | `query.job_ttl` | `5m` |
| `LYNXDB_QUERY_JOB_GC_INTERVAL` | `query.job_gc_interval` | `30s` |
| `LYNXDB_QUERY_SLOW_QUERY_THRESHOLD_MS` | `query.slow_query_threshold_ms` | `1000` |
| `LYNXDB_QUERY_BITMAP_SELECTIVITY_THRESHOLD` | `query.bitmap_selectivity_threshold` | `0.9` |
| `LYNXDB_QUERY_MAX_BRANCH_PARALLELISM` | `query.max_branch_parallelism` | `0` |
| `LYNXDB_QUERY_DEDUP_EXACT` | `query.dedup_exact` | `false` |
| `LYNXDB_QUERY_BROAD_SOURCE_LINT_THRESHOLD` | `query.broad_source_lint_threshold` | `10` |
| `LYNXDB_QUERY_BROAD_SEGMENT_LINT_THRESHOLD` | `query.broad_segment_lint_threshold` | `1000` |

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
| `LYNXDB_INGEST_ES_COMPAT_STRIP_LOGSTASH_DATE_SUFFIX` | `ingest.es_compat.strip_logstash_date_suffix` | `true` |
| `LYNXDB_INGEST_OTLP_HTTP_LISTEN` | `ingest.otlp.http_listen` | `0.0.0.0:4318` |
| `LYNXDB_INGEST_OTLP_GRPC_LISTEN` | `ingest.otlp.grpc_listen` | empty |
| `LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES` | `ingest.otlp.grpc_max_recv_bytes` | `16mb` |
| `LYNXDB_INGEST_SPLUNK_HEC_ENABLED` | `ingest.splunk_hec.enabled` | `true` |
| `LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN` | `ingest.splunk_hec.require_token` | `false` |
| `LYNXDB_INGEST_STAGING_ENABLED` | `ingest.staging.enabled` | `true` |
| `LYNXDB_INGEST_STAGING_MAX_BYTES` | `ingest.staging.max_bytes` | `64mb` |
| `LYNXDB_INGEST_STAGING_MAX_AGE` | `ingest.staging.max_age` | `5s` |
| `LYNXDB_INGEST_STAGING_MAX_INFLIGHT_EVENTS` | `ingest.staging.max_inflight_events` | `1000000` |
| `LYNXDB_INGEST_STAGING_FLUSH_RETRIES` | `ingest.staging.flush_retries` | `3` |
| `LYNXDB_INGEST_STAGING_FLUSH_BACKOFF_MAX` | `ingest.staging.flush_backoff_max` | `5s` |

### HTTP, Tail, TLS, Auth

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_HTTP_IDLE_TIMEOUT` | `http.idle_timeout` | `120s` |
| `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `http.shutdown_timeout` | `30s` |
| `LYNXDB_HTTP_RATE_LIMIT` | `http.rate_limit` | `0` |
| `LYNXDB_TAIL_MAX_CONCURRENT_SESSIONS` | `tail.max_concurrent_sessions` | `100` |
| `LYNXDB_TAIL_MAX_SESSION_DURATION` | `tail.max_session_duration` | `24h` |
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

### Memory Pool and Buffer Manager

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_SERVER_TOTAL_MEMORY_POOL_BYTES` | `server.total_memory_pool_bytes` | `0` (40% of RAM) |
| `LYNXDB_SERVER_CACHE_RESERVE_PERCENT` | `server.cache_reserve_percent` | `20` |
| `LYNXDB_BUFFER_MANAGER_ENABLED` | `buffer_manager.enabled` | `false` |
| `LYNXDB_BUFFER_MANAGER_MAX_MEMORY_BYTES` | `buffer_manager.max_memory_bytes` | `0` |
| `LYNXDB_BUFFER_MANAGER_PAGE_SIZE` | `buffer_manager.page_size` | `65536` |
| `LYNXDB_BUFFER_MANAGER_ENABLE_OFF_HEAP` | `buffer_manager.enable_off_heap` | `true` |
| `LYNXDB_BUFFER_MANAGER_CACHE_TARGET_PERCENT` | `buffer_manager.cache_target_percent` | `60` |
| `LYNXDB_BUFFER_MANAGER_QUERY_TARGET_PERCENT` | `buffer_manager.query_target_percent` | `30` |
| `LYNXDB_BUFFER_MANAGER_BATCHER_TARGET_PERCENT` | `buffer_manager.batcher_target_percent` | `10` |
| `LYNXDB_BUFFER_MANAGER_MAX_PINNED_PAGES_PER_QUERY` | `buffer_manager.max_pinned_pages_per_query` | `1024` |

### Cluster

| Variable | Key | Default |
|----------|-----|---------|
| `LYNXDB_CLUSTER_ENABLED` | `cluster.enabled` | `false` |
| `LYNXDB_CLUSTER_NODE_ID` | `cluster.node_id` | empty |
| `LYNXDB_CLUSTER_ROLES` | `cluster.roles` | empty |
| `LYNXDB_CLUSTER_SEEDS` | `cluster.seeds` | empty |
| `LYNXDB_CLUSTER_GRPC_PORT` | `cluster.grpc_port` | `9400` |
| `LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT` | `cluster.virtual_partition_count` | `1024` |
| `LYNXDB_CLUSTER_ACK_LEVEL` | `cluster.ack_level` | `one` |
| `LYNXDB_CLUSTER_REPLICATION_FACTOR` | `cluster.replication_factor` | `1` |
| `LYNXDB_CLUSTER_META_LOSS_TIMEOUT` | `cluster.meta_loss_timeout` | `30s` |

## Keys Without a Variable

These keys are read from the config file only:

- `query.max_query_length`, `query.preview_size`
- `ingest.fsync`, `ingest.dedup_enabled`, `ingest.dedup_capacity`
- `http.read_header_timeout`
- `cluster.heartbeat_interval`, `cluster.lease_duration`, `cluster.max_clock_skew`, `cluster.time_bucket_size`, `cluster.max_concurrent_shard_queries`, `cluster.shard_query_timeout`, `cluster.partial_results`, `cluster.partial_failure_threshold`, `cluster.dc_hll_threshold`
- `profiles.*`

## AWS Variables

The S3 client reads the standard AWS SDK variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_PROFILE`, `AWS_SHARED_CREDENTIALS_FILE`, `AWS_CONFIG_FILE`, and `AWS_WEB_IDENTITY_TOKEN_FILE` with `AWS_ROLE_ARN`. The bucket region always comes from `storage.s3_region`.

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
  - name: LYNXDB_STORAGE_S3_BUCKET
    value: "example-lynxdb-logs"
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: lynxdb-s3-credentials
        key: access-key-id
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: lynxdb-s3-credentials
        key: secret-access-key
```

## Related

- [Configuration Overview](./overview.md)
- [Docker Deployment](../deployment/docker.md)
- [Kubernetes Deployment](../deployment/kubernetes.md)

---
title: Configuration Overview
description: How LynxDB configuration works -- cascade precedence, config file locations, hot-reload, and the config CLI.
---

# Configuration Overview

LynxDB works out of the box with zero configuration. Every setting has a sensible production-ready default. When you need to tune behavior, LynxDB provides a layered configuration system with CLI flags, environment variables, config files, and project files.

## Configuration Cascade

Settings are resolved in the following order (highest priority first):

| Priority | Source | Example |
|----------|--------|---------|
| 1 (highest) | CLI flags | `--addr 0.0.0.0:3100` |
| 2 | Environment variables | `LYNXDB_LISTEN=0.0.0.0:3100` |
| 3 | Project file (`.lynxdbrc`) | `server: https://staging.company.com` |
| 4 | Config file (YAML) | `listen: "0.0.0.0:3100"` |
| 5 (lowest) | Compiled defaults | `localhost:3100` |

A CLI flag always wins. If no flag is set, the environment variable is checked. If no environment variable is set, the project file is checked (for client settings). Then the config file. Finally, the compiled default.

## Config File Locations

When `--config` is not specified, LynxDB searches for a config file in this order:

1. `LYNXDB_CONFIG` environment variable
2. `./lynxdb.yaml` (current directory)
3. `$XDG_CONFIG_HOME/lynxdb/config.yaml` (or `~/.config/lynxdb/config.yaml`)
4. `~/.lynxdb/config.yaml`
5. `/etc/lynxdb/config.yaml`

The first file found is used. If none exist, compiled defaults apply.

### Creating a Config File

```bash
# Create user-level config with documented defaults
lynxdb config init

# Create system-level config at /etc/lynxdb/config.yaml
lynxdb config init --system
```

### Viewing the Effective Config

```bash
# Show the resolved configuration (all sources merged)
lynxdb config

# Show the path to the active config file
lynxdb config path

# Get a single setting
lynxdb config get retention
lynxdb config get storage.compression
```

## Full Config File Reference

```yaml
# ~/.config/lynxdb/config.yaml

# Server listen address
listen: "localhost:3100"

# Persistent storage directory (empty string = in-memory only)
data_dir: "/var/lib/lynxdb"

# Data retention period
retention: "7d"

# Log level: debug, info, warn, error
log_level: "info"

# Storage engine settings
storage:
  compression: "lz4"                  # lz4 or zstd
  row_group_size: 65536               # Rows per column chunk
  flush_threshold: "512mb"            # Memtable flush trigger
  memtable_shards: 0                  # 0 = auto (num CPUs)
  max_immutable: 2                    # Max immutable memtables before backpressure
  wal_sync_mode: "write"             # none, write, or fsync
  wal_sync_interval: "100ms"          # Batch sync interval
  wal_sync_bytes: "0"                 # Sync after N bytes (0 = interval only)
  wal_max_segment_size: "256mb"       # WAL segment rotation size
  compaction_interval: "30s"          # Compaction check interval
  compaction_workers: 2               # Concurrent compaction threads
  compaction_rate_limit_mb: 0         # 0 = unlimited
  l0_threshold: 4                     # L0 files before compaction triggers
  l1_threshold: 10                    # L1 files before L1->L2 compaction
  l2_target_size: "1gb"              # Target size for L2 segments
  s3_bucket: ""                       # S3 bucket for warm/cold storage
  s3_region: "us-east-1"             # AWS region
  s3_prefix: ""                       # Key prefix in S3
  s3_endpoint: ""                     # Custom S3 endpoint (for MinIO)
  s3_force_path_style: false          # Use path-style S3 URLs
  tiering_interval: "5m"             # How often to check for tier-eligible segments
  tiering_parallelism: 2             # Concurrent tier uploads
  segment_cache_size: "1gb"          # Local cache for warm-tier segments
  cache_max_bytes: "1gb"             # Query result cache size
  cache_ttl: "5m"                    # Query result cache TTL

# Query engine settings
query:
  sync_timeout: "30s"                 # Timeout for synchronous queries
  max_query_runtime: "5m"            # Hard limit on any query
  max_concurrent: 10                  # Max concurrent queries
  default_result_limit: 1000          # Default LIMIT if not specified
  max_result_limit: 50000             # Hard cap on result rows
  job_ttl: "10m"                     # How long completed async jobs are kept
  job_gc_interval: "1m"              # Cleanup interval for expired jobs

# Ingest settings
ingest:
  max_body_size: "10mb"              # Max HTTP body for ingest requests
  max_batch_size: 1000                # Max events per batch

# HTTP server settings
http:
  idle_timeout: "2m"                 # Keep-alive idle timeout
  shutdown_timeout: "30s"            # Graceful shutdown deadline

# Cluster settings (see Configuration > Cluster for full reference)
cluster:
  enabled: false                      # Enable cluster mode
  node_id: ""                         # Unique node identifier
  roles: []                           # Roles: meta, ingest, query
  seeds: []                           # Seed node addresses (host:grpc_port)
  grpc_port: 9400                     # Inter-node gRPC port
  heartbeat_interval: "5s"            # Heartbeat frequency to meta leader
  lease_duration: "10s"               # Shard lease validity period
  max_clock_skew: "50ms"             # Maximum tolerated clock skew
  virtual_partition_count: 1024       # Hash partitions for sharding
  time_bucket_size: "24h"            # Time granularity for shard bucketing (1h/6h/24h)
  ack_level: "one"                   # Replication ACK: none/one/all
  replication_factor: 1              # Replicas per shard (including primary)
  meta_loss_timeout: "30s"           # Degraded-mode timeout on meta quorum loss
  max_concurrent_shard_queries: 50   # Concurrent shard RPCs per query
  shard_query_timeout: "30s"         # Per-shard query timeout
  partial_results: true              # Return partial results on shard failures
  partial_failure_threshold: 0.5     # Min success rate before query fails
  dc_hll_threshold: 10000            # dc() cardinality before HLL promotion
```

## Project File (`.lynxdbrc`)

A `.lynxdbrc` YAML file in the current directory (or any parent directory) sets per-project client defaults:

```yaml
# .lynxdbrc
server: https://staging.company.com
format: table
profile: staging
```

These are applied as defaults and overridden by explicit CLI flags. This is useful for teams where different projects point to different LynxDB instances.

## Hot-Reload

Some settings can be changed without restarting the server. Send `SIGHUP` to the server process or use the CLI:

```bash
# Hot-reload config from file
lynxdb config reload
```

### Hot-Reloadable Settings

These take effect immediately after reload:

- `log_level`
- `retention`
- `query.max_concurrent`
- `query.default_result_limit`
- `query.max_result_limit`
- `query.max_query_runtime`
- `cluster.max_concurrent_shard_queries`
- `cluster.shard_query_timeout`
- `cluster.partial_results`

### Settings Requiring Restart

The server will warn if you try to reload these:

- `listen` -- the listen address cannot change at runtime
- `data_dir` -- the data directory cannot change at runtime

## Config CLI Commands

```bash
# Show resolved config
lynxdb config

# Create a default config file
lynxdb config init
lynxdb config init --system

# Validate config file syntax and values
lynxdb config validate

# Hot-reload a running server
lynxdb config reload

# Get/set individual values
lynxdb config get retention
lynxdb config set retention 30d
lynxdb config set storage.compression zstd

# Open config in $EDITOR
lynxdb config edit

# Reset to defaults (with confirmation)
lynxdb config reset
```

## Connection Profiles

For teams managing multiple LynxDB instances, connection profiles let you switch between servers quickly:

```bash
# Add profiles
lynxdb config add-profile prod --url https://lynxdb.company.com --token lxk_abc123
lynxdb config add-profile staging --url https://staging.company.com

# List profiles
lynxdb config list-profiles

# Use a profile
lynxdb query 'level=error | stats count' --profile prod

# Set default profile via environment
export LYNXDB_PROFILE=prod
```

## Next Steps

- [Server Settings](/docs/configuration/server) -- listen address, data directory, TLS, auth
- [Storage Settings](/docs/configuration/storage) -- compression, WAL, compaction
- [Query Settings](/docs/configuration/query) -- concurrency, timeouts, limits
- [Ingest Settings](/docs/configuration/ingest) -- body size, batch size
- [S3 Tiering](/docs/configuration/s3-tiering) -- S3 bucket, region, tiering policies
- [Cluster Settings](/docs/configuration/cluster) -- cluster mode, sharding, replication, failover
- [Environment Variables](/docs/configuration/environment-variables) -- complete `LYNXDB_*` reference

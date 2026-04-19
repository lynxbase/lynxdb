---
title: Cluster Settings
description: Complete reference for LynxDB cluster configuration -- enabling cluster mode, node roles, sharding, replication, failover, and distributed query settings.
---

# Cluster Settings

This page documents all `cluster:` configuration keys for distributed LynxDB deployments. For an overview of the distributed architecture, see [Distributed Architecture](/docs/architecture/distributed).

:::note Current implementation status
These settings describe the distributed cluster surface present in the codebase. Single-node and co-located small-cluster deployments are the safest path today. For larger separated-role clusters, validate the exact behavior of failover, replication, and rebalancing in staging against the version you plan to run.
:::

## Enabling Cluster Mode

Set `cluster.enabled: true` and provide `node_id`, `roles`, and `seeds`:

```yaml
cluster:
  enabled: true
  node_id: "node-1"
  roles: [meta, ingest, query]
  seeds:
    - "node-1:9400"
    - "node-2:9400"
    - "node-3:9400"
```

When `enabled` is `false` (default), LynxDB runs as a single-node server with no cluster coordination.

## Node Identity and Roles

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | bool | `false` | Enable cluster mode |
| `node_id` | string | `""` | Unique identifier for this node. Must be unique across the cluster. |
| `roles` | list | `[]` | Roles this node performs: `meta`, `ingest`, `query`. In small clusters, use all three. |
| `seeds` | list | `[]` | Seed node addresses for cluster discovery (`host:grpc_port`). Include at least the meta nodes. |
| `grpc_port` | int | `9400` | Port for inter-node gRPC communication. |

### Role Examples

```yaml
# Small cluster: all roles on every node
cluster:
  enabled: true
  node_id: "node-1"
  roles: [meta, ingest, query]
  seeds: ["node-1:9400", "node-2:9400", "node-3:9400"]
```

```yaml
# Large cluster: dedicated meta node
cluster:
  enabled: true
  node_id: "meta-1"
  roles: [meta]
  seeds: ["meta-1:9400", "meta-2:9400", "meta-3:9400"]
```

```yaml
# Large cluster: dedicated ingest node
cluster:
  enabled: true
  node_id: "ingest-1"
  roles: [ingest]
  seeds: ["meta-1:9400", "meta-2:9400", "meta-3:9400"]
```

```yaml
# Large cluster: dedicated query node
cluster:
  enabled: true
  node_id: "query-1"
  roles: [query]
  seeds: ["meta-1:9400", "meta-2:9400", "meta-3:9400"]
```

## Sharding

LynxDB uses a two-level sharding scheme: time bucketing followed by hash partitioning.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `virtual_partition_count` | int | `1024` | Number of virtual hash partitions. Higher values allow finer-grained rebalancing. Should be significantly larger than the number of ingest nodes. |
| `time_bucket_size` | duration | `24h` | Time granularity for shard time bucketing. Supported values: `1h`, `6h`, `24h`. Smaller buckets enable finer time-range pruning but create more shards. |

The shard ID is computed as:
```
Level 1: ts.Truncate(time_bucket_size)
Level 2: xxhash64(source + "\x00" + host) % virtual_partition_count
ShardID: "<index>/t<date>/p<partition>"
```

### Choosing Partition Count

- **1024** (default): Good for clusters up to ~100 ingest nodes. Each node gets ~10 partitions.
- **4096**: For larger clusters with 100-500 ingest nodes.
- **Do not change** on a running cluster without a full rebalance.

### Choosing Time Bucket Size

- **24h** (default): One time bucket per day. Best for most workloads.
- **6h**: Four buckets per day. Better time-range pruning for queries spanning hours.
- **1h**: Finest granularity. Best for very high volume with short query windows.

## Replication

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `replication_factor` | int | `1` | Number of replicas per shard (including primary). Set to 3 for production HA. |
| `ack_level` | string | `"one"` | When the primary considers a batch committed. `none`: fire-and-forget. `one`: after local commit. `all`: after all ISR replicas ACK. |

### ACK Level Trade-offs

| ACK Level | Durability | Latency | Use Case |
|-----------|-----------|---------|----------|
| `none` | Lowest -- data may be lost on any failure | Lowest | Development, non-critical logs |
| `one` | Medium -- survives primary crash if replicas caught up | Medium | Default; good balance |
| `all` | Highest -- survives any single-node failure | Highest | Critical audit/security logs |

## Failover and Health

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `heartbeat_interval` | duration | `5s` | How often nodes send heartbeats to the meta leader. |
| `lease_duration` | duration | `10s` | Validity period of a shard leader lease. Must be > `heartbeat_interval`. |
| `meta_loss_timeout` | duration | `30s` | How long ingest nodes continue in degraded mode when meta quorum is lost. After this timeout, writes are rejected. |

### Failover Timing

With default settings, node failure detection takes approximately **25 seconds**:

1. Heartbeats stop arriving (0s)
2. Node transitions to `Suspect` after 3 missed heartbeats (~15s)
3. Node transitions to `Dead` after 5 missed heartbeats (~25s)
4. Shards are reassigned and ISR replicas promoted

Reducing `heartbeat_interval` speeds up detection but increases meta leader load.

## Query Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_concurrent_shard_queries` | int | `50` | Maximum concurrent shard RPCs during a single scatter-gather query. Acts as a semaphore. |
| `shard_query_timeout` | duration | `30s` | Per-shard query timeout. Shards exceeding this are marked as timed out. |
| `partial_results` | bool | `true` | Return partial results when some shards fail. |
| `partial_failure_threshold` | float | `0.5` | Minimum fraction of successful shards before the query fails entirely. |
| `dc_hll_threshold` | int | `10000` | Cardinality at which `dc()` promotes from exact set tracking to HyperLogLog approximation. |

## Clock Synchronization

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_clock_skew` | duration | `50ms` | Maximum tolerated clock difference between nodes. Nodes exceeding this on startup log a warning. |

**NTP requirement**: All cluster nodes should run NTP or a similar time synchronization service. Clock skew affects:
- Shard time bucketing (events may land in wrong time buckets)
- Lease expiration accuracy
- Heartbeat timing

## Environment Variables

All cluster settings can be overridden via environment variables using the `LYNXDB_CLUSTER_` prefix:

| Environment Variable | Config Key |
|---------------------|------------|
| `LYNXDB_CLUSTER_ENABLED` | `cluster.enabled` |
| `LYNXDB_CLUSTER_NODE_ID` | `cluster.node_id` |
| `LYNXDB_CLUSTER_ROLES` | `cluster.roles` (comma-separated) |
| `LYNXDB_CLUSTER_SEEDS` | `cluster.seeds` (comma-separated) |
| `LYNXDB_CLUSTER_GRPC_PORT` | `cluster.grpc_port` |
| `LYNXDB_CLUSTER_HEARTBEAT_INTERVAL` | `cluster.heartbeat_interval` |
| `LYNXDB_CLUSTER_LEASE_DURATION` | `cluster.lease_duration` |
| `LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT` | `cluster.virtual_partition_count` |
| `LYNXDB_CLUSTER_TIME_BUCKET_SIZE` | `cluster.time_bucket_size` |
| `LYNXDB_CLUSTER_ACK_LEVEL` | `cluster.ack_level` |
| `LYNXDB_CLUSTER_REPLICATION_FACTOR` | `cluster.replication_factor` |
| `LYNXDB_CLUSTER_META_LOSS_TIMEOUT` | `cluster.meta_loss_timeout` |

Example:

```bash
LYNXDB_CLUSTER_ENABLED=true \
LYNXDB_CLUSTER_NODE_ID=node-1 \
LYNXDB_CLUSTER_ROLES=meta,ingest,query \
LYNXDB_CLUSTER_SEEDS=node-1:9400,node-2:9400,node-3:9400 \
lynxdb server
```

## Hot-Reloadable Settings

The following cluster settings can be changed without restart via `lynxdb config reload`:

- `cluster.max_concurrent_shard_queries`
- `cluster.shard_query_timeout`
- `cluster.partial_results`

All other cluster settings require a node restart.

## Next Steps

- [Distributed Architecture](/docs/architecture/distributed) -- how sharding, replication, and queries work
- [Small Cluster](/docs/deployment/small-cluster) -- deploy a 3-10 node cluster
- [Large Cluster](/docs/deployment/large-cluster) -- deploy with role separation
- [Rebalancing and Splitting](/docs/operations/rebalancing) -- partition management
- [Monitoring](/docs/operations/monitoring) -- cluster metrics

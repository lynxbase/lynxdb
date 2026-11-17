---
title: Cluster Settings
description: Reference for the cluster section of the LynxDB config file - node identity, roles, sharding, replication, meta leases and distributed query settings.
---

# Cluster Settings

This page documents every key of the `cluster` section. For the topology and the way queries split across shards, see [Distributed Architecture](../architecture/distributed.md), [Small Cluster](../deployment/small-cluster.md) and [Large Cluster](../deployment/large-cluster.md).

:::caution Cluster runtime
`lynxdb server` validates the `cluster` section, but it does not start the cluster runtime: it opens no gRPC listener on `cluster.grpc_port`, joins no Raft group, and routes no shards. A server started with `cluster.enabled: true` stores and queries only its own data, and `GET /api/v1/cluster/status` reports `node_count: 1`. The settings below describe the cluster components in `pkg/cluster`.
:::

## Turning On Cluster Mode

```yaml
cluster:
  enabled: true
  node_id: "node-1"
  roles: [meta, ingest, query]
  seeds:
    - "node-1.example.com:9400"
    - "node-2.example.com:9400"
    - "node-3.example.com:9400"
```

```bash
lynxdb server \
  --cluster.enabled \
  --cluster.node-id node-1 \
  --cluster.roles meta,ingest,query \
  --cluster.seeds node-1.example.com:9400,node-2.example.com:9400,node-3.example.com:9400
```

With `enabled: true`, validation requires a `node_id`, at least one role, at least one seed, and a `grpc_port` between 1 and 65535. All cluster settings need a restart.

## Node Identity and Roles

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `cluster.enabled` | `--cluster.enabled` | `LYNXDB_CLUSTER_ENABLED` | `false` | Turn on cluster mode. |
| `cluster.node_id` | `--cluster.node-id` | `LYNXDB_CLUSTER_NODE_ID` | empty | Unique node name. |
| `cluster.roles` | `--cluster.roles` | `LYNXDB_CLUSTER_ROLES` | empty | Any of `meta`, `ingest`, `query`. Small clusters run all three on every node. |
| `cluster.seeds` | `--cluster.seeds` | `LYNXDB_CLUSTER_SEEDS` | empty | `host:grpc_port` addresses used to join; list the meta nodes. |
| `cluster.grpc_port` | `--cluster.grpc-port` | `LYNXDB_CLUSTER_GRPC_PORT` | `9400` | Port for inter-node gRPC (Raft, replication, shard queries). |

| Role | Components |
|------|------------|
| `meta` | Raft group (hashicorp/raft, state under `data_dir`) holding the shard map, node registry, shard leases, field catalog and source registry. Run 3 or 5 meta nodes for quorum. |
| `ingest` | Routes events to shards, writes parts, replicates batches to in-sync replicas according to `ack_level`. |
| `query` | Coordinates distributed queries: prunes shards, fans out shard queries, merges results. |

## Sharding

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `cluster.virtual_partition_count` | `LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT` | `1024` | Hash partitions per time bucket. Keep it well above the number of ingest nodes, and do not change it on a cluster with data. |
| `cluster.time_bucket_size` | none | `24h` | Time granularity of shards. Queries skip buckets outside their time range. |

An event's shard is:

```text
bucket    = event_time truncated to time_bucket_size (UTC)
partition = xxhash64(source + "\x00" + host) mod virtual_partition_count
shard     = (index, bucket, partition)
```

## Replication

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `cluster.replication_factor` | `LYNXDB_CLUSTER_REPLICATION_FACTOR` | `1` | Copies of each shard, primary included. |
| `cluster.ack_level` | `LYNXDB_CLUSTER_ACK_LEVEL` | `one` | When an ingest request is acknowledged: `none` (fire and forget), `one` (after the primary's local commit), `all` (after every in-sync replica acknowledges). |

## Meta Leases and Raft Timing

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `cluster.heartbeat_interval` | none | `5s` | Raft heartbeat timeout of meta nodes. |
| `cluster.lease_duration` | none | `10s` | Raft election and leader-lease timeout, and the lifetime of a shard lease granted to an ingest node. |
| `cluster.meta_loss_timeout` | `LYNXDB_CLUSTER_META_LOSS_TIMEOUT` | `30s` | Time without a lease renewal before an ingest node enters meta-loss mode: it keeps accepting writes without lease validation, marks the parts it writes as unvalidated, and skips compaction coordination until the meta group is reachable again. |
| `cluster.max_clock_skew` | none | `50ms` | Accepted in the config file but not read by the cluster runtime. Run NTP on every node: shard buckets and leases depend on wall-clock time. |

## Distributed Queries

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `cluster.max_concurrent_shard_queries` | none | `50` | Shard RPCs in flight for one query. |
| `cluster.shard_query_timeout` | none | `30s` | Timeout of one shard query. |
| `cluster.partial_results` | none | `true` | Return results when some shards fail. |
| `cluster.partial_failure_threshold` | none | `0.5` | Fraction of shards that must succeed for a partial result; below it the query fails. Range 0.0 to 1.0. |
| `cluster.dc_hll_threshold` | none | `10000` | Distinct-set size at which `dc()` partial states switch to a HyperLogLog sketch. |

A distributed query is a single pipeline over one scan. The coordinator splits it after the longest row-local prefix (search terms and commands that map each row on its own, such as `where`, `eval`, `fields`, `rename`, `parse`, `rex`, `grok`, `spath`, `expand` and `flatten`): every shard runs that prefix, and the coordinator runs the remaining commands over the merged rows. When the first remaining command is `stats` with only `count`, `sum`, `avg`, `min` and `max` over fields, grouped by plain fields, shards return partial aggregates that the coordinator merges. Queries with joins, unions or subsearches fail with `only a single pipeline over one scan runs distributed`. See [Large Cluster](../deployment/large-cluster.md#distributed-query-execution) for examples.

## Environment Variables

Only these cluster keys have environment variables. The rest are set in the config file.

```bash
LYNXDB_CLUSTER_ENABLED=true \
LYNXDB_CLUSTER_NODE_ID=node-1 \
LYNXDB_CLUSTER_ROLES=meta,ingest,query \
LYNXDB_CLUSTER_SEEDS=node-1:9400,node-2:9400,node-3:9400 \
LYNXDB_CLUSTER_GRPC_PORT=9400 \
LYNXDB_CLUSTER_REPLICATION_FACTOR=3 \
lynxdb server
```

| Variable | Key |
|----------|-----|
| `LYNXDB_CLUSTER_ENABLED` | `cluster.enabled` |
| `LYNXDB_CLUSTER_NODE_ID` | `cluster.node_id` |
| `LYNXDB_CLUSTER_ROLES` | `cluster.roles` (comma-separated) |
| `LYNXDB_CLUSTER_SEEDS` | `cluster.seeds` (comma-separated) |
| `LYNXDB_CLUSTER_GRPC_PORT` | `cluster.grpc_port` |
| `LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT` | `cluster.virtual_partition_count` |
| `LYNXDB_CLUSTER_ACK_LEVEL` | `cluster.ack_level` |
| `LYNXDB_CLUSTER_REPLICATION_FACTOR` | `cluster.replication_factor` |
| `LYNXDB_CLUSTER_META_LOSS_TIMEOUT` | `cluster.meta_loss_timeout` |

## Related

- [Distributed Architecture](../architecture/distributed.md)
- [Small Cluster](../deployment/small-cluster.md)
- [Large Cluster](../deployment/large-cluster.md)
- [Rebalancing](../operations/rebalancing.md)

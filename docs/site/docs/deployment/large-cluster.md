---
title: Large Cluster (10+ Nodes)
description: Role-separated LynxDB cluster topology with dedicated meta, ingest and query nodes, and how distributed queries split between shards and the coordinator.
---

# Large Cluster (10+ Nodes)

A large cluster gives each node one role, so metadata, write throughput and query concurrency scale separately.

:::caution Cluster runtime
`lynxdb server` validates the `cluster` settings but does not start the cluster runtime: it opens no gRPC listener, joins no Raft group and routes no shards. Each node configured as below stores and queries only its own data. This page describes the topology and query planning of the cluster components in `pkg/cluster`. For production, run a [single node](./single-node.md).
:::

## Roles

| Role | Count | Responsibility |
|------|-------|----------------|
| `meta` | 3 or 5 | Raft group with the shard map, node registry, shard leases, field catalog and source registry. Holds no event data. |
| `ingest` | scales with write volume | Routes events to shards, writes parts, replicates batches to in-sync replicas. |
| `query` | scales with query load | Prunes shards by time and source, fans out shard queries, merges results. |

```text
              ingest LB                          query LB
                  |                                  |
    +-------------+-------------+      +-------------+-------------+
    | ingest-1  ingest-2  ...   |      | query-1  query-2  ...     |
    +-------------+-------------+      +-------------+-------------+
                  |        gRPC :9400        |
                  +-----------+--------------+
                              |
                  meta-1  meta-2  meta-3  (Raft)
```

## Node Configuration

Meta nodes:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"

cluster:
  enabled: true
  node_id: "meta-1"          # unique per node
  roles: [meta]
  seeds: ["meta-1.example.com:9400", "meta-2.example.com:9400", "meta-3.example.com:9400"]
```

Ingest nodes:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"

cluster:
  enabled: true
  node_id: "ingest-1"
  roles: [ingest]
  seeds: ["meta-1.example.com:9400", "meta-2.example.com:9400", "meta-3.example.com:9400"]
  replication_factor: 3
  ack_level: "one"

storage:
  s3_bucket: "example-lynxdb-logs"
  s3_region: "us-east-1"
  compaction_workers: 4
```

Query nodes:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"

cluster:
  enabled: true
  node_id: "query-1"
  roles: [query]
  seeds: ["meta-1.example.com:9400", "meta-2.example.com:9400", "meta-3.example.com:9400"]
  max_concurrent_shard_queries: 50
  shard_query_timeout: "30s"

storage:
  s3_bucket: "example-lynxdb-logs"
  s3_region: "us-east-1"

query:
  max_concurrent: 64
  max_query_runtime: "30m"
```

See [Cluster Settings](../configuration/cluster.md) for every key.

## Distributed Query Execution

A distributed query must be a single pipeline over one scan. The coordinator compiles the query and splits it in two:

1. **Shard prefix.** The scan with its search terms and time range, followed by the longest run of row-local commands: commands that map each row on its own, such as `where`, `eval`, `fields`, `rename`, `parse`, `rex`, `grok`, `spath`, `expand` and `flatten`. Every shard runs this prefix over its own data. Shards receive the query text and derive the same split.
2. **Coordinator operators.** Everything after the prefix runs on the coordinator over the merged shard rows: `sort`, `head`, `dedup`, `eventstats`, `streamstats`, `top`, `timechart` and the rest.

When the first coordinator operator is a `stats` whose aggregations are all `count`, `sum`, `avg`, `min` or `max` over fields, grouped by plain fields, shards compute partial aggregates and the coordinator merges them, so only one row per group travels per shard. Any other aggregation (`dc`, `p95`, `count(eval(...))`, grouping by `span(...)` or an expression) runs entirely on the coordinator over all prefix rows.

| Query | On each shard | On the coordinator |
|-------|---------------|--------------------|
| `source=main level=ERROR \| where status >= 500 \| eval kb = bytes / 1024 \| fields host, path, kb` | Whole query | Concatenates rows |
| `source=main status>=500 \| stats count() as errors, avg(response_time) as avg_ms by path \| sort - errors \| head 10` | Scan, partial `count` and `avg` by `path` | Merges partials, keeps the top 10 |
| `source=main \| rex field=msg "(?<verb>[A-Z]+) (?<route>\S+)" \| stats count() as n, max(response_time) as worst by verb` | Scan, `rex`, partial `count` and `max` | Merges partials |
| `source=main \| stats dc(user) as users, p95(response_time) as p95_ms by host` | Scan | Full `stats` over all rows |
| `source=main \| stats count() by span(_time, 5m)` | Scan | Full `stats` (the group key is an expression) |

Queries with a join, a union (`append`, `union`, `multisearch`) or a subsearch do not run distributed. For example, `source=main | where host in [ source=main | where status = 503 | fields host ] | stats count() by host` fails with:

```text
cluster query: only a single pipeline over one scan runs distributed; joins, unions and subqueries do not
```

Shard RPCs per query are limited by `cluster.max_concurrent_shard_queries`, and each times out after `cluster.shard_query_timeout`. With `cluster.partial_results` on, a query returns the rows from the shards that answered when at least `cluster.partial_failure_threshold` of them did.

To make queries cheap on many shards, filter early so the shard prefix discards most rows, and aggregate with the mergeable functions. See [PPL performance](../ppl/performance.md).

## Network

| Port | Between | Purpose |
|------|---------|---------|
| 3100/tcp | clients and nodes | HTTP API |
| 9400/tcp | all nodes | gRPC: Raft, replication, shard queries |

Route traffic by path:

- **Ingest load balancer**: `/api/v1/ingest*`, `/api/v1/es/`, `/_bulk`, `/services/collector*`, `/api/v1/otlp/` to ingest nodes.
- **Query load balancer**: `/api/v1/query*`, `/api/v1/tail`, `/api/v1/fields*` and the remaining API routes to query nodes.
- Health checks: `GET /health` on port 3100.

## Meta Quorum Loss

Without a meta quorum, shard map changes and lease renewals stop. Ingest nodes that go `cluster.meta_loss_timeout` without a renewal enter meta-loss mode: they keep accepting writes without lease validation, mark those parts as unvalidated, skip compaction coordination, and re-validate when the meta group returns.

## Next Steps

- [Small Cluster](./small-cluster.md)
- [Cluster Settings](../configuration/cluster.md)
- [Distributed Architecture](../architecture/distributed.md)
- [PPL overview](../ppl/overview.md)

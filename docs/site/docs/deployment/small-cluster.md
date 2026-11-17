---
title: Small Cluster (3-10 Nodes)
description: Topology and configuration for a small LynxDB cluster where every node runs the meta, ingest and query roles.
---

# Small Cluster (3-10 Nodes)

In a small cluster every node runs all three roles (`meta`, `ingest`, `query`) with the same binary and the same configuration, except for `node_id`.

:::caution Cluster runtime
`lynxdb server` validates the `cluster` settings but does not start the cluster runtime: it opens no gRPC listener, joins no Raft group and routes no shards. Each node configured as below stores and queries only its own data. This page describes the topology the cluster components in `pkg/cluster` are built for. For production, run a [single node](./single-node.md).
:::

## Topology

```text
   +-------------------+  +-------------------+  +-------------------+
   | node-1            |  | node-2            |  | node-3            |
   | meta+ingest+query |  | meta+ingest+query |  | meta+ingest+query |
   +---------+---------+  +---------+---------+  +---------+---------+
             |       gRPC :9400 (Raft, replication, shard queries)
             +----------------------+----------------------+
                                    |
                           S3-compatible bucket
```

- The meta role runs a Raft group (hashicorp/raft) that holds the shard map, node registry and shard leases. Three meta nodes tolerate one failure; five tolerate two.
- Events are sharded by time bucket (`cluster.time_bucket_size`, 24 hours by default) and by `xxhash64(source + "\x00" + host) mod cluster.virtual_partition_count`.
- Ingest nodes replicate batches to in-sync replicas (`cluster.replication_factor`) and acknowledge according to `cluster.ack_level`.
- Any node can coordinate a query; see [Distributed Query Execution](./large-cluster.md#distributed-query-execution).

## Prerequisites

- 3 or 5 nodes with synchronized clocks (NTP)
- TCP 3100 (HTTP) and 9400 (gRPC) open between nodes
- An S3-compatible bucket for tiered segments, if retention exceeds local disk

## Configuration

`/etc/lynxdb/config.yaml` on `node-1`; the other nodes differ only in `node_id`:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"

cluster:
  enabled: true
  node_id: "node-1"
  roles: [meta, ingest, query]
  seeds:
    - "node-1.example.com:9400"
    - "node-2.example.com:9400"
    - "node-3.example.com:9400"
  grpc_port: 9400
  replication_factor: 3
  ack_level: "one"

storage:
  s3_bucket: "example-lynxdb-logs"
  s3_region: "us-east-1"

auth:
  enabled: true
```

The same settings as flags:

```bash
lynxdb server \
  --addr 0.0.0.0:3100 \
  --data-dir /var/lib/lynxdb \
  --cluster.enabled \
  --cluster.node-id node-1 \
  --cluster.roles meta,ingest,query \
  --cluster.seeds node-1.example.com:9400,node-2.example.com:9400,node-3.example.com:9400 \
  --cluster.grpc-port 9400
```

`lynxdb config validate` checks the cluster section: with `enabled: true` it requires `node_id`, at least one valid role, at least one seed, and a valid `grpc_port`. Every key is described in [Cluster Settings](../configuration/cluster.md).

## Load Balancer

Clients reach the nodes through one HTTP load balancer:

```nginx
upstream lynxdb {
    server node-1.example.com:3100;
    server node-2.example.com:3100;
    server node-3.example.com:3100;
}

server {
    listen 443 ssl;
    server_name lynxdb.example.com;

    client_max_body_size 32m;

    location / {
        proxy_pass http://lynxdb;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;

        # live tail (SSE) and NDJSON streaming
        proxy_buffering off;
        proxy_read_timeout 3600s;
    }
}
```

Use `GET /health` on port 3100 as the health check.

## Failure Behavior

| Event | Effect |
|-------|--------|
| One meta node down (of 3) | The Raft group keeps a quorum. |
| Two meta nodes down (of 3) | No quorum: shard map changes and lease renewals stop. After `cluster.meta_loss_timeout` (30s) without a renewal, ingest nodes enter meta-loss mode and keep accepting writes without lease validation until the meta group is back. |
| Shards fail during a query | With `cluster.partial_results` (default on), the query returns results from the shards that answered, as long as at least `cluster.partial_failure_threshold` (0.5) of them did. |

## Next Steps

- [Large Cluster](./large-cluster.md)
- [Cluster Settings](../configuration/cluster.md)
- [Distributed Architecture](../architecture/distributed.md)
- [S3/MinIO Storage Backend](./s3-setup.md)

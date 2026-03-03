---
title: Small Cluster (3-10 Nodes)
description: Deploy a small LynxDB cluster where every node runs all roles -- meta, ingest, and query.
---

# Small Cluster (3-10 Nodes)

In a small cluster, every node runs all three roles (meta, ingest, query). This is the simplest HA deployment -- same binary on every node, same config with different `node_id` values.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Node 1     │    │   Node 2     │    │   Node 3     │
│ meta+ingest  │    │ meta+ingest  │    │ meta+ingest  │
│   +query     │    │   +query     │    │   +query     │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                     ┌─────┴─────┐
                     │  S3/MinIO │
                     │ (source   │
                     │  of truth)│
                     └───────────┘
```

Key properties:
- **Raft consensus** for metadata (hashicorp/raft) -- needs 3+ nodes for quorum
- **WAL-based ISR replication** for data durability (Kafka model)
- **S3 is the source of truth** for segments -- nodes are stateless except for WAL + memtable
- **Sharding** by `fnv32a(host) % partition_count`
- **Node failure** triggers shard reassignment in ~16 seconds with no data loss

## Prerequisites

- 3+ nodes (5 recommended for production)
- S3-compatible object store (AWS S3 or MinIO)
- Network connectivity between nodes on port 9400 (cluster) and 3100 (HTTP)

## Configuration

Each node gets the same config with a unique `node_id`:

### Node 1: `/etc/lynxdb/config.yaml`

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

cluster:
  node_id: "node-1"
  roles: [meta, ingest, query]
  seeds:
    - "node-1.example.com:9400"
    - "node-2.example.com:9400"
    - "node-3.example.com:9400"

storage:
  s3_bucket: "my-lynxdb-logs"
  s3_region: "us-east-1"
  compression: "lz4"
  flush_threshold: "512mb"
  cache_max_bytes: "4gb"
  segment_cache_size: "10gb"

query:
  max_concurrent: 20
```

### Node 2: `/etc/lynxdb/config.yaml`

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

cluster:
  node_id: "node-2"
  roles: [meta, ingest, query]
  seeds:
    - "node-1.example.com:9400"
    - "node-2.example.com:9400"
    - "node-3.example.com:9400"

storage:
  s3_bucket: "my-lynxdb-logs"
  s3_region: "us-east-1"
  compression: "lz4"
  flush_threshold: "512mb"
  cache_max_bytes: "4gb"
  segment_cache_size: "10gb"

query:
  max_concurrent: 20
```

### Node 3: `/etc/lynxdb/config.yaml`

Same as above with `node_id: "node-3"`.

## Starting the Cluster

Start all nodes. Order does not matter -- they will discover each other via the seed list:

```bash
# On each node
lynxdb server --config /etc/lynxdb/config.yaml
```

Verify the cluster is formed:

```bash
lynxdb status
# Should show all 3 nodes
```

## Load Balancing

Place a load balancer in front of the cluster for both ingest and query traffic:

```
                   ┌──────────────┐
                   │ Load Balancer│
                   │ (nginx/HAProxy)
                   └──────┬───────┘
              ┌───────────┼───────────┐
              │           │           │
        ┌─────┴─────┐ ┌──┴──────┐ ┌──┴──────┐
        │  Node 1   │ │ Node 2  │ │ Node 3  │
        └───────────┘ └─────────┘ └─────────┘
```

Example nginx config:

```nginx
upstream lynxdb {
    server node-1.example.com:3100;
    server node-2.example.com:3100;
    server node-3.example.com:3100;
}

server {
    listen 80;
    server_name lynxdb.company.com;

    client_max_body_size 50m;

    location / {
        proxy_pass http://lynxdb;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;

        # SSE support for tail and streaming
        proxy_buffering off;
        proxy_read_timeout 3600s;
    }

    location /health {
        proxy_pass http://lynxdb;
    }
}
```

## Scaling

### Adding a Node

1. Deploy LynxDB on the new node with the same config (new `node_id`, same seeds)
2. Start the server -- it auto-joins the cluster
3. Shards are rebalanced automatically

```yaml
# node-4 config
cluster:
  node_id: "node-4"
  roles: [meta, ingest, query]
  seeds:
    - "node-1.example.com:9400"
    - "node-2.example.com:9400"
    - "node-3.example.com:9400"
```

### Removing a Node

1. Stop the LynxDB process on the node
2. Shards are reassigned to remaining nodes within ~16 seconds
3. No data loss (WAL replicated via ISR, segments in S3)

## Failure Handling

| Scenario | Impact | Recovery |
|----------|--------|----------|
| 1 node down (of 3) | Raft quorum maintained, reads and writes continue | Automatic shard reassignment in ~16s |
| 2 nodes down (of 3) | Raft quorum lost, writes fail, reads may work from cache | Bring at least 1 node back |
| S3 unavailable | New segments cannot be tiered, local storage fills up | S3 recovery; pending segments are uploaded |
| Network partition | Nodes on minority side lose Raft quorum | Network recovery; automatic rejoin |

## Monitoring

Monitor cluster health:

```bash
# Check status from any node
lynxdb status

# Health check for load balancers
curl http://node-1:3100/health
```

See [Monitoring](/docs/operations/monitoring) for Prometheus and alerting setup.

## When to Upgrade to a Large Cluster

Consider splitting roles when:
- Ingest throughput exceeds what 10 nodes can handle
- Query latency is affected by compaction or ingest load
- You need independent scaling of ingest and query capacity

See [Large Cluster](/docs/deployment/large-cluster) for role-separated architecture.

## Next Steps

- [Large Cluster](/docs/deployment/large-cluster) -- scale beyond 10 nodes with role splitting
- [S3 Storage Setup](/docs/deployment/s3-setup) -- configure the shared S3 backend
- [Kubernetes Deployment](/docs/deployment/kubernetes) -- deploy the cluster on K8s
- [Performance Tuning](/docs/operations/performance-tuning) -- optimize cluster performance

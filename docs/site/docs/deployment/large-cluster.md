---
title: Large Cluster (10-1000+ Nodes)
description: Deploy a large LynxDB cluster with separated meta, ingest, and query roles for independent scaling.
---

# Large Cluster (10-1000+ Nodes)

For large-scale deployments, LynxDB supports role splitting. Each node runs one specific role, allowing independent scaling of metadata management, write throughput, and query concurrency.

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Meta Nodes   │    │ Ingest Nodes │    │ Query Nodes  │
│ (3-5, Raft)  │    │ (N, stateless│    │ (M, stateless│
│              │    │  except WAL) │    │  + cache)    │
│ - Shard map  │    │ - WAL write  │    │ - Scatter-   │
│ - Node reg   │    │ - Memtable   │    │   gather     │
│ - Failover   │    │ - Flush→S3   │    │ - Partial    │
│              │    │ - Replicate  │    │   merge      │
└──────┬───────┘    └──────┬───────┘    └──────┬───────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                     ┌─────┴─────┐
                     │  S3/MinIO │
                     │ (source   │
                     │  of truth)│
                     └───────────┘
```

## Roles

| Role | Scales with | Count | Responsibility |
|------|-------------|-------|----------------|
| **Meta** | Cluster size | 3-5 (fixed) | Raft consensus, shard map, node registry, failure detection |
| **Ingest** | Write throughput | N nodes | WAL, memtable, segment flush, S3 upload, WAL replication |
| **Query** | Query concurrency | M nodes | Scatter-gather, partial aggregation merge, result caching |

## Meta Node Configuration

Meta nodes run the Raft consensus protocol. You need 3 for production (tolerates 1 failure) or 5 (tolerates 2 failures). Meta nodes are lightweight and do not store log data.

```yaml
# /etc/lynxdb/config.yaml (meta-1, meta-2, meta-3)
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
log_level: "info"

cluster:
  node_id: "meta-1"    # Unique per node
  roles: [meta]
  seeds:
    - "meta-1.example.com:9400"
    - "meta-2.example.com:9400"
    - "meta-3.example.com:9400"
```

## Ingest Node Configuration

Ingest nodes receive data, write to WAL, buffer in memtable, flush segments to S3, and replicate WAL to ISR peers. They are stateless after flush -- if an ingest node dies, its shards are reassigned and the WAL is replayed from replicas.

```yaml
# /etc/lynxdb/config.yaml (ingest nodes)
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
log_level: "info"

cluster:
  node_id: "ingest-1"    # Unique per node
  roles: [ingest]
  seeds:
    - "meta-1.example.com:9400"
    - "meta-2.example.com:9400"
    - "meta-3.example.com:9400"

storage:
  s3_bucket: "my-lynxdb-logs"
  s3_region: "us-east-1"
  compression: "lz4"
  flush_threshold: "512mb"
  wal_sync_mode: "write"
  compaction_workers: 4

ingest:
  max_body_size: "50mb"
```

## Query Node Configuration

Query nodes execute distributed queries using scatter-gather. They pull segments from S3 (with a local cache) and merge partial aggregation results from shards.

```yaml
# /etc/lynxdb/config.yaml (query nodes)
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
log_level: "info"

cluster:
  node_id: "query-1"    # Unique per node
  roles: [query]
  seeds:
    - "meta-1.example.com:9400"
    - "meta-2.example.com:9400"
    - "meta-3.example.com:9400"

storage:
  s3_bucket: "my-lynxdb-logs"
  s3_region: "us-east-1"
  segment_cache_size: "50gb"
  cache_max_bytes: "8gb"

query:
  max_concurrent: 50
  max_query_runtime: "30m"
```

## Distributed Query Execution

The optimizer automatically splits queries for distributed execution:

```
"source=nginx | where status>=500 | stats count by uri | sort -count | head 10"

  Shard-level (pushed):    | where status>=500 | stats count by uri   (partial agg)
  Coordinator (merged):    | sort -count | head 10
```

Operators that push down to shards: scan, filter, eval, partial stats, TopK.
Operators that run on the coordinator: sort, join, global dedup, streamstats.

## Network Architecture

```
                    ┌─────────────────────────────────────┐
                    │         Load Balancer                │
                    │   (ingest LB)    (query LB)         │
                    └────────┬──────────────┬──────────────┘
                             │              │
              ┌──────────────┤              ├──────────────┐
              │              │              │              │
        ┌─────┴────┐  ┌─────┴────┐  ┌─────┴────┐  ┌─────┴────┐
        │Ingest-1  │  │Ingest-2  │  │ Query-1  │  │ Query-2  │
        │Ingest-3  │  │Ingest-4  │  │ Query-3  │  │ Query-4  │
        └──────────┘  └──────────┘  └──────────┘  └──────────┘
              │              │              │              │
              └──────────────┼──────────────┼──────────────┘
                             │              │
                     ┌───────┴───┐  ┌───────┴───┐
                     │  Meta-1   │  │  Meta-2   │
                     │  Meta-3   │  │           │
                     └───────────┘  └───────────┘
```

### Required Ports

| Port | Protocol | Direction | Purpose |
|------|----------|-----------|---------|
| 3100 | TCP | Inbound | HTTP API (ingest, query, health) |
| 9400 | TCP | Between nodes | Cluster communication (Raft, shard rebalancing, replication) |

### Load Balancer Setup

Use separate load balancers (or URL-based routing) for ingest and query traffic:

- **Ingest LB** routes `POST /api/v1/ingest*` to ingest nodes
- **Query LB** routes all other traffic to query nodes
- Health check: `GET /health` on port 3100

## Scaling Guidelines

### Ingest Nodes

Scale ingest nodes based on write throughput:

| Events/sec | Recommended Ingest Nodes |
|------------|--------------------------|
| 100K | 2-3 |
| 500K | 5-10 |
| 1M | 10-20 |
| 5M+ | 50+ |

### Query Nodes

Scale query nodes based on concurrent query load:

| Concurrent Queries | Recommended Query Nodes |
|---|---|
| 10 | 2-3 |
| 50 | 5-10 |
| 200 | 20-30 |
| 500+ | 50+ |

### Meta Nodes

Meta nodes are lightweight. 3 nodes handle clusters up to ~500 nodes. Use 5 for larger clusters.

## Node Failure Recovery

| Scenario | Impact | Recovery Time |
|----------|--------|---------------|
| Ingest node failure | Shards reassigned to surviving ingest nodes | ~16 seconds |
| Query node failure | Load balancer routes to surviving query nodes | Immediate (health check) |
| Meta node failure (1 of 3) | Raft quorum maintained, cluster operates normally | Automatic |
| Meta node failure (2 of 3) | Raft quorum lost, no new shard assignments | Bring 1 meta node back |

S3 is the source of truth. No data is lost when ingest nodes fail because WAL is replicated to ISR peers.

## Example: 50-Node Cluster

```
Meta:   3 nodes (m5.large)
Ingest: 20 nodes (c5.2xlarge, 8 vCPU, 16GB RAM)
Query:  27 nodes (r5.2xlarge, 8 vCPU, 64GB RAM)
S3:     Single bucket, us-east-1
```

Estimated capacity:
- Ingest: ~2M events/sec
- Query: ~100 concurrent queries
- Storage: Unlimited (S3)
- Hot cache: ~1.3TB across query nodes

## Next Steps

- [S3 Storage Setup](/docs/deployment/s3-setup) -- configure the S3 backend
- [Small Cluster](/docs/deployment/small-cluster) -- simpler deployment for fewer than 10 nodes
- [Performance Tuning](/docs/operations/performance-tuning) -- optimize cluster performance
- [Monitoring](/docs/operations/monitoring) -- cluster-wide observability

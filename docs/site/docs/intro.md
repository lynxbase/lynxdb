---
sidebar_position: 0
slug: /
title: What is LynxDB?
description: LynxDB is an open-source log analytics database. Single binary, zero dependencies, SPL2 query language.
---

# What is LynxDB?

LynxDB is an open-source log analytics database built from scratch in Go. A single static binary that works as a pipe-mode CLI tool (like `grep` meets `awk`), a standalone server, or a distributed cluster -- same binary, same query language, same API at every scale.

## The Problem

The log analytics market is broken:

- **On one end:** `grep` and `jq` -- fast, scriptable, but no persistence, no indexing, no aggregation across time ranges.
- **On the other end:** Splunk ($2,000+/GB/day), Elasticsearch (cluster of 6+ nodes), Datadog (opaque pricing and vendor lock-in).

There is nothing in between. **LynxDB fills the gap.**

## Three Modes, One Binary

```
Developer laptop  →  cat app.log | lynxdb query '| stats count by level'
Single server     →  lynxdb server --data-dir /var/lib/lynxdb
3-node HA         →  lynxdb server --cluster.seeds node1:9400,node2:9400
1000-node fleet   →  lynxdb server --cluster.role query --cluster.seeds meta1:9400
```

### Pipe Mode (No Server)

Query local files and stdin using the full SPL2 engine with zero network overhead:

```bash
cat /var/log/syslog | lynxdb query '| where level="ERROR" | stats count by service'
kubectl logs deploy/api | lynxdb query '| stats avg(duration_ms), p99(duration_ms) by endpoint'
```

No daemon, no config file, no data directory. The binary creates an ephemeral in-memory engine, ingests input, runs the SPL2 pipeline, prints results, and exits.

### Server Mode (Single Node)

Persistent storage with a full REST API, materialized views, alerts, and dashboards:

```bash
lynxdb server
lynxdb query 'source=nginx status>=500 | stats count by uri | sort -count | head 10'
```

### Cluster Mode (Distributed)

Scale to 1000+ nodes with S3-backed shared storage, Raft consensus, and distributed query execution:

```bash
lynxdb server --cluster.seeds node1:9400,node2:9400,node3:9400
```

## How LynxDB Compares

| | LynxDB | Splunk | Elasticsearch | Loki | ClickHouse |
|---|---|---|---|---|---|
| **Deployment** | Single binary | Standalone or distributed | Single node or cluster | Single binary or microservices | Single binary or cluster |
| **Dependencies** | None | - | Bundled JVM | Object storage (prod) | Keeper (for replication) |
| **Query language** | SPL2 | SPL | Lucene DSL / ES\|QL | LogQL | SQL |
| **Pipe mode** | Yes | No | No | No | Yes (`clickhouse-local`) |
| **Schema** | On-read | On-read | On-write | Labels + line | On-write |
| **Full-text index** | FST + roaring bitmaps | tsidx | Lucene | Label index only | Token bloom filter |
| **Memory (idle)** | ~50 MB | ~12 GB (min spec) | ~1 GB+ | ~256 MB | ~1 GB |
| **License** | Apache 2.0 | Commercial | ELv2 / AGPL | AGPL | Apache 2.0 |

## Key Features

- **SPL2 Query Language** -- Splunk-inspired, works everywhere (CLI, API, alerts, dashboards)
- **Columnar Storage** -- Custom `.lsg` segment format with delta-varint timestamps, dictionary encoding, LZ4 compression
- **Full-Text Search** -- FST-based inverted index with roaring bitmap posting lists and bloom filters
- **Zero-Allocation VM** -- 22ns/op bytecode evaluation, 2.1M events/sec pipeline throughput
- **Materialized Views** -- Precomputed aggregations with ~400x query acceleration
- **Schema-on-Read** -- No upfront schema, fields discovered and indexed automatically
- **Drop-in Compatibility** -- Elasticsearch `_bulk`, OpenTelemetry OTLP, Splunk HEC
- **Alerts & Dashboards** -- SPL2-powered alerting with 8 notification channels

## Next Steps

- **[Quick Start](/docs/getting-started/quickstart)** -- Install and run your first query in 5 minutes
- **[SPL2 Overview](/docs/spl2/overview)** -- Learn the query language
- **[CLI Reference](/docs/cli/overview)** -- Explore all commands
- **[REST API](/docs/api/overview)** -- Integrate with your stack

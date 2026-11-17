---
sidebar_position: 0
slug: /
title: What is LynxDB?
description: LynxDB is an open-source log analytics database in a single binary, queried with OpenSearch PPL.
---

# What is LynxDB?

LynxDB is an open-source log analytics database written in Go. One static
binary works as a command-line tool over files and pipes and as a standalone
server. Queries are written in OpenSearch Piped Processing Language (PPL).

## One binary, two ways to run it

```
Developer laptop  →  cat app.log | lynxdb query 'source=main | stats count() by level'
Single server     →  lynxdb server --data-dir /var/lib/lynxdb
```

### Files and pipes

Query local files and standard input without a server:

```bash
cat /var/log/app.log | lynxdb query 'source=main | where level = "ERROR" | stats count() by service'
kubectl logs deploy/api | lynxdb query 'source=main | stats avg(duration_ms), p99(duration_ms) by endpoint'
lynxdb query --file access.log 'source=main status>=500 | top 10 path'
```

The CLI loads the input into an in-memory index named `main`, extracts
`key=value` pairs and the fields of common log formats, runs the query and
exits. No daemon, configuration file or data directory is involved.

### Server

Persistent storage, ingest endpoints and a REST API:

```bash
lynxdb server
lynxdb ingest access.log
lynxdb query 'source=main status>=500 | stats count() as errors by path | sort - errors | head 10' --since 1h
```

## The query language

```
source=main level=ERROR earliest=-1h
| stats count() as errors, p95(response_time) as p95 by host
| sort - errors
```

LynxDB implements OpenSearch PPL: search terms, `where`, `eval`, `stats`,
`timechart`, `top`, `rex`, `spath`, `join`, `lookup` and the rest of the
command set, with the PPL function library. Queries compile to a
language-independent intermediate representation, are optimized, and run on
LynxDB's columnar engine. The [PPL documentation](./ppl/overview.md) lists
what is supported, how LynxDB's behavior differs from OpenSearch, and which
features it does not implement.

## Features

- **OpenSearch PPL** in the CLI, the REST API and live tail, with structured
  errors that carry a position, the expected tokens and a suggestion.
- **Columnar storage** in its own `.lsg` segment format: delta-encoded
  timestamps, dictionary encoding, LZ4 compression.
- **Full-text search** through an FST-based term index with roaring bitmap
  posting lists and per-segment bloom filters.
- **Schema-on-read**: no upfront schema; fields are discovered at ingest and
  query time.
- **Ingest compatibility**: Elasticsearch `_bulk`, OpenTelemetry OTLP,
  Splunk HEC, syslog, and a native JSON API.

## How LynxDB compares

| | LynxDB | Elasticsearch | Loki | ClickHouse |
|---|---|---|---|---|
| **Deployment** | Single binary, single node | Single node or cluster | Single binary or microservices | Single binary or cluster |
| **Dependencies** | None | Bundled JVM | Object storage (production) | Keeper (for replication) |
| **Query files without a server** | Yes | No | No | Yes (`clickhouse-local`) |
| **Schema** | On read | On write | Labels + line | On write |
| **Full-text index** | FST + roaring bitmaps | Lucene | Label index only | Token bloom filter |
| **License** | Apache 2.0 | ELv2 / AGPL | AGPL | Apache 2.0 |

## Next steps

- **[Quick Start](/docs/getting-started/quickstart)**: install LynxDB and run a first query
- **[PPL](/docs/ppl/overview)**: the query language
- **[CLI Reference](/docs/cli/overview)**: every command
- **[REST API](/docs/api/overview)**: integrate with your stack

---
sidebar_position: 1
title: Architecture Overview
description: How LynxDB fits together -- the columnar store, the query compiler and executors, the database that joins them, and the two ways to run it.
---

# Architecture Overview

LynxDB is a log analytics database written in Go. One binary holds the storage engine, the query engine, the HTTP API and the command-line client; there is no embedded third-party database. Queries are written in OpenSearch Piped Processing Language (PPL). This page maps the system and how its parts fit together.

## System Diagram

```
   lynxdb CLI                                   log shippers
   query · ingest · import · status · config    _bulk · OTLP · HEC · syslog
        │                        │                     │
        │ pipe mode              │ HTTP                │
        │                        ▼                     ▼
        │            ┌──────────────────────────────────────┐
        │            │  REST API and receivers              │
        │            │  pkg/api/rest, pkg/ingest/receiver   │
        │            └──────────────────┬───────────────────┘
        ▼                               ▼
┌────────────────────────────────────────────────────────────────┐
│  pkg/database                                                  │
│  indexes, query jobs, retention                                │
└───────┬──────────────────────────────────────────┬─────────────┘
        │ query text                               │ NDJSON, plans
        ▼                                          ▼
┌──────────────────────────┐    ┌────────────────────────────────┐
│  pkg/query               │    │  pkg/engine/columnar           │
│  PPL → IR → optimizer    │    │  runs IR plans over the stores │
│  pkg/ppl, pkg/ir/opt     │    │                                │
└──────────────────────────┘    │  columnar executor             │
                                │    pkg/colstore/exec           │
                                │  row operators                 │
                                │    pkg/engine/physical,        │
                                │    pkg/engine/pipeline, pkg/vm │
                                └───────────────┬────────────────┘
                                                ▼
                                ┌────────────────────────────────┐
                                │  pkg/colstore                  │
                                │  one store per index:          │
                                │  time partitions, sorted runs, │
                                │  immutable parts, merges       │
                                └────────────────────────────────┘
```

## Two Ways to Run

Both ways open the same database and run queries with the same compiler and executors.

### Pipe Mode

```bash
lynxdb query --file testdata/logs/backend_server.log 'source=main level=ERROR | stats count() by service'
cat app.log | lynxdb query 'source=main | stats count() by level'
```

The CLI:

1. Compiles the query, so a query that does not compile fails before any input is read.
2. Opens a database in a temporary directory.
3. Loads stdin or the given files into the index `main`. A line that is a JSON object is kept as it is; any other line gets its leading timestamp and `key=value` pairs extracted, and the fields of combined, syslog and Docker logs when the first lines are in one of those formats (`--raw` turns that detection off).
4. Runs the query, prints the result and deletes the temporary directory.

There is no daemon, no config file and no network I/O.

### Server Mode

```bash
lynxdb server --data-dir /var/lib/lynxdb
```

The server:

1. Opens the data directory and the store of every index in it.
2. Serves the HTTP API on port 3100, and starts the OTLP and syslog receivers that are configured.
3. Appends ingested events to the stores, which write them within the flush interval.
4. Runs queries as jobs; before a query starts, the server makes every event ingested so far visible.
5. Drops partitions older than the retention period once a minute.

## Packages

| Package | Role |
|---------|------|
| `cmd/lynxdb` | The binary: the CLI commands, pipe mode and `lynxdb server` |
| `pkg/api/rest` | HTTP routes: ingest, query, jobs, fields, stats, configuration, API keys, Elasticsearch and Splunk HEC compatibility |
| `pkg/ingest` | Receivers for syslog, OTLP (HTTP and gRPC) and Splunk HEC; conversion of log text to NDJSON |
| `pkg/database` | The database of a server: one columnar store per index, PPL queries run as jobs, retention |
| `pkg/query` | `query.Compile`: PPL text to an optimized IR plan, with a cache of recent plans |
| `pkg/ppl` | The PPL frontend: lexer, parser, semantic analysis, lowering to the IR, the command and function catalogs |
| `pkg/ir`, `pkg/ir/opt` | The language-independent plan and its rewrite rules |
| `pkg/engine/columnar` | Runs a plan: hands what it can to the columnar executor and the rest to the row operators |
| `pkg/colstore` | The store of an index, its parts and the columnar executor (`pkg/colstore/exec`) |
| `pkg/engine/physical`, `pkg/engine/pipeline`, `pkg/vm` | The row operators and the bytecode VM that evaluates expressions over rows |
| `pkg/auth`, `pkg/config` | API keys and TLS; settings from files, environment and flags |

## Data Flow

### Write Path

```
POST /api/v1/ingest          NDJSON, appended as it is
POST /api/v1/ingest/raw      log text: JSON lines as they are; other lines get
                             their leading timestamp, key=value fields and _raw
POST /_bulk                  Elasticsearch bulk: document lines as they are
HEC, OTLP, syslog            decoded into one flat JSON object per event
  → pkg/database: Append(index, lines)
  → the store of the index, created with its first events
      → each line parsed once into typed columns by field path;
        the time from the first of storage.time_fields the event holds,
        else the time of arrival
      → buffered; written as one sorted run per time partition
        within storage.flush_interval, when the buffers are full, or on sync
      → a manifest record makes the run durable and visible
  → in the background:
      → runs of one tier are merged into a run of the next
      → a partition that has closed is merged into one run
      → partitions that end before the retention period are dropped
```

See [Storage](./storage.md) for each of these steps.

### Read Path

```
PPL text
  → query.Compile: parse, analyze, lower to an IR plan, optimize
    (plans of recent queries are reused)
  → the server syncs the stores and starts the query as a job
  → pkg/database: narrows every scan to the request's from and to
  → pkg/engine/columnar:
      → membership tests against small subqueries become lists of values
      → the columnar executor takes every part of the plan it can,
        from a scan up: filters, computed columns, one aggregate,
        a choice of columns, a sort, a limit, the first row of each key;
        it reads only the blocks that the time range, the token filters
        and the column statistics admit, on every core
      → the row operators run the rest of the plan over those rows
  → rows as JSON
```

See [Query Engine](./query-engine.md).

## Next Steps

- [Storage](./storage.md) -- per-index stores, partitions, runs, parts, flush and sync, retention
- [Query Engine](./query-engine.md) -- compilation, the columnar executor and the row operators
- [Elasticsearch Compatibility Stubs](./es-stubs.md) -- what log shippers see
- [Design Decisions](./design-decisions.md) -- why columnar, why PPL, why Go, why a single binary

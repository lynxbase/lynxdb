---
sidebar_position: 7
title: Design Decisions
description: Key design decisions behind LynxDB -- columnar storage, a store per index partitioned by time, buffered writes without a write-ahead log, per-block token filters, two executors, OpenSearch PPL compiled to a language-independent IR, Go, a single binary and schema-on-read.
---

# Design Decisions

## Columnar Storage

**Decision**: Store events in columns: every field path, and every kind of value it holds, is a column of its own with an encoding that suits it.

**Alternatives**: Row-oriented storage (each event as a contiguous blob, such as JSON on disk), hybrid formats.

**Reasoning**:

Log analytics queries almost always touch a few fields. `stats count() by level` needs only the `level` column; a columnar format reads that column, while a row-oriented format reads every byte of every event to extract one field.

| Query | Columnar | Row-oriented |
|-------|----------|--------------|
| `stats count() by level` (1 field) | Reads the `level` column | Reads every event |
| `where status >= 500` (1 field predicate) | Reads the `status` column | Reads every event |
| `fields _time, level, msg` (3 fields) | Reads 3 columns | Reads every event |
| Full event reconstruction | Reads every column | Reads every event |

Columns also compress and evaluate well: a column of few distinct values is a dictionary and small codes, and a filter tests each dictionary entry once; a column that holds one value in a block is a constant that a filter settles without reading rows; times are stored in the coarsest unit that keeps them exact. See [Storage](./storage.md#parts).

## A Store per Index, Partitioned by Time

**Decision**: Each index has its own directory and persists the settings for its sort key, time fields and partition width. The default partition width is 24 hours.

**Alternatives**: One table for all indexes with the index as a column; one time-ordered set of files per index without partitions.

**Reasoning**:

- **Retention works on files.** The store removes whole partitions from the manifest and deletes their files after readers release them.
- **Time bounds skip parts.** The scan can rule out parts and blocks using their stored time bounds.
- **Merges stay small and local.** A merge never mixes rows of two partitions, so the store can compact a quiet partition on its own. Late arrivals can make that partition eligible again.
- **Indexes own their parts.** Each index has its own manifest, buffers and merge scheduler. The stores share the process and its disk and CPU resources.

## Buffered Writes and a Manifest, without a Write-Ahead Log

**Decision**: Buffer incoming events in memory and write them as sorted, immutable runs; a manifest, an append-only log of changes to the set of parts, makes each write durable and visible in one record.

**Alternatives**: A write-ahead log with a memtable; an LSM tree (RocksDB, LevelDB); a B-tree (SQLite, BoltDB).

**Reasoning**:

- **Columnar writes.** A flush encodes rows directly into parts. Merges can rewrite those rows later.
- **Batching.** A flush collects pending batches and commits one run per partition represented in them. Concurrent syncs can share a flush.
- **Durability at commit.** The store syncs the part files, their directory and the manifest. Native ingest can acknowledge buffered rows that a process crash can lose. See [Acknowledgements and visibility](./storage.md#acknowledgements-and-visibility) for the protocol boundaries.
- **Recovery uses the manifest.** The store replays the part-set records and opens the live files. It can set aside unreferenced parts when it cannot establish that the manifest is whole.

## Token Filters per Block

**Decision**: Store per-block token fingerprints and scan the columns in candidate blocks to verify matches.

**Alternatives**: An inverted index with posting lists per term; embedding Lucene.

**Reasoning**:

- **Writes stay one pass.** The writer builds a block's filters as it encodes the columns. Merges can transfer filter data from blocks they copy.
- **Small and local.** The writer arranges filter data in stripes so the probes for one token in a group of blocks stay close together.
- **Column-level skipping.** A dedicated filter can rule out a column even when another column in the same block needs to be searched.

A word that appears in most blocks skips little. See [Indexing and Pruning](./indexing.md) for how token filters combine with other metadata.

## Two Executors

**Decision**: Run what the columnar executor can take straight over the column blocks, and the rest of the plan on row operators.

**Alternatives**: Only row-at-a-time operators (the Volcano model) over decoded events; only a vectorized columnar engine.

**Reasoning**:

- **The common queries are columnar.** Filters, computed columns, one aggregate, a choice of columns, a sort and a limit cover search and most dashboards. The columnar executor runs them over encoded blocks on every core and builds no row until the result is written.
- **PPL is larger than that.** `join`, `eventstats`, `streamstats`, `transpose`, `patterns` and others need rows. The row operators run them over the rows that the columnar executor produces, so every query that compiles also runs.
- **One plan.** Both executors take the same IR, and the split is decided per query, from the scans up.

See [Query Engine](./query-engine.md).

## OpenSearch PPL as the Query Language

**Decision**: Use OpenSearch Piped Processing Language (PPL), an open, documented pipeline language, and compile it to a language-independent IR.

**Reasoning**:

- **Pipelines match how logs are analyzed**: filter, extract, aggregate, sort, limit. Each command reads the previous command's output, so queries are built one stage at a time:

  ```
  source=main status>=500
  | eval duration_sec = response_time / 1000.0
  | stats count() as n, avg(duration_sec) as avg_sec by path
  | sort - n
  | head 10
  ```

- **Search first**: `source=main error` is a complete query. Free-text terms and field comparisons go straight into the source command, where the executor uses them to skip blocks.
- **Extraction inside the query**: `parse`, `rex`, `grok` and `spath` pull structure out of raw text at query time:

  ```
  source=main
  | rex field=_raw "user=(?<uname>\w+)"
  | stats dc(uname) as users, count() as n by path
  | where users > 5
  | sort - n
  ```

- **An existing language**: PPL has a published grammar and command reference. Users and tools that know it need not learn a LynxDB-specific dialect, and LynxDB states exactly which commands and functions it supports in its catalogs (`GET /api/v1/catalog`).

## A Language-Independent IR

**Decision**: The PPL frontend lowers queries to a canonical IR (`pkg/ir`); the optimizer and both executors work only on the IR.

**Reasoning**:

- **One engine contract**: plan rewrites and the split between the executors are written once, against IR operators, not against syntax.
- **Testable rewrites**: every optimizer rule is a plan-to-plan function, checked by `ir.Validate` and visible in `explain`.
- **Cheap compilation**: compilation depends only on the query text, so plans are cached by text and reused across requests.

See [PPL IR](../contributing/ir.md).

## Go

**Decision**: Implement LynxDB in Go.

**Alternatives**: Rust, C++, Java.

| Factor | Go | Rust | C++ | Java |
|--------|----|------|-----|------|
| **Single binary** | Native (static link) | Native (static link) | Possible (complex) | Requires a JVM |
| **Cross-compilation** | `GOOS=linux go build` | Possible | Complex | JVM portability |
| **Concurrency** | Goroutines | async/await + threads | Threads | Virtual threads |
| **Memory safety** | GC | Ownership | Manual | GC |
| **Build speed** | Fast | Slow | Slow | Moderate |

The key factors:

1. **Single static binary**: `go build` produces one file with no runtime, shared libraries or JVM.
2. **Fast compilation**: short builds keep contributor iteration and CI fast.
3. **Concurrency**: goroutines fit concurrent ingest, scans on every core and background work (flushes, merges, retention).
4. **Performance under a GC**: the hot paths keep garbage low: column buffers and batches are reused, query state lives in arenas, and the VM runs on a fixed-size stack.
5. **Contributor accessibility**: Go has a gentler learning curve than Rust.

**Why not Rust?** Better raw performance and no GC pauses, at the cost of slower builds and a steeper learning curve.

**Why not C++?** Memory safety. A database that ingests arbitrary data and evaluates user-provided expressions cannot afford use-after-free or buffer overflows.

**Why not Java?** A single binary in Java means shipping a JRE, and the JVM's baseline memory use is high for a tool that also runs in pipes.

## A Single Binary

**Decision**: Ship one static binary that works as a CLI tool and a server.

**Alternatives**: Separate binaries per role, microservices.

**Reasoning**:

1. **Operational simplicity**: one binary to install, upgrade and monitor, with no version matrix between components.
2. **Pipe mode**: `lynxdb query --file app.log 'source=main | stats count()'` opens the same database and runs the same compiler and executors as the server, in process.
3. **Testing**: integration tests exercise the real server code path.

## Schema-on-Read

**Decision**: No upfront schema. Every field path becomes a column when it first appears, and structure can be extracted at query time.

**Alternatives**: Schema-on-write (index mappings, table definitions), label-based indexing.

**Reasoning**:

Log data is heterogeneous:

- Different services emit different fields.
- Fields change between deployments.
- The same field name may hold different types in different sources.
- Raw text logs (syslog, access logs) have no schema at all.

Schema-on-write systems need the schema before ingest; when logs change shape, they produce mapping conflicts, ingest failures or reindexing. LynxDB keeps the values of one path in a column per kind, so `status` as an integer in one service and as a string in another coexist, and extracts structure when it is needed:

```
source=main "Invalid user"
| rex field=_raw "Invalid user (?<user>\S+) from (?<ip>\S+)"
| where isnotnull(user)
| stats count() as n by user, ip
| sort - n
```

The trade-off is that a field extracted at query time is computed for every row the query reads. Time ranges and the token filters of the search terms keep that set small.

## Related

- [Architecture Overview](./overview.md) -- how these decisions shape the system
- [Storage](./storage.md) -- partitions, runs, parts and the manifest
- [Query Engine](./query-engine.md) -- compilation and the two executors

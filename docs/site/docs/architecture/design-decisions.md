---
sidebar_position: 7
title: Design Decisions
description: Key design decisions behind LynxDB -- columnar storage, a store per index partitioned by time, buffered writes without a write-ahead log, per-block token filters, two executors, OpenSearch PPL compiled to a language-independent IR, Go, a single binary and schema-on-read.
---

# Design Decisions

This page explains the reasoning behind the major design decisions in LynxDB: the decision, the alternatives, and why the chosen approach fits log analytics.

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

**Decision**: Each index is a store of its own in a directory of its own, and its rows are kept apart by time partition (`storage.partition`, 24 hours by default).

**Alternatives**: One table for all indexes with the index as a column; one time-ordered set of files per index without partitions.

**Reasoning**:

- **Retention is a file operation.** Log data is append-only; the only deletion is age. Dropping a whole partition deletes its files and touches nothing else.
- **Time ranges read only their partitions.** Nearly every log query is bounded in time.
- **Merges stay small and local.** A merge never mixes rows of two partitions, so a partition that has closed is merged once into one run and then left alone.
- **Indexes are independent.** An index that takes a burst of events does not slow the merges of another, and its directory can be copied or deleted on its own.

## Buffered Writes and a Manifest, without a Write-Ahead Log

**Decision**: Buffer incoming events in memory and write them as sorted, immutable runs; a manifest, an append-only log of changes to the set of parts, makes each write durable and visible in one record.

**Alternatives**: A write-ahead log with a memtable; an LSM tree (RocksDB, LevelDB); a B-tree (SQLite, BoltDB).

**Reasoning**:

- **One write per event.** Events go to disk once, already in columns, and not first to a log and later to a table.
- **Batching.** A flush writes everything that arrived within the flush interval as one run per partition, and concurrent syncs share one flush, so many small requests cost one write and one wait for the disk.
- **A clear durability point.** Rows are durable once their manifest record is on disk. The server syncs before it answers an Elasticsearch `_bulk` request and before every query; plain ingest requests rely on the flush interval, and a crash can lose at most that long of their events. Shippers that retry resend them.
- **Recovery is reading the manifest.** There is nothing to replay: the manifest names the live parts, and files it does not name are left behind by writes that did not commit.

## Token Filters per Block

**Decision**: Give every block of a part token filters, split-block Bloom filters over the case-folded words of its strings, and answer searches by scanning the blocks that the filters admit.

**Alternatives**: An inverted index with posting lists per term; embedding Lucene.

**Reasoning**:

- **Writes stay one pass.** A block's filters are built while the block is written and rebuilt by every merge; there is no term dictionary to maintain or merge.
- **Small and local.** A probe for one word reads one page of a part's filters for a whole group of blocks.
- **Scans are fast anyway.** A block the filters admit is read column by column on every core, so the cost of a false positive is one block.
- **Not Lucene**: Lucene is Java. Embedding it needs a JVM or a sidecar process, which breaks the single-binary model.

The trade-off: a word that appears in most blocks skips little, and a search then reads the strings of those blocks.

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

See [PPL IR](../ppl/ir.md).

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

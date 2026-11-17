---
sidebar_position: 7
title: Design Decisions
description: Key design decisions behind LynxDB -- columnar storage, OpenSearch PPL compiled to a language-independent IR, Go, a single binary, schema-on-read, FST + roaring bitmaps, the Volcano model, and direct-to-part writes.
---

# Design Decisions

This page explains the reasoning behind the major design decisions in LynxDB: the decision, the alternatives, and why the chosen approach fits log analytics.

## Columnar Storage

**Decision**: Store log data in a columnar format where each field is a separate column with type-specific encoding.

**Alternatives**: Row-oriented storage (each event as a contiguous blob, such as JSON on disk), hybrid formats.

**Reasoning**:

Log analytics queries almost always touch a few fields. `stats count() by level` needs only the `level` column; a columnar format reads that column, while a row-oriented format reads every byte of every event to extract one field.

| Query | Columnar | Row-oriented |
|-------|----------|--------------|
| `stats count() by level` (1 field) | Reads the `level` column | Reads every event |
| `where status >= 500` (1 field predicate) | Reads the `status` column | Reads every event |
| `fields _time, level, msg` (3 fields) | Reads 3 columns | Reads every event |
| Full event reconstruction | Reads every column | Reads every event |

Row-oriented storage only wins for full event reconstruction. Even for `source=main error`, which shows whole events, the term index finds the matching rows first and only those rows are read in full.

Columnar storage also allows type-specific encoding:

- Timestamps: delta-varint
- Strings: dictionary encoding for low cardinality, LZ4 otherwise
- Floats: Gorilla encoding
- Raw text: LZ4, or ZSTD with `storage.compression: zstd`

A row-oriented store can only apply generic compression to whole rows, which compresses worse.

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

- **Search first**: `source=main error` is a complete query. Free-text terms and field comparisons go straight into the source command, where the storage engine can use them to skip data.
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

**Decision**: The PPL frontend lowers queries to a canonical IR (`pkg/ir`); the optimizer, physical planner and executor work only on the IR.

**Reasoning**:

- **One engine contract**: storage hints and plan rewrites are written once, against IR operators, not against syntax.
- **Testable rewrites**: every optimizer rule is a plan-to-plan function, checked by `ir.Validate` and visible in `explain`.
- **Cheap compilation**: compilation depends only on the query text, so plans are cached by text and reused across requests.

See [Query Engine](/docs/architecture/query-engine) and [PPL IR](../ppl/ir.md).

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

1. **Single static binary**: `go build` produces one file with no runtime, shared libraries or JVM, which the single-binary model needs.
2. **Fast compilation**: short builds keep contributor iteration and CI fast.
3. **Concurrency**: goroutines fit concurrent ingest, streaming query execution and background work (compaction, tiering, retention).
4. **Performance**: the hot paths avoid per-row allocations: the VM runs on a fixed-size stack, and batches and buffers are reused.
5. **Contributor accessibility**: Go has a gentler learning curve than Rust.

**Why not Rust?** Better raw performance and no GC pauses, at the cost of slower builds and a steeper learning curve. With allocation-free hot paths, GC is not the bottleneck.

**Why not C++?** Memory safety. A database that ingests arbitrary data and evaluates user-provided expressions cannot afford use-after-free or buffer overflows.

**Why not Java?** A single binary in Java means shipping a JRE, and the JVM's baseline memory use is high for a tool that also runs in pipes.

## A Single Binary

**Decision**: Ship one static binary that works as a CLI tool and a server.

**Alternatives**: Separate binaries per role, microservices.

**Reasoning**:

1. **Operational simplicity**: one binary to install, upgrade and monitor, with no version matrix between components.
2. **Pipe mode**: `lynxdb query --file app.log 'source=main | stats count()'` runs the same compiler and operators as the server in process. This works because the whole query engine is linked into the CLI.
3. **Testing**: integration tests exercise the real server code path.

## Schema-on-Read

**Decision**: No upfront schema. Fields are discovered at ingest and structure can be extracted at query time.

**Alternatives**: Schema-on-write (index mappings, table definitions), label-based indexing.

**Reasoning**:

Log data is heterogeneous:

- Different services emit different fields.
- Fields change between deployments.
- The same field name may hold different types in different sources.
- Raw text logs (syslog, access logs) have no schema at all.

Schema-on-write systems need the schema before ingest; when logs change shape, they produce mapping conflicts, ingest failures or reindexing. Schema-on-read accepts any data and extracts structure when it is needed:

```
source=main "connection refused"
| rex field=_raw "host=(?<h>\S+) port=(?<port>\d+)"
| stats count() by h, port
```

The trade-off is that predicates on fields without index support are evaluated row by row. Time bounds, bloom filters, the term index and column statistics keep the common cases fast.

## FST + Roaring Bitmaps

**Decision**: Build the term index from an FST (finite state transducer) term dictionary and roaring bitmap posting lists.

**Alternatives**: Embedding Lucene, a hash-based term dictionary, token bloom filters alone.

**Reasoning**:

- **Not Lucene**: Lucene is Java. Embedding it in a Go binary needs a JVM or a sidecar process, which breaks the single-binary model, and it brings machinery (codec versioning, near-real-time search) that immutable log segments do not need.
- **FST instead of a hash map**: FSTs share prefixes and suffixes between terms, so the dictionary is compact, and prefix and glob searches iterate the FST directly. LynxDB uses `github.com/blevesearch/vellum`.
- **Roaring bitmaps**: each 65536-row block is stored as an array, a bitmap or runs, whichever is smallest, so both rare and common terms are compact, and AND/OR are fast block by block.
- **Not bloom filters alone**: a bloom filter only answers whether a token may be present. LynxDB keeps per-row-group bloom filters for skipping, and the term index adds the exact rows.

| Feature | FST + Roaring | Hash + sorted arrays | Token bloom filters |
|---------|---------------|---------------------|---------------------|
| Exact term lookup | Yes | Yes | Membership only |
| Prefix and glob search | Yes | No | No |
| Row-level posting lists | Yes | Yes | No |
| Pure Go | Yes | Yes | Yes |

## Volcano Iterator Model

**Decision**: Execute queries with pull-based iterators (the Volcano model) over row batches.

**Alternatives**: Full materialization (compute each stage before starting the next), fully vectorized columnar execution.

**Reasoning**:

The Volcano model stops early. `head 10` stops pulling after its first batch, so a search over a large index returns as soon as it has 10 rows, and remaining segments are never opened. Log exploration nearly always limits its results.

The cost is per-batch call overhead compared with tight loops over column vectors. LynxDB reduces it with:

- **Batches of up to 1024 rows** per `Next()` call
- **A bytecode VM** for expressions instead of walking an expression tree per row
- **Storage hints** that skip segments and row groups before any operator sees their rows

## Direct-to-Part Writes (No WAL, No LSM)

**Decision**: Buffer events in memory and write immutable columnar parts directly, with atomic renames; no write-ahead log and no general-purpose LSM or B-tree.

**Alternatives**: WAL + memtable, LSM tree (RocksDB, LevelDB), B-tree (SQLite, BoltDB).

**Reasoning**:

Log data is append-only. The only mutations are appending events and deleting whole time partitions on retention. A storage path specialized for immutable segments fits that:

- **AsyncBatcher**: fast writes, bounded in-memory buffering, backpressure when compaction falls behind
- **Segment writer**: columnar encoding and index building in one pass
- **Compaction**: merges small parts into large ones

Without a WAL:

- **Latency**: no fsync per write; the batcher absorbs bursts and writes parts in bulk.
- **Recovery**: startup is a filesystem scan that deletes stale `tmp_*` files and rebuilds the registry from the surviving `.lsg` parts; there is nothing to replay.
- **Accepted data window**: events buffered in the batcher are in memory and can be lost on a crash. ClickHouse makes the [same trade-off](https://github.com/ClickHouse/ClickHouse/issues/64906) by default. Shippers that retry on failure resend them.

## Related

- [Architecture Overview](/docs/architecture/overview) -- how these decisions shape the system
- [Storage Engine](/docs/architecture/storage-engine) -- part writes and compaction
- [Segment Format](/docs/architecture/segment-format) -- columnar encoding details
- [Query Engine](/docs/architecture/query-engine) -- compilation, the Volcano model and the bytecode VM
- [Indexing](/docs/architecture/indexing) -- bloom filters and the term index

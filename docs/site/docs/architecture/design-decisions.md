---
sidebar_position: 7
title: Design Decisions
description: Key design decisions behind LynxDB -- why columnar storage, why SPL2, why Go, why a single binary, why schema-on-read, and why FST + roaring bitmaps.
---

# Design Decisions

This page explains the reasoning behind the major design decisions in LynxDB. Each section describes the decision, the alternatives we considered, and why we chose the approach we did.

## Why Columnar Storage (Not Row-Oriented)

**Decision**: Store log data in a columnar format where each field is stored in a separate column with type-specific encoding.

**Alternatives**: Row-oriented storage (each event stored as a contiguous blob, e.g., JSON on disk), hybrid formats (e.g., PAX/Parquet).

**Reasoning**:

Log analytics queries almost always access a subset of fields. A query like `stats count by level` needs only the `level` column -- in a columnar format, it reads that single column. In a row-oriented format, it reads every byte of every event to extract one field.

| Access pattern | Columnar | Row-oriented |
|---------------|----------|-------------|
| `stats count by level` (1 field) | Read ~5% of data | Read 100% of data |
| `where status>=500` (1 field predicate) | Read ~5% of data | Read 100% of data |
| `table _time, level, message` (3 fields) | Read ~15% of data | Read 100% of data |
| Full event reconstruction | Read 100% of data | Read 100% of data |

The only case where row-oriented wins is full event reconstruction, which is rare in analytics workloads. Even for `search "error"` where you display the full event, the inverted index resolves matching events first, and then only the matching rows are read in full.

Additionally, columnar storage enables type-specific encoding:

- Timestamps: delta-varint (5-10x compression)
- String fields: dictionary encoding (10-50x for low cardinality)
- Float fields: Gorilla encoding (10-15x for low variance)
- Raw text: LZ4 compression (3-5x)

A row-oriented store can only apply generic compression (e.g., gzip) to entire rows, which is significantly less effective.

## Why SPL2 (Not SQL)

**Decision**: Use SPL2 (Search Processing Language 2) as the query language, not SQL.

**Alternatives**: SQL, LogQL (Grafana Loki), Lucene DSL (Elasticsearch), custom DSL.

**Reasoning**:

SPL (and SPL2) was specifically designed for log analytics. SQL was designed for relational data. The mismatch shows in practice:

**Pipeline vs. nested subqueries**: Log analysis is naturally a pipeline: filter -> transform -> aggregate -> sort -> limit. SPL2 expresses this directly:

```spl
source=nginx status>=500
  | eval duration_sec = duration_ms / 1000
  | stats count, avg(duration_sec) by uri
  | sort -count
  | head 10
```

The SQL equivalent requires nested subqueries or CTEs:

```sql
SELECT uri, COUNT(*) as cnt, AVG(duration_ms / 1000.0) as avg_dur
FROM logs
WHERE source = 'nginx' AND status >= 500
GROUP BY uri
ORDER BY cnt DESC
LIMIT 10
```

For simple queries, SQL is comparable. But SPL2 shines for multi-stage transformations:

```spl
source=nginx
  | rex field=_raw "client=(?P<ip>\d+\.\d+\.\d+\.\d+)"
  | stats dc(ip) as unique_ips, count by uri
  | where unique_ips > 100
  | sort -count
```

This would require nested subqueries in SQL, and the `rex` (regex extraction) step has no clean SQL equivalent.

**Practical benefits**:

- **Splunk ecosystem**: Millions of engineers already know SPL. SPL2 is a natural evolution that makes migration from Splunk straightforward.
- **Search-first**: Bare `level=error` is a valid query. In SQL, you always need `SELECT ... FROM ... WHERE`.
- **Pipeline composability**: Each stage is independent. You can build queries incrementally by adding pipe stages.
- **Compatibility hints**: LynxDB detects SPL1 syntax and suggests the SPL2 equivalent, easing the learning curve.

## Why Go (Not Rust or C++)

**Decision**: Implement LynxDB in Go.

**Alternatives**: Rust, C++, Java.

**Reasoning**:

| Factor | Go | Rust | C++ | Java |
|--------|----|----|-----|------|
| **Build speed** | ~5 sec | ~60 sec | ~120 sec | ~30 sec |
| **Single binary** | Native (static link) | Native (static link) | Possible (complex) | Requires JVM |
| **Cross-compilation** | `GOOS=linux go build` | Possible (complex) | Very complex | JVM portability |
| **Concurrency** | Goroutines (M:N) | async/await + threads | Threads | Virtual threads |
| **Memory safety** | GC (no use-after-free) | Ownership (no GC) | Manual (risky) | GC |
| **Ecosystem** | Good (cloud-native) | Growing | Mature | Mature |
| **Contributor pool** | Large | Growing | Large | Large |

The key factors:

1. **Single static binary**: Go produces statically linked binaries by default. `go build` gives you one file that runs anywhere -- no runtime, no shared libraries, no JVM. This is essential for LynxDB's "one binary, every scale" philosophy.

2. **Fast compilation**: Go compiles the entire LynxDB codebase in under 5 seconds. This matters for contributor productivity and CI/CD speed. Rust compiles can take minutes for a project this size.

3. **Concurrency**: Go's goroutines and channels are ideal for LynxDB's architecture -- concurrent ingest (sharded memtable), streaming query execution (Volcano iterators pulling from multiple segments), and background operations (compaction, tiering).

4. **Performance**: Go's performance is sufficient. The bytecode VM achieves 22ns/op with zero allocations through careful design (fixed-size stack, no interfaces on the hot path). Where Go's GC could be a concern (the VM hot loop), we pre-allocate and reuse, eliminating GC pressure entirely.

5. **Contributor accessibility**: Go has a gentler learning curve than Rust. For an open-source project, this matters -- more potential contributors can ramp up quickly.

**Why not Rust?** Rust would give marginally better raw performance and eliminates GC pauses entirely. But LynxDB's hot paths are already zero-allocation, so GC is not a practical concern. The trade-off is significantly slower builds, a steeper contributor learning curve, and more complex cross-compilation.

**Why not C++?** Memory safety. A database that ingests arbitrary user data and evaluates user-provided expressions cannot afford use-after-free or buffer overflow vulnerabilities. Go's memory safety eliminates this entire class of bugs.

**Why not Java?** The JVM. LynxDB's idle memory footprint is ~50 MB. The JVM alone consumes hundreds of MB. A "single binary" in Java means shipping a JRE, which defeats the purpose.

## Why a Single Binary

**Decision**: Ship one static binary that works as a CLI tool, a server, and a cluster node.

**Alternatives**: Separate binaries for each role (e.g., `lynxdb-ingest`, `lynxdb-query`, `lynxdb-meta`), microservices architecture.

**Reasoning**:

1. **Operational simplicity**: One binary to install, update, and monitor. `curl | sh` gives you everything. No version matrix between components.

2. **Pipe mode**: The CLI `lynxdb query --file app.log '| stats count'` creates an ephemeral in-memory engine inside the same binary. This only works because the entire storage engine and query engine are linked into the binary. Separate binaries cannot offer this.

3. **Gradual scaling**: Start with `lynxdb server` on a laptop. When you need a cluster, add `--cluster.seeds` to the same binary. No re-architecture, no new binaries, no migration. Role selection is a config flag (`--cluster.role ingest`).

4. **Testing**: Every integration test exercises the real code path, not a mock or a separate test harness. The test binary includes the full server.

The trade-off is a larger binary size (~30 MB), which is negligible for a server-side application.

## Why Schema-on-Read

**Decision**: No upfront schema definition. Fields are discovered and indexed automatically. Structure is extracted at query time.

**Alternatives**: Schema-on-write (Elasticsearch mappings, ClickHouse table definitions), label-based (Grafana Loki).

**Reasoning**:

Log data is inherently heterogeneous:

- Different services emit different fields.
- Fields change between deployments (new fields added, old fields removed).
- The same field name may have different types across sources.
- Raw text logs (syslog, nginx access logs) have no schema at all.

Schema-on-write systems require defining the schema before ingesting data. When logs change shape (which they always do), you get mapping conflicts (Elasticsearch), ingestion failures, or the need to reindex.

Schema-on-read accepts any data and extracts structure at query time:

```spl
# Extract fields from raw text with rex
search "connection refused"
  | rex field=_raw "host=(?P<host>\S+) port=(?P<port>\d+)"
  | stats count by host, port
```

This is more flexible and more forgiving. The trade-off is that queries over unindexed fields require scanning, but the bloom filter and inverted index provide efficient full-text search for the common case, and materialized views handle the repeated-query case.

## Why FST + Roaring Bitmaps (Not Lucene)

**Decision**: Build a custom inverted index using FST (Finite State Transducers) for the term dictionary and roaring bitmaps for posting lists.

**Alternatives**: Embed Lucene (via cgo or a JNI bridge), use a simpler hash-based index, use token bloom filters (ClickHouse approach).

**Reasoning**:

**Why not Lucene?** Lucene is Java. Embedding it in a Go binary requires either cgo (with JNI, adding the JVM as a dependency) or a sidecar process. Both violate the "single binary, zero dependencies" principle. Lucene is also a general-purpose library with significant complexity (segment merging, codec versioning, near-real-time search) that LynxDB does not need.

**Why FST instead of a hash map?** FSTs share prefixes and suffixes between terms, making them 5-10x more compact than a hash map. They also support prefix iteration and fuzzy matching natively, which a hash map cannot do. The FST library (`github.com/blevesearch/vellum`) is a mature Go implementation with excellent performance.

**Why roaring bitmaps instead of compressed bitmaps or sorted arrays?** Roaring bitmaps automatically choose the best container type (array, bitmap, or run-length) based on density. This makes them efficient for both sparse posting lists (a rare term appearing in 10 events) and dense posting lists (a common term appearing in 90% of events). Set operations (AND, OR, NOT) are implemented with container-level specialization and operate at memory bandwidth speed.

**Why not token bloom filters (ClickHouse approach)?** Token bloom filters only support exact term membership tests. They cannot answer prefix queries, fuzzy queries, or phrase queries. They also have no notion of which events contain a term -- only whether the term exists in the segment at all. LynxDB's bloom filters serve the same segment-skipping role, but the FST + roaring inverted index adds row-level precision on top.

| Feature | LynxDB (FST + Roaring) | Lucene | Hash + Sorted Array | Token Bloom |
|---------|------------------------|--------|---------------------|-------------|
| Prefix search | Yes | Yes | No | No |
| Fuzzy search | Yes | Yes | No | No |
| Row-level posting lists | Yes | Yes | Yes | No |
| Compressed | Yes (FST + roaring) | Yes | Partially | Yes |
| Dependencies | Pure Go | JVM | None | None |
| Memory model | mmap | JVM heap | mmap | mmap |

## Why Volcano Iterator Model (Not Materialization)

**Decision**: Use the Volcano (pull-based iterator) model for query execution, not a materialization (batch) approach.

**Alternatives**: Full materialization (compute each stage fully before starting the next), vectorized batch processing (like DuckDB).

**Reasoning**:

The Volcano model excels at **short-circuiting**. `head 10` on 100M events reads only the first batch (1024 rows or fewer), not the entire dataset. This is the difference between 0.23ms and hundreds of milliseconds.

Log analytics queries frequently use `head`, `tail`, and `limit`. Interactive exploration (the Web UI, the CLI TUI) almost always limits results. The Volcano model makes these queries nearly instant regardless of data size.

The trade-off is that the Volcano model has higher per-row overhead than vectorized processing (function call per `Next()` vs. tight loop over a column vector). LynxDB compensates with:

- **1024-row batches**: Each `Next()` returns a batch, amortizing the function call overhead.
- **Bytecode VM**: Expression evaluation uses a stack-based VM with zero allocations, not interpreted AST walking.
- **Partial aggregation**: Aggregations are computed per-segment (batch-like) and merged globally (streaming).

The result is ~2.1M events/sec pipeline throughput, which is sufficient for single-node query volumes.

## Why Append-Only WAL (Not LSM Tree)

**Decision**: Use a simple append-only WAL for durability, not a full LSM tree.

**Alternatives**: LSM tree (RocksDB, LevelDB), B-tree (SQLite, BoltDB).

**Reasoning**:

Log data is **append-only by nature**. Events are never updated or deleted in place. The only mutations are:

1. Append (ingest a new event).
2. Delete (retention policy removes entire segments).

This means LynxDB does not need the complexity of an LSM tree's merge-on-read or a B-tree's in-place updates. A simple append-only WAL provides durability, and the separate memtable-to-segment flush path provides efficient batch writes.

The WAL + memtable + compaction architecture is simpler to reason about, debug, and tune than a general-purpose LSM tree. Every component serves one purpose:

- WAL: durability
- Memtable: fast writes + recent-event queries
- Segment writer: columnar encoding + indexing
- Compaction: merge small segments into large ones

## Related

- [Architecture Overview](/docs/architecture/overview) -- how these decisions manifest in the system
- [Storage Engine](/docs/architecture/storage-engine) -- WAL and compaction implementation
- [Segment Format](/docs/architecture/segment-format) -- columnar encoding details
- [Query Engine](/docs/architecture/query-engine) -- Volcano model and bytecode VM
- [Indexing](/docs/architecture/indexing) -- FST + roaring bitmap implementation

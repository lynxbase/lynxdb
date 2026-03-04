# LynxDB
<div align="center">
  <picture>
    <img alt="LynxDB logo" src="docs/assets/lynxdb-logo-transparent.png" height="300">
  </picture>
</div>

<p align="center">
  <a href="https://github.com/lynxbase/lynxdb/releases"><img src="https://img.shields.io/github/v/release/lynxbase/lynxdb?color=brightgreen&display_name=tag" alt="Latest Release"></a>
  <a href="https://github.com/lynxbase/lynxdb/actions/workflows/ci.yaml"><img src="https://img.shields.io/github/actions/workflow/status/lynxbase/lynxdb/ci.yaml?branch=main&label=build" alt="Build Status"></a>
  <a href="https://docs.lynxdb.org/"><img src="https://img.shields.io/badge/docs-lynxdb.org-blue" alt="Docs"></a>
</p>

SPL2-powered log analytics. Single binary. No dependencies.

## Quick start

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

Pipe logs through lynxdb — no server, no config:

```bash
kubectl logs deploy/api | lynxdb query '| stats avg(duration_ms), p99(duration_ms) by endpoint'
cat app.json | lynxdb query '| where duration_ms > 1000 | stats count by endpoint'
```

Or run as a persistent server:

```bash
lynxdb server
lynxdb ingest nginx_access.log --source nginx_access --index balancer --batch-size 100000
lynxdb query 'index balancer | status>=500 | stats count, p99(duration_ms) by uri | sort -count'
```

Generate sample data and explore:

```bash
# Start the demo (streams realistic logs from 4 sources at 200 events/sec)
lynxdb demo

# Try in another terminal:
lynxdb query '_source=nginx | stats count by status'
lynxdb query 'level=ERROR | stats count by host' --since 5m
lynxdb tail 'level=ERROR'

```

## Features

- **Pipe mode** — reads from stdin or files, works like `grep`. No server, no config.
- **SPL2** — `stats`, `eval`, `rex`, `timechart`, `join`, CTEs, and [more](https://docs.lynxdb.org//spl2)
- **Full-text search** — FST inverted index + roaring bitmaps, bloom filters for segment skipping
- **Columnar storage** — custom `.lsg` format, delta-varint timestamps, dictionary encoding, Gorilla XOR, LZ4
- **Materialized views** — precomputed aggregations with automatic query rewrite, up to ~400× speedup
- **Cluster mode** — add `--cluster.seeds` to go distributed; S3-backed shared storage
- **Drop-in ingestion** — Elasticsearch `_bulk`, OpenTelemetry OTLP, Splunk HEC

## Comparison

|                   | lynxdb        | Splunk        | Elasticsearch | Loki               |
|-------------------|---------------|---------------|---------------|--------------------|
| Deployment        | Single binary | Standalone    | Cluster       | Single binary      |
| Dependencies      | None          | —             | JVM           | Object storage     |
| Query language    | SPL2          | SPL           | Lucene/ES\|QL | LogQL              |
| Pipe mode         | ✓             | —             | —             | —                  |
| Full-text index   | FST + bitmaps | tsidx         | Lucene        | Label index only   |
| Memory (idle)     | ~50 MB        | ~12 GB        | ~1 GB+        | ~256 MB            |
| License           | Apache 2.0    | Commercial    | ELv2 / AGPL   | AGPL               |

## Configuration

Zero config needed — sensible defaults for everything. Customize in `~/.config/lynxdb/config.yaml`:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/data/lynxdb"
retention: 30d

storage:
  compression: lz4
  cache_max_bytes: 4gb
```

Cascade: CLI flags → `LYNXDB_*` env vars → config file → defaults.

<details>
<summary>Full configuration reference</summary>

```yaml
listen: "0.0.0.0:3100"
data_dir: "/data/lynxdb"
retention: 30d

storage:
  compression: lz4          # lz4 | zstd
  flush_threshold: 512mb
  cache_max_bytes: 4gb
  s3_bucket: my-logs-bucket
  s3_region: us-east-1

query:
  max_concurrent: 20
  max_query_runtime: 10m
```

</details>

## CLI reference

```
lynxdb server                start server
lynxdb query <spl>           run a query
lynxdb tail <spl>            live tail
lynxdb ingest <file>         ingest a file
lynxdb shell                 interactive REPL with completion
lynxdb count <spl>           quick event count
lynxdb sample N <spl>        peek at data shape
lynxdb watch <spl> -i 5s     periodic refresh with deltas
lynxdb diff <spl> -p 1h      this period vs previous period
lynxdb explain <spl>         query plan without executing
lynxdb mv create/list        materialized views
lynxdb status                server metrics
lynxdb bench                 benchmark
lynxdb demo                  generate sample data
```

## Build from source

```bash
git clone https://github.com/lynxbase/lynxdb && cd lynxdb
go build -o lynxdb ./cmd/lynxdb/
go test ./...
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## Feedback
- [Issues](https://github.com/lynxbase/lynxdb/issues)

---

LynxDB wouldn't exist without the projects that inspired it:

- **[Splunk](https://www.splunk.com/)** - for creating SPL, the most expressive log query language. LynxDB's SPL2 implementation is a love letter to Splunk's query design.
- **[ClickHouse](https://clickhouse.com/)** - for proving that a single-binary analytical database with incredible performance is possible. The MergeTree architecture deeply influenced LynxDB's storage engine design.
- **[VictoriaLogs](https://docs.victoriametrics.com/victorialogs/)** - for showing that log analytics can be resource-efficient and operationally simple.
- **`grep`, `awk`, `sed`** - for the Unix philosophy of composable tools and piping. LynxDB's pipe mode is a direct homage to this tradition.

This project started in early 2025 out of a deep appreciation for these tools and a desire to bring Splunk-level analytics to everyone in a single, lightweight binary.

## License

[Apache 2.0](LICENSE)

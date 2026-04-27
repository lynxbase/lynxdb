# LynxDB
<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/lynxdb-logo-transparent-w.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/lynxdb-logo-transparent.png">
    <img alt="LynxDB logo" src="docs/assets/lynxdb-logo-transparent.png" height="300">
  </picture>
</div>

<p align="center">
  <a href="https://github.com/lynxbase/lynxdb/releases"><img src="https://img.shields.io/github/v/release/lynxbase/lynxdb?color=brightgreen&display_name=tag" alt="Latest Release"></a>
  <a href="https://github.com/lynxbase/lynxdb/actions/workflows/ci.yaml"><img src="https://img.shields.io/github/actions/workflow/status/lynxbase/lynxdb/ci.yaml?branch=main&label=build" alt="Build Status"></a>
  <a href="https://docs.lynxdb.org/"><img src="https://img.shields.io/badge/docs-lynxdb.org-blue" alt="Docs"></a>
</p>

Log analytics in a single binary. No dependencies. Lynx Flow query language.

> LynxDB is in active development and **not yet production-ready**. APIs, storage format, and query behavior may change without notice between releases. Feedback and contributions are welcome

<p align="center">
  <img src="docs/assets/pg_demo.gif" alt="LynxDB demo">
</p>

## Lynx Flow

Lynx Flow is LynxDB's query language - a pipeline language where data flows left-to-right through commands separated by `|`. Commands are named for what they do: `parse`, `let`, `where`, `group`, `order by`, `take`.

```
from nginx
| parse combined(_raw)
| status >= 500
| group by uri compute count() as hits, avg(duration_ms) as latency
| order by hits desc
| take 10
```

## Quick start

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

Pipe logs through lynxdb - no server, no config:

```bash
# From raw logs to p99 latency in one line
kubectl logs deploy/api | lynxdb query '
      | group by endpoint compute avg(duration_ms), perc99(duration_ms)'

# Three nested formats, one pipeline, zero config
docker logs api-server 2>&1 | lynxdb query '
      | parse docker(_raw)
      | parse json(message)
      | explode errors
      | group by errors.code, errors.service compute count() as cnt
      | order by cnt desc | take 10'

# Wildcard array extraction - like jq, but with aggregation:
cat orders.json | lynxdb query '
      | json items[*].price AS price, items[*].product AS product
      | explode product, price                     
      | let revenue = price * qty                          
      | group by product compute sum(revenue) as total_revenue          
      | order by total_revenue desc''
```

Or run as a persistent server:

```bash
lynxdb server
lynxdb ingest nginx_access.log --source nginx_access --index balancer --batch-size 100000
lynxdb query '
      | parse combined(_raw)
      | method="POST" AND status < 300
      | parse json(request_body)
      | json items[*].sku AS skus
      | explode skus
      | group by skus compute count() as purchases, dc(client_ip) as unique_buyers
      | order by purchases desc
      | take 20'
```

Generate sample data and explore:

```bash
# Start the demo (streams realistic logs from 4 sources at 200 events/sec)
lynxdb demo

# Try in another terminal:
lynxdb query 'from nginx | group by status compute count()'
lynxdb query '| level="ERROR" | group by host compute count()' --since 5m
lynxdb tail 'level=ERROR'

```

## Features

- **Pipe mode** - reads from stdin or files, works like `grep`. No server, no config.
- **Lynx Flow** - `group`, `let`, `parse`, `order by`, `join`, CTEs, domain sugar, and [more](https://docs.lynxdb.org/docs/lynx-flow/overview). Partial SPL2 compatibility.
- **Full-text search** - FST inverted index + roaring bitmaps, bloom filters for segment skipping
- **Columnar storage** - custom `.lsg` format, delta-varint timestamps, dictionary encoding, Gorilla XOR, LZ4
- **Materialized views** - precomputed aggregations with automatic query rewrite, up to ~400x speedup
- **Cluster mode** - add `--cluster.seeds` to go distributed; S3-backed shared storage
- **Drop-in ingestion** - Elasticsearch `_bulk`, OpenTelemetry OTLP, Splunk HEC

## Comparison

|                   | lynxdb           | Splunk        | Elasticsearch | Loki               |
|-------------------|------------------|---------------|---------------|--------------------|
| Deployment        | Single binary    | Standalone    | Cluster       | Single binary      |
| Dependencies      | None             | --            | JVM           | Object storage     |
| Query language    | Lynx Flow / SPL2 | SPL           | Lucene/ES\|QL | LogQL              |
| Pipe mode         | Yes              | --            | --            | --                 |
| Full-text index   | FST + bitmaps    | tsidx         | Lucene        | Label index only   |
| Memory (idle)     | ~50 MB           | ~12 GB        | ~1 GB+        | ~256 MB            |
| License           | Apache 2.0       | Commercial    | ELv2 / AGPL   | AGPL               |

## Configuration

Zero config needed - sensible defaults for everything. Customize in `~/.config/lynxdb/config.yaml`:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/data/lynxdb"
retention: 30d

storage:
  compression: lz4
  cache_max_bytes: 4gb
```

Cascade: CLI flags -> `LYNXDB_*` env vars -> config file -> defaults.

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
lynxdb query <query>         run a query (Lynx Flow or SPL2)
lynxdb tail <query>          live tail
lynxdb ingest <file>         ingest a file
lynxdb shell                 interactive REPL with completion
lynxdb count <query>         quick event count
lynxdb sample N <query>      peek at data shape
lynxdb watch <query> -i 5s   periodic refresh with deltas
lynxdb diff <query> -p 1h    this period vs previous period
lynxdb explain <query>       query plan without executing
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

- **[Splunk](https://www.splunk.com/)** - for creating SPL, the most expressive log query language. LynxDB's SPL2 compatibility and Lynx Flow design owe everything to Splunk's query model.
- **[ClickHouse](https://clickhouse.com/)** - for proving that a single-binary analytical database with incredible performance is possible. The MergeTree architecture deeply influenced LynxDB's storage engine design.
- **[VictoriaLogs](https://docs.victoriametrics.com/victorialogs/)** - for showing that log analytics can be resource-efficient and operationally simple.
- **`grep`, `awk`, `sed`** - for the Unix philosophy of composable tools and piping. LynxDB's pipe mode is a direct homage to this tradition.

This project started in early 2025 out of a deep appreciation for these tools and a desire to bring Splunk-level analytics to everyone in a single, lightweight binary.


## Star History

<a href="https://www.star-history.com/?repos=lynxbase%2Flynxdb&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=lynxbase/lynxdb&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=lynxbase/lynxdb&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=lynxbase/lynxdb&type=date&legend=top-left" />
 </picture>
</a>

## License

[Apache 2.0](LICENSE)

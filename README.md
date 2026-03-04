[//]: # (<p align="center">)

[//]: # (  <img src="docs/assets/lynxdb-logo.png" alt="LynxDB" width="280" />)

[//]: # (</p>)

<h3 align="center">Log analytics in a single binary</h3>

<p align="center">
  <strong>Zero dependencies · SPL2 query language · ~50 MB memory · Pipe mode to 1000-node clusters</strong>
</p>

<p align="center">
  <a href="https://github.com/lynxbase/lynxdb/releases"><img src="https://img.shields.io/github/v/release/lynxbase/Lynxdb?color=blue&label=release" alt="Release"></a>
  <a href="https://github.com/lynxbase/lynxdb/actions/workflows/ci.yaml"><img src="https://img.shields.io/github/actions/workflow/status/lynxbase/Lynxdb/ci.yaml?branch=main&label=CI" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/lynxbase/lynxdb"><img src="https://goreportcard.com/badge/github.com/lynxbase/lynxdb" alt="Go Report"></a>
  <a href="https://codecov.io/gh/lynxbase/Lynxdb"><img src="https://img.shields.io/codecov/c/github/lynxbase/Lynxdb" alt="Coverage"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License"></a>
  <a href="https://docs.lynxdb.org"><img src="https://img.shields.io/badge/docs-lynxdb.org-blue" alt="Docs"></a>
  <a href="https://discord.gg/RgggCFdgWK"><img src="https://img.shields.io/discord/000000000?label=discord&color=5865F2" alt="Discord"></a>
</p>

<p align="center">
  <a href="https://docs.lynxdb.org/docs/getting-started/quickstart">Quickstart</a> ·
  <a href="https://docs.lynxdb.org/docs/spl2/overview">SPL2 Reference</a> ·
  <a href="https://docs.lynxdb.org/docs/api/overview">API Docs</a> ·
  <a href="https://docs.lynxdb.org/docs/deployment/single-node/">Deployment Guide</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

---

LynxDB is an open-source log analytics database built from scratch in Go. A single static binary that works as a pipe-mode CLI tool (like `grep` meets `awk`), a standalone server, or a distributed cluster - same binary, same query language, same API at every scale.

Born out of love for Splunk's query language and Unix philosophy of `grep`/`awk`/`sed`, LynxDB brings SPL2-powered analytics to everyone - from a developer's laptop to production infrastructure.

```bash
# Pipe mode - no server, no config, instant results
kubectl logs deploy/api | lynxdb query '| stats avg(duration_ms), p99(duration_ms) by endpoint | sort -avg(duration_ms)'

# Server mode - persistent storage, full API
lynxdb server
lynxdb query 'source=nginx status>=500 | stats count, avg(duration_ms) by uri | sort -count | head 10'
```

## Quick Start

### Install

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

### 60 Seconds to First Insight

```bash
# 1. Start the demo - generates realistic logs from 4 sources at 200 events/sec
lynxdb demo &

# 2. Find which endpoints are failing and how slow they are
lynxdb query 'source=nginx status>=500
  | stats count, avg(duration_ms) as avg_lat, p99(duration_ms) as p99_lat by uri
  | sort -count
  | head 5'

# 3. Correlate error spikes with slow database queries
lynxdb query 'source=postgres duration_ms>1000
  | bin _timestamp span=5m
  | stats count as slow_queries, avg(duration_ms) as avg_latency by _timestamp
  | where slow_queries > 10'

# 4. Live tail errors across all sources
lynxdb tail 'level=error | eval sev=upper(level) | fields _timestamp, source, sev, message'
```

Or skip the server entirely - query any file or stdin:

```bash
lynxdb query --file '/var/log/nginx/*.log' '| where status>=500 | stats count, p99(duration_ms) by uri | sort -count'
cat app.json | lynxdb query '| stats dc(user_id) as unique_users, avg(duration_ms) by endpoint | sort -unique_users'
docker logs myapp 2>&1 | lynxdb query '| search "OOM" | stats count by container'
```

## How LynxDB Compares

Every tool in this space makes different trade-offs. Here's where LynxDB sits:

| | LynxDB | Splunk | Elasticsearch | Loki | ClickHouse |
|---|---|---|---|---|---|
| **Deployment** | Single binary | Standalone or distributed | Single node or cluster | Single binary or microservices | Single binary or cluster |
| **Dependencies** | None | - | Bundled JVM | Object storage (prod) | Keeper (for replication) |
| **Query language** | SPL2 | SPL | Lucene DSL / ES|QL | LogQL | SQL |
| **Pipe mode (no server)** | ✓ | - | - | - | ✓ (`clickhouse-local`) |
| **Schema** | On-read | On-read | On-write | Labels + line | On-write |
| **Full-text index** | FST + roaring bitmaps | tsidx | Lucene | Label index only | Token bloom filter |
| **Memory (idle)** | ~50 MB | ~12 GB (min spec) | ~1 GB+ | ~256 MB | ~1 GB |
| **License** | Apache 2.0 | Commercial | ELv2 / AGPL | AGPL | Apache 2.0 |

LynxDB's sweet spot: the power of Splunk's query language with the simplicity of a single binary and the Unix philosophy of piping data through tools.

## Features

### One Binary, Every Scale

```
Developer laptop  →  cat app.log | lynxdb query '| stats count by level'
Single server     →  lynxdb server --data-dir /var/lib/lynxdb
3-node HA         →  lynxdb server --cluster.seeds node1:9400,node2:9400
1000-node fleet   →  lynxdb server --cluster.role query --cluster.seeds meta1:9400
```

Role selection is a config flag. Small clusters run all roles on every node. At scale, split meta/ingest/query for independent scaling. S3 is the shared source of truth - nodes are stateless after flush.

### SPL2 - One Query Language for Everything

CLI, API, alerts, dashboards, materialized views - one language everywhere. Accidentally write SPL1 syntax? LynxDB detects it and suggests the SPL2 equivalent.

```spl
# Find the slowest endpoints with error rates above 5%
source=nginx
  | stats count as total,
          count(eval(status>=500)) as errors,
          avg(duration_ms) as avg_latency,
          p99(duration_ms) as p99_latency
    by uri
  | eval error_rate = round(errors/total*100, 1)
  | where error_rate > 5
  | sort -error_rate
  | table uri, total, errors, error_rate, avg_latency, p99_latency

# Time series - error count by source every 5 minutes
level=error | timechart count span=5m by source

# Field extraction at query time - no upfront schema needed
search "connection refused"
  | rex field=_raw "host=(?P<host>\S+) port=(?P<port>\d+)"
  | stats count by host, port
  | sort -count

# Cross-source security correlation with CTEs
$threats = FROM idx_backend WHERE threat_type IN ("sqli", "path_traversal") | FIELDS client_ip, threat_type;
$failed  = FROM idx_audit WHERE type="USER_LOGIN" AND res="failed" | STATS count AS failures BY src_ip;
FROM $threats | JOIN type=inner client_ip [$failed] | WHERE failures > 5
  | TABLE client_ip, threat_type, failures
```

<details>
<summary><strong>Full SPL2 reference</strong></summary>

**Commands:** `FROM`, `SEARCH`, `WHERE`, `EVAL`, `STATS`, `SORT`, `TABLE`, `FIELDS`, `RENAME`, `HEAD`, `TAIL`, `DEDUP`, `REX`, `BIN`, `TIMECHART`, `TOP`, `RARE`, `STREAMSTATS`, `EVENTSTATS`, `JOIN`, `APPEND`, `MULTISEARCH`, `TRANSACTION`, `XYSERIES`, `FILLNULL`, `LIMIT`

**Aggregations:** `count`, `sum`, `avg`, `min`, `max`, `dc` (distinct count), `values`, `stdev`, `perc50`/`perc75`/`perc90`/`perc95`/`perc99`, `earliest`, `latest`

**Eval functions:** `IF`, `CASE`, `match`, `coalesce`, `tonumber`, `tostring`, `round`, `substr`, `lower`, `upper`, `len`, `ln`, `mvjoin`, `mvappend`, `mvdedup`, `isnotnull`, `isnull`, `strftime`

</details>

### CLI That Respects Your Time

TTY-aware formatting, human-readable numbers, structured errors with fix suggestions, and zero boilerplate.

```bash
# Smart error messages with caret positioning and suggestions
$ lynxdb query 'level=error | staats count'
  ✖ Unknown command 'staats'

    level=error | staats count
                  ^^^^^^
    Did you mean: stats

# Progress bars for long queries, spinners for fast ones
  Scanning ━━━━━━━━━━━━━━━━━━━╸━━━━━━━━━━  52%  42.3B/84.7B events  ETA 18s

# Human-readable numbers in terminal, raw JSON when piped
$ lynxdb query '| stats count' --since 1h
  847,291
  ✔ 89ms - scanned 12.4M events

$ lynxdb query '| stats count' --since 1h | jq .
  {"count": 847291}

# Zero-result guidance with field value suggestions
$ lynxdb query 'level=CRITICAL'
  No results found.
  • Known values for 'level': info, error, warn, debug
  • Try: lynxdb query 'level=error' --since 1h

# Interactive shell with history and completion
$ lynxdb shell
  lynxdb> source=nginx | stats avg(duration_ms), p99(duration_ms) by uri | where avg(duration_ms) > 100
```

```bash
# Daily workflow shortcuts
lynxdb count 'level=error' --since 1h          # Quick count
lynxdb sample 5 'source=nginx'                 # Peek at data shape
lynxdb tail 'level=error'                      # Live stream with catchup
lynxdb watch '| stats count by level' -i 5s    # Periodic refresh with deltas
lynxdb diff '| stats count by source' -p 1h    # This hour vs previous hour
lynxdb explain 'status>=500 | stats count'     # Query plan without executing
```

### Storage Engine Built for Logs

Custom `.lsg` segment format purpose-built for log analytics - not a wrapper around an existing storage library.

- **Full-text search** - FST-based inverted index with roaring bitmap posting lists, bloom filters per row group for term-level segment skipping
- **Adaptive encoding** - delta-varint for timestamps, dictionary encoding for low-cardinality strings, Gorilla XOR for floats, LZ4 for raw text
- **Zero-copy reads** - memory-mapped segments with prefetch: row group N+1 loads while N is being processed
- **Tiered storage** - Hot (SSD) → Warm (S3) → Cold (Glacier), with lazy column-level fetching for remote segments
- **WAL + sharded memtable** - one shard per CPU core, lock-free concurrent ingestion, batch sync every 100ms
- **Size-tiered compaction** - L0 → L1 → L2, rate-limited to avoid I/O starvation

### Query Engine

Streaming execution with a 23-rule optimizer and zero-allocation bytecode VM.

| Metric | Value |
|--------|-------|
| VM predicate (`status >= 500`) | 22 ns/op, 0 allocs |
| VM compound predicate | 55 ns/op, 0 allocs |
| Streaming `head 10` on 100K events | 0.23 ms |
| Cache hit latency | 299 ns |
| Pipeline throughput (WHERE + STATS) | ~2.1M events/sec |
| Single-node ingest | 300K+ events/sec |
| MV-accelerated query | ~400× speedup |

Volcano iterator model with 1024-row batches - `head 10` on 100M events reads 1 batch, not the dataset. The optimizer handles predicate pushdown, column pruning, bloom pruning, TopK pushdown, partial aggregation, and automatic MV rewrite.

### Materialized Views

Precomputed aggregations that accelerate repeated queries by up to 400×. Automatic backfill, versioned rebuilds (zero downtime), cascading views, and retention policies.

```bash
# Create - backfills existing data automatically
lynxdb mv create mv_errors_5m \
  'level=error | stats count, avg(duration) by source, time_bucket(timestamp, "5m") AS bucket' \
  --retention 90d

# Queries are automatically accelerated - no changes to your queries needed
$ lynxdb query 'level=error | stats count by source' --since 7d

  SOURCE      COUNT
  ─────────────────────
  nginx       142,847
  api-gw       89,234

  ⚡ Accelerated by mv_errors_5m (~400x, 3ms vs ~1.2s)

# Stack views on views for multi-granularity rollups
lynxdb mv create mv_errors_1h \
  '| from mv_errors_5m | stats sum(count) AS count by source, time_bucket(bucket, "1h") AS hour' \
  --retention 365d
```

### Drop-in Compatibility

Migrate existing log pipelines without changing your config.

```yaml
# Filebeat → LynxDB
output.elasticsearch:
  hosts: ["http://lynxdb:3100/api/v1/es"]

# OpenTelemetry Collector → LynxDB
exporters:
  otlphttp:
    endpoint: http://lynxdb:3100/api/v1/otlp
```

Elasticsearch `_bulk` API, OpenTelemetry OTLP, and Splunk HEC compatible. Timestamp auto-detection across `_timestamp`, `timestamp`, `@timestamp`, `time`, `ts`, `datetime`. Schema-on-read - send any JSON, any format, fields are indexed automatically.

### Alerts & Dashboards

SPL2-powered alerting with 8 notification channels (Slack, Telegram, PagerDuty, OpsGenie, incident.io, email, webhook, generic HTTP) and panel-based dashboards with template variables.

```bash
lynxdb alerts create \
  --name "Error rate spike" \
  --query 'level=error | stats count as errors | where errors > 100' \
  --interval 5m \
  --channel slack:webhook_url=https://hooks.slack.com/... \
  --channel pagerduty:routing_key=...,severity=critical

lynxdb alerts                              # List all alerts
lynxdb alerts alt_xyz789 test              # Dry-run without notifications
lynxdb alerts alt_xyz789 test-channels     # Verify connectivity
```

### Benchmark

```bash
lynxdb bench --events 100000

# LynxDB Benchmark - 100,000 events
# ============================================================
# Ingest:   100,000 events in 312ms (320,512 events/sec)
#
# QUERY                                    RESULTS       TIME
# ----------------------------------------------------------------
# Filtered aggregate                             1      4.2ms
# Full scan aggregate                            5      8.7ms
# Full-text search                             142      3.1ms
# Range filter + top                            10      5.8ms
# Time bucketed                                 48      6.3ms
```

## CLI Reference

```bash
# Server
lynxdb server                                        # Start with defaults
lynxdb server --data-dir /data/lynxdb                # Custom storage
lynxdb server --s3-bucket my-logs                    # S3 tiering

# Query (against server)
lynxdb query 'level=error | stats count, avg(duration_ms) by source | sort -count'
lynxdb query 'source=nginx | top 10 uri' --since 1h --format table

# Query (local files - no server needed)
lynxdb query --file access.log '| stats count by status'
cat app.json | lynxdb query '| where duration_ms > 1000 | stats avg(duration_ms) by endpoint'

# Live tail
lynxdb tail 'level=error'                            # Stream errors in real time

# Shortcuts
lynxdb count 'level=error' --since 1h                # Quick count
lynxdb sample 5 'source=nginx'                       # Random events
lynxdb watch '| stats count by level' -i 5s          # Periodic refresh
lynxdb diff '| stats count by source' -p 1h          # Compare time periods

# Ingest
lynxdb ingest access.log --source web-01

# Interactive shell
lynxdb shell                                         # REPL with completion

# Materialized views
lynxdb mv create <n> <query> [--retention 90d]
lynxdb mv list
lynxdb mv status <n>

# Management
lynxdb status                    # Server metrics
lynxdb doctor                    # Diagnose environment
lynxdb fields                    # Field catalog
lynxdb explain '<query>'         # Query plan
lynxdb config                    # Show config
lynxdb bench --events 100000     # Benchmark
lynxdb demo                      # Generate sample data
```

## Configuration

Zero config needed - sensible defaults for everything. Customize when you want to.

```yaml
# ~/.config/lynxdb/config.yaml
listen: "0.0.0.0:3100"
data_dir: "/data/lynxdb"
retention: 30d

storage:
  compression: lz4
  flush_threshold: 512mb
  s3_bucket: my-logs-bucket
  s3_region: us-east-1
  cache_max_bytes: 4gb

query:
  max_concurrent: 20
  max_query_runtime: 10m
```

**Cascade:** CLI flags → environment variables (`LYNXDB_*`) → config file → defaults. Hot-reload without restart for log level, query limits, and retention.

## Acknowledgments

LynxDB wouldn't exist without the projects that inspired it:

- **[Splunk](https://www.splunk.com/)** - for creating SPL, the most expressive log query language. LynxDB's SPL2 implementation is a love letter to Splunk's query design.
- **[ClickHouse](https://clickhouse.com/)** - for proving that a single-binary analytical database with incredible performance is possible. The MergeTree architecture deeply influenced LynxDB's storage engine design.
- **[VictoriaLogs](https://docs.victoriametrics.com/victorialogs/)** - for showing that log analytics can be resource-efficient and operationally simple.
- **`grep`, `awk`, `sed`** - for the Unix philosophy of composable tools and piping. LynxDB's pipe mode is a direct homage to this tradition.

This project started in early 2025 out of a deep appreciation for these tools and a desire to bring Splunk-level analytics to everyone in a single, lightweight binary.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for architecture overview, project structure, and development setup.

```bash
git clone https://github.com/lynxbase/lynxdb.git && cd Lynxdb
go build -o lynxdb ./cmd/lynxdb/
go test ./...
```

## Community

- **Docs:** [docs.lynxdb.org](https://docs.lynxdb.org)
- **Discord:** [discord.gg/RgggCFdgWK](https://discord.gg/RgggCFdgWK)
- **Issues:** [Bug reports and feature requests](https://github.com/lynxbase/lynxdb/issues)

## License

[Apache License 2.0](LICENSE)

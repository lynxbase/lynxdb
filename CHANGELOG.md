# Changelog

All notable changes to LynxDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Storage engine**: Columnar segment format (`.lsg` V2) with delta-varint timestamps, LZ4 compression, dictionary-encoded strings, Gorilla-encoded floats.
- **Full-text search**: FST-based inverted index with roaring bitmap posting lists and bloom filters for segment skipping.
- **Write-ahead log (WAL)**: Append-only WAL with configurable sync policy and crash recovery.
- **Compaction**: Size-tiered compaction (L0 -> L1 -> L2) with rate limiting.
- **Tiered storage**: Hot (SSD) -> Warm (S3) -> Cold (Glacier) with automatic policy-driven tiering and local segment cache.
- **SPL2 query language**: Full parser with 20+ commands, 15+ aggregation functions, 20+ eval functions, CTEs, and subsearches.
- **Query engine**: Volcano iterator model with 18 streaming operators, stack-based bytecode VM (22ns/op, 0 allocs), and 23-rule optimizer.
- **REST API**: Ingest (JSON/NDJSON/plain text), query (sync/async/streaming), live tail (SSE), field catalog, and management endpoints.
- **Compatibility layer**: Elasticsearch `_bulk` API, OpenTelemetry OTLP/HTTP, and Splunk HEC receivers.
- **Pipe mode**: Query local files and stdin with the full SPL2 engine — no server required.
- **Materialized views**: Precomputed aggregations with automatic backfill, versioned rebuilds, retention policies, and cascading views.
- **Alerts**: SPL2-powered alerting with multi-channel notifications (webhook, Slack, Telegram).
- **Dashboards**: Panel-based dashboards with grid layout and template variables.
- **Live tail**: Real-time SSE streaming with historical catchup and full SPL2 pipeline support.
- **Field catalog**: Automatic field discovery with types, coverage stats, and top values.
- **CLI**: `server`, `query`, `ingest`, `status`, `mv`, `config`, `bench`, `demo`, and shell completion.
- **Interactive TUI**: Colorized JSON output, progress tracking, and query statistics when stdout is a TTY.
- **Benchmark command**: Built-in `lynxdb bench` for self-testing ingest and query performance.
- **Demo mode**: `lynxdb demo` generates realistic log traffic from nginx, api-gateway, postgres, and redis.
- **Install script**: `curl -fsSL https://lynxdb.org/install.sh | sh` with platform auto-detection and checksum verification.
- **Docker images**: Multi-arch (`amd64`/`arm64`) scratch-based images on Docker Hub.
- **Homebrew tap**: `brew install lynxbase/tap/lynxdb`.

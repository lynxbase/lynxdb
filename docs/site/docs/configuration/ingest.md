---
title: Ingest Settings
description: Configure LynxDB ingest pipeline -- max body size, batch size, and WAL fsync settings.
---

# Ingest Settings

The `ingest` section controls how the HTTP ingest endpoint accepts and processes incoming data.

## Max Body Size

The maximum size of a single HTTP request body for ingest endpoints.

| Config Key | `ingest.max_body_size` |
|---|---|
| **Env Var** | `LYNXDB_INGEST_MAX_BODY_SIZE` |
| **Default** | `10mb` |

```yaml
ingest:
  max_body_size: "10mb"
```

Requests exceeding this limit receive a `413 Payload Too Large` response. Increase for bulk imports or if your log lines are very large:

```bash
LYNXDB_INGEST_MAX_BODY_SIZE=50mb lynxdb server
```

:::tip
For large bulk imports, use the CLI `lynxdb ingest` or `lynxdb import` commands which automatically chunk data into batches, rather than sending one huge HTTP request.
:::

## Max Batch Size

The maximum number of events in a single ingest batch.

| Config Key | `ingest.max_batch_size` |
|---|---|
| **Env Var** | `LYNXDB_INGEST_MAX_BATCH_SIZE` |
| **Default** | `1000` |

```yaml
ingest:
  max_batch_size: 1000
```

This limits the number of events accepted in a single HTTP request. The CLI `--batch-size` flag controls the client-side batch size:

```bash
# Client-side batching (default: 5000 lines per batch)
lynxdb ingest access.log --batch-size 10000
```

## WAL Settings

The WAL (Write-Ahead Log) ensures data durability. Every event is written to the WAL before entering the memtable.

WAL settings are in the `storage` section but directly affect ingest behavior:

```yaml
storage:
  wal_sync_mode: "write"         # none, write, or fsync
  wal_sync_interval: "100ms"     # Batch sync interval
  wal_sync_bytes: "0"            # Sync after N bytes (0 = interval-only)
  wal_max_segment_size: "256mb"  # WAL segment rotation size
```

### Choosing a Sync Mode

| Mode | Data at Risk | Throughput | Use Case |
|------|-------------|------------|----------|
| `none` | All data since last OS flush | Highest | Development, ephemeral data |
| `write` | Up to `wal_sync_interval` (100ms default) | High | Production (default) |
| `fsync` | None | Lowest | Mission-critical compliance workloads |

For most production workloads, `write` mode with the default 100ms interval provides a good balance between durability and performance. At worst, you lose 100ms of data on a crash.

```bash
# Maximum durability
LYNXDB_STORAGE_WAL_SYNC_MODE=fsync lynxdb server

# Maximum throughput (development)
LYNXDB_STORAGE_WAL_SYNC_MODE=none lynxdb server
```

## Ingest Endpoints

LynxDB accepts data through multiple HTTP endpoints:

| Endpoint | Format | Description |
|----------|--------|-------------|
| `POST /api/v1/ingest` | JSON, NDJSON, plain text | Primary ingest endpoint |
| `POST /api/v1/ingest/bulk` | Elasticsearch `_bulk` format | Drop-in Elasticsearch compatibility |
| OTLP/HTTP | OpenTelemetry protobuf | Native OTLP receiver |
| Splunk HEC | Splunk HEC JSON | Splunk forwarder compatibility |

### Timestamp Auto-Detection

LynxDB automatically detects timestamps from these fields (in order): `_timestamp`, `timestamp`, `@timestamp`, `time`, `ts`, `datetime`. If no timestamp field is found, the current server time is used.

## Ingest via CLI

The CLI provides two commands for sending data to a running server:

### `lynxdb ingest` -- Raw Log Lines

```bash
# From file
lynxdb ingest access.log
lynxdb ingest access.log --source web-01 --sourcetype nginx

# From stdin
cat events.json | lynxdb ingest

# Custom batch size
lynxdb ingest huge.log --batch-size 10000
```

### `lynxdb import` -- Structured Data

```bash
# NDJSON
lynxdb import events.ndjson

# CSV with headers
lynxdb import splunk_export.csv

# Elasticsearch _bulk export
lynxdb import es_dump.json --format esbulk

# Dry run (validate without importing)
lynxdb import events.json --dry-run

# Apply transform during import
lynxdb import events.json --transform '| where level!="DEBUG"'
```

## Complete Example

```yaml
ingest:
  max_body_size: "50mb"
  max_batch_size: 5000

storage:
  wal_sync_mode: "write"
  wal_sync_interval: "100ms"
  wal_max_segment_size: "256mb"
```

## Tuning Guidelines

| Scenario | Recommendation |
|---|---|
| High-throughput ingest (>100K events/sec) | Increase `max_body_size` to `50mb`, use `wal_sync_mode: write` |
| Bulk import from files | Use `lynxdb import` with `--batch-size 10000` |
| Mission-critical audit logs | Use `wal_sync_mode: fsync` |
| Development/testing | Use `wal_sync_mode: none` for maximum speed |
| Large log lines (>100KB each) | Increase `max_body_size` accordingly |

## Next Steps

- [Storage Settings](/docs/configuration/storage) -- memtable and compaction tuning
- [Migration from Elasticsearch](/docs/migration/from-elasticsearch) -- `_bulk` API compatibility
- [Migration from Splunk](/docs/migration/from-splunk) -- HEC endpoint setup
- [Performance Tuning](/docs/operations/performance-tuning) -- optimize ingest throughput

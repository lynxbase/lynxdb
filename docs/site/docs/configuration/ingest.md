---
title: Ingest Settings
description: Configure LynxDB ingest limits, batching behavior, request parsing, and part-write durability.
---

# Ingest Settings

The `ingest` section controls how the HTTP ingest endpoints accept data before it is flushed to immutable part files.

Accepted events first enter an in-memory batcher. They become durable only after LynxDB finishes writing and publishing the corresponding `.lsg` part.

## Max Body Size

The maximum size of a single HTTP request body for ingest endpoints.

| Config Key | `ingest.max_body_size` |
|---|---|
| **Env Var** | `LYNXDB_INGEST_MAX_BODY_SIZE` |
| **Default** | `100mb` |

```yaml
ingest:
  max_body_size: "100mb"
```

Requests exceeding this limit receive `413 Payload Too Large`.

## Max Batch Size

The maximum number of events accepted in one ingest request.

| Config Key | `ingest.max_batch_size` |
|---|---|
| **Env Var** | `LYNXDB_INGEST_MAX_BATCH_SIZE` |
| **Default** | `1000` |

```yaml
ingest:
  max_batch_size: 1000
```

## Max Line Size

The maximum size of a single raw log line.

| Config Key | `ingest.max_line_bytes` |
|---|---|
| **Env Var** | `LYNXDB_INGEST_MAX_LINE_BYTES` |
| **Default** | `1mb` |

```yaml
ingest:
  max_line_bytes: 1048576
```

## Parse Mode

Choose how much work LynxDB does during ingest.

| Config Key | `ingest.mode` |
|---|---|
| **Env Var** | `LYNXDB_INGEST_MODE` |
| **Default** | `full` |

```yaml
ingest:
  mode: "full"
```

Valid values:

| Value | Description |
|---|---|
| `full` | Extract JSON fields into columns during ingest. Default. |
| `lightweight` | Keep most data in `_raw` and defer extraction to query time. |

## Durability and Part Flushes

The current ingest path does not write to a WAL. Instead:

1. events are buffered in memory by `AsyncBatcher`
2. a batch is serialized to a temporary part file
3. the part is optionally `fsync`'d
4. the file is atomically renamed into place

Events that have been accepted but not yet flushed are not durable.

### `ingest.fsync`

Control whether LynxDB `fsync`s each part file before the atomic rename.

| Config Key | `ingest.fsync` |
|---|---|
| **Env Var** | `LYNXDB_INGEST_FSYNC` |
| **Default** | `true` |

```yaml
ingest:
  fsync: true
```

| Value | Behavior | Tradeoff |
|---|---|---|
| `true` | Sync the part file before rename | Higher durability, more flush latency |
| `false` | Leave durability to the OS page cache | Faster flushes, higher power-loss risk |

## Deduplication

Optional ingest deduplication helps with at-least-once delivery pipelines.

| Config Key | Env Var | Default | Description |
|---|---|---|---|
| `ingest.dedup_enabled` | `LYNXDB_INGEST_DEDUP_ENABLED` | `false` | Enable xxhash64-based ingest dedup |
| `ingest.dedup_capacity` | `LYNXDB_INGEST_DEDUP_CAPACITY` | `100000` | Number of recent hashes to retain |

## Ingest Endpoints

| Endpoint | Format | Description |
|---|---|---|
| `POST /api/v1/ingest` | JSON event arrays | Primary structured ingest path |
| `POST /api/v1/ingest/raw` | Newline-delimited raw text | Raw log ingest |
| `POST /api/v1/ingest/hec` | Splunk HEC JSON | Splunk forwarder compatibility |
| `POST /api/v1/ingest/bulk` | Elasticsearch bulk format | Alias for the Elasticsearch compatibility bulk handler |

## Complete Example

```yaml
ingest:
  max_body_size: "100mb"
  max_batch_size: 5000
  max_line_bytes: 1048576
  mode: "full"
  fsync: true
  dedup_enabled: false
  dedup_capacity: 100000
```

## Tuning Guidelines

| Scenario | Recommendation |
|---|---|
| High-throughput ingest | Increase `max_body_size` and consider `lightweight` mode |
| Large raw log lines | Increase `max_line_bytes` |
| Stricter flush durability | Keep `fsync: true` |
| Replay-heavy pipelines | Enable deduplication |

## Next Steps

- [Storage Settings](/docs/configuration/storage)
- [Compatibility API](/docs/api/compatibility)
- [Migration from Splunk](/docs/migration/from-splunk)
- [Performance Tuning](/docs/operations/performance-tuning)

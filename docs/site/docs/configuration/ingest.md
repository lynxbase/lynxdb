---
title: Ingest Settings
description: Request limits, parse mode, durability, deduplication, and the Elasticsearch, Splunk HEC and OTLP receivers in the ingest section of the LynxDB config file.
---

# Ingest Settings

The `ingest` section controls the HTTP ingest endpoints, the shipper-compatible receivers, and how accepted events reach the [storage write path](./storage.md#write-path).

## Endpoints

| Endpoint | Format |
|----------|--------|
| `POST /api/v1/ingest` | JSON array of events |
| `POST /api/v1/ingest/raw` | Newline-delimited raw lines or NDJSON |
| `POST /api/v1/ingest/hec`, `POST /services/collector`, `/services/collector/event`, `/services/collector/raw` | Splunk HEC |
| `POST /api/v1/es/_bulk`, `POST /api/v1/ingest/bulk`, `POST /_bulk`, `POST /{index}/_bulk` | Elasticsearch bulk API |
| `POST /api/v1/otlp/v1/logs` on the API port, `POST /v1/logs` on `ingest.otlp.http_listen` | OTLP/HTTP logs |
| `ingest.otlp.grpc_listen` | OTLP/gRPC logs |

See [Ingest API](../api/ingest.md) and [Compatibility API](../api/compatibility.md) for payload formats.

## Request Limits

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `ingest.max_body_size` | none | `LYNXDB_INGEST_MAX_BODY_SIZE` | `100mb` | Maximum request body on the API port. Minimum `1kb`. |
| `ingest.limits.max_compressed_body_bytes` | `--ingest-max-compressed-body-bytes` | `LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES` | `32mb` | Maximum body bytes on the wire, compressed or not. Requests over it get `413`. |
| `ingest.limits.max_decompressed_body_bytes` | `--ingest-max-decompressed-body-bytes` | `LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES` | `256mb` | Maximum body after `gzip`, `zstd` or `snappy` decoding. Must be at least the compressed limit. |
| `ingest.max_line_bytes` | none | `LYNXDB_INGEST_MAX_LINE_BYTES` | `1048576` | Longest single line or NDJSON record. Minimum `1024`. |
| `ingest.max_batch_size` | none | `LYNXDB_INGEST_MAX_BATCH_SIZE` | `1000` | Events per internal batch while a streaming request is read. It does not cap the events in a request. |

Both body limits apply to every request on the API port, so with the defaults the effective wire limit is 32 MB. Raise `max_compressed_body_bytes` together with `max_body_size` to accept larger uncompressed bodies.

## Parse Mode

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `ingest.mode` | `LYNXDB_INGEST_MODE` | empty (`full`) | `full` extracts JSON and key=value fields into columns at ingest. `lightweight` extracts only `_time`, `host`, `source`, `sourcetype`, `level` and `index`, and leaves the rest in `_raw` for query-time extraction with `rex`, `parse` or `spath`. |

## Durability

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `ingest.fsync` | none | `true` | Fsync each part file before the rename that publishes it. `false` leaves durability to the OS page cache: faster flushes, but a power loss can drop recently flushed parts. |

Events accepted but not yet flushed are held in memory either way. See [Storage Settings](./storage.md#write-path) for when flushes happen.

## Deduplication

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `ingest.dedup_enabled` | none | `false` | Drop events whose `_raw` hash (xxhash64) was seen in the same batch or among recent events. Useful for at-least-once shippers that retry. |
| `ingest.dedup_capacity` | none | `100000` | Number of recent hashes remembered (about 16 bytes each). Must be at least 1 when dedup is on. |

## Elasticsearch Compatibility

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `ingest.es_compat.enabled` | `--ingest-es-enabled` | `LYNXDB_INGEST_ES_COMPAT_ENABLED` | `true` | Serve the Elasticsearch bulk API and the handshake endpoints that Filebeat, Fluent Bit and Vector call at startup. |
| `ingest.es_compat.advertised_version` | `--ingest-es-version` | `LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION` | `8.15.0` | Version reported to shippers, in `X.Y.Z` form. |
| `ingest.es_compat.cluster_name` | none | `LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME` | `lynxdb` | Cluster name in the handshake. Must not be empty when compatibility is on. |
| `ingest.es_compat.strip_logstash_date_suffix` | none | `LYNXDB_INGEST_ES_COMPAT_STRIP_LOGSTASH_DATE_SUFFIX` | `true` | Map date-suffixed index names such as `logstash-2026.01.15` (Fluent Bit `Logstash_Format`) to one source. |

## Splunk HEC

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `ingest.splunk_hec.enabled` | `LYNXDB_INGEST_SPLUNK_HEC_ENABLED` | `true` | Serve the `/services/collector` routes. When off they answer `404`. |
| `ingest.splunk_hec.require_token` | `LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN` | `false` | With `false`, the `/services/collector` routes accept any non-empty `Authorization: Splunk <token>` header. With `true`, the token must be on a configured list, and the server configures no tokens, so every request is rejected. |

`POST /api/v1/ingest/hec` sits under `/api/` and uses API key authentication when auth is on.

## OTLP

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `ingest.otlp.http_listen` | `--otlp-http-listen` | `LYNXDB_INGEST_OTLP_HTTP_LISTEN` | `0.0.0.0:4318` | Separate OTLP/HTTP listener (`POST /v1/logs`). Empty disables it. |
| `ingest.otlp.grpc_listen` | `--otlp-grpc-listen` | `LYNXDB_INGEST_OTLP_GRPC_LISTEN` | empty | OTLP/gRPC listener. Empty disables it. |
| `ingest.otlp.grpc_max_recv_bytes` | `--otlp-grpc-max-recv-bytes` | `LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES` | `16mb` | Largest gRPC message. Minimum `1mb`. |

The OTLP listeners serve plain HTTP and gRPC without API key checks, and the HTTP listener binds all interfaces by default. On a shared network, restrict port 4318 with a firewall, bind it to a private address, or set `http_listen: ""` and send OTLP to `/api/v1/otlp/v1/logs` on the API port, which uses the server's TLS and auth.

## Staging Buffer

Shipper protocols (Elasticsearch bulk, Splunk HEC, OTLP) often send many small batches. The staging buffer coalesces them before they reach the batcher.

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `ingest.staging.enabled` | `--ingest-staging-enabled` | `LYNXDB_INGEST_STAGING_ENABLED` | `true` | Use the staging buffer. |
| `ingest.staging.max_bytes` | `--ingest-staging-max-bytes` | `LYNXDB_INGEST_STAGING_MAX_BYTES` | `64mb` | Pending bytes before the buffer flushes; when it cannot accept more, requests get `503` with `Retry-After: 5`. |
| `ingest.staging.max_age` | `--ingest-staging-max-age` | `LYNXDB_INGEST_STAGING_MAX_AGE` | `5s` | Flush the oldest pending batch after this age. |
| `ingest.staging.max_inflight_events` | none | `LYNXDB_INGEST_STAGING_MAX_INFLIGHT_EVENTS` | `1000000` | Pending events allowed in the buffer. |
| `ingest.staging.flush_retries` | none | `LYNXDB_INGEST_STAGING_FLUSH_RETRIES` | `3` | Retries of a failed flush before the staged events are dropped. |
| `ingest.staging.flush_backoff_max` | none | `LYNXDB_INGEST_STAGING_FLUSH_BACKOFF_MAX` | `5s` | Maximum backoff between flush retries. |

When compaction falls behind and an index has too many parts, ingest requests get `503` with `Retry-After: 1` until compaction catches up.

## Complete Example

```yaml
ingest:
  max_body_size: "100mb"
  max_line_bytes: 1048576
  mode: "full"
  fsync: true
  dedup_enabled: true
  dedup_capacity: 200000
  limits:
    max_compressed_body_bytes: "64mb"
    max_decompressed_body_bytes: "512mb"
  es_compat:
    enabled: true
    advertised_version: "8.15.0"
  otlp:
    http_listen: "127.0.0.1:4318"
    grpc_listen: ""
  staging:
    enabled: true
    max_bytes: "64mb"
    max_age: "5s"
```

## Related

- [Ingest API](../api/ingest.md)
- [Compatibility API](../api/compatibility.md)
- [Ingest Data Guide](../guides/ingest-data.md)
- [Storage Settings](./storage.md)
- [Syslog Receiver](./syslog.md)

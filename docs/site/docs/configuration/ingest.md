---
title: Ingest Settings
description: Request limits, parse mode, and the Elasticsearch, Splunk HEC and OTLP receivers in the ingest section of the LynxDB config file.
---

# Ingest Settings

The `ingest` section controls the HTTP ingest endpoints and the shipper-compatible receivers. Every endpoint appends events to the store of the index it names; [Storage Settings](./storage.md#writing-events) describe how they are written.

## Endpoints

| Endpoint | Format | Stored as |
|----------|--------|-----------|
| `POST /api/v1/ingest` | NDJSON, one event per line | Each line unchanged |
| `POST /api/v1/ingest/raw` | Log lines | JSON object lines unchanged; any other line as an event with `_raw`, its timestamp and its `key=value` fields |
| `POST /services/collector`, `/services/collector/event`, `/services/collector/raw` | Splunk HEC | One flat JSON object per event |
| `POST /_bulk`, `POST /{index}/_bulk`, `POST /_data_stream/{name}/_bulk` | Elasticsearch bulk API | Each document line unchanged |
| `POST /v1/logs` on `ingest.otlp.http_listen` | OTLP/HTTP logs, protobuf or JSON | One flat JSON object per log record |
| `ingest.otlp.grpc_listen` | OTLP/gRPC logs | One flat JSON object per log record |

See [Ingest API](../api/ingest.md) and [Compatibility API](../api/compatibility.md) for payload formats.

## Request Limits

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `ingest.max_body_size` | none | `LYNXDB_INGEST_MAX_BODY_SIZE` | `100mb` | Maximum request body on the API port, as sent. For `POST /api/v1/ingest` and `/api/v1/ingest/raw` it is the only limit, as sent and after decoding. Minimum `1kb`. |
| `ingest.limits.max_compressed_body_bytes` | `--ingest-max-compressed-body-bytes` | `LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES` | `32mb` | Maximum body bytes on the wire, compressed or not, on the Elasticsearch, Splunk HEC and OTLP routes. Requests over it get `413`. |
| `ingest.limits.max_decompressed_body_bytes` | `--ingest-max-decompressed-body-bytes` | `LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES` | `256mb` | Maximum body after `gzip`, `zstd` or `snappy` decoding on the same routes. Must be at least the compressed limit. |
| `ingest.max_line_bytes` | none | `LYNXDB_INGEST_MAX_LINE_BYTES` | `1048576` | Longest single line of a Splunk HEC or Elasticsearch bulk body. Minimum `1024`. |
| `ingest.max_batch_size` | none | `LYNXDB_INGEST_MAX_BATCH_SIZE` | `1000` | Events per internal batch while a Splunk HEC request is read. It does not cap the events in a request. |

`POST /api/v1/ingest` and `/api/v1/ingest/raw` are bounded by `ingest.max_body_size` alone, compressed or not. The Elasticsearch, Splunk HEC and OTLP routes are bounded by the two `ingest.limits` settings, and by `max_body_size` as sent, so with the defaults their wire limit is 32 MB. A `413` names the setting that applies, for example `compressed body exceeds limit 33554432 set by ingest.limits.max_compressed_body_bytes`. The body limits need a restart; `max_line_bytes` and `max_batch_size` take effect on reload.

## Parse Mode

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `ingest.mode` | `LYNXDB_INGEST_MODE` | empty (`full`) | How Splunk HEC, OTLP and syslog events are parsed before they are written. `full` extracts JSON and `key=value` fields from the event text. `lightweight` extracts only `_time`, `host`, `source`, `sourcetype` and `level`, and leaves the rest in `_raw` for query-time extraction with `rex`, `parse` or `spath`. Needs a restart. |

## Elasticsearch Compatibility

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `ingest.es_compat.enabled` | `--ingest-es-enabled` | `LYNXDB_INGEST_ES_COMPAT_ENABLED` | `true` | Accept Elasticsearch bulk requests. When off, they get `503` with `Retry-After: 5`; the handshake endpoints that Filebeat, Fluent Bit and Vector call at startup keep answering. Reloadable. |
| `ingest.es_compat.advertised_version` | `--ingest-es-version` | `LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION` | `8.15.0` | Version reported to shippers, in `X.Y.Z` form. Needs a restart. |
| `ingest.es_compat.cluster_name` | none | `LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME` | `lynxdb` | Cluster name in the handshake. Must not be empty when compatibility is on. Needs a restart. |

## Splunk HEC

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `ingest.splunk_hec.enabled` | `LYNXDB_INGEST_SPLUNK_HEC_ENABLED` | `true` | Serve the `/services/collector` routes. When off they answer `404`. |

With auth off, the `/services/collector` routes accept every request. With auth on, `Authorization: Splunk <token>` must carry an API key with the `ingest` scope.

## OTLP

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `ingest.otlp.http_listen` | `--otlp-http-listen` | `LYNXDB_INGEST_OTLP_HTTP_LISTEN` | `localhost:4318` | Separate OTLP/HTTP listener (`POST /v1/logs`). Empty disables it. |
| `ingest.otlp.grpc_listen` | `--otlp-grpc-listen` | `LYNXDB_INGEST_OTLP_GRPC_LISTEN` | empty | OTLP/gRPC listener. Empty disables it. |
| `ingest.otlp.grpc_max_recv_bytes` | `--otlp-grpc-max-recv-bytes` | `LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES` | `16mb` | Largest gRPC message. Minimum `1mb`. |

The OTLP/HTTP listener binds `localhost` by default; set `http_listen: "0.0.0.0:4318"` to take OTLP from other hosts. Both listeners serve plain HTTP and gRPC without TLS. With auth on they need a key with the `ingest` scope, as `Authorization: Bearer <key>` on OTLP/HTTP and as the `authorization` metadata on OTLP/gRPC.

## Complete Example

```yaml
ingest:
  max_body_size: "100mb"
  max_line_bytes: 1048576
  mode: "full"
  limits:
    max_compressed_body_bytes: "64mb"
    max_decompressed_body_bytes: "512mb"
  es_compat:
    enabled: true
    advertised_version: "8.15.0"
  otlp:
    http_listen: "127.0.0.1:4318"
    grpc_listen: ""
```

## Related

- [Ingest API](../api/ingest.md)
- [Compatibility API](../api/compatibility.md)
- [Ingest Data Guide](../guides/ingest-data.md)
- [Storage Settings](./storage.md)
- [Syslog Receiver](./syslog.md)

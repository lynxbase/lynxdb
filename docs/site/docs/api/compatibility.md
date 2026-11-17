---
sidebar_position: 11
title: Compatibility
description: Elasticsearch bulk ingest and setup stubs, Splunk HEC ingest and OpenTelemetry OTLP log ingest.
---

# Compatibility API

LynxDB accepts logs over three shipper protocols, so existing Filebeat, Logstash, Vector, Fluent Bit, Fluentd, Splunk forwarder and OpenTelemetry Collector setups only need a different address:

| Protocol | Endpoint | Typical senders |
|---|---|---|
| Elasticsearch bulk | `http://lynxdb:3100` (or `/api/v1/es`) | Filebeat, Logstash, Vector, Fluent Bit, Fluentd |
| Splunk HEC | `http://lynxdb:3100/services/collector` | Splunk forwarders, HEC clients |
| OTLP/HTTP | `http://lynxdb:4318/v1/logs` | OpenTelemetry Collector and SDKs |

These are ingest protocols only. Read the data back with [PPL](../ppl/overview.md) through the [Query API](./query.md):

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=logs level=error | stats count() by host", "from": "-1h"}'
```

## Elasticsearch

### Bulk ingest

| Route | Index taken from |
|---|---|
| `POST /_bulk`, `POST /api/v1/es/_bulk`, `POST /api/v1/ingest/bulk` | Each action's `_index` |
| `POST /{index}/_bulk` | `_index`, else `{index}` |
| `POST /_data_stream/{name}/_bulk`, `POST /api/v1/es/_data_stream/{name}/_bulk` | `{name}` |

The body is Elasticsearch bulk NDJSON: an action line followed by a document line. `index` and `create` actions are stored; `update` and `delete` actions fail per item with `action_request_validation_exception`. `Content-Type` must be `application/x-ndjson`, `application/json` or absent. Bodies may be compressed with `gzip`, `zstd` or `snappy` (`Content-Encoding`).

```bash
curl -s -X POST localhost:3100/_bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"index":{"_index":"logs"}}\n{"message":"hello from filebeat","@timestamp":"2026-11-17T09:00:00Z","level":"info"}\n{"create":{"_index":"logs"}}\n{"message":"cpu usage high","@timestamp":"2026-11-17T09:00:01Z","host":"web-01"}\n'
```

```json
{
  "took": 14,
  "errors": false,
  "items": [
    {"index": {"_id": "7e09be76021487a477741b2e", "_index": "logs", "status": 201, "result": "created"}},
    {"create": {"_id": "a7a8e87ecb20d263fe897b43", "_index": "logs", "status": 201, "result": "created"}}
  ]
}
```

A failed item carries `status` and `error: {"type", "reason"}`, and the top-level `errors` is `true`. Responses carry the `X-Elastic-Product: Elasticsearch` header that Elasticsearch clients check.

### How documents become events

| Document | Event |
|---|---|
| `@timestamp` (or the field named by the `_time_field` query parameter), else `timestamp` | `_time`. RFC 3339 strings and epoch numbers in seconds, milliseconds, microseconds or nanoseconds are accepted |
| `host` (string) or `host.name`, else `agent.hostname` | `host` |
| `log.file.path`, else `source` | `_source` |
| The whole document as JSON, or the field named by `_msg_field` | `_raw` |
| Every other field | A field of the same name |

The target index is resolved in this order: the data stream name, the action's `_index`, the `{index}` path segment, a `target_index` field in the document (also `attributes.target_index`, `resource.attributes.target_index`, `fields.target_index`), and finally `default`. `_index` may contain `${field}` or `%{[field]}` / `%{[field]:fallback}` templates filled from the document. Index names use lowercase letters, digits, `.`, `_`, `-` and `+`, do not start with `-`, `_` or `+`, and are at most 255 bytes; an invalid name fails that item with `invalid_index_name_exception`.

`_id` values are echoed in the response, or generated when absent. They are not used for deduplication or updates.

### POST /api/v1/es/\{index\}/_doc

Stores one document in `{index}`, with the same mapping:

```bash
curl -s -X POST localhost:3100/api/v1/es/logs/_doc \
  -H 'Content-Type: application/json' \
  -d '{"message": "single event", "@timestamp": "2026-11-17T09:00:02Z", "level": "info"}'
```

```json
{"_id": "83902dbd6e6c4283f4314362", "_index": "logs", "result": "created"}
```

### Handshake and setup probes

Shippers probe the server before sending. `GET /` and `GET /api/v1/es/` return an Elasticsearch-style cluster description:

```json
{
  "name": "lynxdb-host1-90011",
  "cluster_name": "lynxdb",
  "cluster_uuid": "Lp9Zztp3164yLH-aHknLAn",
  "version": {"number": "8.15.0", "build_flavor": "default", "lucene_version": "9.7.0", "minimum_wire_compatibility_version": "7.17.0", "minimum_index_compatibility_version": "7.0.0"},
  "tagline": "You Know, for Logs"
}
```

The advertised version and cluster name come from `ingest.es_compat.advertised_version` and `ingest.es_compat.cluster_name`. LynxDB also answers the license, `_cat`, cluster health, index template, ILM, ingest pipeline, nodes, alias, data stream, `_search`, security and `HEAD /{index}` probes with fixed responses; [Elasticsearch compatibility stubs](../architecture/es-stubs.md) lists each route and response. These stubs store nothing, and `PUT` requests for ILM policies, ingest pipelines and indexes under `/api/v1/es` answer `501`.

### Authentication

The root routes (`/_bulk`, `/{index}/_bulk`, `/_data_stream/...`, `/`) sit outside `/api/`, so the API key middleware does not read their credentials. With authentication on, they answer `401` to bulk requests; point shippers at `http://lynxdb:3100/api/v1/es` and give them a key with `ingest` scope, sent as `Authorization: ApiKey <base64(id:token)>` (the `api_key` value returned at key creation), `Bearer` or `Basic`.

Setting `ingest.es_compat.enabled: false` turns the bulk and `_doc` routes off; they then answer `503`.

### Shipper settings

```yaml
# filebeat.yml
output.elasticsearch:
  hosts: ["http://lynxdb:3100"]
```

```toml
# vector.toml
[sinks.lynxdb]
type = "elasticsearch"
endpoints = ["http://lynxdb:3100"]
```

```ruby
# logstash.conf
output {
  elasticsearch { hosts => ["http://lynxdb:3100"] }
}
```

See the [Filebeat](../guides/ingest-data/filebeat.md), [Vector](../guides/ingest-data/vector.md) and [Fluent Bit](../guides/ingest-data/fluent-bit.md) guides for full configurations.

## Splunk HEC

| Route | Body |
|---|---|
| `POST /services/collector`, `POST /services/collector/event` | HEC event objects, one per line |
| `POST /services/collector/raw` | Raw text, one event per line |
| `POST /services/collector/ack` | `{"acks": [1, 2]}` |
| `GET /services/collector/health` | |
| `POST /api/v1/ingest/hec` | HEC event objects, authenticated with a LynxDB API key |

The `/services/collector` routes need an `Authorization: Splunk <token>` header with a non-empty token; a missing token answers `401` with `{"text": "Token is required", "code": 2}`. They sit outside `/api/` and do not check LynxDB API keys. Setting `ingest.splunk_hec.enabled: false` turns them off (`404`, `{"text": "HEC is disabled", "code": 4}`).

```bash
curl -s -X POST localhost:3100/services/collector/event \
  -H 'Authorization: Splunk my-token' \
  -d '{"event": "user login succeeded", "time": 1763370000, "host": "web-01", "source": "auth-service", "sourcetype": "json", "fields": {"level": "info"}}'
```

```json
{"code": 0, "text": "Success"}
```

Lines that are not valid JSON are skipped and counted in `skipped_lines`. `GET /services/collector/health` answers `{"text": "HEC is healthy", "code": 17}`.

| HEC field | Event |
|---|---|
| `time` | `_time` (Unix seconds, fractions kept) |
| `event` | `_raw`; an object is stored as its JSON text |
| `host` | `host` |
| `source` | `_source`; when empty, the HEC `index` value is used |
| `sourcetype` | `_sourcetype` |
| `index` | Stored as the field `source_tag` when `source` is set |
| `fields` | Fields of the same name |

HEC events are stored in the `main` index. `/services/collector/raw` takes `source`, `sourcetype`, `host` and `index` as query parameters.

**Acknowledgements.** When a request carries `X-Splunk-Request-Channel`, the server flushes the events before answering and adds an `ackId` to the response. `POST /services/collector/ack` with the same channel header reports each ID: `{"acks": {"1": true, "2": false}}`.

See the [Splunk HEC guide](../guides/ingest-data/splunk-hec.md).

## OpenTelemetry OTLP

### OTLP listener

The OTLP/HTTP listener runs on its own port, `ingest.otlp.http_listen` (default `0.0.0.0:4318`):

| Route | Behavior |
|---|---|
| `POST /v1/logs` | Stores log records. Protobuf (`application/x-protobuf`) or JSON (`application/json`) |
| `POST /v1/traces`, `POST /v1/metrics` | Accepted and discarded, so mixed pipelines do not fail |

An OTLP/gRPC listener runs when `ingest.otlp.grpc_listen` is set (empty by default). Requests are limited by `ingest.limits` and `ingest.otlp.grpc_max_recv_bytes`.

```yaml
# otel-collector-config.yaml
exporters:
  otlphttp:
    endpoint: http://lynxdb:4318

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [batch]
      exporters: [otlphttp]
```

| OTLP | Event |
|---|---|
| `timeUnixNano`, else `observedTimeUnixNano`, else the receive time | `_time` |
| `body` | `_raw`; records with an empty body are dropped |
| `resource.attributes["service.name"]`, else `host.name`, else `otlp` | `_source` |
| `resource.attributes["host.name"]` | `host` |
| `severityText` or `severityNumber` | `level` |
| `traceId`, `spanId` | `trace_id`, `span_id` (hex) |
| Scope name and version | `scope.name`, `scope.version` |
| Every resource attribute | `resource.<key>` |
| Log record attributes | Fields of the same name, as strings |

Records go to the `main` index with `_sourcetype` `otlp`.

### POST /api/v1/otlp/v1/logs

The same logs signal on the main API port, for setups that can only reach port 3100. It takes OTLP JSON only; `Content-Type: application/x-protobuf` answers `415`. A successful request answers `200` with `{}`.

```bash
curl -s -X POST localhost:3100/api/v1/otlp/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceLogs": [{
      "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "api-gateway"}}]},
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1763370000000000000",
          "body": {"stringValue": "GET /api/users 200 12ms"},
          "severityText": "INFO",
          "attributes": [{"key": "http.method", "value": {"stringValue": "GET"}}]
        }]
      }]
    }]
  }'
```

This route maps records differently from the OTLP listener:

| OTLP | Event |
|---|---|
| `timeUnixNano` | `_time` |
| `body` | `_raw` |
| `resource.attributes["service.name"]` | `_source` |
| `resource.attributes["host.name"]` | `host` |
| `resource.attributes["deployment.environment"]` | The target index |
| Other resource attributes | `resource.<key>` |
| `severityText` or `severityNumber` | `level`; `severity_number` is kept as a field |
| `traceId`, `spanId`, `flags` | `trace_id`, `span_id`, `flags` |
| Scope name and version | `otel.scope.name`, `otel.scope.version` |
| Log record attributes | Fields of the same name, with their OTLP types |

See the [OpenTelemetry guide](../guides/ingest-data/opentelemetry.md).

## Related

- [Ingest API](./ingest.md): native ingest endpoints
- [Ingest configuration](../configuration/ingest.md): `es_compat`, `splunk_hec`, `otlp`, `limits` and `staging` settings
- [Shipper conformance](../operations/shipper-conformance.md)

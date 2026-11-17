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
  -d '{"q": "source=logs level=info | stats count() by host", "from": "-1h"}'
```

## Elasticsearch

### Bulk ingest

| Route | Index of an action without `_index` |
|---|---|
| `POST /_bulk`, `POST /api/v1/es/_bulk`, `POST /api/v1/ingest/bulk` | `main` |
| `POST /{index}/_bulk` | `{index}` |
| `POST /_data_stream/{name}/_bulk`, `POST /api/v1/es/_data_stream/{name}/_bulk` | `{name}` |

The body is Elasticsearch bulk NDJSON: an action line followed by a document line. An action's `_index` always wins over the index of the URL.

- `index` and `create` actions append their document line, unchanged, to the index. The document must be a JSON object; anything else fails that item with `mapper_parsing_exception`.
- `update` and `delete` actions fail per item with `action_request_validation_exception`.
- The document becomes an event like a line of [`POST /api/v1/ingest`](./ingest.md#how-events-are-stored): its time comes from the first of `storage.time_fields` it holds, so `@timestamp` sets it, and every member is a field. Nothing is renamed or dropped.
- `_id` values are echoed in the response, or generated when absent. They are not stored and not used for deduplication or updates.
- The server makes the documents durable before it answers, so a `200` means they are stored.

`Content-Type` must be `application/x-ndjson`, `application/json` or absent; anything else answers `400 content_type_exception`. A line longer than `ingest.max_line_bytes` answers `400 mapper_parsing_exception`.

```bash
curl -s -X POST localhost:3100/_bulk \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"index":{"_index":"logs"}}\n{"message":"hello from filebeat","@timestamp":"2026-11-17T09:00:00Z","level":"info"}\n{"create":{"_index":"logs"}}\n{"message":"cpu usage high","@timestamp":"2026-11-17T09:00:01Z","host":"web-01"}\n'
```

```json
{
  "took": 11,
  "errors": false,
  "items": [
    {"index": {"_id": "6d9b0922f5d2a1dc62c2e0cb", "_index": "logs", "status": 201, "result": "created"}},
    {"create": {"_id": "a69dde78334e9ebc64b0a249", "_index": "logs", "status": 201, "result": "created"}}
  ]
}
```

The first document reads back as:

```json
{"_time": "2026-11-17T09:00:00Z", "@timestamp": "2026-11-17T09:00:00Z", "level": "info", "message": "hello from filebeat"}
```

A failed item carries `status` and `error: {"type", "reason"}`, and the top-level `errors` is `true`:

```json
{"index": {"_id": "", "_index": "Logs", "status": 400, "error": {"type": "invalid_index_name_exception", "reason": "index name \"Logs\" contains unsupported character 'L'"}}}
```

Index names on these routes use lowercase letters, digits, `.`, `_` and `-`, do not start with `-`, `_`, `+` or `.`, hold no `..`, and are at most 255 bytes. Responses carry the `X-Elastic-Product: Elasticsearch` header that Elasticsearch clients check.

### POST /api/v1/es/\{index\}/_doc

Appends one JSON object to `{index}`:

```bash
curl -s -X POST localhost:3100/api/v1/es/logs/_doc \
  -H 'Content-Type: application/json' \
  -d '{"message": "single event", "@timestamp": "2026-11-17T09:00:02Z", "level": "info"}'
```

```json
{"_id": "57b75ae236fa4b422d6c131e", "_index": "logs", "result": "created"}
```

It answers `201`. A body that is not a JSON object answers `400 mapper_parsing_exception`, and an invalid index name `400 invalid_index_name_exception`.

### Handshake and setup probes

Shippers probe the server before sending. `GET /`, `GET /api/v1/es` and `GET /api/v1/es/` return an Elasticsearch-style cluster description:

```json
{
  "name": "lynxdb-host1-39428",
  "cluster_name": "lynxdb",
  "cluster_uuid": "DG0lHwHLgu4iz6W388thzP",
  "version": {
    "number": "8.15.0",
    "build_flavor": "default",
    "build_type": "tarball",
    "build_hash": "lynxdb",
    "build_date": "2025-01-01T00:00:00.000Z",
    "build_snapshot": false,
    "lucene_version": "9.7.0",
    "minimum_wire_compatibility_version": "7.17.0",
    "minimum_index_compatibility_version": "7.0.0"
  },
  "tagline": "You Know, for Logs"
}
```

The advertised version and cluster name come from `ingest.es_compat.advertised_version` and `ingest.es_compat.cluster_name`. LynxDB also answers the license, `_cat`, cluster health, index template, ILM, ingest pipeline, nodes, alias, data stream, `_search`, security and `HEAD /{index}` probes with fixed responses; [Elasticsearch compatibility stubs](../architecture/es-stubs.md) lists each route and response. These stubs store nothing. `PUT` requests for ILM policies, ingest pipelines and indexes under `/api/v1/es` answer `501`:

```json
{"error": "unsupported Elasticsearch management endpoint", "status": 501}
```

### Authentication

The root routes (`/_bulk`, `/{index}/_bulk`, `/_data_stream/...`) sit outside `/api/`, where the API key middleware does not read credentials. With authentication on, they answer `401 AUTH_REQUIRED`. Point shippers at `http://lynxdb:3100/api/v1/es` instead and give them a key with `ingest` scope, sent as `Authorization: ApiKey <base64(id:token)>` (the `api_key` value returned at key creation), `Bearer` or `Basic`.

Setting `ingest.es_compat.enabled: false` turns the bulk and `_doc` routes off; they then answer `503` with `{"error": {"type": "unavailable", "reason": "Elasticsearch compatibility ingest is disabled"}, "status": 503}`.

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

The `/services/collector` routes, except `health`, need an `Authorization: Splunk <token>` header with a non-empty token; a missing token answers `401` with `{"text": "Token is required", "code": 2}`. They sit outside `/api/` and do not check LynxDB API keys. LynxDB keeps no list of HEC tokens, so with `ingest.splunk_hec.require_token: true` every token is refused with `{"text": "Invalid token", "code": 3}`. Setting `ingest.splunk_hec.enabled: false` turns these routes off (`404`, `{"text": "HEC is disabled", "code": 4}`).

```bash
curl -s -X POST localhost:3100/services/collector/event \
  -H 'Authorization: Splunk my-token' \
  -d '{"event": "user login succeeded", "time": 1763370000, "host": "web-01", "source": "auth-service", "sourcetype": "json", "fields": {"level": "info"}}'
```

```json
{"code": 0, "text": "Success"}
```

The event reads back as:

```json
{"_time": "2025-11-17T09:00:00Z", "_raw": "user login succeeded", "host": "web-01", "source": "auth-service", "sourcetype": "json", "level": "info"}
```

Lines that are not valid JSON are skipped and counted in `skipped_lines`. Events go to the `main` index.

| HEC field | Event |
|---|---|
| `time` | `_time` (Unix seconds, fractions kept). Without it, a timestamp in the text, else the time of arrival |
| `event` | `_raw`; an object is stored as its JSON text, and its top-level text, number and boolean members become fields |
| `host` | `host` |
| `source` | `source`; when empty, the HEC `index` value |
| `sourcetype` | `sourcetype` |
| `index` | The field `source_tag` when `source` is set |
| `fields` | Fields of the same name |

`key=value` pairs in a text `event` become fields too. `/services/collector/raw` takes `source`, `sourcetype`, `host` and `index` as query parameters and stores each line as one event.

**Acknowledgements.** When a request carries `X-Splunk-Request-Channel`, the server makes the events durable before it answers and adds an `ackId` to the response. `POST /services/collector/ack` with the same channel header reports each ID:

```json
{"acks": {"1": true, "2": false}}
```

A malformed ack body answers `400` with `{"text": "Invalid ack request", "code": 5}`. `GET /services/collector/health` answers `{"text": "HEC is healthy", "code": 17}`.

Events are written in batches of `ingest.max_batch_size`. A line longer than `ingest.max_line_bytes` ends the request with `500 INTERNAL_ERROR`; the batches written before it stay stored. See the [Splunk HEC guide](../guides/ingest-data/splunk-hec.md).

## OpenTelemetry OTLP

### OTLP listener

The OTLP/HTTP listener runs on its own port, `ingest.otlp.http_listen` (default `0.0.0.0:4318`; empty turns it off). It does not check API keys.

| Route | Behavior |
|---|---|
| `POST /v1/logs` | Stores log records. Protobuf (`application/x-protobuf`) or JSON (`application/json`); any other content type answers `415` |
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

Records go to the `main` index as one flat JSON object each:

| OTLP | Event |
|---|---|
| `timeUnixNano`, else `observedTimeUnixNano`, else the time of arrival | `_time` |
| `body` | `_raw`; records with an empty body are dropped |
| `resource.attributes["service.name"]`, else `host.name`, else `otlp` | `source` |
| `resource.attributes["host.name"]` | `host` |
| | `sourcetype` `otlp` |
| `severityText` (lower-cased), else `severityNumber` | `level` |
| `traceId`, `spanId` | `trace_id`, `span_id` (hex) |
| Scope name and version | `scope.name`, `scope.version` |
| Every resource attribute | `resource.<key>`, as text |
| Log record attributes | Fields of the same name, as text |

### POST /api/v1/otlp/v1/logs

The same logs signal on the main API port, for setups that can only reach port 3100. It takes OTLP JSON only; `Content-Type: application/x-protobuf` answers `415 INVALID_REQUEST` and a body that does not decode answers `400 INVALID_JSON`. A successful request answers `200` with `{}`. With authentication on, any valid key can send to it.

```bash
curl -s -X POST localhost:3100/api/v1/otlp/v1/logs \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceLogs": [{
      "resource": {"attributes": [
        {"key": "service.name", "value": {"stringValue": "api-gateway"}},
        {"key": "deployment.environment", "value": {"stringValue": "prod"}}
      ]},
      "scopeLogs": [{
        "scope": {"name": "app.logger"},
        "logRecords": [{
          "timeUnixNano": "1763370000000000000",
          "body": {"stringValue": "GET /api/users 200 12ms"},
          "severityText": "INFO",
          "attributes": [
            {"key": "http.method", "value": {"stringValue": "GET"}},
            {"key": "http.status", "value": {"intValue": "200"}}
          ]
        }]
      }]
    }]
  }'
```

The record lands in the index `prod` and reads back as:

```json
{"_raw": "GET /api/users 200 12ms", "_time": "2025-11-17T09:00:00Z", "http.method": "GET", "http.status": 200, "level": "info", "otel.scope.name": "app.logger", "source": "api-gateway", "sourcetype": "otlp"}
```

This route maps records differently from the OTLP listener:

| OTLP | Event |
|---|---|
| `timeUnixNano` | `_time`; without it, a timestamp in the body text, else the time of arrival |
| `body` | `_raw`, records with an empty body included |
| `resource.attributes["service.name"]` | `source` |
| `resource.attributes["host.name"]` | `host` |
| `resource.attributes["deployment.environment"]` | The target index; `main` without it |
| Other resource attributes | `resource.<key>`, with their OTLP types |
| | `sourcetype` `otlp` |
| `severityText` (lower-cased), else `severityNumber` | `level`; a non-zero `severityNumber` is also kept as `severity_number` |
| `traceId`, `spanId`, `flags` | `trace_id`, `span_id`, `flags` |
| Scope name and version | `otel.scope.name`, `otel.scope.version` |
| Log record attributes | Fields of the same name, with their OTLP types |

See the [OpenTelemetry guide](../guides/ingest-data/opentelemetry.md).

## Related

- [Ingest API](./ingest.md): native ingest endpoints
- [Ingest configuration](../configuration/ingest.md): `es_compat`, `splunk_hec`, `otlp` and `limits` settings
- [Shipper conformance](../operations/shipper-conformance.md)

---
sidebar_position: 11
title: Compatibility
description: Elasticsearch _bulk, single-doc ingest, OpenTelemetry OTLP, and Splunk HEC compatibility endpoints.
---

# Compatibility API

Drop-in compatibility endpoints for migrating from existing log pipelines. No code changes required in your log shippers -- just point them at LynxDB.

## Elasticsearch Compatibility

### POST /es/_bulk

Elasticsearch `_bulk` API compatible endpoint. Works out of the box with Filebeat, Logstash, Vector, Fluentd, and any Elasticsearch client library.

#### Mapping

- `_index` is accepted but mapped to `_source` tag (LynxDB is single-index by design).
- `_type` is ignored.
- Timestamp aliases are auto-detected: `@timestamp`, `timestamp`, `time`, `ts`, `datetime`.

#### Example

```bash
curl -X POST localhost:3100/api/v1/es/_bulk \
  -H "Content-Type: application/x-ndjson" \
  -d '{"index": {"_index": "logs"}}
{"message": "hello from filebeat", "@timestamp": "2026-02-14T12:00:00Z", "level": "info"}
{"index": {"_index": "metrics"}}
{"message": "cpu usage high", "@timestamp": "2026-02-14T12:00:01Z", "host": "web-01"}'
```

**Response (200):**

```json
{
  "took": 12,
  "errors": false,
  "items": [
    {
      "index": {
        "_id": "01JKNM3VXQP...",
        "status": 201
      }
    },
    {
      "index": {
        "_id": "01JKNM4ABCD...",
        "status": 201
      }
    }
  ]
}
```

#### Filebeat Configuration

Point Filebeat at LynxDB with zero changes to your existing configuration:

```yaml
# filebeat.yml
output.elasticsearch:
  hosts: ["http://lynxdb:3100/api/v1/es"]
```

#### Logstash Configuration

```ruby
# logstash.conf
output {
  elasticsearch {
    hosts => ["http://lynxdb:3100/api/v1/es"]
  }
}
```

#### Vector Configuration

```toml
# vector.toml
[sinks.lynxdb]
type = "elasticsearch"
endpoints = ["http://lynxdb:3100/api/v1/es"]
```

#### Fluentd Configuration

```xml
<!-- fluentd.conf -->
<match **>
  @type elasticsearch
  host lynxdb
  port 3100
  path /api/v1/es
</match>
```

---

### POST /es/\{index\}/_doc

Elasticsearch single-document ingest endpoint.

#### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `index` | Yes | Index name (mapped to `_source` tag) |

```bash
curl -X POST localhost:3100/api/v1/es/logs/_doc \
  -d '{"message": "single event", "@timestamp": "2026-02-14T12:00:00Z", "level": "info"}'
```

**Response (201):**

```json
{
  "_id": "01JKNM3VXQP...",
  "result": "created"
}
```

---

### GET /es/

Minimal Elasticsearch cluster info for client handshake compatibility. Filebeat and other ES clients query this endpoint on startup to verify connectivity.

```bash
curl -s localhost:3100/api/v1/es/ | jq .
```

**Response (200):**

```json
{
  "name": "lynxdb",
  "cluster_name": "lynxdb",
  "version": {
    "number": "8.0.0",
    "build_flavor": "default"
  },
  "tagline": "LynxDB \u2014 Splunk-power log analytics in a single binary"
}
```

This response satisfies the Elasticsearch client handshake protocol. The version number `8.0.0` ensures compatibility with modern Elasticsearch clients.

---

## OpenTelemetry OTLP

### POST /otlp/v1/logs

Native OTLP/HTTP receiver for logs from OpenTelemetry collectors. Accepts both protobuf and JSON encoding.

#### Content Types

| Content-Type | Format |
|---|---|
| `application/x-protobuf` | OTLP protobuf encoding (default, most efficient) |
| `application/json` | OTLP JSON encoding |

#### OpenTelemetry Collector Configuration

```yaml
# otel-collector-config.yaml
exporters:
  otlphttp:
    endpoint: http://lynxdb:3100/api/v1/otlp

service:
  pipelines:
    logs:
      receivers: [filelog, otlp]
      processors: [batch]
      exporters: [otlphttp]
```

#### Example with JSON Encoding

```bash
curl -X POST localhost:3100/api/v1/otlp/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [
      {
        "resource": {
          "attributes": [
            {"key": "service.name", "value": {"stringValue": "api-gateway"}}
          ]
        },
        "scopeLogs": [
          {
            "logRecords": [
              {
                "timeUnixNano": "1707912000000000000",
                "body": {"stringValue": "GET /api/users 200 12ms"},
                "severityText": "INFO",
                "attributes": [
                  {"key": "http.method", "value": {"stringValue": "GET"}},
                  {"key": "http.status_code", "value": {"intValue": "200"}}
                ]
              }
            ]
          }
        ]
      }
    ]
  }'
```

**Response (200):** Empty response body (per OTLP specification).

#### Field Mapping

OTLP fields are mapped to LynxDB fields as follows:

| OTLP Field | LynxDB Field |
|---|---|
| `timeUnixNano` | `_timestamp` |
| `body.stringValue` | `_raw` |
| `severityText` | `level` |
| `resource.attributes["service.name"]` | `_source` |
| Other `attributes` | Flattened as top-level fields |

---

## Splunk HEC

LynxDB also supports the Splunk HTTP Event Collector (HEC) protocol for existing Splunk forwarders. Configure your Splunk forwarders to send to:

```
http://lynxdb:3100/api/v1/hec
```

### Splunk Universal Forwarder Configuration

```ini
# outputs.conf
[httpout]
httpEventCollectorToken = any-token-here
uri = http://lynxdb:3100/api/v1/hec
```

### HEC Event Format

```bash
curl -X POST localhost:3100/api/v1/hec \
  -H "Authorization: Splunk any-token-here" \
  -d '{
    "event": "user login succeeded",
    "time": 1707912000,
    "host": "web-01",
    "source": "auth-service",
    "sourcetype": "json"
  }'
```

#### Field Mapping

| HEC Field | LynxDB Field |
|---|---|
| `time` | `_timestamp` |
| `event` | `_raw` (or parsed as JSON if object) |
| `host` | `host` |
| `source` | `_source` |
| `sourcetype` | `_sourcetype` |

---

## Migration Quick Reference

| Source | Protocol | LynxDB Endpoint | Config Change |
|---|---|---|---|
| Filebeat | ES bulk | `/api/v1/es` | Change `hosts` to LynxDB |
| Logstash | ES bulk | `/api/v1/es` | Change `hosts` to LynxDB |
| Vector | ES bulk | `/api/v1/es` | Change `endpoints` to LynxDB |
| Fluentd | ES bulk | `/api/v1/es` | Change `host`/`port`/`path` |
| OTEL Collector | OTLP/HTTP | `/api/v1/otlp` | Change `endpoint` to LynxDB |
| Splunk Forwarder | HEC | `/api/v1/hec` | Change `uri` to LynxDB |
| Splunk HEC client | HEC | `/api/v1/hec` | Change URL to LynxDB |
| Any ES client | ES API | `/api/v1/es` | Change base URL |

## Related

- **[Ingest API](/docs/api/ingest)** -- native LynxDB ingest endpoints
- **[Migration from Elasticsearch](/docs/migration/from-elasticsearch)** -- full migration guide
- **[Migration from Splunk](/docs/migration/from-splunk)** -- Splunk to LynxDB migration
- **[Configuration: Ingest](/docs/configuration/ingest)** -- OTLP and syslog receiver settings

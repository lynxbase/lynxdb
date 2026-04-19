---
title: Ingest Data
description: How to get logs into LynxDB using the CLI, REST API, NDJSON, bulk import, and compatibility endpoints for Filebeat and OpenTelemetry.
---

# Ingest Data

LynxDB accepts data through multiple paths: the `lynxdb ingest` CLI command, the REST API, structured import, and drop-in compatibility endpoints for existing log pipelines. This guide covers each method with practical examples.

## Prerequisites

Start a LynxDB server (or use pipe mode for local-only workflows):

```bash
lynxdb server
```

See the [server mode guide](/docs/getting-started/server-mode) for persistent storage options.

---

## Ingest from the CLI

The [`lynxdb ingest`](/docs/cli/ingest) command sends log files or stdin to a running server.

### Ingest a file

```bash
lynxdb ingest access.log
```

### Ingest with metadata

Tag events with `--source` and `--sourcetype` so you can filter on them later:

```bash
lynxdb ingest access.log --source web-01 --sourcetype nginx
lynxdb ingest app.log --source api-server --index production
```

### Ingest from stdin

Pipe any output directly into LynxDB:

```bash
cat access.log | lynxdb ingest --source web-01
kubectl logs deploy/api --since=1h | lynxdb ingest --source k8s-api
docker logs myapp 2>&1 | lynxdb ingest --source docker-myapp
```

### Tune batch size

For large files, increase the batch size to reduce HTTP round-trips:

```bash
lynxdb ingest huge.log --batch-size 10000
```

The default batch size is 5000 lines per request.

---

## Ingest via the REST API

Use the structured ingest endpoint for event payload arrays, and the raw ingest endpoint for newline-delimited logs:

- [`POST /api/v1/ingest`](/docs/api/ingest) for structured JSON event arrays
- [`POST /api/v1/ingest/raw`](/docs/api/ingest) for raw text lines

### Send structured events

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '[
    {
      "event": "user login",
      "source": "auth-api",
      "fields": {
        "user_id": 42,
        "ip": "10.0.1.5",
        "level": "info"
      }
    }
  ]'
```

### Send a structured batch

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '[
    {
      "event": "request started",
      "source": "api",
      "fields": {"path": "/api/users", "level": "info"}
    },
    {
      "event": "connection refused",
      "source": "api",
      "fields": {"service": "redis", "level": "error"}
    }
  ]'
```

### Send raw text

For unstructured log lines, post the raw text:

```bash
echo '192.168.1.1 - - [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234' \
  | curl -X POST localhost:3100/api/v1/ingest/raw --data-binary @-
```

Or send an entire file:

```bash
curl -X POST localhost:3100/api/v1/ingest/raw \
  -H "Content-Type: text/plain" \
  --data-binary @access.log
```

---

## Structured import

The [`lynxdb import`](/docs/cli/ingest) command handles structured formats (NDJSON, CSV, Elasticsearch bulk exports) and preserves field types and timestamps. `ndjson` and `csv` are normalized into LynxDB's structured event envelope; `esbulk` uses the Elasticsearch-compatible bulk API.

### Import NDJSON

```bash
lynxdb import events.json
lynxdb import events.ndjson
```

### Import CSV

```bash
lynxdb import splunk_export.csv
lynxdb import data.csv --source web-01 --index nginx
```

### Import Elasticsearch bulk export

```bash
lynxdb import es_dump.json --format esbulk
```

### Validate before importing

Use `--dry-run` to check the file without writing any data:

```bash
lynxdb import events.json --dry-run
```

## Timestamp auto-detection

LynxDB automatically detects timestamps from these commonly used field names:

- `_timestamp`
- `timestamp`
- `@timestamp`
- `time`
- `ts`
- `datetime`

If none of these fields are present, LynxDB assigns the server receive time. You do not need to configure timestamp parsing.

---

## Drop-in compatibility endpoints

LynxDB provides compatibility endpoints so you can migrate existing log pipelines without changing your shipper configuration.

### Filebeat / Logstash / Vector (Elasticsearch `_bulk` API)

Point any tool that speaks the Elasticsearch `_bulk` protocol at LynxDB:

```yaml
# filebeat.yml
output.elasticsearch:
  hosts: ["http://lynxdb:3100/api/v1/es"]
```

```yaml
# vector.toml
[sinks.lynxdb]
type = "elasticsearch"
endpoints = ["http://lynxdb:3100/api/v1/es"]
```

The `_index` field from the bulk request is mapped to the `_source` tag in LynxDB. No other configuration is needed. Prefer `/api/v1/es/_bulk`; `/api/v1/ingest/bulk` is an alias. See the [compatibility API reference](/docs/api/compatibility) for details.

### OpenTelemetry Collector (OTLP)

Send logs from an OpenTelemetry Collector using the OTLP/HTTP exporter:

```yaml
# otel-collector-config.yaml
exporters:
  otlphttp:
    endpoint: http://lynxdb:3100/api/v1/otlp
```

### Splunk HEC (HTTP Event Collector)

If you have existing Splunk forwarders, point them at the HEC-compatible endpoint:

```
http://lynxdb:3100/api/v1/ingest/hec
```

---

## Pipe mode (no server)

You do not need a running server to analyze logs. LynxDB can ingest data into an ephemeral in-memory engine and query it in one step:

```bash
cat app.log | lynxdb query '| stats count by level'
lynxdb query --file '/var/log/nginx/*.log' '| where status>=500 | top 10 uri'
```

Data is not persisted. The engine starts, ingests, queries, prints results, and exits. See the [pipe mode guide](/docs/getting-started/pipe-mode) for more details.

---

## Monitoring ingestion

After ingesting data, verify it landed correctly:

```bash
# Check server stats
lynxdb status

# Count recently ingested events
lynxdb count --since 5m

# Peek at a sample of events
lynxdb sample 5

# See all discovered fields
lynxdb fields
```

See the [`lynxdb status`](/docs/cli/server) and [`lynxdb fields`](/docs/cli/shortcuts) commands for more options.

---

## Next steps

- [Search and filter logs](/docs/guides/search-and-filter) -- query the data you just ingested
- [Run aggregations](/docs/guides/aggregations) -- compute statistics across your logs
- [REST API: Ingest](/docs/api/ingest) -- full API reference for the ingest endpoint
- [CLI: `ingest`](/docs/cli/ingest) -- complete flag reference for the ingest command

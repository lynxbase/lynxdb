---
title: Ingest Data
description: Get logs into LynxDB with the CLI, the REST API, structured import, log shippers and the syslog receiver.
---

# Ingest Data

A LynxDB server accepts data from the `lynxdb ingest` and `lynxdb import`
commands, the REST API, log shippers that speak the Elasticsearch bulk API,
OTLP or Splunk HEC, and syslog. Every path ends in the same storage and is
queried with the same [PPL](../ppl/overview.md) queries.

## Prerequisites

Start a server:

```bash
lynxdb server
```

See [Server Mode](../getting-started/server-mode.md) for storage options.
To analyze a file once without storing it, use
[pipe mode](../getting-started/pipe-mode.md) instead.

## Ingest from the CLI

[`lynxdb ingest`](../cli/ingest.md) sends a file or standard input to the
server, one event per line.

```bash
# A file
lynxdb ingest access.log

# Source metadata and a target index
lynxdb ingest access.log --source web-01 --sourcetype nginx
lynxdb ingest app.log --source api-server --index production

# Standard input
cat access.log | lynxdb ingest --source web-01
kubectl logs deploy/api --since=1h | lynxdb ingest --source k8s-api
docker logs myapp 2>&1 | lynxdb ingest --source docker-myapp
```

Lines are sent in batches of 5000; `--batch-size` changes that. The source
appears in queries as `_source`, the index as the name after `source=`:

```bash
lynxdb query 'source=production _source=api-server | stats count() by level'
```

## Ingest through the REST API

- `POST /api/v1/ingest`: a JSON array of structured events
- `POST /api/v1/ingest/raw`: newline-delimited text or NDJSON, one event per
  line

See the [Ingest API](../api/ingest.md) for every option.

### Structured events

Each object carries the event text in `event`, optional metadata (`time` in
epoch seconds, `source`, `sourcetype`, `host`, `index`) and typed `fields`:

```bash
curl -X POST localhost:3100/api/v1/ingest \
  -H "Content-Type: application/json" \
  -d '[
    {
      "event": "user login",
      "source": "auth-api",
      "fields": {"user_id": 42, "ip": "10.0.1.5", "level": "info"}
    },
    {
      "event": "connection refused",
      "source": "api",
      "fields": {"service": "redis", "level": "error"}
    }
  ]'
```

```json
{"data":{"accepted":2,"failed":0}}
```

### Raw lines

```bash
echo '192.168.1.1 - - [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234' \
  | curl -X POST localhost:3100/api/v1/ingest/raw -H 'X-Source: nginx' --data-binary @-

# A whole file into the index "web"
curl -X POST localhost:3100/api/v1/ingest/raw \
  -H "Content-Type: text/plain" \
  -H "X-Source: web-01" \
  -H "X-Index: web" \
  --data-binary @access.log
```

The `X-Source`, `X-Source-Type` and `X-Index` headers set `_source`
(default `http`), `_sourcetype` (default `raw`) and the index (default
`main`).

## What gets extracted

The server's ingest pipeline detects the timestamp of each line, turns
top-level JSON values into typed fields and extracts `key=value` pairs from
other lines. Everything else stays in `_raw` and is extracted at query
time. See [Field Extraction](./field-extraction.md).

## Structured import

[`lynxdb import`](../cli/ingest.md) reads NDJSON, CSV and Elasticsearch bulk
exports and keeps field types and timestamps. The format is detected from the
file extension and content, or set with `--format`.

```bash
# NDJSON
lynxdb import events.ndjson

# CSV with a header row
lynxdb import export.csv
lynxdb import data.csv --source web-01 --index nginx

# Elasticsearch bulk export
lynxdb import es_dump.json --format esbulk

# Check a file without importing it
lynxdb import events.json --dry-run

# Standard input
cat events.ndjson | lynxdb import - --format ndjson
```

For NDJSON and CSV, the event time is read from the first of these fields
present: `time`, `_time`, `timestamp`, `_timestamp`, `@timestamp`, `ts`,
`datetime`. Otherwise the server assigns the time as it does for raw lines.

## Log shippers

LynxDB accepts the protocols of common log shippers, so an existing shipper
only needs LynxDB's address:

| Shipper | Protocol | LynxDB endpoint |
|---|---|---|
| [Filebeat](./ingest-data/filebeat.md) | Elasticsearch bulk | `http://lynxdb:3100` |
| [Fluent Bit](./ingest-data/fluent-bit.md) | Elasticsearch bulk | `http://lynxdb:3100` |
| [Vector](./ingest-data/vector.md) | Elasticsearch bulk | `http://lynxdb:3100` |
| [OpenTelemetry Collector](./ingest-data/opentelemetry.md) | OTLP/HTTP, OTLP/gRPC | `http://lynxdb:4318`; gRPC on the address set with `--otlp-grpc-listen` |
| [Splunk HEC senders](./ingest-data/splunk-hec.md) | HTTP Event Collector | `http://lynxdb:3100/services/collector/event` |

### Elasticsearch bulk API

Tools that write to Elasticsearch with `_bulk` can point at LynxDB:

```yaml
# filebeat.yml
output.elasticsearch:
  hosts: ["http://lynxdb:3100"]
```

```yaml
# vector.yaml
sinks:
  lynxdb:
    type: elasticsearch
    inputs: [app_logs]
    endpoints: ["http://lynxdb:3100"]
    mode: bulk
```

Each document is stored in the LynxDB index named by the bulk request's
`_index` (a document without one goes to `default`). Query it with
`source=<index>`, or `source=filebeat-*` for a family of indexes; `lynxdb
indexes` lists the names. `POST /_bulk`, `POST /<index>/_bulk`,
`/api/v1/es/_bulk` and `/api/v1/ingest/bulk` all accept bulk requests. See
the [compatibility API](../api/compatibility.md).

### OpenTelemetry (OTLP)

The OTLP/HTTP receiver listens on `0.0.0.0:4318` by default. OTLP/gRPC is
off until `--otlp-grpc-listen` (or `ingest.otlp.grpc_listen`) sets an
address.

```yaml
# otel-collector-config.yaml
exporters:
  otlphttp/lynxdb:
    endpoint: http://lynxdb:4318
```

### Splunk HEC

Senders that post to Splunk's HTTP Event Collector can use:

```text
http://lynxdb:3100/services/collector/event
```

## Native syslog receiver

LynxDB receives syslog over UDP and TCP without an intermediary, which suits
network devices and hosts running rsyslog or syslog-ng.

```bash
# UDP and TCP on port 5514 (the default when no port is given)
lynxdb server --syslog :5514

# The standard port 514 (needs root or CAP_NET_BIND_SERVICE)
lynxdb server --syslog :514
```

Or in the config file:

```yaml
syslog:
  udp: ":5514"
  tcp: ":5514"
```

### rsyslog

```bash
# /etc/rsyslog.d/50-lynxdb.conf
# Over TCP
*.* @@lynxdb-host:5514

# Or over UDP
*.* @lynxdb-host:5514
```

### syslog-ng

```text
destination d_lynxdb {
  tcp("lynxdb-host" port(5514));
};
log { source(s_src); destination(d_lynxdb); };
```

### TLS

The TCP listener can use the server's TLS configuration:

```bash
lynxdb server --tls --syslog-tcp :6514 --syslog-tls
```

```text
# /etc/rsyslog.d/50-lynxdb-tls.conf
$ActionSendStreamDriver gtls
$ActionSendStreamDriverMode 1
$ActionSendStreamDriverAuthMode anon
*.* @@lynxdb-host:6514
```

The receiver parses RFC 5424, RFC 3164 and raw messages, detecting the
dialect per message. See the [syslog configuration](../configuration/syslog.md)
for framing, batching and connection limits.

## Pipe mode (no server)

To analyze logs without storing them, query them directly. The CLI loads the
input into an in-process engine, runs the query and exits:

```bash
cat app.log | lynxdb query 'source=main | stats count() by level'
lynxdb query --file '/var/log/nginx/*.log' 'source=main status>=500 | top 10 uri'
```

See [Pipe Mode](../getting-started/pipe-mode.md).

## Check what arrived

```bash
# Server overview: event counts, storage, indexes
lynxdb status

# Indexes
lynxdb indexes

# Events received in the last 5 minutes, per index
lynxdb query 'source=* | stats count() by index' --since 5m

# A few recent events
lynxdb query 'source=main | head 5' --since 5m

# The field catalog
lynxdb fields
```

Shipper data can take a few seconds to appear: the server buffers small
shipper batches before writing them (`ingest.staging.max_age`, 5 seconds by
default).

## Next steps

- [Field Extraction](./field-extraction.md): what ingestion extracts and how to extract more
- [Your First Query](../getting-started/first-query.md): query the data you ingested
- [Ingest API](../api/ingest.md): request and response reference
- [CLI: `ingest`](../cli/ingest.md): every flag

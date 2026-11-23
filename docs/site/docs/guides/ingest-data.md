---
title: Ingest Data
description: Get logs into LynxDB with the CLI, the REST API, log shippers and the syslog receiver.
---

# Ingest Data

A LynxDB server accepts data from the `lynxdb ingest` command, the REST API, log shippers that speak the Elasticsearch bulk API,
OTLP or Splunk HEC, and syslog. Every path ends in the same storage and is
queried with the same [PPL](../ppl/overview.md) queries.

Each event goes to an index, `main` unless the request names another. An
index comes into being with its first events. Its events are kept as JSON
objects: the keys of a JSON event are its fields, and the members of a nested
object are fields named by their path, such as `http.status`.

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
server: log lines, NDJSON, CSV or an Elasticsearch bulk export. `--format`
names the format; by default the file extension or the first line decides.

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
and source type are stored in the fields `source` and `sourcetype`, and the
index is the name after `source=` in a query:

```bash
lynxdb ingest OpenSSH_2k.log --source sshd-01 --sourcetype sshd --index auth-logs
lynxdb query 'source=auth-logs | where source = "sshd-01" | stats count() by sourcetype'
```

```json
{"count()":2000,"sourcetype":"sshd"}
```

## Ingest through the REST API

- `POST /api/v1/ingest`: NDJSON, one JSON object per line, stored as sent
- `POST /api/v1/ingest/raw`: log text, one event per line

Both take the index from `?index=` or the `X-Index` header. See the
[Ingest API](../api/ingest.md) for every option.

### JSON events

Each line is one event, and its keys are its fields:

```bash
curl -X POST 'localhost:3100/api/v1/ingest?index=app' \
  --data-binary @- <<'EOF'
{"timestamp":"2026-03-02T09:00:01.000Z","level":"error","service":"checkout","http":{"method":"POST","status":502}}
{"timestamp":"2026-03-02T09:00:02.000Z","level":"info","service":"catalog","http":{"method":"GET","status":200}}
EOF
```

```json
{"data":{"accepted":2,"failed":0,"expired":0}}
```

The event time comes from the first of `_time`, `timestamp`, `@timestamp`,
`time` and `ts` that the event holds (`storage.time_fields`), otherwise from
the time the event arrived. A line that is not a JSON object is kept as an
event holding the text in `_raw`.

### Raw lines

```bash
echo '2026-11-17T08:00:00.105Z host=web-03 level=ERROR status=502 msg="GET /api/v1/users completed"' \
  | curl -X POST localhost:3100/api/v1/ingest/raw -H 'X-Source: web-03' --data-binary @-

# A whole file into the index "web"
curl -X POST localhost:3100/api/v1/ingest/raw \
  -H "Content-Type: text/plain" \
  -H "X-Source: web-01" \
  -H "X-Index: web" \
  --data-binary @access.log
```

A JSON line passes as it is. Any other line becomes an event with the line
in `_raw`, its leading timestamp in `_time` and its `key=value` pairs as
fields. The `X-Source`, `X-Source-Type` and `X-Host` headers set `source`
(default `http`), `sourcetype` (default `raw`) and `host`. See
[Field Extraction](./field-extraction.md) for what is extracted.

## Structured files

`lynxdb ingest` sends NDJSON as it is, each CSV row as a JSON object keyed by
the header row, and an Elasticsearch bulk export to the server's bulk
endpoint:

```bash
# NDJSON
lynxdb ingest events.ndjson

# CSV with a header row
lynxdb ingest export.csv
lynxdb ingest data.csv --source web-01 --index nginx

# Elasticsearch bulk export
lynxdb ingest es_dump.json --format esbulk

# Standard input
cat events.ndjson | lynxdb ingest --format ndjson
```

The server takes the event time from the first of its time fields
(`storage.time_fields`) that the event holds. See
[ingest](../cli/ingest.md) for how each format is read.

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

With authentication on, each shipper needs a key with the `ingest` scope;
the guides show where it goes.

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

Each document is stored as sent, in the LynxDB index named by the bulk
request's `_index`. A document without one goes to the index in the URL
(`POST /<index>/_bulk`, `POST /_data_stream/<name>/_bulk`), otherwise to
`main`. Query it with `source=<index>`, or `source=filebeat-*` for a family
of indexes; `lynxdb indexes` lists the names. `POST /_bulk`,
`POST /<index>/_bulk` and `POST /_data_stream/<name>/_bulk` all accept bulk requests. See the [compatibility API](../api/compatibility.md).

### OpenTelemetry (OTLP)

The OTLP/HTTP receiver listens on `localhost:4318` by default;
`--otlp-http-listen 0.0.0.0:4318` takes OTLP from other hosts. OTLP/gRPC is
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
input into a database of its own, runs the query and exits:

```bash
cat app.log | lynxdb query --file - 'stats count() by level'
lynxdb query --file '/var/log/nginx/*.log' 'status>=500 | top 10 uri'
```

See [Pipe Mode](../getting-started/pipe-mode.md).

## Check what arrived

```bash
# Server overview: event counts, storage, indexes
lynxdb status

# Events, bytes and time range of each index
lynxdb indexes

# A few events
lynxdb query 'source=main | head 5'

# Events of the last 5 minutes
lynxdb query 'source=main | stats count()' --since 5m

# The field catalog
lynxdb fields
```

A query syncs the database before it runs, so it sees every event whose
ingest request was answered.

## Next steps

- [Field Extraction](./field-extraction.md): what ingestion extracts and how to extract more
- [Your First Query](../getting-started/first-query.md): query the data you ingested
- [Ingest API](../api/ingest.md): request and response reference
- [CLI: `ingest`](../cli/ingest.md): every flag

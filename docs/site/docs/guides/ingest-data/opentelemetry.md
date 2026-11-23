---
title: OpenTelemetry Collector to LynxDB
description: Send OpenTelemetry Collector logs to LynxDB using OTLP HTTP or OTLP gRPC.
---

# OpenTelemetry Collector to LynxDB

## If you already run the Collector

Point the OTLP/HTTP exporter at LynxDB. The server's OTLP/HTTP receiver
listens on `0.0.0.0:4318` by default (`--otlp-http-listen` or
`ingest.otlp.http_listen` changes it).

```diff
 exporters:
   otlphttp/lynxdb:
-    endpoint: "http://collector-backend:4318"
+    endpoint: "http://lynxdb:4318"
```

The OTLP/gRPC receiver is off by default. Start the server with an address
for it, then use the `otlp` exporter:

```bash
lynxdb server --otlp-grpc-listen 0.0.0.0:4317
```

```yaml
exporters:
  otlp/lynxdb:
    endpoint: "lynxdb:4317"
    tls:
      insecure: true
```

## Full annotated config

```yaml
# OpenTelemetry Collector logs pipeline for LynxDB.
receivers:
  filelog:
    include: [/var/log/app/*.log]

exporters:
  otlphttp/lynxdb:
    endpoint: "http://lynxdb:4318"
    compression: gzip

service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlphttp/lynxdb]
```

## How records are stored

OTLP log records are written to the `main` index with `sourcetype` `otlp`.
The record body becomes `_raw`. `_time` is the record's time, or its
observed time when it has none, or the time it arrived when it has neither. The
`service.name` resource attribute becomes `source` (falling back to
`host.name`, then `otlp`), and `host.name` also fills `host`. Other fields:

| Field | From |
|-------|------|
| `resource.<name>` | Each resource attribute, such as `resource.service.name` |
| `<name>` | Each log record attribute, under its own name |
| `level` | The severity text in lowercase, or `trace` ... `fatal` from the severity number |
| `trace_id`, `span_id` | The record's trace context, hex encoded |
| `scope.name`, `scope.version` | The instrumentation scope |

Attribute names keep their dots, so they are queried as written:

```bash
lynxdb query 'source=main level=error | where source = "checkout" | stats count() by resource.host.name'
```

```json
{"count()":1,"resource.host.name":"node-7"}
```

## Troubleshooting

Use `http://` for OTLP/HTTP unless LynxDB sits behind a TLS-terminating
proxy. For gRPC, set `tls.insecure: true` when connecting directly to the
plaintext listener. Large gRPC batches are limited by
`--otlp-grpc-max-recv-bytes` (default 16 MB).

## Supported behavior

Supported: OTLP/HTTP with JSON or protobuf, OTLP/gRPC with protobuf, gzip
compression, and partial-success export responses.

Not supported: OTLP traces and metrics. The OTLP/HTTP listener answers
`404` to `/v1/traces` and `/v1/metrics`, and the gRPC listener answers
`Unimplemented`, so an exporter that sends them reports the data as not
sent. Point only logs pipelines at LynxDB.

---
title: OpenTelemetry Collector to LynxDB
description: Send OpenTelemetry Collector logs to LynxDB using OTLP HTTP or OTLP gRPC.
---

# OpenTelemetry Collector to LynxDB

## If you are already running the Collector

Change the OTLP exporter endpoint to LynxDB. LynxDB listens on the standard OTLP ports by default: `4318` for HTTP and `4317` for gRPC.

```diff
 exporters:
   otlphttp/lynxdb:
-    endpoint: "http://collector-backend:4318"
+    endpoint: "http://lynxdb:4318"
```

For gRPC:

```yaml
exporters:
  otlp/lynxdb:
    endpoint: "lynxdb:4317"
    tls:
      insecure: true
```

## Full annotated config

```yaml
# OpenTelemetry Collector logs exporter for LynxDB.
exporters:
  otlphttp/lynxdb:
    endpoint: "http://lynxdb:4318"
    compression: gzip

service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlphttp/lynxdb]
# Point endpoint at LynxDB's OTLP HTTP listener, usually http://host:4318.
```

Generate the same shape locally with:

```bash
lynxdb shippers config otelcol --remote http://lynxdb:4318
```

## Migrating from an OTLP backend

LynxDB accepts OTLP logs. Resource attributes are flattened with a `resource.` prefix, scope metadata is preserved, and `service.name` becomes the LynxDB `_source` when present.

## Troubleshooting

Use `http://` for OTLP HTTP unless LynxDB is behind a TLS-terminating proxy. For gRPC, set `tls.insecure: true` when connecting directly to the default plaintext listener.

## Supported behavior

Supported: OTLP HTTP JSON/protobuf, OTLP gRPC protobuf, gzip compression, and partial-success compatible log export responses.

Not supported: OTLP traces and metrics ingestion. LynxDB accepts log records only.

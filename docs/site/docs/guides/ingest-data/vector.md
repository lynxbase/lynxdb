---
title: Vector to LynxDB
description: Send Vector logs to LynxDB through the Elasticsearch sink.
---

# Vector to LynxDB

## If you are already running Vector

Keep the Elasticsearch sink and point `endpoints` at LynxDB.

```diff
 sinks:
   lynxdb:
     type: elasticsearch
-    endpoints: ["http://elasticsearch:9200"]
+    endpoints: ["http://lynxdb:3100"]
     mode: bulk
```

## Full annotated config

```yaml
# Vector configuration for LynxDB.
sources:
  files:
    type: file
    include:
      - /var/log/*.log

sinks:
  lynxdb:
    type: elasticsearch
    inputs: [files]
    endpoints: ["http://lynxdb:3100"]
    compression: zstd
    mode: bulk
# Vector zstd compression is accepted by LynxDB's ES bulk endpoint.
```

Generate the same shape locally with:

```bash
lynxdb shippers config vector --remote http://lynxdb:3100
```

## Migrating from Elasticsearch

Vector's Elasticsearch sink can continue using `mode: bulk`. LynxDB handles Vector's health check and zstd request compression.

## Troubleshooting

If the Vector health check fails, confirm it is sending `HEAD /<index>` to the same host and port as the bulk sink. LynxDB returns `200 OK` for index health probes.

## Supported behavior

Supported: bulk mode, zstd compression, health checks, and `_index` to `_source` mapping.

Not supported yet: Vector `mode: data_stream`; that path is tracked separately from bulk mode.

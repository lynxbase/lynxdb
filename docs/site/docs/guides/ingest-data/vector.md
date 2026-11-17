---
title: Vector to LynxDB
description: Send Vector logs to LynxDB through the Elasticsearch sink.
---

# Vector to LynxDB

## If you already run Vector

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
    mode: bulk
    bulk:
      index: "vector-logs"
    compression: zstd
# LynxDB's bulk endpoint accepts zstd-compressed requests.
```

## Where the events go

Each event is stored in the LynxDB index named by the sink's `bulk.index`
(here `vector-logs`). Vector's default index, `vector-%Y.%m.%d`, creates one
index per day; query them together with `source=vector-*`.

```bash
lynxdb query 'source=vector-logs | stats count() by _source'
```

The file source's `file` field and the `host` field arrive as fields;
`message` holds the log line. Extract more from it at query time; see
[Field Extraction](../field-extraction.md).

## Troubleshooting

Vector's health check sends `GET /` and `GET /_cluster/health` to the
endpoint; LynxDB answers both. If the sink reports `400` responses, check
that the index name uses only lowercase letters, digits, `.`, `_`, `-` and
`+`.

## Supported behavior

Supported: `mode: bulk`, `index` and `create` bulk actions, zstd and gzip
compression, health checks, and `_index` routing to LynxDB indexes.

Not supported: Elasticsearch query APIs, and update or delete actions.

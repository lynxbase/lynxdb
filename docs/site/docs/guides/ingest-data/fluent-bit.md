---
title: Fluent Bit to LynxDB
description: Send Fluent Bit logs to LynxDB through the Elasticsearch output plugin.
---

# Fluent Bit to LynxDB

## If you are already running Fluent Bit

Keep the `es` output and change only the host and port.

```diff
 [OUTPUT]
     Name es
     Match *
-    Host elasticsearch
-    Port 9200
+    Host lynxdb
+    Port 3100
     Suppress_Type_Name On
```

## Full annotated config

```ini
# Fluent Bit configuration for LynxDB.
[INPUT]
    Name tail
    Path /var/log/*.log
    Tag app
    Read_From_Head On

[OUTPUT]
    Name es
    Match *
    Host lynxdb
    Port 3100
    HTTP_User ""
    Logstash_Format Off
    Index fluent-bit
    Suppress_Type_Name On
    Generate_ID On
    Compress gzip
# Generate_ID is harmless; LynxDB echoes IDs but remains append-only.
```

Generate the same shape locally with:

```bash
lynxdb shippers config fluent-bit --remote http://lynxdb:3100
```

## Migrating from Elasticsearch

The Fluent Bit Elasticsearch output sends NDJSON bulk requests. LynxDB stores the configured `Index` value as `_source`, writes every event to `main`, and accepts gzip-compressed request bodies.

## Troubleshooting

Use `Suppress_Type_Name On` for modern Fluent Bit versions. If you see retry loops, run `lynxdb doctor shippers` and check the Fluent Bit container logs for HTTP 4xx or 5xx responses.

## Supported behavior

Supported: `Name es`, bulk mode, gzip compression, generated IDs echoed in responses, and setup-free log ingestion.

Not supported: Elasticsearch template installation, update/delete actions, and Elasticsearch query APIs.

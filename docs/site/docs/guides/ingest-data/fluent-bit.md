---
title: Fluent Bit to LynxDB
description: Send Fluent Bit logs to LynxDB through the Elasticsearch output plugin.
---

# Fluent Bit to LynxDB

## If you already run Fluent Bit

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
    Logstash_Format Off
    Index fluent-bit
    Suppress_Type_Name On
    Generate_ID On
    Compress gzip
# LynxDB echoes generated IDs in bulk responses; storage stays append-only.
```

## Where the events go

The `es` output sends NDJSON bulk requests. LynxDB stores the events in the
index named by `Index` (here `fluent-bit`) and accepts gzip-compressed
bodies. With `Logstash_Format On`, the index name carries the date, such as
`logstash-2026.02.17`; `lynxdb indexes` shows the names.

```bash
lynxdb query 'source=fluent-bit | stats count() by _source'
```

Records from Fluent Bit's Kubernetes filter keep their `kubernetes` object;
unpack it with `flatten` or `spath`:

```bash
lynxdb query 'source=fluent-bit | flatten kubernetes | stats count() by namespace_name, pod_name'
```

## Troubleshooting

Use `Suppress_Type_Name On` with current Fluent Bit versions. If you see
retry loops, check the Fluent Bit logs for HTTP 4xx or 5xx responses; a
`400` with `invalid_index_name_exception` means the index name has
characters LynxDB does not accept (index names use lowercase letters,
digits, `.`, `_`, `-` and `+`).

## Supported behavior

Supported: `Name es`, bulk requests, gzip compression, generated IDs echoed
in responses, and ingestion without template setup.

Not supported: Elasticsearch template installation, update and delete
actions, and Elasticsearch query APIs.

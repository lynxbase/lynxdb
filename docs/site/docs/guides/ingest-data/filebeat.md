---
title: Filebeat to LynxDB
description: Point Filebeat at LynxDB using the Elasticsearch bulk-compatible endpoint.
---

# Filebeat to LynxDB

## If you already run Filebeat

Change the Elasticsearch host to LynxDB and keep the output type
`elasticsearch`.

```diff
 output.elasticsearch:
-  hosts: ["http://elasticsearch:9200"]
+  hosts: ["http://lynxdb:3100"]
+  allow_older_versions: true
```

Restart Filebeat, then check that events arrive:

```bash
lynxdb indexes
lynxdb query 'source=filebeat-* | stats count() as total by log.file.path'
```

## Full annotated config

```yaml
# Filebeat configuration for LynxDB.
# Change only hosts if LynxDB is not reachable at this address.
filebeat.inputs:
  - type: filestream
    id: lynxdb-files
    paths:
      - /var/log/*.log

output.elasticsearch:
  hosts: ["http://lynxdb:3100"]
  allow_older_versions: true

# LynxDB accepts the setup probes, but there are no templates or ILM
# policies to install.
setup.template.enabled: false
setup.ilm.enabled: false
```

## Where the events go

LynxDB accepts `POST /_bulk`, `POST /<index>/_bulk` and the setup requests
Filebeat sends at startup. Each document is stored in the LynxDB index named
by its bulk `_index`, which for Filebeat is usually `filebeat-<version>`.
`lynxdb indexes` lists the exact names; `source=filebeat-*` reads them all.

Each document is stored as sent. Its `@timestamp` sets the event time, its
`message` holds the log line, and the members of nested objects are fields
named by their path, such as `log.file.path`, `host.name` and `agent.version`
(see [Working with JSON Logs](../json-processing.md)):

```bash
lynxdb query 'source=filebeat-* | fields _time, host.name, log.file.path, message | head 2'
```

## Troubleshooting

If Filebeat rejects the server version, set `allow_older_versions: true` or
change the version LynxDB advertises with `ingest.es_compat.advertised_version`
(default `8.15.0`).

If startup logs mention ILM or templates, set `setup.ilm.enabled: false` and
`setup.template.enabled: false`. LynxDB answers the common setup endpoints,
but disabling setup avoids extra requests.

## Supported behavior

Supported: Elasticsearch bulk ingest, gzip, zstd and snappy request bodies,
Filebeat's setup probes, and `_id` values echoed in bulk responses.

Not supported: the Elasticsearch query DSL, ILM state, template storage,
`_id`-based deduplication, document updates and deletes.

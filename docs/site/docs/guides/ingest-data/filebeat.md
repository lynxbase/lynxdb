---
title: Filebeat to LynxDB
description: Point Filebeat at LynxDB using the Elasticsearch bulk-compatible endpoint.
---

# Filebeat to LynxDB

## If you are already running Filebeat

Change the Elasticsearch host to LynxDB and keep the output type as `elasticsearch`.

```diff
 output.elasticsearch:
-  hosts: ["http://elasticsearch:9200"]
+  hosts: ["http://lynxdb:3100"]
+  allow_older_versions: true
```

Save the config, restart Filebeat, then check:

```bash
lynxdb shippers
lynxdb query 'FROM main | STATS count AS total BY _source'
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

setup.template.enabled: false
setup.ilm.enabled: false
# LynxDB accepts Filebeat's ES bulk protocol but stores logs append-only.
```

Generate the same shape locally with:

```bash
lynxdb shippers config filebeat --remote http://lynxdb:3100
```

## Migrating from Elasticsearch

Filebeat can keep its Elasticsearch output. LynxDB accepts `POST /_bulk`, indexed bulk paths, and the setup probes Filebeat performs during startup.

LynxDB maps the bulk `_index` value to the `_source` field. Every shipper-originated event is written to the LynxDB `main` index.

## Troubleshooting

If Filebeat rejects the server version, set `allow_older_versions: true` or configure LynxDB's advertised Elasticsearch version with `ingest.es_compat.advertised_version`.

If startup logs mention ILM or templates, disable `setup.ilm.enabled` and `setup.template.enabled`. LynxDB stubs the common setup endpoints, but disabling setup avoids extra probes.

Run:

```bash
lynxdb doctor shippers
```

## Supported behavior

Supported: Elasticsearch bulk ingest, gzip/zstd/snappy HTTP request decoding, setup probes, `_id` echoing in bulk responses.

Not supported: Elasticsearch query DSL, ILM state, template storage, `_id`-based deduplication, document updates, and deletes.

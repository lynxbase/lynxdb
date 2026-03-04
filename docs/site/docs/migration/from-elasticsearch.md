---
title: Migrating from Elasticsearch
description: Migrate from Elasticsearch to LynxDB -- _bulk API compatibility, Filebeat and Logstash configuration, and data export.
---

# Migrating from Elasticsearch

LynxDB provides a drop-in compatible `_bulk` API endpoint, making migration from Elasticsearch straightforward. Most log shippers (Filebeat, Logstash, Fluentd, Vector) can be repointed to LynxDB with a single config change.

## _bulk API Compatibility

LynxDB's `POST /api/v1/ingest/bulk` endpoint accepts the Elasticsearch `_bulk` format:

```bash
curl -X POST localhost:3100/api/v1/ingest/bulk -H 'Content-Type: application/x-ndjson' -d '
{"index": {"_index": "nginx"}}
{"@timestamp": "2026-01-15T14:23:01Z", "level": "error", "message": "connection refused"}
{"index": {"_index": "nginx"}}
{"@timestamp": "2026-01-15T14:23:02Z", "level": "info", "message": "request handled"}
'
```

Key mapping:
- `_index` is mapped to the `_source` tag in LynxDB
- `@timestamp` is auto-detected as the event timestamp
- All other fields are indexed with schema-on-read

## Migration Steps

### Step 1: Set Up LynxDB

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
lynxdb server --data-dir /var/lib/lynxdb
```

### Step 2: Repoint Log Shippers

#### Filebeat

Change the output in `filebeat.yml`:

```yaml
# Before (Elasticsearch)
# output.elasticsearch:
#   hosts: ["localhost:9200"]
#   index: "filebeat-%{+yyyy.MM.dd}"

# After (LynxDB)
output.elasticsearch:
  hosts: ["localhost:3100"]
  path: "/api/v1/ingest"
  index: "filebeat"
  # No template or ILM configuration needed
```

LynxDB accepts the Elasticsearch output format from Filebeat with zero additional configuration.

#### Logstash

Change the output in your Logstash pipeline:

```ruby
# Before (Elasticsearch)
# output {
#   elasticsearch {
#     hosts => ["localhost:9200"]
#     index => "logstash-%{+YYYY.MM.dd}"
#   }
# }

# After (LynxDB)
output {
  elasticsearch {
    hosts => ["localhost:3100"]
    path => "/api/v1/ingest"
    index => "logstash"
    # Disable template management (not needed)
    manage_template => false
    ilm_enabled => false
  }
}
```

#### Fluentd

```xml
# Before (Elasticsearch)
# <match **>
#   @type elasticsearch
#   host localhost
#   port 9200
# </match>

# After (LynxDB)
<match **>
  @type elasticsearch
  host localhost
  port 3100
  path /api/v1/ingest
  type_name _doc
  logstash_format false
</match>
```

#### Vector

```toml
# Before (Elasticsearch)
# [sinks.elasticsearch]
# type = "elasticsearch"
# endpoints = ["http://localhost:9200"]

# After (LynxDB)
[sinks.lynxdb]
type = "elasticsearch"
endpoints = ["http://localhost:3100"]
api_version = "v7"
bulk.index = "vector"
```

#### Fluent Bit

```ini
# Before (Elasticsearch)
# [OUTPUT]
#     Name  es
#     Host  localhost
#     Port  9200

# After (LynxDB)
[OUTPUT]
    Name  es
    Host  localhost
    Port  3100
    Path  /api/v1/ingest
    Suppress_Type_Name On
```

### Step 3: Export Historical Data

Export data from Elasticsearch for import into LynxDB:

```bash
# Using elasticdump
elasticdump \
  --input=http://localhost:9200/my-index \
  --output=es_export.json \
  --type=data \
  --limit=10000

# Import into LynxDB
lynxdb import es_export.json --format esbulk
```

```bash
# Using Elasticsearch scroll API
curl -s 'localhost:9200/my-index/_search?scroll=5m&size=10000' \
  -d '{"query":{"match_all":{}}}' | jq -c '.hits.hits[]._source' > export.ndjson

# Import into LynxDB
lynxdb import export.ndjson
```

```bash
# Validate before importing
lynxdb import es_export.json --format esbulk --dry-run
```

### Step 4: Convert Queries

| Elasticsearch DSL | LynxDB SPL2 |
|---|---|
| `{"match": {"level": "error"}}` | `level=error` |
| `{"range": {"status": {"gte": 500}}}` | `status>=500` |
| `{"bool": {"must": [...]}}` | `level=error AND status>=500` |
| `{"aggs": {"by_host": {"terms": {"field": "host"}}}}` | `\| stats count by host` |
| `{"sort": [{"@timestamp": "desc"}]}` | `\| sort -_time` |
| `_search?size=10` | `\| head 10` |

**Example conversions:**

```bash
# Elasticsearch: GET /nginx/_search
# {"query": {"bool": {"must": [
#   {"range": {"status": {"gte": 500}}},
#   {"range": {"@timestamp": {"gte": "now-1h"}}}
# ]}}, "aggs": {"by_uri": {"terms": {"field": "uri", "size": 10}}}}

# LynxDB SPL2:
lynxdb query '_source=nginx status>=500 | stats count by uri | sort -count | head 10' --since 1h
```

```bash
# Elasticsearch: Full-text search
# {"query": {"match": {"message": "connection refused"}}}

# LynxDB SPL2:
lynxdb query 'search "connection refused"'
```

### Step 5: Update Dashboards and Alerts

If you use Kibana dashboards, recreate them in LynxDB:

```bash
# Create a dashboard
curl -X POST localhost:3100/api/v1/dashboards -d '{
  "name": "Nginx Overview",
  "panels": [
    {"id": "p1", "title": "Status Codes", "type": "timechart",
     "q": "source=nginx | timechart count by status span=5m", "from": "-6h",
     "position": {"x": 0, "y": 0, "w": 12, "h": 4}},
    {"id": "p2", "title": "Top URIs", "type": "table",
     "q": "source=nginx | stats count by uri | sort -count | head 20", "from": "-1h",
     "position": {"x": 0, "y": 4, "w": 6, "h": 4}}
  ]
}'
```

## Feature Comparison

| Feature | Elasticsearch | LynxDB |
|---------|---------------|--------|
| Query language | Lucene DSL / ES\|QL | SPL2 |
| Schema | On-write (mappings) | On-read (auto-discovery) |
| Full-text search | Lucene inverted index | FST + roaring bitmaps |
| Deployment | 3+ nodes minimum | Single binary |
| Dependencies | JVM | None |
| Memory (idle) | ~4GB minimum | ~50MB |
| _bulk API | Native | Compatible endpoint |
| Aggregations | Powerful but verbose JSON | Concise SPL2 pipes |
| License | ELv2 / AGPL | Apache 2.0 |

## Advantages of Switching

- **No schema management**: No mappings, no index templates, no dynamic field type conflicts
- **No cluster management**: No shard allocation, no replica settings, no cluster health yellow/red
- **Simpler queries**: `source=nginx status>=500 | stats count by uri` instead of 20-line JSON DSL
- **Lower resource usage**: ~50MB idle vs ~4GB per node
- **Single binary**: No JVM, no node discovery, no split-brain concerns

## Next Steps

- [SPL2 Overview](/docs/spl2/overview) -- learn the query language
- [Quick Start](/docs/getting-started/quickstart) -- get started in 5 minutes
- [REST API](/docs/api/overview) -- full API reference
- [Materialized Views](/docs/guides/materialized-views) -- accelerate repeated queries

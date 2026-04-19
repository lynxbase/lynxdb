---
title: Migrating from Splunk
description: Migrate from Splunk to LynxDB -- SPL to SPL2 syntax differences, HEC endpoint setup, data export, and forwarder configuration.
---

# Migrating from Splunk

LynxDB speaks SPL2, a modern evolution of Splunk's SPL query language. Most SPL knowledge transfers directly. This guide covers the key differences and provides a step-by-step migration path.

## SPL vs SPL2 Differences

### Index Selection

```
# Splunk SPL
index=main sourcetype=nginx

# LynxDB SPL2
FROM main WHERE sourcetype="nginx"
# Or simply:
source=nginx
```

LynxDB auto-prepends `FROM main` when your query starts with `|` or a field filter, so in most cases you can simply write:

```
source=nginx status>=500 | stats count by uri
```

### Compatibility Hints

LynxDB detects common Splunk SPL1 patterns and suggests SPL2 equivalents:

```
$ lynxdb query 'index=main sourcetype=nginx'
hint: "index=main" is Splunk SPL syntax. In LynxDB SPL2, use "FROM main" instead.
```

### Quick Syntax Reference

| Operation | Splunk SPL | LynxDB SPL2 |
|-----------|-----------|--------------|
| Select index | `index=main` | `FROM main` |
| Search | `search error` | `search "error"` or `level=error` |
| Filter | `where status>500` | `where status>=500` (same) |
| Aggregate | `stats count by host` | `stats count by host` (same) |
| Time chart | `timechart count span=5m` | `timechart count span=5m` (same) |
| Top values | `top limit=10 uri` | `top 10 uri` |
| Field extraction | `rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"` | `rex field=_raw "(?P<ip>\d+\.\d+\.\d+\.\d+)"` |
| Rename | `rename src AS source_ip` | `rename src AS source_ip` (same) |
| Eval | `eval duration_sec=duration_ms/1000` | `eval duration_sec=duration_ms/1000` (same) |
| Dedup | `dedup host` | `dedup host` (same) |
| CTE / subsearch | `[search index=threats \| fields ip]` | `$threats = FROM threats \| FIELDS ip;` |
| Macro | `\`my_macro\`` | Not yet supported |

Most SPL commands work identically in SPL2. The main differences are in index selection (`FROM` vs `index=`) and the regex named group syntax (`(?P<name>...)` vs `(?<name>...)`).

### Commands Available in Both

These commands work the same way in Splunk and LynxDB:

`WHERE`, `EVAL`, `STATS`, `SORT`, `TABLE`, `FIELDS`, `RENAME`, `HEAD`, `TAIL`, `DEDUP`, `REX`, `BIN`, `TIMECHART`, `TOP`, `RARE`, `STREAMSTATS`, `EVENTSTATS`, `JOIN`, `APPEND`, `MULTISEARCH`, `TRANSACTION`, `XYSERIES`, `FILLNULL`

### Aggregation Functions

All common Splunk aggregation functions are supported:

`count`, `sum`, `avg`, `min`, `max`, `dc` (distinct count), `values`, `stdev`, `perc50`, `perc75`, `perc90`, `perc95`, `perc99`, `earliest`, `latest`

## Migration Steps

### Step 1: Set Up LynxDB

```bash
# Install
curl -fsSL https://lynxdb.org/install.sh | sh

# Start server
lynxdb server --data-dir /var/lib/lynxdb
```

### Step 2: Forward New Data via HEC

LynxDB includes a Splunk HTTP Event Collector (HEC) compatible endpoint. Point your existing Splunk forwarders at LynxDB with minimal configuration changes.

**Universal Forwarder (`outputs.conf`):**

```ini
# outputs.conf
[httpout]
httpEventCollectorToken = your-lynxdb-token
uri = https://lynxdb.company.com/api/v1/ingest/hec

[httpout:lynxdb]
uri = https://lynxdb.company.com/api/v1/ingest/hec
token = your-lynxdb-token
```

**Heavy Forwarder (`outputs.conf`):**

```ini
[tcpout]
disabled = true

[httpout]
disabled = false

[httpout:lynxdb]
uri = https://lynxdb.company.com/api/v1/ingest/hec
token = your-lynxdb-token
```

### Step 3: Export Historical Data from Splunk

Export data from Splunk for import into LynxDB:

```bash
# Export from Splunk as CSV
splunk search 'index=main earliest=-30d' -output csv > splunk_export.csv

# Or export as JSON
splunk search 'index=main earliest=-7d' -output json > splunk_export.json
```

Import into LynxDB:

```bash
# Import CSV export
lynxdb import splunk_export.csv --source splunk-migration

# Import JSON export
lynxdb import splunk_export.json --format ndjson

# Validate before importing
lynxdb import splunk_export.csv --dry-run

# Import into a dedicated index
lynxdb import splunk_export.csv --index splunk
```

### Step 4: Convert Saved Searches

Convert your Splunk saved searches to LynxDB saved queries:

```bash
# Splunk: index=main sourcetype=nginx status>=500 | stats count by uri | sort -count | head 10
# LynxDB:
lynxdb save "5xx-by-uri" '_source=nginx status>=500 | stats count by uri | sort -count | head 10'

# Run saved query
lynxdb run 5xx-by-uri --since 24h
```

### Step 5: Migrate Alerts

```bash
# Splunk alert -> LynxDB alert
curl -X POST localhost:3100/api/v1/alerts -d '{
  "name": "High Error Rate",
  "query": "level=error | stats count as errors | where errors > 100",
  "interval": "5m",
  "channels": [
    {"type": "slack", "config": {"webhook_url": "https://hooks.slack.com/..."}}
  ]
}'
```

### Step 6: Migrate Dashboards

Export your Splunk dashboards and recreate them in LynxDB:

```bash
# Create a LynxDB dashboard
curl -X POST localhost:3100/api/v1/dashboards -d '{
  "name": "Production Overview",
  "panels": [
    {
      "id": "p1",
      "title": "Error Rate",
      "type": "timechart",
      "q": "level=error | timechart count span=5m",
      "from": "-6h",
      "position": {"x": 0, "y": 0, "w": 6, "h": 4}
    },
    {
      "id": "p2",
      "title": "Top Error Sources",
      "type": "table",
      "q": "level=error | stats count by source | sort -count | head 10",
      "from": "-1h",
      "position": {"x": 6, "y": 0, "w": 6, "h": 4}
    }
  ]
}'
```

## Cost Comparison

| | Splunk Enterprise | LynxDB |
|---|---|---|
| License | $2,000+/GB/day ingested | Free (Apache 2.0) |
| Infrastructure | 6+ components (indexer, search head, deployer, license server, etc.) | Single binary |
| Memory | ~8GB minimum per component | ~50MB idle |
| Scaling | Complex deployment server setup | Config flag change |

## Feature Comparison

| Feature | Splunk | LynxDB |
|---------|--------|--------|
| Query language | SPL | SPL2 (SPL-compatible) |
| Full-text search | tsidx | FST + roaring bitmaps |
| Schema | On-read | On-read |
| Alerts | Yes | Yes (3 built-in channels) |
| Dashboards | Yes (XML) | Yes (JSON) |
| Materialized views | Data model acceleration | Materialized views (~400x) |
| Pipe mode | No | Yes |
| REST API | Yes | Yes (streaming-first) |
| HEC compatibility | Native | Compatible endpoint |

## Next Steps

- [Lynx Flow Reference](/docs/lynx-flow/overview) -- learn the full SPL2 query language
- [SPL to SPL2 Migration Guide](/docs/lynx-flow/splunk-migration) -- detailed syntax comparison
- [Quick Start](/docs/getting-started/quickstart) -- get started with LynxDB
- [Alerts](/docs/guides/alerts) -- set up alerting

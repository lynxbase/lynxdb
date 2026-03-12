---
title: Migrating from Splunk SPL
description: Differences between Splunk SPL and LynxDB's Lynx Flow / SPL2, with migration examples.
---

# Migrating from Splunk SPL

LynxDB accepts two query syntaxes: **Lynx Flow** (primary) and **SPL2** (compatible). Both compile to the same engine. This guide helps Splunk users transition quickly -- you can paste SPL2 queries immediately, then adopt Lynx Flow idioms over time.

## Quick Translation Table

| Splunk SPL | LynxDB Lynx Flow | LynxDB SPL2 |
|-----------|-------------------|-------------|
| `index=main` | `from main` | `FROM main` |
| `index=main sourcetype=nginx` | `from main \| where source="nginx"` | `FROM main \| where _sourcetype="nginx"` |
| `\| eval x=expr` | `\| let x = expr` | `\| eval x=expr` |
| `\| stats count by field` | `\| group by field compute count()` | `\| stats count by field` |
| `\| timechart span=5m count` | `\| every 5m compute count()` | `\| timechart span=5m count` |
| `\| sort -count` | `\| order by count desc` | `\| sort -count` |
| `\| head 10` | `\| take 10` | `\| head 10` |
| `\| fields field1, field2` | `\| keep field1, field2` | `\| fields field1, field2` |
| `\| fields - _raw` | `\| omit _raw` | `\| fields - _raw` |
| `\| rex "(?P<host>\\S+)"` | `\| parse regex(_raw, "(?P<host>\\S+)")` | `\| rex "(?P<host>\\S+)"` |
| `\| streamstats count` | `\| running count()` | `\| streamstats count` |
| `\| eventstats avg(dur)` | `\| enrich avg(dur)` | `\| eventstats avg(dur)` |
| `\| bin _time span=1h` | `\| bucket _time span=1h` | `\| bin _time span=1h` |
| `\| unpack_json` | `\| parse json(_raw)` | `\| unpack_json` |
| `\| table f1 f2` | `\| table f1, f2` | `\| table f1, f2` |
| `\| rename f1 as f2` | `\| rename f1 as f2` | `\| rename f1 AS f2` |
| `\| dedup field1 field2` | `\| dedup field1, field2` | `\| dedup field1, field2` |
| `earliest=-1h` | `--since 1h` or `"from": "-1h"` | `--since 1h` |

## Automatic Compatibility Hints

When you use Splunk SPL1 syntax, LynxDB detects it and suggests the Lynx Flow equivalent:

```
$ lynxdb query 'index=main level=error'
hint: In LynxDB, 'index=' maps to the '_source' field. Both 'index=main' and '_source=main' work identically.
```

When you use SPL2 commands, advisory hints suggest the Lynx Flow equivalent:

```
info: Lynx Flow: | let <field> = <expr>   (SPL2: eval)
info: Lynx Flow: | group by <fields> compute <aggs>   (SPL2: stats)
```

## Migration Examples

### Basic Search

```
-- Splunk SPL
index=main sourcetype=nginx status>=500

-- LynxDB Lynx Flow
from nginx
| where status >= 500

-- LynxDB SPL2 (also works)
source=nginx status>=500
```

### Aggregation

```
-- Splunk SPL
index=main sourcetype=nginx | stats count by uri status | sort -count | head 10

-- LynxDB Lynx Flow
from nginx
| group by uri, status compute count() as hits
| order by hits desc
| take 10

-- LynxDB SPL2 (also works)
source=nginx | stats count by uri, status | sort -count | head 10
```

### Time Series

```
-- Splunk SPL
index=main | timechart span=5m count by source

-- LynxDB Lynx Flow
from main
| every 5m by source compute count() as events

-- LynxDB SPL2 (also works)
| timechart span=5m count by source
```

### Time Range

```
-- Splunk SPL
index=main earliest=-1h latest=now | stats count by level

-- LynxDB (time range is external to the query)
lynxdb query 'from main | group by level compute count()' --since 1h
```

### Field Extraction

```
-- Splunk SPL
index=main | rex field=_raw "host=(?P<host>\S+)"

-- LynxDB Lynx Flow
from main
| parse regex(_raw, "host=(?P<host>\\S+)")

-- LynxDB: structured format parsing (no regex needed)
from nginx
| parse combined(_raw)
| where status >= 500
```

### Subsearch / Join with CTEs

```
-- Splunk SPL
index=main [search index=threats | fields src_ip] | stats count by src_ip

-- LynxDB Lynx Flow (CTEs)
$threats = from main
  | where threat_type is not null
  | keep client_ip;

from main
| join type=inner client_ip [from $threats]
| group by client_ip compute count()
```

### Tstats / Materialized Views

```
-- Splunk SPL
| tstats count where index=main by source _time span=5m

-- LynxDB Lynx Flow (materialized views)
-- Create the view:
from main
| every 5m by source compute count() as events
| materialize "mv_source_5m" retention=90d

-- Query the view (automatic acceleration):
from view.mv_source_5m
| where source = "nginx"
| order by events desc
```

### Window Functions

```
-- Splunk SPL
index=main | streamstats window=10 avg(duration_ms) as rolling_avg

-- LynxDB Lynx Flow
from main
| running window=10 avg(duration_ms) as rolling_avg

-- Splunk SPL
index=main | eventstats avg(duration_ms) as global_avg by service

-- LynxDB Lynx Flow
from main
| enrich avg(duration_ms) as global_avg by service
| where duration_ms > global_avg * 2
```

## Lynx Flow Additions (No Splunk Equivalent)

These Lynx Flow commands have no direct Splunk equivalent:

| Command | Description | Example |
|---------|-------------|---------|
| `select` | Ordered projection with inline rename | `select uri as path, status as code` |
| `rank top/bottom` | Row-level ranking (O(N log K) heap) | `rank top 10 by duration_ms` |
| `topby`/`bottomby` | Grouped metric ranking | `topby 10 uri using avg(duration_ms)` |
| `lookup` | Sugar for left join enrichment | `lookup geo_db on client_ip` |
| `latency` | Percentile time-series | `latency duration_ms every 5m by service` |
| `errors` | Error analysis shortcut | `errors by service compute count()` |
| `rate` | Event rate over time | `rate per 1m by service` |
| `percentiles` | Multi-percentile summary | `percentiles duration_ms by endpoint` |
| `slowest` | Top N by duration | `slowest 10 uri` |
| `??` operator | Null coalesce | `let name = user ?? "anonymous"` |
| `?` operator | Existence test | `where trace_id?` |
| `between` | Range check | `where x between 10 and 100` |

## Ingestion Migration

### From Splunk Forwarders (HEC)

LynxDB supports the Splunk HTTP Event Collector (HEC) protocol:

```bash
# Point your Splunk forwarders to LynxDB's HEC endpoint
# Before: https://splunk-server:8088/services/collector
# After:  https://lynxdb-server:3100/api/v1/compat/hec
```

### From Elasticsearch (Filebeat, Logstash, Vector)

LynxDB supports the Elasticsearch `_bulk` API:

```bash
# Point your pipeline to LynxDB's _bulk endpoint
# https://lynxdb-server:3100/api/v1/ingest/bulk
```

### From Splunk to LynxDB (data export)

```bash
# Export from Splunk as CSV, import into LynxDB
lynxdb import splunk_export.csv --source splunk-migration
```

## What's the Same

Most core SPL concepts translate directly:

- Pipeline model (`from | command | command`)
- Aggregation functions (`count`, `avg`, `sum`, `min`, `max`, `dc`, `values`, `perc95`, etc.)
- Full-text search, field=value, wildcards, boolean operators
- `join`, `append`, `multisearch`, `transaction`
- `top`, `rare`, `dedup`
- `table`, `xyseries`
- CTEs with `$variable` syntax
- Materialized views

The query logic is identical. The differences are syntactic: Lynx Flow uses more explicit, intent-based command names while SPL2 stays closer to traditional Splunk naming.

## See Also

- [Lynx Flow Reference](/docs/lynx-flow/reference) -- Complete language reference with SPL2 equivalents
- [Lynx Flow Overview](/docs/lynx-flow/overview) -- Language introduction and design principles

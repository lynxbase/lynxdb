---
title: Migrating from Splunk SPL
description: Differences between Splunk SPL and LynxDB SPL2, with migration examples.
---

# Migrating from Splunk SPL

LynxDB's SPL2 is inspired by Splunk's SPL but has some differences. This guide helps Splunk users transition quickly.

## Key Differences

| Splunk SPL | LynxDB SPL2 | Notes |
|-----------|-------------|-------|
| `index=main` | `FROM main` | Explicit FROM clause |
| `index=main sourcetype=nginx` | `FROM main \| where _sourcetype="nginx"` | Sourcetype as filter |
| `\| table field1 field2` | `\| table field1, field2` | Comma-separated |
| `\| rename field1 as alias1` | `\| rename field1 AS alias1` | Uppercase AS |
| `\| eval x=if(...)` | `\| eval x=IF(...)` | Uppercase function names |
| `\| stats count by field1 field2` | `\| stats count by field1, field2` | Comma-separated BY fields |
| `\| dedup field1 field2` | `\| dedup field1, field2` | Comma-separated |
| `earliest=-1h` | `--since 1h` or `"from": "-1h"` | Time range outside query |

## Automatic Compatibility Hints

When you accidentally use Splunk SPL1 syntax, LynxDB detects it and suggests the correct SPL2:

```
$ lynxdb query 'index=main level=error'
hint: "index=main" is Splunk SPL syntax. In LynxDB SPL2, use "FROM main" instead.
```

## Migration Examples

### Basic Search

```spl
-- Splunk SPL
index=main sourcetype=nginx status>=500

-- LynxDB SPL2
FROM main | where source="nginx" AND status>=500
-- or simply:
source=nginx status>=500
```

### Aggregation

```spl
-- Splunk SPL
index=main sourcetype=nginx | stats count by uri status | sort -count | head 10

-- LynxDB SPL2
source=nginx | stats count by uri, status | sort -count | head 10
```

### Time Range

```spl
-- Splunk SPL
index=main earliest=-1h latest=now | stats count by level

-- LynxDB SPL2 (time range is separate from query)
lynxdb query 'FROM main | stats count by level' --since 1h
```

### Subsearch / Join

```spl
-- Splunk SPL
index=main [search index=threats | fields src_ip] | stats count by src_ip

-- LynxDB SPL2 (CTEs)
$threats = FROM main WHERE threat_type IS NOT NULL | FIELDS client_ip;
FROM main | JOIN type=inner client_ip [$threats] | stats count by client_ip
```

### Tstats / Materialized Views

```spl
-- Splunk SPL
| tstats count where index=main by source _time span=5m

-- LynxDB SPL2 (materialized views)
-- Create the view:
lynxdb mv create mv_source_5m '| stats count by source, time_bucket(_time, "5m") AS bucket'
-- Query the view (automatic acceleration):
| stats count by source
```

## Ingestion Migration

### From Splunk Forwarders (HEC)

LynxDB supports the Splunk HTTP Event Collector (HEC) protocol:

```bash
# Point your Splunk forwarders to LynxDB's HEC endpoint
# Change the HEC URL from Splunk to LynxDB:
# Before: https://splunk-server:8088/services/collector
# After:  https://lynxdb-server:3100/api/v1/compat/hec
```

### From Splunk to LynxDB (data export)

```bash
# Export from Splunk as CSV
# Import into LynxDB
lynxdb import splunk_export.csv --source splunk-migration
```

## What's the Same

Most of the core SPL concepts translate directly:

- Pipeline model (`search | command | command`)
- `stats`, `eval`, `where`, `sort`, `head`, `table` -- same semantics
- `rex` with named capture groups
- `timechart` with `span`
- `top`, `rare`, `dedup`
- `streamstats`, `eventstats`
- `join`, `append`

The query logic is the same. The differences are mostly syntactic (commas, AS keyword, FROM clause).

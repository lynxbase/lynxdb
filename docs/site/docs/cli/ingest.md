---
sidebar_position: 3
title: ingest & import
description: Ingest raw logs and import structured data into LynxDB.
---

# ingest & import

Two commands for getting data into LynxDB: `ingest` for raw log lines, and `import` for structured data formats.

## ingest

Ingest logs from a file or stdin into a running LynxDB server.

```
lynxdb ingest [file] [flags]
```

**Alias:** `i`

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | | Source metadata for events |
| `--sourcetype` | | Sourcetype metadata for events |
| `--index` | | Target index name |
| `--batch-size` | `5000` | Number of lines per batch |

Data is sent in batches via `POST /api/v1/ingest/raw`. Empty lines are skipped. Shows a progress bar (file mode) or counter (stdin mode) on TTY.

### Examples

```bash
# Ingest a log file
lynxdb ingest access.log

# With source metadata
lynxdb ingest access.log --source web-01 --sourcetype nginx

# Target a specific index
lynxdb ingest data.log --index production

# From stdin
cat events.json | lynxdb ingest

# Tune batch size for large files
lynxdb ingest huge.log --batch-size 10000
```

## import

Bulk import structured data from files (NDJSON, CSV, Elasticsearch `_bulk` export).

```
lynxdb import <file> [flags]
```

Unlike `ingest` which handles raw log lines, `import` understands structured formats and preserves field types, timestamps, and metadata from the source system.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `auto` | Input format: `auto`, `ndjson`, `csv`, `esbulk` |
| `--source` | | Source metadata for all events |
| `--index` | | Target index name |
| `--batch-size` | `5000` | Number of events per batch |
| `--dry-run` | `false` | Validate and count events without importing |
| `--transform` | | SPL2 pipeline to apply during import |
| `--delimiter` | `,` | Field delimiter for CSV format |

Format is auto-detected from file extension and content. Use `-` as the file argument to read from stdin (requires `--format`).

### Supported Formats

| Format | Extensions | Description |
|--------|-----------|-------------|
| `ndjson` | `.json`, `.ndjson` | Newline-delimited JSON, one object per line |
| `csv` | `.csv` | RFC 4180 CSV with header row |
| `esbulk` | | Elasticsearch `_bulk` export format |

### Examples

```bash
# Import NDJSON (auto-detected)
lynxdb import events.json
lynxdb import events.ndjson

# Import CSV with headers
lynxdb import splunk_export.csv
lynxdb import data.csv --source web-01 --index nginx

# Import Elasticsearch _bulk export
lynxdb import es_dump.json --format esbulk

# Validate without importing
lynxdb import events.json --dry-run

# Apply SPL2 transform during import
lynxdb import events.json --transform '| where level!="DEBUG"'

# Import from stdin
cat events.ndjson | lynxdb import - --format ndjson

# Import TSV with tab delimiter
lynxdb import data.tsv --format csv --delimiter '\t'
```

## ingest vs. import

| | `ingest` | `import` |
|---|----------|----------|
| Input | Raw log lines (text) | Structured data (JSON, CSV) |
| Field handling | Schema-on-read at query time | Fields preserved from source |
| Timestamp | Auto-detected from line content | Preserved from structured field |
| Typical use | Shipping raw log files | Migrating from Splunk, ES, or CSV exports |
| Stdin support | Automatic detection | Requires `-` file argument + `--format` |

## Pipe Integration

```bash
# Filter logs before ingesting
grep "2026-01-15" /var/log/app.log | lynxdb ingest --source app

# Decompress and ingest
zcat archive.log.gz | lynxdb ingest --sourcetype nginx

# Stream from another tool
kubectl logs deploy/api --since=1h | lynxdb ingest --source k8s-api
```

## See Also

- [query](/docs/cli/query) for querying ingested data
- [Server](/docs/cli/server) for running the LynxDB server that receives ingested data

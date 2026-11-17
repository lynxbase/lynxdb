---
sidebar_position: 5
title: ingest & import
description: Send raw log lines or structured NDJSON, CSV and Elasticsearch bulk files to a LynxDB server.
---

# ingest & import

Two commands send data to a running server:

- `ingest` sends raw log lines. The server stores each line as an event and extracts fields when it is queried.
- `import` reads structured files (NDJSON, CSV, Elasticsearch `_bulk`) and keeps their field values, timestamps and metadata.

To query a file without a server, use [`lynxdb query --file`](./query.md#local-mode-files-and-stdin) instead.

## ingest

```
lynxdb ingest [file] [flags]
```

**Alias:** `i`

Reads a file, or stdin when no file is given, and sends the lines to `POST /api/v1/ingest/raw` in batches. Empty lines are skipped. On a terminal it shows a progress bar for a file and a running count for stdin.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--index` | `main` | Target index |
| `--source` | `http` | `source` of every event (`default_source` in `.lynxdbrc` applies when the flag is absent) |
| `--sourcetype` | `raw` | `sourcetype` of every event |
| `--batch-size` | `5000` | Lines per request, from 1 to 1,000,000 |

The CLI sends `--index`, `--source` and `--sourcetype` as the `X-Index`, `X-Source` and `X-Source-Type` headers; the defaults above are the server's when a header is absent.

### Examples

```bash
lynxdb ingest access.log
lynxdb ingest access.log --source web-01 --sourcetype nginx
lynxdb ingest app.log --index production
cat app.log | lynxdb ingest --source app
zcat archive.log.gz | lynxdb ingest --sourcetype nginx
kubectl logs deploy/api --since=1h | lynxdb ingest --source k8s-api
lynxdb ingest huge.log --batch-size 20000
```

After the upload, query the data:

```bash
lynxdb query 'source=main | stats count() by level'
```

### Partial batches

The server reports, per batch, how many lines it accepted. The CLI warns when:

- the server truncated a request body at `ingest.max_body_size`: reduce `--batch-size` or raise the limit;
- the server rejected events under backpressure: reduce `--batch-size` or check the server log.

The summary line counts accepted events only.

## import

```
lynxdb import <file> [flags]
```

Reads a structured file and sends its events in batches. Use `-` as the file to read stdin; stdin needs an explicit `--format`.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `auto` | Input format: `auto`, `ndjson`, `csv`, `esbulk` |
| `--index` | | Target index for events that do not name one |
| `--source` | | `source` for events that do not name one |
| `--batch-size` | `5000` | Events per request |
| `--delimiter` | `,` | CSV field delimiter; `'\t'` for tab |
| `--dry-run` | `false` | Parse and count events without sending them |

On `import`, `--format` names the input format; the global output `--format` does not apply.

### Format detection

With `--format auto` the file extension decides: `.csv` and `.tsv` are CSV, `.ndjson` and `.jsonl` are NDJSON. For `.json`, `.log` and files without an extension, the first line decides: an Elasticsearch bulk action line (`{"index":{"_index":...}}` or `{"create":{...}}`) means `esbulk`, a JSON object means NDJSON, a line with commas means CSV. Anything else is read as NDJSON. The CLI prints the format it picked.

### NDJSON

One JSON object per line. Two shapes are accepted:

- **Envelope.** An object whose keys are only `event`, `time`, `source`, `sourcetype`, `host`, `index` and `fields`, with a string `event`. `event` becomes the raw text, `time` the timestamp, and the scalar values of `fields` become fields.
- **Flat object.** `_raw` becomes the raw text (the whole line when there is no `_raw`). The first of `time`, `_time`, `timestamp`, `_timestamp`, `@timestamp`, `ts`, `datetime` that holds epoch seconds or an RFC 3339 / `2006-01-02 15:04:05` time becomes the timestamp. `source`, `sourcetype`, `host` and `index` become metadata, and every other scalar value (string, number, boolean) becomes a field. Nested objects and arrays are dropped.

Lines that are not valid JSON are skipped and counted in the summary.

### CSV

The first row is the header. Each later row becomes an event with one field per non-empty column. Numeric values are stored as numbers. A `_time` column becomes the timestamp and a `_raw` column the raw text. Rows that fail to parse are skipped and counted.

### Elasticsearch bulk

Action and document line pairs, as written by the Elasticsearch `_bulk` API or by tools that export from it. The CLI forwards them to the server's Elasticsearch-compatible endpoint, `POST /api/v1/es/_bulk`. Each action's `_index` names the target index, so `--index` is rejected with this format; `--source` replaces the `_index` of every action. The import fails when the server reports item errors.

### Examples

```bash
lynxdb import events.ndjson
lynxdb import export.csv --source web-01 --index nginx
lynxdb import data.tsv --format csv --delimiter '\t'
lynxdb import es_dump.json --format esbulk
lynxdb import events.json --dry-run
cat events.ndjson | lynxdb import - --format ndjson
```

## ingest compared with import

| | `ingest` | `import` |
|---|---|---|
| Input | Log lines | NDJSON, CSV, Elasticsearch bulk |
| Fields | Extracted from the raw text when queried | Taken from the structured record |
| Timestamp | Found in the line by the server | Read from the record's time field |
| Endpoint | `/api/v1/ingest/raw` | `/api/v1/ingest`, or `/api/v1/es/_bulk` for `esbulk` |
| Stdin | Default when no file is given | File argument `-` with `--format` |

## See also

- [Ingest API](../api/ingest.md)
- [query](./query.md) to query what you loaded
- [server](./server.md) for the syslog, OTLP, Elasticsearch and HEC receivers

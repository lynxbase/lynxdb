---
sidebar_position: 5
title: ingest
description: Send log lines, NDJSON, CSV and Elasticsearch bulk files to a LynxDB server.
---

# ingest

`lynxdb ingest` sends a file, or standard input, to a running server in batches.

```
lynxdb ingest [file] [flags]
```

**Alias:** `i`

With no file, or with `-`, it reads standard input. On a terminal it shows a progress bar for a file and a running count for standard input. To query a file without a server, use [`lynxdb query --file`](./query.md#local-mode-files-and-stdin) instead.

## Formats

`--format` names the input format:

| Format | Input | Sent to |
|--------|-------|---------|
| `text` | Log lines. The server keeps a JSON object line as it is and turns every other line into an event with its text in `_raw`, its timestamp and its `key=value` fields. | `POST /api/v1/ingest/raw` |
| `ndjson` | One JSON object per line, stored as it is. | `POST /api/v1/ingest` |
| `csv` | A header row, then one row per event. | `POST /api/v1/ingest`, one JSON object per row |
| `esbulk` | An Elasticsearch bulk request body: action and document lines. | `POST /_bulk` |
| `auto` | The default: picks one of the above. | |

`auto` goes by the file extension first: `.ndjson` and `.jsonl` are NDJSON, `.csv` is CSV, and `.tsv` is CSV with a tab delimiter. Otherwise the first non-empty line decides: an Elasticsearch bulk action line, such as `{"index":{"_index":"web"}}`, means `esbulk`, a JSON object means `ndjson`, and anything else means `text`. On a terminal the CLI says which format it picked.

The server takes the time of an NDJSON, CSV or bulk event from the first of its [time fields](../configuration/storage.md#event-time) the event holds; the CLI does not parse timestamps.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `auto` | Input format: `auto`, `text`, `ndjson`, `csv`, `esbulk` |
| `--index` | `main` | Target index; it is created by its first events. For `esbulk`, the index of the actions that name none |
| `--source` | | `source` of the events: every text line, and each NDJSON or CSV event that has no `source` (`default_source` in `.lynxdbrc` applies when the flag is absent) |
| `--sourcetype` | | `sourcetype` of the events, applied like `--source` |
| `--delimiter` | `,` | CSV field delimiter, one character; `'\t'` for tab |
| `--batch-size` | `5000` | Lines, rows or bulk actions per request, from 1 to 1,000,000 |

A flag that does not apply to the input fails the command: `--delimiter` on input that is not CSV, and `--source` or `--sourcetype` on Elasticsearch bulk input. On `ingest`, `--format` names the input format; the global output `--format` does not apply.

## Text

Empty lines are skipped. The CLI sends `--index`, `--source` and `--sourcetype` as the `X-Index`, `X-Source` and `X-Source-Type` headers; without them the server uses `main`, `http` and `raw`. A JSON object line is stored unchanged, without `source` or `sourcetype`.

```bash
lynxdb ingest access.log
lynxdb ingest access.log --source web-01 --sourcetype nginx
cat app.log | lynxdb ingest --source app
zcat archive.log.gz | lynxdb ingest --sourcetype nginx
kubectl logs deploy/api --since=1h | lynxdb ingest --source k8s-api
lynxdb ingest huge.log --batch-size 20000
```

## NDJSON

Each line is sent as it is. With `--source` or `--sourcetype`, the CLI adds those members to each object that lacks them. A line that is not a JSON object is counted as failed by the server.

With this `events.ndjson`:

```json
{"timestamp":"2026-01-15T08:00:00Z","host":"web-01","status":200,"user":"alice"}
{"timestamp":"2026-01-15T08:00:05Z","host":"web-02","status":503,"user":"bob","source":"lb"}
```

```bash
lynxdb ingest events.ndjson --index demo --source api
lynxdb query 'source=demo | fields _time, host, status, source'
```

```json
{"_time":"2026-01-15T08:00:00Z","host":"web-01","source":"api","status":200}
{"_time":"2026-01-15T08:00:05Z","host":"web-02","source":"lb","status":503}
```

## CSV

The first row names the fields. Each later row becomes a JSON object with one member per non-empty column: a value written as a JSON number, such as `19.90` or `-3`, is a number, and any other value, including `02134`, is a string. `--source` and `--sourcetype` set those members on rows without such a column. A row that does not parse stops the command with an error.

With this `orders.csv`:

```text
timestamp,order_id,amount,country,note
2026-01-15T08:00:00Z,1001,19.90,DE,
2026-01-15T08:01:00Z,1002,5,US,"gift, wrapped"
```

```bash
lynxdb ingest orders.csv --index orders --sourcetype shop
lynxdb query 'source=orders | fields _time, order_id, amount, country, note, sourcetype'
```

```json
{"_time":"2026-01-15T08:00:00Z","amount":19.9,"country":"DE","order_id":1001,"sourcetype":"shop"}
{"_time":"2026-01-15T08:01:00Z","amount":5,"country":"US","note":"gift, wrapped","order_id":1002,"sourcetype":"shop"}
```

```bash
lynxdb ingest export.tsv
lynxdb ingest export.txt --format csv --delimiter ';'
```

## Elasticsearch bulk

Action and document lines, as the Elasticsearch `_bulk` API takes them or as tools export them, go to the server's [Elasticsearch-compatible endpoint](../api/ingest.md). Each `index` or `create` action's `_index` names the index of its document; `--index` names the index of the actions that name none (`POST /{index}/_bulk`), and `main` is used without it. The server stores each document unchanged and rejects `update` and `delete` actions; the CLI counts rejected actions as failed.

```bash
lynxdb ingest dump.json --format esbulk
lynxdb ingest dump.json --index archive
```

## Partial batches

The server reports, per batch, how many events it accepted. The CLI warns when:

- the server cut a request body at `ingest.max_body_size`: reduce `--batch-size` or raise the limit;
- the server reports events it did not accept: reduce `--batch-size` or check the server log.

The summary line counts accepted events only.

## See also

- [Ingest API](../api/ingest.md)
- [query](./query.md) to query what you loaded
- [server](./server.md) for the syslog, OTLP, Elasticsearch and HEC receivers

---
sidebar_position: 12
title: Output Formats
description: The --format values of the LynxDB CLI, what auto chooses, and the flags that shape human tables.
---

# Output Formats

The global `--format` (`-F`) flag picks how commands print rows. It applies to `query` and the inspection commands; `lynxdb import` uses its own `--format` for the input format.

```bash
lynxdb query 'source=main level=ERROR | stats count() as errors by host' --format csv
```

## Formats

| Format | Output |
|--------|--------|
| `auto` | The default. On a terminal: a table, or a bare value for a one-row, one-column result. Otherwise: `json` |
| `json` | One JSON object per row, one row per line |
| `ndjson` | Same as `json` |
| `table` | Table with box-drawing borders and a row count |
| `box` | Same layout as `table` |
| `ascii` | Table drawn with plain ASCII characters, for terminals without box drawing |
| `markdown` | Markdown table |
| `vertical` | One block per row, one `field  value` line per field |
| `line`, `G` | Same as `vertical` |
| `csv` | RFC 4180 CSV with a header row |
| `tsv` | Tab-separated values with a header row |
| `raw` | The `_raw` text of each row; rows without `_raw` print as tab-separated `field=value` pairs |

A `\G` at the end of a query switches `auto` to `vertical`, as in a MySQL client:

```bash
lynxdb query 'source=main level=ERROR | head 1 \G'
```

## Examples

The query for every example:

```bash
lynxdb query 'source=main level=ERROR | stats count() as errors by host | sort - errors | head 3' --format <format>
```

`json`:

```json
{"errors":67,"host":"api-02"}
{"errors":58,"host":"web-02"}
{"errors":57,"host":"web-03"}
```

`table`:

```
┌────────┬────────┐
│ host   │ errors │
├────────┼────────┤
│ api-02 │     67 │
│ web-02 │     58 │
│ web-03 │     57 │
└────────┴────────┘
(3 rows)
```

`ascii`:

```
+--------+--------+
| host   | errors |
+--------+--------+
| api-02 |     67 |
| web-02 |     58 |
| web-03 |     57 |
+--------+--------+
(3 rows)
```

`markdown`:

```
| host   | errors |
|--------|--------|
| api-02 |     67 |
| web-02 |     58 |
| web-03 |     57 |
(3 rows)
```

`csv`:

```
host,errors
api-02,67
web-02,58
web-03,57
```

`vertical`, for wide event rows:

```bash
lynxdb query 'source=main level=ERROR | fields host, path, status, response_time | head 1' --format vertical
```

```
  record 1
           host  api-02
           path  /static/app.js
  response_time  11
         status  200
```

`raw` prints the original lines, ready for another tool or for a local query:

```bash
lynxdb query 'source=main status>=500 | head 2' --format raw
```

```
2025-01-15T08:00:45.63Z host=api-02 level=ERROR method=DELETE path=/api/v1/orders status=500 user=bob response_time=42 bytes=10811 msg="DELETE /api/v1/orders completed"
2025-01-15T08:01:10.09Z host=web-01 level=ERROR method=POST path=/api/v1/users status=503 user=alice response_time=344 bytes=45705 msg="POST /api/v1/users completed"
```

The table, CSV and vertical formats order columns the same way, not in the order of the query's `fields` or `stats` clause: the fields `_time`, `_raw`, `index`, `source`, `sourcetype` and `host` first, then the other fields alphabetically. JSON objects list their keys alphabetically.

## Human table flags

These global flags shape `table`, `box`, `ascii`, `markdown`, `vertical` and terminal `auto` output:

| Flag | Description |
|------|-------------|
| `--max-rows <n>` | Show at most `n` rows |
| `--max-width <n>` | Fit tables into `n` columns (`0` = terminal width) |
| `--null-value <text>` | Print this for null or empty values |
| `--compact` | Less padding |
| `--theme <name>` | `auto`, `dark`, `light` or `plain` |
| `--no-color` | No color; `NO_COLOR` with any value does the same |

## Data on stdout, everything else on stderr

Rows go to stdout. Statistics, hints, warnings and errors go to stderr, so a redirect or a pipe receives only data:

```bash
lynxdb query 'source=main | stats count() by host' --format csv > hosts.csv
lynxdb query 'source=main level=ERROR | fields host, path' --since 1h | jq -r .path | sort | uniq -c
```

When stdout is not a terminal, a query also writes its statistics to stderr as one JSON line, `{"stats":{...}}`, with the total and execution times in milliseconds (`total_ms`, `exec_ms`) and the rows returned (`result_rows`). `--no-stats` drops it and `--quiet` drops everything except data.

## Commands other than query

`status`, `health`, `indexes`, `jobs`, `fields`, `auth list`, `config get` and `config list-profiles` print a styled view on a terminal and plain rows otherwise. `--format json` gives each command's JSON document or one JSON object per item; `csv`, `tsv`, `markdown` and the table formats print the same data as rows.

## See also

- [query](./query.md#output)
- [CLI overview](./overview.md#global-flags)

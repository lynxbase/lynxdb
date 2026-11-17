---
sidebar_position: 12
title: Output Formats
description: The --format values of the LynxDB CLI, what auto chooses, and the flags that shape human tables.
---

# Output Formats

The global `--format` (`-F`) flag picks how commands print rows. It applies to `query`, `tail` and the inspection commands; `lynxdb import` uses its own `--format` for the input format.

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
           host  web-03
           path  /api/v1/users
  response_time  786
         status  502
```

`raw` prints the original lines, ready for another tool or for a local query:

```bash
lynxdb query 'source=main status>=500 | head 2' --format raw
```

```
2026-01-15T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502 user=grace response_time=786 bytes=32228 msg="GET /api/v1/users completed"
2026-01-15T08:00:00.543Z host=api-02 level=ERROR method=DELETE path=/api/v1/search status=502 user=grace response_time=462 bytes=46701 msg="DELETE /api/v1/search completed"
```

Columns come in a fixed order, not the order of the query's `fields` or `stats` clause: built-in fields such as `_time`, `_raw`, `index`, `_source`, `_sourcetype` and `host` first, then the other fields alphabetically. JSON objects list their keys alphabetically.

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

Rows go to stdout. Statistics, hints, warnings, lints and errors go to stderr, so a redirect or a pipe receives only data:

```bash
lynxdb query 'source=main | stats count() by host' --format csv > hosts.csv
lynxdb query 'source=main level=ERROR | fields host, path' --since 1h | jq -r .path | sort | uniq -c
```

When stdout is not a terminal, a query also writes its statistics as one JSON line to stderr:

```json
{"stats":{"total_ms":1,"scanned_rows":294,"result_rows":5,"scanned_segments":2,"total_segments":4,"scan_type":"full_scan","skip_ratio":0.5,"processed_bytes":238997}}
```

`--no-stats` drops it and `--quiet` drops everything except data.

## Commands other than query

`status`, `health`, `indexes`, `cache stats`, `jobs`, `fields`, `auth list`, `config get` and `config list-profiles` print a styled view on a terminal and plain rows otherwise. `--format json` gives each command's JSON document or one JSON object per item; `csv`, `tsv`, `markdown` and the table formats print the same data as rows.

## See also

- [query](./query.md#output)
- [CLI overview](./overview.md#global-flags)

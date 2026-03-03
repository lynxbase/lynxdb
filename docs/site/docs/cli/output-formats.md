---
sidebar_position: 12
title: Output Formats
description: LynxDB output format options -- json, ndjson, table, csv, tsv, raw, and auto-detection behavior.
---

# Output Formats

Control output format with the `--format` / `-F` global flag, available on all commands.

```bash
lynxdb query 'level=error | stats count by source' --format table
lynxdb query 'level=error | stats count by source' -F csv
```

## Format Reference

| Format | Description | Auto-selected when |
|--------|-------------|--------------------|
| `auto` | Auto-detect based on context (default) | Always the default |
| `json` | Pretty-printed JSON (one object per line) | Pipe (non-TTY) output |
| `ndjson` | Newline-delimited JSON (compact, one object per line) | Never auto-selected |
| `table` | Aligned columns with headers and separator | Never auto-selected |
| `csv` | RFC 4180 CSV with header row | Never auto-selected |
| `tsv` | Tab-separated values with header row | Never auto-selected |
| `raw` | `_raw` field value per line, or tab-separated k=v | Never auto-selected |

## Auto Behavior

The default `--format auto` adapts based on context:

| Context | Behavior |
|---------|----------|
| TTY + single scalar result | Plain value (just the number or string) |
| TTY + multiple results | Colorized JSON with numbered results (`#1`, `#2`, ...) |
| Non-TTY (pipe) | `json` (one JSON object per line) |

## Examples by Format

### json

```bash
lynxdb query 'level=error | stats count by source' --format json
```

```json
{
  "source": "nginx",
  "count": 340
}
{
  "source": "api-gateway",
  "count": 120
}
```

### ndjson

```bash
lynxdb query 'level=error | stats count by source' --format ndjson
```

```
{"source":"nginx","count":340}
{"source":"api-gateway","count":120}
```

### table

```bash
lynxdb query 'level=error | stats count by source' --format table
```

```
source          count
--------------  -----
nginx             340
api-gateway       120
```

### csv

```bash
lynxdb query 'level=error | stats count by source' --format csv
```

```
source,count
nginx,340
api-gateway,120
```

### tsv

```bash
lynxdb query 'level=error | stats count by source' --format tsv
```

```
source	count
nginx	340
api-gateway	120
```

### raw

```bash
lynxdb query 'source=nginx | head 3' --format raw
```

```
192.168.1.1 - - [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234
192.168.1.1 - - [14/Feb/2026:14:23:02 +0000] "POST /login HTTP/1.1" 302 0
192.168.1.1 - - [14/Feb/2026:14:23:03 +0000] "GET /dashboard HTTP/1.1" 200 5678
```

## Disabling Colors

Colors are enabled by default when output goes to a TTY. Disable them with:

```bash
# Flag
lynxdb query 'level=error | stats count' --no-color

# Environment variable (any non-empty value)
NO_COLOR=1 lynxdb query 'level=error | stats count'
```

## Piping and Scripting

When stdout is not a terminal, `--format auto` produces clean JSON suitable for piping:

```bash
# Pipe to jq
lynxdb query 'FROM main | stats count by host' | jq '.host'

# Export to file
lynxdb query 'FROM main | where level="ERROR"' --since 24h > errors.json

# Export as CSV for spreadsheets
lynxdb query 'FROM main | stats count by host' --format csv > report.csv

# Chain with other tools
lynxdb query '| stats count by status' --format csv | sort -t, -k2 -rn
```

### Metadata to stderr

Stats and summary lines are written to stderr so they do not pollute piped output:

```bash
# Only JSON goes to the file; stats go to the terminal
lynxdb query --file access.log '| stats count' > result.json
# stderr shows: Scanned 50,000 events | 1 results | 89ms
```

## See Also

- [CLI Overview](/docs/cli/overview) for global flags and TTY behavior
- [query](/docs/cli/query) for query-specific output details
- [Shell Completion](/docs/cli/completion) for setting up tab completion

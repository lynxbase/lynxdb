---
title: unpack_docker
description: Parse Docker json-file log driver output into fields.
---

# unpack_docker

Parse a field containing Docker json-file logging driver output and extract structured fields. This is the default output format of `docker logs` when using the json-file or journald driver.

## Syntax

```spl
| unpack_docker [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing Docker JSON log line |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `log` | string | Log message (trailing `\n` stripped automatically) |
| `stream` | string | Output stream (`stdout` or `stderr`) |
| `time` | string | ISO 8601 timestamp from Docker |

Any additional JSON keys in the log line are extracted with standard JSON type inference.

## Examples

```spl
-- Parse Docker json-file log output
-- Input: {"log":"hello world\n","stream":"stderr","time":"2026-01-01T00:00:00.000000000Z"}
| unpack_docker

-- Filter by stream
| unpack_docker
| where stream="stderr"
| stats count by log

-- Chain with logfmt for Go applications
-- Docker wraps the app's logfmt output in JSON
| unpack_docker
| unpack_logfmt field=log

-- Chain with JSON for structured app logs
| unpack_docker
| unpack_json field=log prefix=app_
```

## Notes

- Docker's json-file driver appends `\n` to every log line. `unpack_docker` automatically strips trailing newlines from the `log` field.
- Internally uses the JSON parser, so nested JSON in other fields is flattened with dot-notation.
- `unpack_docker` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_json](/docs/lynx-flow/commands/unpack-json) -- Parse JSON fields
- [unpack_logfmt](/docs/lynx-flow/commands/unpack-logfmt) -- Parse logfmt (common in Go applications)
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns

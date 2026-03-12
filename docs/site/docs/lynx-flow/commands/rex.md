---
title: rex
description: Extract fields from event data using regular expressions.
---

# rex

Extract new fields from event data using named capture groups in regular expressions.

## Syntax

```spl
| rex [field=<field>] "<regex-with-named-groups>"
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Field to extract from |
| regex | Required | Regular expression with `(?P<name>...)` named groups |

## Examples

```spl
-- Extract host and port from raw text
| rex field=_raw "host=(?P<host>\S+) port=(?P<port>\d+)"

-- Extract from a specific field
| rex field=message "user=(?P<username>\w+)"

-- Use extracted fields in aggregation
search "connection refused"
  | rex field=_raw "host=(?P<host>\S+)"
  | stats count by host
  | sort -count

-- Extract multiple fields
| rex field=_raw "(?P<ip>\d+\.\d+\.\d+\.\d+) .* \"(?P<method>\w+) (?P<path>\S+)"
  | stats count by method, path
```

## Notes

- Uses Go's `regexp` syntax with `(?P<name>...)` for named capture groups.
- `rex` is a streaming operator -- it processes events one at a time without buffering.
- The optimizer extracts literal prefixes from regex patterns for bloom filter pruning.

## See Also

- [eval](/docs/lynx-flow/commands/eval) -- Compute fields from expressions
- [search](/docs/lynx-flow/commands/search) -- Full-text search
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns

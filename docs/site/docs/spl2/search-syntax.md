---
title: Search Syntax
description: Boolean operators, wildcards, field=value, and full-text search syntax.
---

# Search Syntax

The search expression is the first part of an SPL2 pipeline (before the first `|`). It determines which events enter the pipeline.

## Keywords (Full-Text Search)

```spl
error                        -- events containing "error"
"connection refused"         -- exact phrase
timeout exception            -- both words (implicit AND)
```

## Field=Value

```spl
level=error                  -- exact match
status=500                   -- numeric match
source=nginx                 -- source filter
host="web-01"               -- quoted value (required for values with spaces)
```

## Comparisons

```spl
status>=500
duration_ms>1000
status!=200
bytes_sent<1024
```

## Boolean Operators

```spl
level=error source=nginx            -- implicit AND
level=error AND source=nginx        -- explicit AND
level=error OR level=warn           -- OR
level=error NOT source=redis        -- NOT
(level=error OR level=warn) source=nginx  -- grouping with parentheses
```

Precedence (highest to lowest): `NOT` > `AND` > `OR`. Use parentheses to override.

## Wildcards

```spl
host=web-*                   -- prefix wildcard
uri="/api/*/users"           -- embedded wildcard
source=*gateway*             -- contains
```

`*` matches zero or more characters. Wildcards use the inverted index for efficient evaluation.

## IN Operator

```spl
level IN ("error", "warn", "fatal")
status IN (500, 502, 503)
```

## Quoting Rules

- Field values with spaces, special characters, or starting with a number must be quoted: `message="out of memory"`
- Field names are unquoted: `level`, `duration_ms`, `_time`
- Phrases use double quotes: `"connection refused"`

## Examples

```spl
-- Find 5xx errors from nginx in the last hour
source=nginx status>=500

-- Full-text search combined with field filter
"timeout" source=api-gateway duration_ms>5000

-- Complex boolean
(level=error OR level=fatal) NOT source=healthcheck host=prod-*
```

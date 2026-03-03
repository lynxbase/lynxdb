---
title: search
description: Full-text keyword search across events.
---

# search

Perform a full-text keyword search across event data. The `search` command is often implicit -- the first part of a pipeline before any `|` is treated as a search expression.

## Syntax

```spl
search <search-expression>
```

## Search Expression Syntax

```spl
-- Keyword search
search error
search "connection refused"

-- Field=value
search level=error
search status=500

-- Comparisons
search status>=500
search duration_ms>1000

-- Boolean operators (AND is implicit)
search level=error source=nginx
search level=error OR level=warn
search level=error NOT source=redis

-- Wildcards
search host=web-*
search uri="/api/*"

-- Quoted strings (exact phrase)
search "out of memory"
```

## Examples

```spl
-- Find all error events
search error

-- Search with field filter
search level=error source=nginx

-- Search and aggregate
search "connection refused" | stats count by host

-- Search with pipeline
search "timeout" | where duration_ms > 5000 | table _time, host, message
```

## Notes

- The `search` keyword is optional when it appears at the start of a pipeline. `level=error` and `search level=error` are equivalent.
- Full-text search uses the inverted index and bloom filters for fast lookups.
- Quoted strings search for exact phrases.
- Wildcards use `*` for any number of characters.

## See Also

- [where](/docs/spl2/commands/where) -- Programmatic filtering
- [rex](/docs/spl2/commands/rex) -- Extract fields from search results

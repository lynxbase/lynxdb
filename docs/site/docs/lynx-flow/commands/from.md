---
title: FROM
description: Specify a data source -- index or materialized view.
---

# FROM

Specify a data source to read events from. By default, queries read from the `main` index.

## Syntax

```spl
FROM <source> [WHERE <expression>]
```

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `source` | Yes | Index name, materialized view name, or CTE variable |

## Examples

```spl
-- Read from the default index
FROM main

-- Read from a named index
FROM production

-- Read from a materialized view
FROM mv_errors_5m | where source="nginx"

-- Read from a CTE variable
$errors = FROM main WHERE level="error" | FIELDS source, message;
FROM $errors | stats count by source

-- With inline WHERE
FROM main WHERE level="error" AND source="nginx"
```

## Notes

- If your query starts with `|`, `FROM main` is automatically prepended.
- When reading from a materialized view, the optimizer checks whether the view can accelerate the query.

## See Also

- [search](/docs/lynx-flow/commands/search) -- Full-text search
- [where](/docs/lynx-flow/commands/where) -- Filter by expression

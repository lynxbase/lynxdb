---
title: join
description: Join two datasets on a common field.
---

# join

Join the current result set with a subsearch on a common field.

## Syntax

```spl
| join type=<join-type> <field> [<subsearch>]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `type` | `inner` | Join type: `inner`, `left`, `outer` |
| `field` | Required | Field(s) to join on |
| subsearch | Required | Subsearch enclosed in `[...]` |

## Examples

```spl
-- Inner join
source=nginx | join type=inner user_id [search source=auth | stats count AS login_count by user_id]

-- Left join
source=nginx | join type=left client_ip [search source=geo | fields client_ip, country, city]

-- CTE-based join
$threats = FROM main WHERE threat_type IN ("sqli", "xss") | FIELDS client_ip, threat_type;
$logins = FROM main WHERE type="login" AND result="failed" | STATS count AS failures BY src_ip;
FROM $threats | JOIN type=inner client_ip [$logins] | WHERE failures > 5
```

## Notes

- Join loads the right-side (subsearch) into memory. For large datasets, consider using `stats` with computed keys instead.
- Join executes on the coordinator, not pushed to shards.

## See Also

- [append](/docs/spl2/commands/append) -- Append subsearch results
- [CTEs and Joins Guide](/docs/guides/ctes-and-joins) -- Advanced join patterns

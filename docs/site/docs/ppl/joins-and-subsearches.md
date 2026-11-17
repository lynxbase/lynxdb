---
title: Joins and subsearches
sidebar_label: Joins and subsearches
description: join, lookup, append, union, multisearch and subqueries in PPL.
---

# Joins and subsearches

A subsearch is a pipeline in square brackets. It starts with its own source
(`[ source=owners | where team = 'edge' ]`), except inside `appendpipe`,
`appendcol` and `append` without a source, where it continues the outer
pipeline's rows. The examples below combine access logs in `main` with an
index `owners` holding `host`, `owner` and `team`.

## join

```
source=main | stats count() as n by host
| join left=l right=r on l.host = r.host owners

source=main | stats count() as n by host
| left join left=l right=r on l.host = r.host [ source=owners | where team = 'edge' ]

source=main | stats count() as n by host
| join type=left host owners
```

| Form | Meaning |
| --- | --- |
| `join ... on <condition> <right>` | rows match when the condition is TRUE |
| `join <fields> <right>` | rows match when the listed fields are equal |
| `inner`, `left` (`left outer`), `right`, `full`, `cross`, `semi` (`left semi`), `anti` (`left anti`) | the join kind, as a prefix or `type=` |
| `left=l right=r` | aliases; columns of each side are also available as `l.host`, `r.owner` |
| `max=N` | at most N matching right rows per left row |
| `overwrite=true` (default) | on a name collision the right side's value wins |

The right side is an index name or a subsearch. A `semi` join keeps the left
rows that have a match, an `anti` join those that have none; neither adds
right columns.

### LynxDB behavior

- LynxDB splits the condition into equality keys and a residual condition:
  `on l.host = r.host AND r.team = 'edge'` joins on `host` and keeps pairs
  whose residual is TRUE.
- The right side is held in memory; put the smaller input on the right and
  filter it in the subsearch.

## lookup

```
source=main | stats count() as n by host | lookup owners host append owner, team
source=main | lookup owners host output owner as who
source=main | lookup geo ip as client_ip replace country
```

`lookup <index> <index field> [as <field>] (replace | append | output) <fields>`
adds fields from the first matching row of another index; `as` names the
current pipeline's field when it differs from the index's. `replace` and
`output` overwrite existing values; `append` fills only missing ones. LynxDB
requires the output field list; `lookup owners host` alone fails with
`UNSUPPORTED_FEATURE`.

## append, appendpipe, appendcol

```
source=main | stats count() as n by host | append [ source=owners | fields host, owner ]
source=main | stats count() as n by level | appendpipe [ stats sum(n) as n | eval level = 'total' ]
source=main | stats count() by host | appendcol [ stats count() as total ]
```

- `append` adds the rows of another search after the current rows, matching
  columns by name.
- `appendpipe` runs commands over the current rows and appends their
  output; LynxDB computes the input twice.
- `appendcol` runs commands over the source rows and pairs their output with
  the current rows by position.

## union and multisearch

```
union owners, [ source=main | where status = 503 | head 2 ]
multisearch [ search source=main status=503 ] [ search source=audit action=login ]
```

`union` concatenates datasets (index names or subsearches) by column name;
`maxout=N` bounds the output. `multisearch` needs at least two subsearches
and merges their events newest first. Both are source commands.

## Subqueries in expressions

```
source=main | where host IN [ source=owners | where team = 'edge' | fields host ]
source=main | where (host, path) IN [ source=allowlist | fields host, path ]
source=main | where NOT EXISTS [ source=maintenance | where active = true ]
source=main | stats count() as n by host
| eval share = n * 100.0 / [ source=main | stats count() ]
```

| Form | Value |
| --- | --- |
| `x IN [ ... ]`, `(x, y) IN [ ... ]` | TRUE when the value (tuple) appears in the subquery's rows |
| `EXISTS [ ... ]` | TRUE when the subquery returns a row |
| `[ ... ]` as a value | the single value of the subquery's single row and column |

LynxDB supports uncorrelated subqueries. Each subquery runs once, before the
outer query, and its rows are held in memory; an `IN` subquery of one column
that returns at most 1024 values becomes a list of those values in the
outer filter. End a subquery with `fields` or `stats` so that its columns
are known when the query is planned: an `IN` or a scalar subquery that
returns whole events fails to compile.

A correlated subquery fails to compile with `UNSUPPORTED_FEATURE`: a
reference through the outer query's source name or alias, or to a field
that only the outer query has, is reported at the reference.

```
source=main as o | where status IN [ source=main | where user = o.user | fields status ]
```

```
unsupported_feature error at line 1, column 65: correlated subqueries are not supported: the subquery refers to o.user of the outer query
```

An unqualified name that the subquery's own rows can also have reads the
subquery's field.

## Not supported

- `graphlookup` (recursive traversal) fails with `UNSUPPORTED_COMMAND`.
- Correlated subqueries fail with `UNSUPPORTED_FEATURE`.

---
title: PPL-to-IR compilation
sidebar_label: PPL-to-IR compilation
description: How LynxDB lowers each PPL command and expression to canonical IR operators.
---

# PPL-to-IR compilation

`ppl.Compile` (`pkg/ppl/compile.go`) parses a query and walks its pipeline
from the source command on. It keeps the current IR node and its output
schema; each command reads that schema, checks its fields and functions, and
wraps the node in one or more IR operators. The result is validated with
`ir.Validate` before the optimizer sees it. `lynxdb query --explain` prints
the logical plan this produces.

## Source commands

| PPL | IR |
| --- | --- |
| `source=main` | `Scan(main)` with the open event schema (`_time`, `_raw`, then any column) |
| `source=a, logs-*` | `Scan(a, logs-*)`; `*` names are patterns |
| search terms | `Filter` over the scan with `Search` predicates and comparisons |
| `earliest=`, `latest=` | `Scan.Time`, kept symbolic (`now-1h`) until execution |
| `source=[ ... ]` | the subsearch's plan as the source |
| `describe main` | `SourceSchema(main)` |
| `makeresults count=2` | `Compute(_time = now())` over `Values(rows=2)` |
| `multisearch [..] [..]` | `Union[by_time]` |
| `union a, b` | `Union` |

Search terms lower as follows:

| Term | IR predicate |
| --- | --- |
| `error` | `SEARCH_WORD(_raw, "error")` |
| `conn*` | `SEARCH_PREFIX(_raw, "conn")` |
| `*err*`, `us?r` | `SEARCH_GLOB(_raw, ...)` |
| `"connection refused"`, `api/v1/users` | `SEARCH_PHRASE(_raw, ...)` |
| `level=ERROR` | `level = "ERROR"` |
| `status>=500` | `status >= 500` |
| `host=web-*` | `like(host, "web-%", TRUE)` |
| `NOT user=bob` | `NOT coalesce(user = "bob", FALSE)` (two-valued) |
| `level IN (WARN, ERROR)` | `(level = "WARN") OR (level = "ERROR")` |

## Commands

| PPL | IR |
| --- | --- |
| `where c` | `Filter(c)` |
| `fields a, b`, `fields - a` | `Project(a, b)`, `Project(drop a)` |
| `rename a as b` | `RenameColumns(a -> b)` |
| `eval x = e, y = f` | `Compute(x = e, y = f)` |
| `replace`, `convert`, `nomv` | `Compute` with `wildcard_replace`, conversion functions (`convert_num`, `dur2sec`, ...), `mvjoin` |
| `fillnull with v in f` | `FillNull(f = v)` |
| `stats aggs by g` | `Aggregate(aggs BY g)`, then `Project` to put group columns first |
| `stats ... by span(_time, 5m)` | a group key `time_bucket(_time, 5, "m")` |
| `eventstats` | `Window[partition]` |
| `streamstats` | `Window[running]` with a row bound or current-row exclusion |
| `trendline sma(n, f)` | `Window[running rows=n](moving_avg(f))` over `Filter(NOT is_null(f))` |
| `sort k` | `Sort` with explicit NULL placement |
| `head n from m` | `Limit(n, offset=m)` |
| `reverse` | `Reverse` |
| `dedup n k` | `Dedup(k, count=n)` |
| `top n f by g` | `Aggregate(count BY g, f)`, `Sort`, `Dedup(g, count=n)`, `Project` |
| `rare n f` | `Aggregate`, ascending `Sort`, `Limit(n)`, `Project` |
| `bin f span=w` | `Compute(f = time_bucket(...))` or a numeric range label |
| `timechart`, `chart` | `Aggregate` per bucket and series; series beyond `limit` are ranked by an `IN` subquery and renamed `OTHER`; then `Sort` |
| `parse`, `grok`, `rex` | `Extract[regex]`; grok patterns expand to RE2 first |
| `rex mode=sed "s/a/b/g"` | `Compute(f = regexp_replace(f, "a", "b"))` |
| `regex f = 're'` | `Filter(regexp_match(f, "re"))` |
| `spath path=p` | `Compute(out = json_extract(in, "$.p"))` |
| `spath` without path | `Compute(in = parse_json(in))` |
| `patterns` | `Patterns[simple, rows]` or `Patterns[simple, aggregate]` |
| `expand`, `mvexpand` | `Unnest` |
| `flatten` | `FlattenObject` |
| `mvcombine` | `CombineValues` |
| `join` | `Join[kind](keys) cond max qualify=l,r` |
| `lookup idx k append f` | left `Join` with `max=1` against `Project(k, f)` of the index, then `Compute(f = coalesce(f, ...))` |
| `append [ ... ]`, `appendpipe [ ... ]` | `Union` |
| `appendcol [ ... ]` | `ZipColumns` |
| `addtotals`, `addcoltotals` | `Totals` |
| `transpose` | `Transpose` |
| `xyseries x y in (v1, v2) d` | `Aggregate` by `x` with one `first(d) FILTER (WHERE y = v)` per value, then `Sort` |

## Expressions

| PPL | IR |
| --- | --- |
| field, `@timestamp` | `FieldRef`; `@timestamp` becomes `_time` |
| literal | `Literal` with its IR type |
| `+ - * / %`, comparisons, `AND OR XOR NOT` | `Binary`, `Unary` |
| `IN (...)`, `BETWEEN` | `InList`, two comparisons |
| `LIKE`, `ILIKE` | `like(s, p, FALSE)` |
| `REGEXP` | `regexp_match` |
| `CONTAINS` | `contains(lower(s), lower(t))` |
| `case(...)`, `if(...)`, `nullif(...)` | `Case` |
| `cast(x as t)` | `Cast`, or `date`/`time`/`json` calls |
| `isnull(x)` | `is_null(x)` |
| `match(f, 'a b')` | `Search` predicates on `f` |
| function call | `Call` of the IR registry function the catalog maps the name to |
| aggregation | `AggCall` inside `Aggregate` or `Window`; `count(eval(c))` sets the call's filter |
| lambda | `Lambda` with `LambdaParam`s |
| `x IN [ ... ]`, `EXISTS [ ... ]`, `[ ... ]` | `Subquery` in mode IN, EXISTS or scalar |
| `INTERVAL n unit`, `DATE '...'` | duration or calendar literals, timestamp literals |
| `earliest('-1d', t)` | `t >= RelativeTime(now-1d)` |

## Names of output columns

An aggregation without `as` is named after its source text with spaces
normalized: `count()`, `percentile(response_time, 99)`, `span(_time,1h)`.
`top` and `rare` add `count` (and `percent`). `patterns` adds
`patterns_field` (and `pattern_count`, `sample_logs` in aggregation mode).
`timechart` keeps `_time` as the bucket column.

## Tests

Each command has a compile golden under `pkg/ppl/testdata/compile`: a `.ppl`
query and the `.ir` plan it must produce. `pkg/query` checks a corpus of
query plans (`testdata/corpus_plans.sum`) and executes queries end to end in
`query_test.go`.

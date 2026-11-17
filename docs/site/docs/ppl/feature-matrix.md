---
title: Supported features
sidebar_label: Feature matrix
description: Which OpenSearch PPL commands, functions and options LynxDB supports, with its deviations.
---

# Supported features

This page summarizes the catalogs in `pkg/ppl` (`catalog.go`,
`functions.go`, `relevance.go`, `aggs.go`). The generated
[command](./commands.md), [function](./functions.md) and
[aggregation](./aggregations.md) tables list every entry; `GET
/api/v1/catalog` returns them as JSON. Support levels:

- **full**: the OpenSearch PPL semantics apply, apart from the deviations
  listed.
- **partial**: runs with the options listed; other options fail with
  `UNSUPPORTED_FEATURE`.
- **unsupported**: fails with `UNSUPPORTED_COMMAND` or `UNSUPPORTED_FUNCTION`.

## 1. OpenSearch PPL semantics LynxDB implements

**Commands with full support:** `where`, `fields`, `table`, `rename`,
`eval`, `convert`, `replace`, `fillnull`, `expand`, `trendline`, `sort`,
`reverse`, `head`, `dedup`, `top`, `rare`, `parse`, `regex`, `append`,
`appendcol`, `appendpipe`, `multisearch`, `union`, `makeresults`,
`addtotals`, `addcoltotals`, `transpose`, `mvcombine`, `nomv`, `mvexpand`,
`xyseries`.

**Commands with partial support:** `search`, `subquery`, `flatten`,
`stats`, `eventstats`, `streamstats`, `bin`, `timechart`, `chart`, `grok`,
`rex`, `spath`, `patterns`, `join`, `lookup`, `describe`, `explain`.

**Language:** the PPL lexical rules and operator precedence, search
expressions with implicit `AND`, time modifiers, three-valued logic in
expressions, `case`, `if`, `cast`, `IN`, `BETWEEN`, `LIKE`, `REGEXP`,
`CONTAINS`, `IS NULL`, intervals, `DATE`/`TIME`/`TIMESTAMP` literals, lambda
arguments, uncorrelated `IN`, `EXISTS` and scalar subqueries, `explain`.

**Functions:** the math, string, condition, conversion, date and time, JSON,
collection, IP, cryptographic and system functions of the
[function table](./functions.md), and the single-field relevance functions
`match`, `match_phrase` and `match_phrase_prefix`. **Aggregations:** every
entry of the [aggregation table](./aggregations.md).

## 2. LynxDB execution behavior

| Feature | LynxDB behavior |
| --- | --- |
| `search` free text | matches whole tokens of the event's raw text through the term index (OpenSearch searches all fields); quoted phrases match at token boundaries |
| `search` field terms | exact and case-sensitive; wildcards match the whole value; a term is two-valued (`NOT`, `!=` match events without the field) |
| `search` sources | a cross-cluster prefix is read as part of the index name |
| relevance functions | plain filters on one field's tokens; no scores; `boost` ignored |
| `stats` | `bucket_nullable` supported; `partitions`, `allnum` and `delim` accepted without effect; `distinct_count` is exact up to 10,000 values per group and estimated beyond; output row order is unspecified |
| `sort` | `ip(field)` sorts by the text form of the address |
| `fillnull` | without a field list, fills NULL values of the columns each row has |
| `flatten` | aliases are assigned to members in key order |
| `spath` | auto-extract parses the input into an object in place, keeping JSON value types; a JSON null stays NULL |
| `parse`, `regex`, `rex` | RE2 regular expressions: no lookaround or backreferences |
| `grok` | built-in pattern library (`pkg/ppl/grok`) |
| `appendpipe` | the input is computed twice |
| `multisearch` | results are merged newest first |
| `join` | equality keys plus a residual condition; the right side is held in memory |
| `subquery` | evaluated once before the outer query |
| `describe` | types derived from the values stored in the index; columns `TABLE_NAME`, `COLUMN_NAME`, `TYPE_NAME`, `ORDINAL_POSITION` |
| `explain` | returns LynxDB's logical and optimized IR plans; all modes render the same plans |
| `percentile` | t-digest; the compression argument is accepted and not used |
| `earliest`, `latest` | order by `_time` |
| types | `date` is a timestamp at 00:00 UTC, `time` a duration, `ip` a string, `struct` an object |
| time | relative time modifiers resolve in UTC against the query's start time |
| `@timestamp` | an alias of `_time` |

## 3. OpenSearch features LynxDB does not support

**Commands** (`UNSUPPORTED_COMMAND`):

| Command | Reason |
| --- | --- |
| `fieldformat` | display formatting with prefix and suffix is not implemented; use `eval` with `concat()` |
| `foreach` | templated per-field evaluation is not implemented; write one `eval` per field |
| `timewrap` | period-over-period wrapping is not implemented; compute the shifted period with `eval` and `join` |
| `graphlookup` | recursive graph traversal needs an iterative join operator |
| `rest` | it reads OpenSearch cluster management endpoints; use the LynxDB REST API |
| `show datasources` | LynxDB has no external data sources; every index is local |
| `ml`, `kmeans`, `ad` | they run the OpenSearch ML Commons plugin |

**Functions** (`UNSUPPORTED_FUNCTION`): `geoip` (no GeoIP database);
`multi_match`, `simple_query_string`, `query_string`, `match_bool_prefix`
(no relevance scoring); the window functions `row_number`, `rank`,
`dense_rank`, `percent_rank`, `cume_dist`, `nth`, `ntile` in `eventstats`
and `streamstats`.

**Options** (`UNSUPPORTED_FEATURE`):

| Command | Options not supported |
| --- | --- |
| `stats` | `dedup_splitvalues` |
| `streamstats` | `time_window`; `window` with `global=true` and a `by` clause |
| `bin` | `bins`, `minspan`, `aligntime`, `start`, `end`, logarithmic spans |
| `timechart` | `bins`, `minspan` |
| `chart` | `bins`, `minspan`, `start`, `end` |
| `rex` | `offset_field` |
| `grok` | custom pattern definitions |
| `patterns` | `show_numbered_token=true`, a `by` clause in label mode |
| `lookup` | a lookup without a `replace`, `append` or `output` field list |
| relevance functions | options other than `boost`, `operator` (`match`) and `zero_terms_query` |

These unsupported options pass compilation and are rejected by the execution
engine before the query reads data, with a message that starts with `not
supported by the execution engine:` (an `EXECUTION_ERROR` on the REST API):
`eventstats bucket_nullable=false`, `streamstats reset_before` and
`reset_after`, and the `patterns` method `brain`.

**Other:** correlated subqueries; the `binary` and `geo_point` types;
cross-cluster search; external data sources.

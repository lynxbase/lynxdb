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
| `search` free text | matches whole tokens of the string values of every field of the event, also inside arrays and objects, and numbers for a term of digits; wildcards match within a token (`*fused`, `us?r`); a line of text that is not JSON is searched through `_raw` and the fields extracted from it; quoted phrases match at token boundaries |
| `search` field terms | exact and case-sensitive; wildcards match the whole value; a term is two-valued (`NOT`, `!=` match events without the field) |
| `search` sources | a cross-cluster prefix is read as part of the index name |
| relevance functions | plain filters on one field's tokens; no scores; `boost` ignored |
| `stats` | `bucket_nullable` supported; rows whose `by` field is NULL or absent form one group under a NULL key; `partitions`, `allnum` and `delim` accepted without effect; output row order is unspecified |
| `distinct_count`, `dc` | counts exactly; a number is one value whether stored as an integer or a float (`1` and `1.0`), and a string is another value than a number (`'1'`), so `1`, `1.0` and `'1'` are two values; `distinct_count_approx` estimates with HyperLogLog |
| `sort` | one order of values for `sort`, `top`, `min`, `max` and `values`: booleans, then numbers and the strings that hold a number by value, then other strings by their bytes, then arrays and objects; `ip(field)` sorts by the text form of the address |
| `list`, `values`, `take` | return arrays of the values as they are, in `stats`, `eventstats` and `streamstats` |
| `fillnull` | without a field list, fills NULL values of the columns each row has |
| `flatten` | the members of a stored object and of a computed one become columns; aliases are assigned to members in key order |
| `spath` | auto-extract parses the input into an object in place, keeping JSON value types; a JSON null stays NULL |
| `parse`, `regex`, `rex` | RE2 regular expressions: no lookaround or backreferences |
| `grok` | built-in pattern library (`pkg/ppl/grok`) |
| `appendpipe` | the input is computed twice |
| `multisearch` | results are merged newest first |
| `join` | equality keys plus a residual condition; the right side is held in memory |
| `subquery` | uncorrelated; evaluated once before the outer query; a reference to the outer query fails to compile |
| `describe` | types derived from the values stored in the index; columns `TABLE_NAME`, `COLUMN_NAME`, `TYPE_NAME`, `ORDINAL_POSITION` |
| `explain` | returns LynxDB's logical and optimized IR plans; all modes render the same plans |
| `percentile` | the value that has `floor(p/100*n)` of the `n` values before it, without interpolation, read from a sketch that is exact while a group holds at most 63 distinct non-zero numbers and within 1% of the value beyond; the compression argument is accepted and not used |
| `earliest`, `latest` | order by `_time`; another time field fails to compile |
| `fields` | the members of an object are columns named by their path (`http.path`); naming the object keeps all of them |
| `transform` | the lambda takes the element, or the element and its 0-based index |
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
| `eventstats` | `bucket_nullable=false` with a `by` clause |
| `streamstats` | `time_window`; `reset_before`; `reset_after`; `window` with `global=true` and a `by` clause; `bucket_nullable=false` with a `by` clause |
| `bin` | `bins`, `minspan`, `aligntime`, `start`, `end`, logarithmic spans |
| `timechart` | `bins`, `minspan` |
| `chart` | `bins`, `minspan`, `start`, `end` |
| `rex` | `offset_field` |
| `grok` | custom pattern definitions |
| `patterns` | `method=brain`, `show_numbered_token=true`, a `by` clause in label mode |
| `lookup` | a lookup without a `replace`, `append` or `output` field list |
| relevance functions | options other than `boost`, `operator` (`match`) and `zero_terms_query` |
| `earliest`, `latest` | a second argument other than `_time` or `@timestamp` |

Every option of this table fails at compile time, before the query reads
data.

**Other:** correlated subqueries, which fail to compile with
`UNSUPPORTED_FEATURE` when a subquery refers to the outer query's source
name or alias (`o.host`) or to a field that only the outer query has; the
`binary` and `geo_point` types; cross-cluster search; external data sources.

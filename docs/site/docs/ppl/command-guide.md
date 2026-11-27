---
title: PPL command guide
sidebar_label: Command guide
description: How to use the main PPL commands in LynxDB, with LynxDB-specific behavior.
---

# Command guide

The [command support table](./commands.md) lists LynxDB's support level for
each OpenSearch PPL command. The [grammar](./grammar.md) lists the options.
Examples use access logs with the fields `host`, `level`, `method`, `path`,
`status`, `user`, `response_time`, `bytes`, and `msg`.

## Source commands

### search

```
source=main
source=main level=ERROR host=web-* earliest=-1h
search source=logs-* "connection refused"
source=main, audit status>=500
```

Reads one or more indexes and filters them with a search expression. See
[Search semantics](./search-and-types.md).

### describe

```
describe main
```

One row per column observed in the index: `TABLE_NAME`, `COLUMN_NAME`,
`TYPE_NAME`, `ORDINAL_POSITION`. LynxDB derives the types from the stored
values, so `describe` reads the index.

### makeresults

```
makeresults count=3 | eval n = 1
makeresults format=json data='[{"a":1,"b":"x"},{"a":2,"b":"y"}]'
makeresults format=csv data='a,b\n1,x\n2,y'
```

Literal rows, without reading an index. With `count`, each row has `_time`
set to the query time.

### multisearch and union

```
multisearch [ search source=main status=500 ] [ search source=main status=503 ]
union main, audit
```

See [Joins and subsearches](./joins-and-subsearches.md).

## Filtering and shaping

### where

```
source=main | where status >= 500 AND NOT host LIKE 'api%'
source=main | where isnull(user) OR user = ''
```

Keeps rows whose condition is TRUE. See [Expressions](./expressions.md).

### fields, table

```
source=main | fields _time, host, status
source=main | fields - _raw, msg
source=main | fields host, resp*
```

Keeps the listed columns in order, or drops them with `-`. Names may contain
`*`. `table` is a synonym.

The members of an object are columns named by their path. For events such
as `{"http": {"path": "/api/users", "status": 200}}`, `fields http.path`
keeps the column `http.path`, and `fields http` keeps every member of the
object as its own column: `http.path`, `http.status`.

### rename

```
source=main | rename response_time as rt, resp* as r_*
```

A `*` in both names carries the matched text over. Renaming a missing field
does nothing.

### eval

```
source=main | eval kb = bytes / 1024, slow = response_time > 500, class = if(status >= 500, 'error', 'ok')
```

Adds or replaces columns. Assignments run left to right and each sees the
ones before it.

### replace

```
source=main | replace 'web-01' with 'WEB1', 'web-02' with 'WEB2' in host
```

Replaces whole values (with `*` wildcards) in the listed fields.

### fillnull

```
source=main | fillnull with 'unknown' in user
source=main | fillnull using user = 'none', bytes = 0
source=main | fillnull value=0
```

Replaces NULL and absent values. Without a field list, LynxDB fills the
NULL values of the columns each row has.

### convert

```
source=main | convert num(status), dur2sec(duration) as secs
```

Converts fields in place or into an alias. Functions: `auto`, `num`,
`rmcomma`, `rmunit`, `memk`, `dur2sec`, `mstime`, `ctime`, `mktime`, `none`.

### dedup

```
source=main | dedup host
source=main | dedup 2 host, level keepempty=true
source=main | dedup level consecutive=true
```

Keeps the first N rows for each combination of values. Rows where a key is
NULL are dropped unless `keepempty=true`.

### sort, reverse, head

```
source=main | sort - response_time, host
source=main | sort response_time desc
source=main | sort 3 - response_time
source=main | sort num(status), ip(client)
source=main | head 10
source=main | head 10 from 20
source=main | reverse
```

`sort` is stable; NULLs sort first in ascending order and last in
descending order. `ip(field)` sorts by the text form of the address. `head`
defaults to 10 rows; `from` skips rows first. LynxDB turns `sort` followed by
`head` into a bounded top-N.

## Aggregation

### stats

```
source=main | stats count()
source=main | stats count() as n, avg(response_time) as avg_ms by host, level
source=main | stats p95(response_time), dc(user) by span(_time, 5m)
source=main | stats count(eval(status >= 500)) as errors, count() as total by host
source=main | stats bucket_nullable=false count() by user
```

One row per group. Without `by`, one row. The aggregates come first, then the
group columns. An aggregate without `as` is named after its text: `count()`,
`p95(response_time)`. Rows whose group value is NULL, and rows that lack the
group field, form a group whose key is NULL, unless `bucket_nullable=false`
drops them. See the [aggregation guide](./aggregation-guide.md).

LynxDB behavior: output row order is unspecified; add `sort`. `partitions`,
`allnum` and `delim` are accepted and have no effect. `dedup_splitvalues`
fails with `UNSUPPORTED_FEATURE`.

### eventstats

```
source=main | eventstats avg(response_time) as avg_rt by host
| where response_time > avg_rt * 2
```

Adds the aggregate of each row's group to the row, keeping every row. The
window functions `row_number`, `rank`, `dense_rank`, `percent_rank`,
`cume_dist`, `nth` and `ntile` fail with `UNSUPPORTED_FUNCTION`;
`bucket_nullable=false` with a `by` clause fails with `UNSUPPORTED_FEATURE`.

### streamstats

```
source=main | streamstats count() as seq
source=main | streamstats current=false count() as before by host
source=main | streamstats window=5 global=false avg(response_time) as moving by host
```

Running aggregates in input order. LynxDB supports `current`, `window` and
`global=false`. `window` with `global=true` (the default) and a `by` clause,
`time_window`, `reset_before`, `reset_after`, and `bucket_nullable=false`
with a `by` clause fail with `UNSUPPORTED_FEATURE`.

### top, rare

```
source=main | where status >= 500 | top 3 path
source=main | top 3 path by host
source=main | top 5 countfield=hits showperc=true path
source=main | rare 3 method
```

The most or least frequent values with a `count` column (and `percent` with
`showperc=true`). The default is 10 values.

### timechart, chart, bin, trendline

```
source=main | timechart span=1m count() by level
source=main | chart count() over host by level
source=main | bin _time span=5m | stats count() by _time
source=main | trendline sma(5, response_time) as sma5
```

See [Time series](./time-series.md).

## Extracting fields

### parse

```
source=main | parse msg '(?<verb>[A-Z]+) (?<route>\S+) completed'
```

Named groups of an RE2 regular expression become columns. Rows that do not
match get empty strings.

### grok

```
source=main | grok msg '%{WORD:verb} %{URIPATH:route} completed'
```

Grok patterns from LynxDB's built-in library (`pkg/ppl/grok`); custom
pattern definitions are not supported.

### rex

```
source=main | rex field=msg "(?<verb>[A-Z]+) (?<route>\S+)"
source=main | rex field=path max_match=3 "(?<seg>[a-z0-9]+)"
source=main | rex field=msg mode=sed "s/completed/done/"
```

Extract mode, with `max_match` collecting several matches into an array, and
sed mode (`s/regex/replacement/` with an optional `g`). `offset_field` fails
with `UNSUPPORTED_FEATURE`.

### regex

```
source=main | regex path = 'users$'
source=main | regex host != '^web'
```

Keeps (or with `!=` drops) rows whose field matches the RE2 expression.

### spath

```
source=main | spath input=payload path=user.id output=uid
source=main | spath input=payload | eval uid = payload.user.id
```

With `path`, extracts one JSON value. Without `path`, LynxDB parses the
input column into an object in place, keeping JSON value types; read its
members with dotted names in expressions.

### patterns

```
source=main | patterns msg
source=main | patterns msg mode=aggregation
```

The `simple_pattern` method: in label mode it adds `patterns_field`; in
aggregation mode it returns `patterns_field`, `pattern_count` and
`sample_logs`. A `by` clause is supported only in aggregation mode.
`method=brain` and `show_numbered_token=true` fail with
`UNSUPPORTED_FEATURE`.

## Arrays and objects

```
source=main | eval tags = split(tag_list, ',') | expand tags
source=main | mvexpand tags limit=3
source=main | fields host, level | mvcombine level
source=main | flatten payload
source=main | nomv levels
```

`expand` and `mvexpand` emit one row per element. `mvcombine` merges rows
that differ only in one field into an array. `flatten` turns object members
into columns; aliases are assigned to members in key order. `nomv` joins an
array into one string, one element per line.

## Combining results

`join`, `lookup`, `append`, `appendpipe`, `appendcol`, `union` and
`multisearch` are described in [Joins and subsearches](./joins-and-subsearches.md).

## Reshaping

```
source=main | stats count() as n by level | addcoltotals label='total' labelfield=level n
source=main | stats count() as n by host | addtotals col=true label='all' labelfield=host
source=main | stats count() as n by level | transpose
source=main | stats count() as n by host, level | xyseries host level in ('ERROR', 'WARN') n
```

`addtotals` adds a row total column (`Total`) and, with `col=true`, a
totals row; `addcoltotals` adds only the totals row. `transpose` turns up to
5 rows (or the given count) into columns `row 1`, `row 2`, ...;
`xyseries` pivots one value column by a series column.

## explain

```
explain source=main | where status >= 500 | stats count() by host
```

Returns the logical plan, the optimized plan and the optimizer rules that
fired as one `plan` row. The `simple`, `standard`, `cost` and `extended`
modes render the same plans. See [Pipeline execution](./pipeline-execution.md).

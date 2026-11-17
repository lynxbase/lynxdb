---
title: Aggregation guide
sidebar_label: Aggregation guide
description: How PPL aggregation functions behave in LynxDB.
---

# Aggregation guide

Aggregation functions run in `stats`, `eventstats`, `streamstats`, `chart`
and `timechart`. The [aggregation table](./aggregations.md) lists every name
with its support and how its partial results merge.

```
source=main
| stats count() as n,
        count(eval(status >= 500)) as errors,
        avg(response_time) as avg_ms,
        p95(response_time) as p95_ms,
        dc(user) as users
  by host
| eval error_pct = round(errors * 100.0 / n, 1)
| sort - error_pct
```

## Rules

- An aggregation's argument is any expression: `avg(response_time * 2)`.
- NULL values are skipped. `count()` and `count(*)` count rows; `count(x)`
  counts rows where `x` is not NULL.
- `count` and `c` may be written without parentheses: `stats count by host`
  produces a column named `count`.
- The output column is named after the call's text unless it has an alias:
  `count()`, `avg(response_time)`, `percentile(response_time, 99)`. Alias
  columns that later commands refer to.
- Aggregations are valid only in the aggregating commands; `eval x = count()`
  is a `SEMANTIC_ERROR`, and a scalar function used as an aggregation
  (`stats lower(host)`) is a `SEMANTIC_ERROR` too.

## Counting

| Function | Result |
| --- | --- |
| `count()`, `count(*)`, `c()` | number of rows |
| `count(x)` | rows where `x` is not NULL |
| `count(eval(cond))` | rows where `cond` is TRUE |
| `distinct_count(x)`, `dc(x)` | number of distinct values |
| `distinct_count_approx(x)` | number of distinct values |

LynxDB behavior: `distinct_count` and `dc` count exactly up to 10,000
distinct values per group and switch to a HyperLogLog estimate beyond that
(the `stats` entry of the command table states this);
`distinct_count_approx` uses the same implementation.

## Numbers

`sum`, `avg`, `min`, `max`, `var_samp`, `var_pop`, `stddev_samp`,
`stddev_pop`. `min` and `max` also order strings; numbers order before
strings. The sample variants are NULL for fewer than two values.

## Percentiles

```
stats percentile(response_time, 99), p95(response_time), perc99.9(response_time), median(response_time)
```

`percentile(x, p)` and `percentile_approx(x, p)` take `p` between 0 and 100.
The shortcuts `pNN` and `percNN` (including fractions such as `p99.9`) and
`median(x)` (`percentile(x, 50)`) are the same function. LynxDB computes
percentiles with a t-digest; the optional compression argument is accepted
and not used.

## Time-ordered values

| Function | Result |
| --- | --- |
| `earliest(x)`, `latest(x)` | the value of `x` at the smallest or largest `_time` |
| `first(x)`, `last(x)` | the first or last non-NULL value in input order |

OpenSearch allows a second argument naming the time field of `earliest` and
`latest`. LynxDB orders by `_time`; another time field fails when the query
runs.

## Collections

| Function | Result |
| --- | --- |
| `values(x)` | the distinct non-NULL values, sorted, at most 100 |
| `list(x)` | the non-NULL values in input order, at most 100 |
| `take(x [, n])` | the first `n` values (default 10) |

## Rates in timechart

`per_second(x)`, `per_minute(x)`, `per_hour(x)` and `per_day(x)` are valid
only in `timechart`: the sum of `x` in each bucket scaled to the unit, so a
`span=1h` bucket with `per_minute(bytes)` reports bytes per minute.

```
source=main | timechart span=1h per_minute(bytes) by host
```

## Window functions

`eventstats` and `streamstats` accept the aggregation functions above.
The OpenSearch window functions `row_number`, `rank`, `dense_rank`,
`percent_rank`, `cume_dist`, `nth` and `ntile` fail with
`UNSUPPORTED_FUNCTION`. `trendline` computes simple (`sma`) and weighted
(`wma`) moving averages.

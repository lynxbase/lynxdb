---
title: Time series
sidebar_label: Time series
description: Time ranges, timechart, bin, span and time functions in PPL.
---

# Time series

Every event has a timestamp in the `_time` column (`@timestamp` is an
alias). LynxDB assigns it at ingest: from the first of the fields
`storage.time_fields` names (`_time`, `timestamp`, `@timestamp`, `time`,
`ts` by default) that a JSON event holds, from a timestamp at the start of a
line of text, and otherwise from the time the event arrives.

## Restricting the time range

```
source=main earliest=-1h
source=main earliest=-1d@d latest=@d
source=main earliest='2025-01-15 08:00:00' latest='2025-01-15 09:00:00'
source=main | where _time >= '2025-01-15 08:05:00'
source=main | where earliest('-15m', _time)
```

A range in the source command, or a comparison of `_time` with a constant
in a `where` directly after it, narrows the scan, which then reads only the
time partitions and blocks the range meets. The REST parameters `from`/`to`
and the CLI flags `--since`, `--from`, `--to` add a range of their own, and
the query reads the intersection. Relative times (`-1h`, `@d`) resolve against the query's
start time in UTC; snaps round down (`@d` is midnight, `@w1` is Monday,
`@mon` the first of the month).

## Buckets: span and bin

`stats ... by span(field, width)` groups by time buckets in one step:

```
source=main | stats count() by span(_time, 5m), status
source=main | stats count() by span(_time, 1m) as minute, level | sort minute
```

`bin` rewrites a column to its bucket and lets any later command group by
it:

```
source=main | bin _time span=5m | stats count() by _time
source=main | bin _time span=1h as hour | stats count() by hour
source=main | bin response_time span=100 | stats count() by response_time
```

A time bucket is its start timestamp. A numeric bucket from `bin` is a
range label such as `100-200`. Calendar units (`mon`, `q`, `y`) align to
calendar boundaries; the others align to multiples of the width since the
Unix epoch.

LynxDB supports `span=` with numeric and time units. `bins`, `minspan`,
`aligntime`, `start`, `end` and logarithmic spans fail with
`UNSUPPORTED_FEATURE`.

## timechart

```
source=main | timechart span=1m count() by level
source=main | timechart span=5m avg(response_time) by host
source=main | timechart span=1m limit=3 count() by host
source=main | timechart span=1h per_minute(bytes) by host
```

`timechart` aggregates one function per time bucket and, with `by`, per
series value. Its output has one row per bucket and series: `_time`, the
series column and the aggregate, sorted by time.

| Option | Default | Meaning |
| --- | --- | --- |
| `span` | `1m` | bucket width; must have a time unit |
| `limit` | 10 | series kept (`N` or `topN`), ranked by the aggregate's sum over all buckets; `0` keeps all |
| `useother` | true | merge the other series into `OTHER` |
| `usenull` | true | keep rows whose series value is NULL, as `NULL` |
| `nullstr` | `NULL` | label of the NULL series |
| `timefield` | `_time` | the time column to bucket |

`per_second`, `per_minute`, `per_hour` and `per_day` are valid only in
`timechart`; see the [aggregation guide](./aggregation-guide.md#rates-in-timechart).
`bins` and `minspan` fail with `UNSUPPORTED_FEATURE`.

## chart

```
source=main | chart count() over host by level
source=main | chart limit=5 useother=false avg(response_time) by path
source=main | chart count() over response_time span=200 by level
```

`chart` aggregates over one or two split fields; `span=` buckets a numeric
or time split. It accepts `limit` (`N`, `topN`), `useother`, `otherstr`,
`usenull` and `nullstr`; `bins`, `minspan`, `start` and `end` fail with
`UNSUPPORTED_FEATURE`.

## Moving windows

```
source=main | trendline sma(5, response_time) as sma5
source=main | trendline sort _time wma(5, response_time) as wma5
source=main | streamstats window=10 avg(response_time) as moving
source=main | streamstats window=5 global=false avg(response_time) as moving by host
```

`trendline` emits NULL until a window is full; LynxDB drops rows where the
averaged field is NULL before computing it. `streamstats window=N` covers
the current row and the N - 1 before it.

## Time functions

```
eval hour = hour(_time), day = date_format(_time, '%Y-%m-%d'), dow = dayname(_time)
eval age_s = timestampdiff(SECOND, _time, now())
eval next_day = date_add(_time, INTERVAL 1 DAY), shifted = _time - INTERVAL 30 MINUTE
where _time > now() - INTERVAL 1 DAY
```

All times are UTC. `now()` is the query's reference time, the same for
every row. See the [function guide](./function-guide.md#date-and-time).

## Not supported

`timewrap` (period-over-period wrapping) fails with `UNSUPPORTED_COMMAND`:
compute the shifted period with `eval` and `join` it instead.

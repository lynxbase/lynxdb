---
title: PPL examples
sidebar_label: Examples
description: Worked PPL queries for common log analysis tasks.
---

# PPL examples

The queries run over access logs in the index `main` with lines such as:

```
2025-01-15T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502 user=grace response_time=786 bytes=32228 msg="GET /api/v1/users completed"
```

Try them on a file with `lynxdb query --file access.log '<query>'`.

## Error rate per host

```
source=main
| stats count() as total, count(eval(status >= 500)) as errors by host
| eval error_rate = round(errors * 100.0 / total, 1)
| sort - error_rate
```

```
host    error_rate  errors  total
api-02  22.6        48      212
web-02  22.3        43      193
web-03  21.1        41      194
web-01  19.7        38      193
api-01  16.3        34      208
```

`errors * 100.0` makes the division a float division; `errors * 100 / total`
would divide integers.

## Slowest endpoints

```
source=main
| stats count() as hits, p50(response_time) as p50, p95(response_time) as p95 by path
| where hits > 50
| sort - p95
| head 5
```

## Most frequent error messages

```
source=main level=ERROR | top 5 msg
```

## Errors over time

```
source=main level=ERROR OR level=WARN earliest=-1h
| timechart span=1m count() by level
```

## Status classes

```
source=main
| eval class = case(status >= 500, '5xx', status >= 400, '4xx', status >= 300, '3xx' else '2xx')
| stats count() as n by class
| sort class
```

```
class  n
2xx    480
3xx    61
4xx    255
5xx    204
```

## Requests far above their host's average

```
source=main
| eventstats avg(response_time) as host_avg by host
| where response_time > host_avg * 1.8
| fields _time, host, path, response_time, host_avg
```

## Extract fields at query time

```
source=main
| rex field=msg "(?<verb>[A-Z]+) (?<route>\S+)"
| stats count() as n by verb, route
| sort - n
| head 5
```

```
n   route            verb
39  /api/v1/orders   GET
33  /api/v1/search   POST
33  /api/v1/users    PUT
32  /api/v1/health   PUT
32  /api/v1/search   PUT
```

## Read JSON

```
makeresults count=1
| eval payload = '{"user":{"id":7},"items":[{"sku":"a"},{"sku":"b"}]}'
| spath input=payload path=user.id output=uid
| eval n = json_array_length(json_extract(payload, '$.items'))
| fields uid, n
```

## Users who saw a 503, and their traffic

```
source=main
| where user IN [ source=main | where status = 503 | fields user ]
| stats count() as requests by user
| sort - requests
| head 3
```

## Latest event per host

```
source=main | sort - _time | dedup host | fields host, _time, status, path
```

## First and last seen

```
source=main
| stats earliest(_time) as first_seen, latest(_time) as last_seen, count() as n by user
| sort user
```

## Failed logins per user

```
source=main "auth/login" status>=400
| stats count() as failures by user
| sort - failures
| head 3
```

## Moving average

```
source=main
| where host = 'web-01'
| trendline sma(10, response_time) as rt_sma10
| fields _time, response_time, rt_sma10
```

## Pivot

```
source=main | chart count() over host by method
source=main | stats count() as n by host, level | xyseries host level in ('ERROR', 'WARN') n
```

## Totals row

```
source=main | stats count() as n by level | addcoltotals label='total' labelfield=level n
```

```
level  n
ERROR  294
INFO   359
WARN   347
total  1000
```

## Traffic per host in 5-minute buckets

```
source=main
| eval kb = bytes / 1024.0
| stats sum(kb) as kb by span(_time, 5m), host
```

## Enrich from another index

```
source=main
| stats count() as n by host
| lookup owners host append owner, team
```

## A fixed time window

```
source=main earliest='2025-01-15 08:00:00' latest='2025-01-15 08:01:00' | stats count()
```

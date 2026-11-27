---
sidebar_position: 4
title: REST API event histogram
sidebar_label: Histogram
description: GET /histogram returns event counts per time bucket.
---

# Histogram API

## GET /histogram

Returns the number of events per time bucket over a time range, the shape a time picker or an event-volume chart needs. It counts every event of the range; it takes no query. Any valid key can call it when authentication is on.

| Parameter | Default | Description |
|---|---|---|
| `from` | `-1h` | Start of the range, inclusive: relative (`-30m`) or absolute (`2026-11-17T08:00:00Z`) |
| `to` | `now` | End of the range, exclusive |
| `buckets` | `60` | Number of buckets asked for, at most 1000; zero or less means 60 |
| `index` | all indexes | Count only this index |

The interval is the range divided by `buckets`, snapped to the nearest of 1s, 5s, 30s, 1m, 5m, 15m, 1h, 6h and 1d; an interval above a day becomes a whole number of days. It is reported as a Go duration (`15m0s`). Buckets start at multiples of the interval since the Unix epoch: the first is the one that holds `from`, the last is the last one that starts before `to`, and buckets without events count zero. Snapping can make the interval shorter than asked, so a response can hold more buckets than `buckets`.

```bash
curl -s 'localhost:3100/api/v1/histogram?from=2025-01-15T08:00:00Z&to=2025-01-15T09:00:00Z&buckets=4'
```

```json
{
  "data": {
    "interval": "15m0s",
    "buckets": [
      {"time": "2025-01-15T08:00:00Z", "count": 889},
      {"time": "2025-01-15T08:15:00Z", "count": 0},
      {"time": "2025-01-15T08:30:00Z", "count": 0},
      {"time": "2025-01-15T08:45:00Z", "count": 0}
    ],
    "total": 889
  },
  "meta": {"took_ms": 0.109}
}
```

The server answers by running `source=<index> | stats count() as count by span(_time, <interval>) as _time` over the range, so the counts match a query.

| Error | When |
|---|---|
| `400 VALIDATION_ERROR` | An unparsable `from` or `to`, `from` not before `to`, or an `index` that holds a backtick or a line break |

For counts filtered by a query, run a PPL query instead:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main level=ERROR | stats count() as n by span(_time, 5m), host", "from": "-1h"}'
```

The result is an aggregate table with the columns `n`, `span(_time,5m)` and `host`.

## Related

- [Query API](./query.md)
- [Time series in PPL](../ppl/time-series.md)

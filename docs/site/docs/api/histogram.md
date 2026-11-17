---
sidebar_position: 4
title: Histogram
description: GET /histogram returns event counts per time bucket.
---

# Histogram API

## GET /histogram

Returns event counts per time bucket. Counts come from segment metadata and timestamps without running a query, so the endpoint is cheap enough to call on every change of a time picker. It counts every event in the range; it takes no query.

| Parameter | Default | Description |
|---|---|---|
| `from` | `-1h` | Start of the range: relative or ISO 8601 |
| `to` | `now` | End of the range |
| `buckets` | `60` | Number of buckets, at most 1000 |
| `index` | all indexes | Count only this index |
| `group_by` | | Split each bucket by the values of this field |

The interval is the range divided by `buckets`, rounded to the nearest of 1s, 5s, 30s, 1m, 5m, 15m, 1h, 6h and 24h, and reported as a Go duration (`15m0s`). Bucket `i` starts at `from + i × interval`; events between the end of the last bucket and `to` are counted in the last bucket.

```bash
curl -s 'localhost:3100/api/v1/histogram?from=-2h&buckets=4&index=main'
```

```json
{
  "data": {
    "interval": "15m0s",
    "buckets": [
      {"time": "2026-11-17T08:48:00Z", "count": 0},
      {"time": "2026-11-17T09:03:00Z", "count": 0},
      {"time": "2026-11-17T09:18:00Z", "count": 0},
      {"time": "2026-11-17T09:33:00Z", "count": 113}
    ],
    "total": 113
  },
  "meta": {"took_ms": 0}
}
```

With `group_by`, each bucket has `counts` keyed by field value instead of `count`. Events without the field are counted under `other`:

```bash
curl -s 'localhost:3100/api/v1/histogram?from=-2h&buckets=4&index=main&group_by=level'
```

```json
{
  "data": {
    "interval": "15m0s",
    "buckets": [
      {"time": "2026-11-17T08:48:00Z", "counts": {}},
      {"time": "2026-11-17T09:03:00Z", "counts": {}},
      {"time": "2026-11-17T09:18:00Z", "counts": {}},
      {"time": "2026-11-17T09:33:00Z", "counts": {"ERROR": 33, "INFO": 35, "WARN": 43, "other": 2}}
    ],
    "total": 113
  },
  "meta": {"took_ms": 0}
}
```

An unparsable `from` or `to`, or `from` not before `to`, answers `400 VALIDATION_ERROR`.

For counts filtered by a query, run a PPL query instead:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main level=ERROR | stats count() as n by span(_time, 5m), host", "from": "-1h"}'
```

The result is an aggregate table with the columns `host`, `n` and `span(_time,5m)`.

## Related

- [Query API](./query.md)
- [Time series in PPL](../ppl/time-series.md)

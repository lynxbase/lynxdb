---
sidebar_position: 4
title: Live Tail & Histogram
description: GET /tail streams matching events over Server-Sent Events; GET /histogram returns event counts per time bucket.
---

# Live Tail & Histogram API

## GET /tail

Streams events that match a PPL query over Server-Sent Events: first the most recent stored events, then events as they arrive. Any valid key can open a tail when authentication is on.

| Parameter | Default | Description |
|---|---|---|
| `q` | | A full PPL query, starting with a source like any other query (required) |
| `count` | `100` | Stored events to replay before going live, at most 10000 |
| `from` | `-1h` | How far back the replay looks: relative (`-30m`) or ISO 8601 |

```bash
curl -N -G localhost:3100/api/v1/tail \
  --data-urlencode 'q=source=main level=ERROR | where status >= 500 | fields _time, host, status' \
  --data-urlencode 'count=2'
```

```text
event: result
data: {"_time":"2026-11-17T10:32:54.573324Z","host":"web-01","status":502}

event: result
data: {"_time":"2026-11-17T10:32:54.573324Z","host":"api-01","status":503}

event: catchup_done
data: {"count":2}

event: heartbeat
data: {"ts":"2026-11-17T10:45:12Z"}
```

The server subscribes to incoming events before it reads the stored ones, so no event is lost or sent twice at the switch from replay to live.

### Events

| Event | When | Data |
|---|---|---|
| `result` | One per matching event, during replay and live | The event as a JSON object |
| `catchup_done` | Replay finished | `{"count": N}` |
| `heartbeat` | Every 15 seconds while live | `{"ts": "<RFC 3339 time>"}` |
| `warning` | The client read too slowly and events were dropped | `{"type": "events_dropped", "count": N, "total_dropped": M}` |
| `close` | The session reached `tail.max_session_duration` | `{"reason": "max_session_duration"}` |
| `error` | The stream failed | `{"error": "<message>"}` |

### Which queries can be tailed

A live tail sees one event at a time, so the query can only use commands that produce output row by row. These work:

- search terms and field comparisons in the source command, `where`, `regex`
- `eval`, `fields`, `rename`, `fillnull`, `replace`, `convert`
- `parse`, `grok`, `rex`, `spath`
- `dedup`, `head`

Commands that need all their input before they produce a row are rejected with `422`: `stats`, `timechart`, `chart`, `top`, `rare`, `sort`, `reverse`, `eventstats`, `streamstats`, `trendline`, `join`, `lookup`, `append`, `appendcol`, `union`, `multisearch`, `transpose`, `addtotals`, `addcoltotals`, `mvcombine`, `patterns` and `describe`. The message names the plan operator that blocked the query:

```bash
curl -s -G localhost:3100/api/v1/tail --data-urlencode 'q=source=main | stats count() by host'
```

```json
{"error": "Aggregate is not supported in live tail: it needs all input rows before producing output"}
```

### Errors

| HTTP | Body | When |
|---|---|---|
| 400 | `{"error": "missing required parameter: q"}` | No `q` |
| 400, 422 | The query error envelope (`SYNTAX_ERROR`, `UNSUPPORTED_COMMAND`, ...) | The query does not compile; see [Query errors](./query.md#errors) |
| 400 | `QUERY_TOO_LARGE` error envelope | The query is longer than `query.max_query_length` |
| 422 | `{"error": "<Operator> is not supported in live tail: ..."}` | The query needs all its input; see above |
| 429 | `{"error": "too many active tail sessions"}` | `tail.max_concurrent_sessions` (default 100) sessions are open; `Retry-After: 5` |

### Browser clients

`EventSource` cannot set headers. With authentication on, pass the token as `_token`:

```javascript
const params = new URLSearchParams({
  q: 'source=main level=ERROR | where status >= 500 | fields _time, host, status, path',
  count: '100',
  from: '-1h',
  _token: token,
});
const es = new EventSource(`/api/v1/tail?${params}`);
es.addEventListener('result', (e) => appendRow(JSON.parse(e.data)));
es.addEventListener('catchup_done', () => showLiveIndicator());
es.addEventListener('warning', (e) => console.warn(JSON.parse(e.data)));
```

See [`lynxdb tail`](../cli/tail.md) for the CLI and [Live tail](../guides/live-tail.md) for patterns.

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
- [`lynxdb tail`](../cli/tail.md)
- [Time series in PPL](../ppl/time-series.md)

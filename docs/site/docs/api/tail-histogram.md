---
sidebar_position: 4
title: Live Tail & Histogram
description: GET /tail for SSE real-time log streaming, GET /histogram for time-bucketed event counts.
---

# Live Tail & Histogram API

Real-time log streaming and time-bucketed event counts. These endpoints power the live tail view and timeline bar chart in the Web UI.

## GET /tail

Server-Sent Events (SSE) stream for real-time log tailing with full SPL2 pipeline support. Uses the same query engine as `POST /query`, but in streaming mode.

### Query Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `q` | Yes | -- | SPL2 query (streaming commands only) |
| `count` | No | `100` | Number of historical events to replay during catchup (max 10,000) |
| `from` | No | `"-1h"` | Catchup lookback window (relative or ISO 8601) |

### Two Phases

1. **Catchup** -- replays the last `count` matching events from storage (time range: `from` to now). Each event is sent as `event: result`. Phase ends with `event: catchup_done`.
2. **Live** -- streams new events from the EventBus through the SPL2 pipeline in real time. Events are sent as `event: result`. Heartbeat sent every 15 seconds as `event: heartbeat`.

### Basic Example

```bash
curl -N "localhost:3100/api/v1/tail?q=level%3Derror&from=-1h&count=100"
```

**SSE event stream:**

```
event: result
data: {"_time":"2026-02-14T14:51:58Z","_raw":"level=ERROR status=502 uri=/api/users","status":502}

event: result
data: {"_time":"2026-02-14T14:52:01Z","_raw":"level=ERROR status=503 uri=/api/orders","status":503}

event: catchup_done
data: {"count":2}

event: result
data: {"_time":"2026-02-14T14:52:15Z","_raw":"level=ERROR status=500 uri=/api/pay","status":500}

event: heartbeat
data: {"ts":"2026-02-14T14:52:30Z"}
```

### With SPL2 Pipeline

Apply streaming transformations to the live tail:

```bash
# Filter and project fields
curl -N "localhost:3100/api/v1/tail?q=search+ERROR+%7C+where+status%3E500+%7C+fields+_time,uri,status&count=50"

# Extract fields with rex
curl -N "localhost:3100/api/v1/tail?q=search+%22connection+refused%22+%7C+rex+field%3D_raw+%22host%3D(%3FP%3Chost%3E%5CS%2B)%22&from=-30m"
```

### Supported SPL2 Commands

Only streaming (event-by-event) commands are supported in live tail:

| Supported | Not Supported |
|---|---|
| `search`, `where`, `eval`, `fields`, `table`, `rename`, `rex`, `fillnull`, `head`, `bin` | `stats`, `sort`, `join`, `dedup`, `timechart`, `top`, `rare`, `streamstats`, `eventstats`, `transaction` |

Commands requiring full materialization (aggregation, sorting, deduplication) are rejected with `422`:

```json
{
  "error": "unsupported commands for tail: stats (aggregation and stateful commands require full materialization)",
  "unsupported": ["stats"]
}
```

### Event Types

| Event | When | Data |
|---|---|---|
| `result` | Each matching event (catchup + live) | Event JSON object |
| `catchup_done` | Historical replay complete | `{"count": N}` |
| `heartbeat` | Every 15s during live phase | `{"ts": "..."}` |
| `error` | Query or pipeline error | Error object |

### JavaScript Example

```javascript
const params = new URLSearchParams({
  q: 'level=error | where status > 500',
  count: 100,
  from: '-1h'
});

const es = new EventSource(`/api/v1/tail?${params}`);

es.addEventListener("result", (e) => {
  appendRow(JSON.parse(e.data));
});

es.addEventListener("catchup_done", (e) => {
  showLiveIndicator();
});

es.addEventListener("heartbeat", (e) => {
  updatePing();
});

es.addEventListener("error", (e) => {
  showError(JSON.parse(e.data));
});
```

### Error Responses

| Status | Code | Description |
|---|---|---|
| `400` | -- | Invalid SPL2 query (parse error, includes `suggestion`) |
| `422` | -- | Query contains unsupported commands for tail |

:::tip Why SSE and not WebSocket?
Live tail is a unidirectional server-to-client stream. SSE auto-reconnects, passes through HTTP proxies without special configuration, and works with the native `EventSource` browser API. WebSocket would add unnecessary complexity for a one-way data flow.
:::

---

## GET /histogram

Time-bucketed event counts for the timeline bar chart. Designed to be fast -- fires on every filter change in the Web UI and runs independently and concurrently with the main query.

### Query Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `q` | No | -- | SPL2 filter expression (search part only, before pipes) |
| `from` | Yes | -- | Start time (relative or ISO 8601) |
| `to` | No | `"now"` | End time |
| `buckets` | No | `60` | Target number of buckets. Server picks the best interval (1s, 5s, 1m, 5m, 1h). |

### Example

```bash
curl -s "localhost:3100/api/v1/histogram?q=level%3Derror&from=-1h&buckets=60" | jq .
```

**Response (200):**

```json
{
  "data": {
    "interval": "1m",
    "buckets": [
      {"time": "2026-02-14T14:00:00Z", "count": 42},
      {"time": "2026-02-14T14:01:00Z", "count": 87},
      {"time": "2026-02-14T14:02:00Z", "count": 156}
    ],
    "total": 8432
  },
  "meta": {
    "took_ms": 12
  }
}
```

### Response Fields

| Field | Type | Description |
|---|---|---|
| `data.interval` | string | Bucket interval chosen by server (e.g., `"1m"`, `"5m"`, `"1h"`) |
| `data.buckets` | array | Array of `{time, count}` objects |
| `data.buckets[].time` | string | ISO 8601 bucket start time |
| `data.buckets[].count` | integer | Number of matching events in this bucket |
| `data.total` | integer | Total matching events across all buckets |
| `meta.took_ms` | number | Query execution time in milliseconds |

### Interval Selection

The server automatically selects the best interval based on the time range and target `buckets` count:

| Time Range | Typical Interval |
|---|---|
| < 5 minutes | 1s or 5s |
| 5 minutes -- 1 hour | 1m |
| 1 -- 6 hours | 5m |
| 6 -- 24 hours | 15m |
| 1 -- 7 days | 1h |
| > 7 days | 6h or 1d |

### Without a Filter

Omit `q` to get a histogram of all events:

```bash
curl -s "localhost:3100/api/v1/histogram?from=-6h&buckets=72" | jq .
```

## Related

- **[`lynxdb tail` CLI command](/docs/cli/tail)** -- live tail from the command line
- **[Live Tail guide](/docs/guides/live-tail)** -- usage patterns and tips
- **[Query API](/docs/api/query)** -- full SPL2 query execution
- **[Time Ranges](/docs/spl2/time-ranges)** -- relative and absolute time syntax

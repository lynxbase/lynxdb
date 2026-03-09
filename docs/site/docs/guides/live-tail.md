---
title: Live Log Tailing
description: How to stream logs in real time using lynxdb tail, the SSE tail endpoint, and understand the catchup-then-live behavior.
---

# Live Log Tailing

LynxDB provides real-time log streaming with full SPL2 pipeline support. Use `lynxdb tail` from the CLI or connect to the SSE `/api/v1/tail` endpoint programmatically. The tail stream first catches up with recent historical events, then transitions to live streaming.

## Tail from the CLI

Use [`lynxdb tail`](/docs/cli/tail) to stream events in real time:

```bash
lynxdb tail
```

### Filter the stream

Apply any search filter to tail only matching events:

```bash
# Tail only errors
lynxdb tail 'level=error'

# Tail 5xx from nginx
lynxdb tail '_source=nginx status>=500'

# Tail with field-value filter
lynxdb tail '_source=api-gateway duration_ms>1000'
```

### Apply an SPL2 pipeline

Use eval, fields, and other commands to transform the stream:

```bash
lynxdb tail 'level=error | eval sev=upper(level) | fields _timestamp, source, sev, message'
```

### Control catchup behavior

By default, `lynxdb tail` fetches the last 100 historical events from the past hour, then switches to live streaming. Adjust this with `--count` and `--from`:

```bash
# Fetch 50 historical events before going live
lynxdb tail --count 50

# Look back 6 hours for historical catchup
lynxdb tail 'level=error' --from -6h

# Fetch last 200 events from the past 24 hours
lynxdb tail 'level=error' --count 200 --from -24h
```

### Console output

Events are colorized by level with timestamp, source, and message:

```
2026-01-15T14:23:01Z [ERROR] nginx: connection refused to upstream
2026-01-15T14:23:02Z [INFO] api-gateway: request handled in 45ms
--- historical catchup complete (47 events, 312ms) --- streaming live ---
2026-01-15T14:23:03Z [ERROR] nginx: timeout exceeded
2026-01-15T14:23:04Z [WARN] redis: slow query detected (45ms)
```

Press `Ctrl+C` to stop tailing.

---

## Tail via the REST API (SSE)

Connect to the [`GET /api/v1/tail`](/docs/api/tail-histogram) endpoint to receive events as Server-Sent Events (SSE):

```bash
curl -N "localhost:3100/api/v1/tail?q=level%3Derror&from=-1h&count=100"
```

### SSE event types

The stream sends three types of SSE events:

| Event type | Description |
|------------|-------------|
| `result` | A matching event (historical or live) |
| `catchup_done` | Marker indicating historical catchup is complete |
| `result` (after catchup) | A live event |

### Example SSE stream

```
event: result
data: {"_time":"2026-01-15T14:22:58Z","level":"error","message":"connection refused","source":"nginx"}

event: result
data: {"_time":"2026-01-15T14:23:01Z","level":"error","message":"timeout exceeded","source":"api-gw"}

event: catchup_done
data: {"count": 47}

event: result
data: {"_time":"2026-01-15T14:23:15Z","level":"error","message":"disk full","source":"postgres"}
```

### Query parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `q` | (none) | SPL2 filter or pipeline |
| `from` | `-1h` | Historical lookback period |
| `count` | `100` | Max historical events to fetch |

---

## Catchup-then-live behavior

The tail stream operates in two phases:

### Phase 1: Historical catchup

LynxDB queries the storage engine for recent events matching your filter, up to the `count` limit within the `from` time range. These events are delivered as `result` SSE events. This gives you immediate context about recent activity.

### Phase 2: Live streaming

After delivering the `catchup_done` event, the stream transitions to live mode. New events matching your filter are delivered in near-real-time as they are ingested. The stream stays open indefinitely until you disconnect.

### Why catchup matters

Without catchup, opening a tail stream would show nothing until the next matching event arrives. The catchup phase gives you immediate context. For example, if you are debugging an error spike, you see the last 100 errors immediately, then new errors appear as they happen.

---

## Practical examples

### Debug a production issue

Open two terminals:

```bash
# Terminal 1: Tail errors from all sources
lynxdb tail 'level=error | fields _timestamp, source, message'

# Terminal 2: Tail slow queries
lynxdb tail '_source=postgres duration_ms>500 | fields _timestamp, query, duration_ms'
```

### Monitor a deployment

Start tailing before deploying and watch for errors:

```bash
lynxdb tail 'level=error OR level=fatal | fields _timestamp, source, level, message' --from -5m
```

### Watch a specific service

```bash
lynxdb tail '_source=api-gateway | fields _timestamp, level, endpoint, duration_ms, message'
```

### Tail with grep-like filtering

Combine tail with Unix tools for additional processing:

```bash
# Pipe tail output to grep for secondary filtering
lynxdb tail '_source=nginx' --format json | grep "timeout"
```

---

## Using tail in the Web UI

The Web UI has a dedicated Live Tail view. Open it by navigating to the tail page in your browser or using:

```bash
lynxdb ui
```

The Web UI connects to the same SSE endpoint and provides a visual interface with syntax-highlighted events, pause/resume, and filter editing.

---

## Differences from `lynxdb query`

| Feature | `lynxdb tail` | `lynxdb query` |
|---------|---------------|----------------|
| Duration | Runs indefinitely (streaming) | Runs once and exits |
| Time range | Catchup window + live | Fixed window |
| New events | Delivered as they arrive | Only existing events |
| Use case | Real-time monitoring | Historical analysis |

---

## The `lynxdb watch` alternative

If you want to see aggregation results update over time (instead of individual events), use [`lynxdb watch`](/docs/cli/shortcuts):

```bash
# Re-run an aggregation every 5 seconds
lynxdb watch 'level=error | stats count by source' --interval 5s

# Show deltas between runs
lynxdb watch '| stats count by level' --since 15m --diff
```

`watch` re-executes the query at each interval and displays the latest results. It is useful for monitoring aggregate trends rather than individual events.

---

## Live tail in cluster mode

In a distributed LynxDB cluster, the tail stream aggregates events from multiple nodes to provide a unified real-time view.

### How cluster tail works

1. **Catchup phase**: The query coordinator performs a distributed query across all relevant shards to fetch historical events. This is the same scatter-gather mechanism used for regular queries.
2. **Live phase**: After catchup, the coordinator opens streaming connections to all ingest nodes that own active shards. New events matching the filter are forwarded in near-real-time.
3. **Ordering**: Events from multiple ingest nodes are merged using watermark-based ordering to provide a roughly time-ordered stream. Due to clock skew and network latency, strict global ordering is not guaranteed -- events may arrive slightly out of order.

### Behavior differences

- **Latency**: In cluster mode, tail events pass through one additional hop (ingest node → query coordinator → client) compared to single-node mode.
- **Completeness**: The tail stream includes events from all ingest nodes. If an ingest node fails during tailing, events from that node stop arriving, but the stream continues from surviving nodes.
- **From the user's perspective**: The CLI, SSE endpoint, and Web UI behave identically in single-node and cluster modes.

---

## Next steps

- [Search and filter logs](/docs/guides/search-and-filter) -- craft effective filter expressions for tail
- [CLI: `tail`](/docs/cli/tail) -- complete CLI reference for the tail command
- [REST API: Tail and histogram](/docs/api/tail-histogram) -- SSE endpoint reference
- [CLI: shortcuts](/docs/cli/shortcuts) -- `watch`, `diff`, and other real-time commands

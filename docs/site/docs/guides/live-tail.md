---
title: Live Log Tailing
description: Stream matching events in real time with lynxdb tail and the SSE tail endpoint, and know which PPL commands a live tail accepts.
---

# Live Log Tailing

A live tail runs a [PPL](../ppl/overview.md) query against events as they
arrive. It first replays the most recent stored events that match, then
streams new matching events until you stop it. Use `lynxdb tail` from a
terminal or the Server-Sent Events endpoint `GET /api/v1/tail` from code.

## Tail from the CLI

[`lynxdb tail`](../cli/tail.md) takes a full PPL query, starting with a
source like any other query. With no query it tails `source=*`, every index.

```bash
# Every event
lynxdb tail

# Errors from the main index
lynxdb tail 'source=main level=ERROR'

# 5xx responses from web hosts
lynxdb tail 'source=main host=web-* | where status >= 500'

# Slow requests, with selected fields
lynxdb tail 'source=main | where response_time > 700 | fields _time, host, path, response_time'
```

### Transform the stream

Commands that work one row at a time can follow the source:

```bash
# Compute a field
lynxdb tail 'source=main level=ERROR | eval slow = if(response_time > 500, "yes", "no")'

# Extract a field from the message
lynxdb tail 'source=main | parse msg "(?<verb>[A-Z]+) (?<route>\S+) completed" | where verb = "DELETE"'

# One event per host
lynxdb tail 'source=main level=ERROR | dedup host'
```

### Replay before going live

By default, `lynxdb tail` replays up to 100 matching events from the past
hour, then switches to live streaming. `--count` (`-n`) and `--from` change
that:

```bash
# Replay 50 events
lynxdb tail 'source=main level=ERROR' --count 50

# Look back 6 hours
lynxdb tail 'source=main level=ERROR' --from -6h

# Up to 200 events from the past 24 hours
lynxdb tail 'source=main level=ERROR' --count 200 --from -24h
```

### Console output

On a terminal each event prints as one line with its time, level, source
and message (or `_raw` when there is no `message` field). A marker line
separates the replay from the live stream:

```text
2025-01-15T08:08:15.554Z [ERROR] web-01: 2025-01-15T08:08:15.554Z host=web-02 level=ERROR method=DELETE path=/api/v1/auth/login status=503 ...
2025-01-15T08:08:16.151Z [ERROR] web-01: 2025-01-15T08:08:16.151Z host=api-02 level=ERROR method=PUT path=/api/v1/search status=500 ...
--- historical catchup complete (2 events, 4ms) — streaming live ---
```

With `--format json`, each event prints as one JSON object per line, which
suits `jq`:

```bash
lynxdb tail 'source=main level=ERROR | fields _time, host, status, msg' --format json
```

```json
{"_time":"2025-01-15T08:08:15.554Z","host":"web-02","msg":"DELETE /api/v1/auth/login completed","status":503}
{"_time":"2025-01-15T08:08:16.151Z","host":"api-02","msg":"PUT /api/v1/search completed","status":500}
```

Press `Ctrl+C` to stop.

## What a tail query can contain

A live stream never ends, so a tail query can only use commands that
produce output row by row:

- search terms and field comparisons in the source command, `where`, `regex`
- `eval`, `fields`, `rename`, `fillnull`, `replace`, `convert`
- `parse`, `rex`, `grok`, `spath`
- `dedup`, `head`

Commands that need all of their input before they produce a row are
rejected: `stats`, `timechart`, `chart`, `top`, `rare`, `sort`, `reverse`,
`eventstats`, `streamstats`, `trendline`, `join`, `lookup`, `append`,
`appendcol`, `union`, `multisearch`, `transpose`, `addtotals`,
`addcoltotals`, `mvcombine`, `patterns` and `describe`. The error names the
plan operator that blocked the query. `lynxdb tail 'source=main | stats
count() by host'` fails with:

```text
Aggregate is not supported in live tail: it needs all input rows before producing output
```

For a periodically refreshed aggregate, run `lynxdb query` with `--since`
from a loop or a scheduler instead.

## Tail over HTTP (SSE)

[`GET /api/v1/tail`](../api/tail-histogram.md) streams the same results as
Server-Sent Events. `q` is required; `from` (default `-1h`) and `count`
(default 100, at most 10000) control the replay.

```bash
curl -N -G localhost:3100/api/v1/tail \
  --data-urlencode 'q=source=main level=ERROR | fields _time, host, status' \
  -d count=2
```

```text
event: result
data: {"_time":"2025-01-15T08:08:15.554Z","host":"web-02","status":503}

event: result
data: {"_time":"2025-01-15T08:08:16.151Z","host":"api-02","status":500}

event: catchup_done
data: {"count":2}

event: heartbeat
data: {"ts":"2026-01-15T14:23:30Z"}
```

| Event | Meaning |
|-------|---------|
| `result` | One matching event, from the replay or live |
| `catchup_done` | The replay is complete; `count` is the number of replayed events |
| `heartbeat` | Sent every 15 seconds while live |
| `warning` | The client read too slowly and events were dropped |
| `close` | The session reached `tail.max_session_duration` |
| `error` | The stream failed |

A query that does not compile answers with the usual query error envelope;
a query with a blocking command answers `422`.

## Replay, then live

The server subscribes to new events before it reads the stored ones, so the
switch from replay to live loses no event and sends none twice. The replay
returns the most recent `count` matching events within the `from` window,
which gives immediate context: when you start tailing during an error spike,
the last errors appear at once and new ones follow as they are ingested.

## Limits

| Setting | Default | Effect |
|---------|---------|--------|
| `tail.max_concurrent_sessions` | 100 | Further sessions get `429` with `Retry-After: 5` |
| `tail.max_session_duration` | 24h | The stream sends a `close` event and ends |

## Examples

### Watch a deployment

```bash
lynxdb tail 'source=main level IN (ERROR, FATAL) | fields _time, host, level, msg' --from -5m
```

### Follow one service

```bash
lynxdb tail 'source=main _source=api-gateway | fields _time, level, path, response_time, msg'
```

### Post-process with jq

```bash
lynxdb tail 'source=main | where status >= 500' --format json \
  | jq -r '"\(.host) \(.path) \(.status)"'
```

## Next steps

- [CLI: `tail`](../cli/tail.md)
- [Tail and histogram API](../api/tail-histogram.md)
- [Search semantics](../ppl/search-and-types.md): the filters a tail query starts with

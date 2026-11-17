---
sidebar_position: 4
title: tail
description: Stream matching events from a LynxDB server as they arrive, with a PPL query as the filter.
---

# tail

Stream events from the server as they are ingested.

```
lynxdb tail ['<PPL query>'] [flags]
```

**Alias:** `t`

The argument is a full [PPL](../ppl/overview.md) query that starts with a source, like any other query. Without an argument the CLI tails `source=*`, every index.

```bash
lynxdb tail 'source=main | where level = "ERROR"'
```

The CLI first compiles the query on the server, so a query that does not compile fails at once with a caret under the error. Then it opens `GET /api/v1/tail` (Server-Sent Events): the server sends up to `--count` matching events from the `--from` window, a marker, and then each new matching event as it is ingested. Press `Ctrl+C` to stop.

## Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--count` | `-n` | `100` | Historical events to send before live streaming (the server caps it at 10000) |
| `--from` | | `-1h` | How far back the historical events reach |

## What a tail query can contain

A live tail produces each output row from one input event. Commands that work row by row are allowed:

- search terms and field comparisons in the source command: `source=main level=ERROR status>=500`
- `where`, `eval`, `fields`, `rename`
- `parse`, `rex`, `grok`, `spath`
- `dedup`, `head`

Commands that need every input row before they can produce output are rejected with HTTP 422: `stats`, `timechart`, `chart`, `top`, `rare`, `sort`, `reverse`, `eventstats`, `streamstats`, `trendline`, `join`, `lookup`, `append`, `union`, `multisearch`, `appendcol`, `transpose`, `addtotals`, `addcoltotals`, `mvcombine`, `patterns` and `describe`.

```
  ✖ HTTP_422: Aggregate is not supported in live tail: it needs all input rows before producing output
```

Use [`lynxdb query`](./query.md) with `--since` for aggregations over recent data.

## Examples

```bash
# Every new event, after the last 100 from the past hour
lynxdb tail

# Errors from one index
lynxdb tail 'source=main level=ERROR'

# Server errors from web hosts, selected fields
lynxdb tail 'source=main host=web-* status>=500 | fields _time, host, path, status'

# Slow requests, with a computed field
lynxdb tail 'source=main | where response_time > 500 | eval seconds = response_time / 1000.0'

# Pull a value out of the message
lynxdb tail 'source=main | rex field=msg "(?<verb>[A-Z]+) (?<route>\S+)" | fields _time, verb, route'

# Last 50 events of the past 15 minutes, then live
lynxdb tail 'source=main' --count 50 --from -15m

# One JSON object per event, for jq
lynxdb tail 'source=main level=ERROR' --format json | jq -r .path
```

## Output

On a terminal, each event prints as one line: the event time, the `level` field (colored by severity), the `source` field and the `message` field, or `_raw` when the event has no `message`:

```
2026-01-15T14:23:01.105Z [ERROR] app: 2026-01-15T14:23:01.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502
2026-01-15T14:23:02.543Z [ERROR] app: 2026-01-15T14:23:02.543Z host=api-02 level=ERROR method=DELETE path=/api/v1/search status=502
--- historical catchup complete (2 events, 41ms) — streaming live ---
2026-01-15T14:23:07.587Z [ERROR] app: 2026-01-15T14:23:07.587Z host=api-02 level=ERROR method=POST path=/api/v1/auth/login status=503
```

A query that drops `level`, `source` or `_raw` with `fields` prints those parts empty; use `--format json` to see exactly the fields the query keeps.

With `--format json` or `ndjson`, each event prints as one JSON object per line.

## See also

- [Live tail API](../api/tail-histogram.md)
- [Pipeline execution](../ppl/pipeline-execution.md) for which operators stream
- [query](./query.md)

---
sidebar_position: 11
title: bench & demo
description: Run performance benchmarks and live demos with the LynxDB CLI -- no server required.
---

# bench & demo

Two self-contained commands for evaluating LynxDB performance and exploring its features. Both use an ephemeral in-memory engine -- no server required.

## bench

Run a local performance benchmark. Generates synthetic events and measures ingest + query performance.

```
lynxdb bench [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--events` | `100000` | Number of events to generate |

### Examples

```bash
# Default benchmark (100K events)
lynxdb bench

# Large benchmark (1M events)
lynxdb bench --events 1000000
```

### Console Output

```
  LynxDB Benchmark — 100,000 events
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Generating events... 100000 lines

  Ingest:  100,000 events in 245ms (408,163 events/sec)

  QUERY                                         RESULTS       TIME
  Filtered aggregate                                  1       12ms
  Full scan aggregate                                 5        8ms
  Full-text search                                  340       15ms
  Range filter + top                                 10       11ms
  Time bucketed                                      96       14ms

  Done.
```

### What It Measures

The benchmark runs five query patterns against the generated dataset:

| Query | Pattern |
|-------|---------|
| Filtered aggregate | `WHERE` + `STATS` on a selective predicate |
| Full scan aggregate | `STATS` across all events |
| Full-text search | Term search in `_raw` with inverted index |
| Range filter + top | Numeric range filter + `TOP` command |
| Time bucketed | `TIMECHART` with time bucket aggregation |

Results include ingest throughput (events/sec) and per-query latency.

---

## demo

Run a live demo that continuously generates realistic log events from 4 sources. Uses an in-memory engine with a built-in server. No external setup required.

```
lynxdb demo [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--rate` | `200` | Events per second |

### Examples

```bash
# Default rate (200 events/sec)
lynxdb demo

# Higher throughput
lynxdb demo --rate 500
```

Press `Ctrl+C` to stop.

### Generated Sources

The demo generates realistic log events from four services:

| Source | Event types |
|--------|------------|
| **nginx** | HTTP access logs with status codes, URIs, response times |
| **api-gateway** | Request routing, authentication, rate limiting events |
| **postgres** | Query execution, connection events, slow queries |
| **redis** | Cache hits/misses, key operations, memory events |

### Querying Demo Data

While the demo is running, query the data in another terminal:

```bash
# Count events by source
lynxdb query 'FROM main | stats count by source'

# Error rate
lynxdb query 'level=error | stats count by source'

# Top slow endpoints
lynxdb query 'source=nginx duration_ms > 1000 | top 10 uri'

# Live tail
lynxdb tail 'level=error'

# Server dashboard
lynxdb top
```

## See Also

- [query](/docs/cli/query) for querying data
- [Server](/docs/cli/server) for running a persistent server
- [CLI Overview](/docs/cli/overview) for all available commands

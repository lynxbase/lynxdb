---
title: Performance Tuning
description: Write LynxDB queries that read less data, and tune ingest, partitions and query concurrency for a workload.
---

# Performance Tuning

LynxDB runs well with its default settings. The largest gains come from the shape of the queries; a few settings change how the store writes events and how many queries run at once. Measure before and after each change (see [Monitoring](/docs/operations/monitoring)).

## Query Shape

Each index keeps its events in immutable columnar parts, cut into blocks of rows. A block records the time range of its rows and a filter of the words its text holds. A scan skips the parts and blocks that the query's time range or search terms rule out, and reads only the columns the query uses.

| Do | Why |
|----|-----|
| Bound the time range: `source=main earliest=-1h ...`, or `--since 1h` on the CLI | Partitions, parts and blocks outside the range are not read |
| Put search terms in the search: `source=main level=ERROR timeout` | Blocks whose word filter lacks a term are skipped |
| Filter before `eval`, `stats` or `sort` | Filters directly above the scan run inside it, block by block |
| Name the fields you need with `fields` | Only those columns are read |
| Use `sort ... \| head N` instead of a full `sort` | The optimizer turns it into a top-N that keeps N rows |

```bash
# Reads only the blocks of the last hour that may hold the word "timeout"
lynxdb query 'source=main earliest=-1h level=ERROR timeout | head 50'

# Top 10 failing paths of one service
lynxdb query 'source=main service="payment-service" status>=500
  | stats count() as errors by path
  | sort - errors
  | head 10'
```

`lynxdb explain '<query>'` prints the optimized plan. The `Scan` line shows the time range, the columns read and the predicates the scan applies:

```text
Optimized plan:
TopN(10, errors DESC NULLS LAST)
└─ Project(errors, path)
   └─ Aggregate(errors = count() BY path)
      └─ Filter((service = "payment-service") AND (status >= 500))
         └─ Scan(main) columns=[_time, path, service, status] prune=service = "payment-service"
```

See [PPL performance](../ppl/performance.md) for the plan rewrites.

## Ingest

### Batch Size

Send events in batches rather than one request per event. For the CLI, the batch size is a flag:

```bash
lynxdb ingest huge.log --batch-size 10000  # Default: 5000
```

Over HTTP, a body sent to `/api/v1/ingest` or `/api/v1/ingest/raw` may be up to `ingest.max_body_size` (default `100mb`), compressed or decompressed. The shipper routes (Elasticsearch bulk, Splunk HEC, OTLP) are also bounded by `ingest.limits.max_compressed_body_bytes` (default `32mb`) and `ingest.limits.max_decompressed_body_bytes` (default `256mb`).

### Flush Interval

`storage.flush_interval` sets the age at which buffered events become due for a flush. The default, `0s`, means the store's own interval of one second.

```yaml
storage:
  flush_interval: 5s
```

A longer interval writes fewer, larger runs under a light, steady load, and leaves more events in memory, where an abrupt stop loses them. An HTTP query syncs the database before execution, so increasing the interval can also leave more data for a query to flush.

### Ingest Mode

For Splunk HEC, OTLP and syslog events, `ingest.mode: lightweight` extracts only the metadata fields from JSON event text instead of every key. See [Ingest configuration](/docs/configuration/ingest).

## Partitions

`storage.partition` supplies the default partition width for new indexes. Each index saves its own width. Queries prune parts by their stored time bounds, and retention deletes whole partitions.

```yaml
storage:
  partition: 6h
```

A shorter partition makes retention finer and gives the store more partitions to keep apart. See [Time partitions](../architecture/storage.md#time-partitions) before choosing the width for an index.

## Concurrent Queries

`query.max_concurrent` (default `32`) bounds the queries that run at once. A query beyond the limit is refused at once with `429 TOO_MANY_REQUESTS` and a `Retry-After: 1` header; it does not wait in a queue.

```yaml
query:
  max_concurrent: 64
```

The columnar executor shares the CPU cores among the queries that run, so a higher limit lets more queries progress at once, each more slowly. Asynchronous jobs count against the limit too. The setting applies at server start.

### Synchronous Wait and Result Size

| Setting | Default | Effect |
|---------|---------|--------|
| `query.sync_timeout` | `30s` | How long `POST /api/v1/query` waits before it answers with a job handle instead of the result |
| `query.default_result_limit` | `1000` | Rows returned when the request sets no `limit` |
| `query.max_result_limit` | `50000` | The largest `limit` a request may set |

Large results cost memory on the server and the client. Aggregate on the server, or end event searches with `head`.

## Workload Profiles

### Heavy Ingest

```yaml
storage:
  flush_interval: 2s

ingest:
  max_body_size: "100mb"
```

Send large batches; one request per event costs a round trip and a buffer append each.

### Many Concurrent Dashboards

```yaml
query:
  max_concurrent: 64
  default_result_limit: 500
```

### Small VMs

```yaml
query:
  max_concurrent: 4
```

## Next Steps

- [Monitoring](/docs/operations/monitoring) -- metrics to watch while tuning
- [Storage Settings](/docs/configuration/storage) -- storage configuration reference
- [Query Settings](/docs/configuration/query) -- query configuration reference
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose performance issues

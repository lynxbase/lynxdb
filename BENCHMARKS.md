# Benchmarks

LynxDB has four kinds of benchmarks: Go micro benchmarks of the hot paths,
two dataset suites, JSONBench and a generated logs suite, that time whole
queries over the columnar store (`pkg/colstore`), and a profiling run of the
server under mixed load. Results are not checked in; record the commit, the
machine, the data size and the store layout with every number you publish.

## Micro Benchmarks

```bash
mise run bench   # row and ingest pipelines; writes artifacts/bench-micro.txt
mise run bench -- ./pkg/vec/...   # the same for other packages
go test -run '^$' -bench . -benchmem ./pkg/vec/...
```

Packages with `Benchmark` functions: `pkg/vec`, `pkg/colstore`,
`pkg/colstore/exec`, `pkg/colstore/part`, `pkg/colstore/shred`,
`pkg/colstore/tokenfilter`, `pkg/engine/columnar`, `pkg/engine/pipeline`,
`pkg/query`, `pkg/mem`, `pkg/api/rest`, `pkg/ingest/limits` and
`pkg/ingest/pipeline`. Compare runs with `benchstat`, and use `-count` of 5 or
more.

## JSONBench

The [JSONBench](https://github.com/ClickHouse/JSONBench) dataset is a stream
of Bluesky events as NDJSON. The harnesses are environment-gated tests: they
skip unless the variables are set.

```bash
# Queries over one file, loaded in arrival order and by the sort key
LYNXDB_BENCH_NDJSON=/data/file_0001.json go test ./pkg/colstore/exec -run '^TestJSONBenchPlans$' -v

# Ingest, disk layouts, merges and small durable writes
LYNXDB_BENCH_NDJSON=/data/file_0001.json go test ./pkg/colstore -run '^TestJSONBenchIngest$' -v
LYNXDB_BENCH_NDJSON=/data/file_0001.json go test ./pkg/colstore -run '^TestJSONBenchDisk$' -v
LYNXDB_BENCH_NDJSON=/data/file_0001.json LYNXDB_BENCH_SMALL_WRITES=1 go test ./pkg/colstore -run '^TestJSONBenchSmallWrites$' -v

# An HTTP server over a stored table, for cold and concurrent runs
LYNXDB_BENCH_DIR=/data/bluesky LYNXDB_BENCH_LISTEN=127.0.0.1:9000 go test ./pkg/colstore/exec -run '^TestJSONBenchServer$' -v -timeout 0
curl -s 'http://127.0.0.1:9000/query?q=Q1'
curl -s --get 'http://127.0.0.1:9000/query' --data-urlencode 'ppl=source=bluesky | stats count()'
```

Other harnesses over the same file:

| Test | Package | Measures |
|---|---|---|
| `TestJSONBench` | `pkg/colstore` | The queries over one loaded table, by block size, codec and compression effort |
| `TestJSONBenchMerge` | `pkg/colstore` | Merging runs as one merge and as range merges on every core |
| `TestJSONBenchOrdered` | `pkg/colstore` | The queries over key-ordered and arrival-ordered parts, which must agree |
| `TestJSONBenchConcurrent` | `pkg/colstore/exec` | Queries per second and latency from concurrent clients, with optional concurrent inserts (`LYNXDB_BENCH_CONCURRENT`) |
| `TestJSONBenchSearchedColumns` | `pkg/colstore/exec` | How many columns searches decode, and how many the token filters spare |
| `TestJSONBenchReference` | `pkg/colstore/exec` | The plans' answers against a reference evaluator over a sample |

Useful variables: `LYNXDB_BENCH_RUNS` (timed runs, best reported),
`LYNXDB_BENCH_QUERY` (keep queries whose name contains it),
`LYNXDB_BENCH_PROFILE` (a directory for CPU profiles), and
`LYNXDB_BENCH_T10_US` / `LYNXDB_BENCH_T60_US` (the time windows as
`lo,hi` in microseconds). `TestJSONBenchStoreLayouts` and
`TestJSONBenchPlansAgree` check that every layout and plan returns the same
answer.

The queries, as PPL (`benchTexts` in `pkg/colstore/exec/jsonbench_test.go`):

| Name | Query |
|---|---|
| Q1 | `source=bluesky \| stats count() as count by commit.collection \| fields commit.collection, count \| sort - count, commit.collection` |
| Q2 | `source=bluesky \| where kind = 'commit' and commit.operation = 'create' \| stats count() as count, distinct_count(did) as users by commit.collection \| fields commit.collection, count, users \| sort - count, commit.collection` |
| Q3 | `source=bluesky \| where kind = 'commit' and commit.operation = 'create' and commit.collection in ('app.bsky.feed.like', 'app.bsky.feed.post', 'app.bsky.feed.repost') \| eval hour = hour(_time) \| stats count() as count by commit.collection, hour \| fields commit.collection, hour, count \| sort hour, commit.collection` |
| Q4 | `source=bluesky \| where kind = 'commit' and commit.operation = 'create' and commit.collection = 'app.bsky.feed.post' \| stats min(_time) as first by did \| fields did, first \| sort first, did \| head 3` |
| S2 | `source=bluesky \| where match(commit.record.text, 'love') \| stats count()` |
| S3 | `source=bluesky \| where match_phrase(commit.record.text, 'good morning') \| stats count()` |
| S5 | `source=bluesky \| where like(commit.record.text, '%ware%') \| stats count()` |
| S6 | `source=bluesky \| where did = 'did:plc:yj3sjq3blzpynh27cumnp5ks' \| stats count()` |
| S7 | `source=bluesky \| where commit.cid = 'bafyreicsj6qpl6dreic6ekdjxvdjltncngl52x3jddf6dxhg2gwo35iqhi' \| stats count()` |
| R1 | `source=bluesky \| where regexp_match(commit.record.text, '[Gg]ood (morning\|night)') \| stats count()` |
| A1 | `source=bluesky sc7btuzqdaepksx43mr74pma \| stats count()` |
| A3 | `source=bluesky love \| stats count()` |
| T1 | `source=bluesky \| where _time >= '<lo>' and _time <= '<hi>' \| stats count()` over a 10-minute window |
| K1 | `source=bluesky \| stats count() as count by did \| fields did, count \| sort - count, did \| head 10` |
| K2 | `source=bluesky \| stats count() as count by did \| eventstats sum(count) as total \| eval percent = round(count * 100.0 / total, 2) \| fields did, count, percent \| sort - count, did \| head 10` |
| T4 | `source=bluesky \| where did = 'did:plc:yj3sjq3blzpynh27cumnp5ks' \| eval event = commit.collection \| fields _time, event \| sort - _time, event \| head 20` |

Plans that PPL cannot state with the same semantics (case-sensitive word
searches, for example) run as executor plans only and have no text.

## Logs Suite

`pkg/colstore/exec/logsbench_*_test.go` generates a log stream (JSON events
with nested HTTP, error and user fields, mixed-type fields, and plain text
lines) and times grouping, search, regex, time-window and missing-field
queries over it:

```bash
LYNXDB_LOGSBENCH=1 go test ./pkg/colstore/exec -run '^TestLogsBench$' -v
```

`LYNXDB_LOGSBENCH_QUERY`, `LYNXDB_LOGSBENCH_LAYOUT`, `LYNXDB_LOGSBENCH_RUNS`
and `LYNXDB_LOGSBENCH_PROFILE` work like their JSONBench counterparts.
`TestLogsBenchReference` checks every answer against a reference evaluator,
and `TestLogsBenchFile` writes the generated stream to
`LYNXDB_LOGSBENCH_FILE` for loading into other systems.
Example queries (`logQueries` in `logsbench_queries_test.go`):

| Name | Query |
|---|---|
| G1 | `source=logs \| stats count() as n by level \| sort - n, level` |
| G5 | `source=logs \| stats p50(http.latency_ms) as p50, p95(http.latency_ms) as p95, p99(http.latency_ms) as p99 by service \| sort service` |
| G12 | `source=logs \| stats count(http.status) as n, count(eval(http.status >= 500)) as failed by service \| sort - failed, service` |
| S2 | `source=logs deadlock \| stats count() as n` |
| S3 | `source=logs \| where regexp_match(message, 'timeout after \d+ms waiting') \| stats count() as n` |
| E2 | `source=logs \| where isnull(user_id) \| stats count() as n` |

## Server Profiling

`tools/dev/profile/run-all.sh` builds the binary, starts a server with runtime
profiling in a fresh data directory under `artifacts/profiles/<time>/`, loads
`testdata/bench/json_100k.log` twenty times, runs a minute of concurrent
ingest and PPL queries, collects CPU, heap, allocation, goroutine, mutex,
block and trace profiles, and writes `FINDINGS.md` with the top functions and
GC pauses. `mise run bench:fixtures` generates the fixtures when they are missing.

```bash
tools/dev/profile/run-all.sh
LYNXDB_ADDR=localhost:3200 tools/dev/profile/run-all.sh
```

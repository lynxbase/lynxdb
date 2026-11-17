---
title: Pipeline execution
sidebar_label: Pipeline execution
description: How LynxDB runs a compiled PPL plan, locally and on a server.
---

# Pipeline execution

This page describes LynxDB's execution; PPL defines what a query returns,
not how it runs.

## From text to rows

1. **Compile.** `query.Compile` parses the text, analyzes it and lowers it to
   an IR plan, then optimizes the plan (see [PPL-to-IR compilation](./compilation.md)
   and [IR optimizer](./optimizer.md)). Compiling depends on the text alone,
   so LynxDB keeps the plans of the 1024 most recently used queries of up to
   2 KiB and reuses them for a repeated text.
2. **Plan storage access.** On a server, `pkg/planner` derives storage hints
   from the plan's scans: sources, time bounds, search terms, field
   predicates, required columns and a row limit
   ([Search semantics](./search-and-types.md#how-the-server-uses-search-terms)).
3. **Build.** `pkg/engine/physical` turns each IR operator into a pipeline
   iterator and compiles each expression to VM bytecode (`pkg/vm`). Before
   building the operator that uses it, it runs every subquery once and keeps
   its rows.
4. **Run.** The root iterator pulls batches of up to 1024 rows from its
   inputs, down to the scan, which reads events from storage.

The plan is the same wherever the query runs; only the scan's event source
differs.

## Streaming and blocking operators

| Operators | Commands | Behavior |
| --- | --- | --- |
| Filter, Compute, Project, RenameColumns, Extract, Unnest, FlattenObject, FillNull | `where`, `eval`, `fields`, `rename`, `parse`, `grok`, `rex`, `spath`, `expand`, `flatten`, `fillnull`, ... | stream: each batch flows through |
| Limit, Dedup | `head`, `dedup` | stream; `head` stops reading once it has its rows |
| Aggregate | `stats`, `top`, `rare`, `timechart`, `chart` | consume all input, then emit groups |
| Sort, TopN, Reverse | `sort`, `sort` + `head`, `reverse` | consume all input; TopN keeps only N rows in a heap |
| Window | `eventstats`, `streamstats`, `trendline` | hold the rows of a partition or window |
| Join | `join`, `lookup` | builds the right side in memory, streams the left |
| Union, ZipColumns | `append`, `appendpipe`, `appendcol`, `union`, `multisearch` | read their inputs in turn |
| Transpose, Totals, CombineValues, Patterns | `transpose`, `addtotals`, `mvcombine`, `patterns` | consume all input |

Live tail runs only plans without the blocking operators; see
[Live tail](#live-tail).

## Memory

On a server, operators that hold rows register with the query's memory
budget (`query.max_query_memory_bytes`) and the server-wide query pool
(`query.global_query_pool_bytes`). Sorts and aggregations spill to disk
under `query.spill_dir` when they exceed their budget; other operators fail
the query with `QUERY_MEMORY_EXCEEDED`. Subquery results and the right side
of a join are held in memory.

## Local execution

`lynxdb query --file` and piped input load the events into an in-process
index named `main`, then run the compiled plan over it with the same
physical planner. Formats are detected while loading (combined, syslog,
docker; `key=value` pairs are always extracted); `--raw` turns detection
off.

## Server execution

`POST /api/v1/query` runs a query synchronously, as an asynchronous job
(`"wait": 0`), or synchronously up to a timeout before turning into a job
(`"wait": N`). `/api/v1/query/stream` writes rows as NDJSON while the query
runs. The request's `from` and `to` bound event time in addition to the
query's own bounds; `limit` and `offset` page the result.

The server reads each index's segments. For every segment it first checks
the time range, column statistics against field predicates and the bloom
filter against search terms, and skips segments that cannot match; within a
segment the term index selects candidate rows. Every predicate is then
evaluated on the rows read, so results do not depend on which indexes a
segment has.

## Live tail

`lynxdb tail` and `GET /api/v1/tail` replay recent matching events (`from`,
default `-1h`, up to `count` events), then stream new events as they are
ingested. The query is compiled like any other; the server rejects plans
that contain an operator needing all of its input (Aggregate, Sort, TopN,
Window, Reverse, Join, Union, Transpose, Totals, CombineValues, Patterns,
SourceSchema) with 422.

## Reading a plan

`lynxdb query --explain` prints the plans locally; `lynxdb explain` and
`GET /api/v1/query/explain` ask the server and add the storage hints.

```
Optimized plan:
TopN(5, n DESC NULLS LAST)
└─ Project(n, host, slow)
   └─ Aggregate(n = count() BY host, slow)
      └─ Compute(slow = response_time > 500)
         └─ Filter(status = 500)
            └─ Scan(main) time=[now-1d, +inf) columns=[_time, host, response_time, status]
```

Read the tree from the bottom: the scan reads `main` for the last day, and
only the four columns the plan uses. `time=`, `columns=`, `limit=` and
`prune=` on the scan are the storage hints. The optimizer rules that fired
are listed under the plans. `--analyze` (CLI) or `analyze=true` (REST) also
runs the query and reports its execution statistics.

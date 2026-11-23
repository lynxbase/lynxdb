---
title: Pipeline execution
sidebar_label: Pipeline execution
description: How LynxDB runs a compiled PPL plan with the columnar executor and the row operators, locally and on a server.
---

# Pipeline execution

This page describes LynxDB's execution; PPL defines what a query returns,
not how it runs.

## From text to rows

1. **Compile.** `query.Compile` parses the text, analyzes it and lowers it to
   an IR plan, then optimizes the plan (see [PPL-to-IR compilation](../contributing/ppl-compilation.md)
   and [IR optimizer](../contributing/ir-optimizer.md)). Compiling depends on the text alone,
   so LynxDB keeps the plans of the 1024 most recently used queries of up to
   2 KiB and reuses them for a repeated text.
2. **Narrow.** The time range of a request (`from` and `to` on the REST API,
   `--since`, `--from` and `--to` in the CLI) narrows every scan of the plan,
   those of subqueries included.
3. **Resolve subqueries.** Every subquery runs once, before the outer
   query: an `EXISTS` becomes TRUE or FALSE, a scalar subquery its value, and
   an `IN` subquery the list of the values of its rows.
4. **Check the sources.** A source that names an index there is not fails
   the query with `no such index: <name>` (`404 NOT_FOUND` on the REST API);
   a pattern may match no index, and `main` is there before its first event.
   A field that the query reads from a scan and that no event of the scanned
   indexes holds gives a warning that names the field and the indexes.
5. **Run on the columnar executor.** Walking down from the root, LynxDB gives
   the columnar executor (`pkg/engine/columnar`, `pkg/colstore/exec`) the
   largest part of the plan above a scan that it can run.
6. **Run the rest on the row operators.** The operators above that part run
   in `pkg/engine/physical` and `pkg/engine/pipeline`. They pull batches of
   rows from their inputs. A part of the columnar executor that only chooses
   columns of a scan, with at most a limit above it, hands its rows over
   block by block as it scans; any other part runs to its end before the row
   operators read its result.

A plan that the columnar executor takes whole never meets the row operators;
its rows stay in columns until the response is written. The plan is the same
wherever the query runs; the CLI and the server differ only in where the
indexes are.

## What the columnar executor takes

`pkg/colstore/exec/irplan` translates a chain of IR operators from a scan
upwards and refuses the first operator it has no translation for. It takes:

| Operators | Commands |
| --- | --- |
| Scan with its time range, Filter | the source command and its search terms, `where`, `regex` |
| Compute, Extract, FillNull, RenameColumns | `eval`, `parse`, `rex`, `fillnull`, `rename` |
| Project | `fields`, `table` |
| one Aggregate, with Filter and Compute above it | `stats`, `top`, `rare`, `timechart`, `chart` and what follows them |
| Window that sums a column of the aggregate over all its groups | the totals of `top` and `rare` with `showperc=true` |
| Sort, TopN, Limit | `sort`, `sort ... \| head`, `head` |
| Dedup of the first row of each key | `dedup` with one row per key |

`irplan` lowers every expression of the IR onto the executor: operators
and comparisons onto its operations, which run vectorized over typed
columns, and the scalar functions onto their implementations in
`pkg/colstore/exec/irfunc`. A volatile function such as `rand()` leaves its
operator to the row operators, which compute it row by row in input order.
`list`, `values` and `take` and any other operator, such as `eventstats`,
`streamstats`, `join`, `append` or a second `stats`, run on the row
operators over the rows of the part below them.

The executor reads a snapshot of each index the scan names, taken when the
query starts, and works on blocks of up to 16384 rows on every core. Before
it opens a block it leaves out:

- the time partitions and blocks outside the scan's time range;
- the blocks whose token filters show that no column can hold a word or
  phrase that the filter needs, or that a field cannot hold the string it
  must equal;
- the blocks in which an earlier query with the same filter found no row.

After it opens a block, the block's column directory (constant columns,
the bounds of integer columns, absent columns) can reject it before any
column is decoded. Every condition is then evaluated on the rows read, so
the skipped blocks never change a result.

## Row operators

The row operators have no semantics of their own. They lower their
expressions with the same `irplan` code and compute them row by row with the
executor's per-value functions (`exec.RowExpr`), aggregate with the
executor's aggregate states (`exec.Accumulator`), order rows with its
comparator and group, deduplicate and join by its keys. A query gives the
same rows whichever part of it runs where; a test runs a corpus of queries
both ways and requires that.

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

## Memory

The row operators hold in memory the rows of the operators that consume all
their input, the right side of a join and the result of every subquery. A
`sort` that the row operators run fails the query when it holds more than
10,000,000 rows. Both keep a state per group of an aggregate; the state of
`distinct_count_approx` takes at most 4 KB per group and the percentiles of
one argument at most 32 KB per group, while `distinct_count` keeps a hash of
every distinct value. A `streamstats` with a `window` aggregates the rows of
the window anew for every row.

## Local execution

`lynxdb query --file`, and `--file -` for standard input, compile the
query first, then load the input into the index `main` of a database in a temporary directory,
which the command removes when it exits, and run the compiled plan with the
same executors as the server. Formats are detected while loading
(combined, syslog; `key=value` pairs are always extracted); `--raw`
turns detection off.

## Server execution

`POST /api/v1/query` runs every query as a job: synchronously, as an
asynchronous job (`"wait": 0`), or synchronously up to a timeout before
answering with the job (`"wait": N`). `/api/v1/query/stream` writes the rows
as NDJSON. Before a query runs, the server makes the events ingested so far
visible, so a query sees every event ingested before it. The request's
`from` and `to` bound event time in addition to the query's own bounds;
`limit` and `offset` page the result. At most `query.max_concurrent` queries
run at once; a query beyond that answers `429 TOO_MANY_REQUESTS`.

## Reading a plan

`lynxdb query --explain` prints the plans locally; `lynxdb explain` and
`GET /api/v1/query/explain` ask the server, which adds the search terms,
the fields read and the source scope of the plan.

```bash
lynxdb query --explain 'source=main status=500 earliest=-1d | eval slow = response_time > 500 | stats count() as n by host, slow | sort - n | head 5'
```

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
`prune=` on the scan are the annotations of the optimizer. Every operator of
this plan is one the columnar executor takes, so it runs the whole plan.
The optimizer rules that fired are listed under the plans.
`lynxdb explain --analyze` and `analyze=true` on
`GET /api/v1/query/explain` also run the query and report its time and row
count.

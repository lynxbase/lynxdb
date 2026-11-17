---
title: Performance considerations
sidebar_label: Performance
description: Which parts of a PPL query let LynxDB read less data, and how to write fast queries.
---

# Performance considerations

A query's cost is mostly the data its scans read and the rows that the row
operators handle. The columnar executor runs the part of the plan from the
scan upwards that it can take, block by block over the columns it needs,
and leaves out blocks that cannot hold a matching row; the row operators
run the rest. [Pipeline execution](./pipeline-execution.md) lists what the
columnar executor takes.

## What lets a scan read less

| Query part | Effect |
| --- | --- |
| `earliest=`/`latest=`, `_time` compared with a constant in a `where` directly after the source, request `from`/`to` | time partitions and blocks outside the range are not read |
| free-text words and quoted phrases (`error`, `"connection refused"`) | blocks whose token filters rule the words out are not read; a word of digits and a single-word prefix (`conn*`) are checked on every row |
| a field equal to a string (`level=ERROR`, `where host = 'web-01'`) | blocks whose token filter for that field rules the string out are not read |
| the columns the plan uses | only those columns are decoded |
| `head N` over a scan, `fields`, `eval` or `rename` | the scan stops after N rows |
| `sort ... \| head N` | a top-N keeps only N rows instead of sorting all of them |

When the columnar executor opens a block, it first settles the filter from
the block's column directory where it can: constant columns, the bounds of
integer columns and absent columns. A comparison such as `status >= 500` can
so reject a block without decoding a column. Conditions the directory cannot
settle, such as `IN` lists, `LIKE`, regular expressions and computed values,
are evaluated on every row of the blocks that remain.

## What runs on the row operators

The row operators run every operator the columnar executor does not take,
over the rows of the part below it: `eventstats`, `streamstats`,
`trendline`, `join`, `lookup`, `append`, `appendpipe`, `appendcol`,
`union`, `multisearch`, `expand`, `mvexpand`, `flatten`, `patterns`,
`transpose`, `addtotals`, `addcoltotals`, `reverse`, a second `stats`, and
anything that follows one of them. In a query that uses one of these
commands after its `stats`, the columnar executor still runs the scan and
the `stats`, and the row operators handle only the groups; when the command
comes before the `stats`, the row operators handle every scanned event.

## Writing fast queries

- **Bound the time.** Put `earliest=` in the query or pass `from`/`to`
  (`--since` in the CLI). Events are kept in time partitions, so a bounded
  query reads only the partitions it meets.
- **Search for rare words.** `source=main "timeout"` lets the scan leave out
  blocks without the word; `where like(msg, '%timeout%')` reads every block
  in the range.
- **Compare strings exactly.** `host="web-01"` lets the scan skip blocks;
  `host=web-*` and `lower(host) = 'web-01'` are evaluated on every row.
- **Filter first.** Write `where` directly after the source, before `eval`,
  `stats` and joins.
- **Aggregate before the row operators.** Put `stats` before commands such
  as `eventstats`, `join` or `transpose`, so that they handle groups rather
  than events.
- **Keep join right sides small.** The right side of `join` and `lookup` and
  every subquery result are held in memory; filter them inside the
  subsearch.
- **Expect estimates where they apply.** The
  [aggregation guide](./aggregation-guide.md) states when distinct counts
  and percentiles are estimated.

## Measuring

- `lynxdb query --explain '<query>'` shows the optimized plan and the scan
  annotations without running it.
- `lynxdb explain '<query>'` asks the server and adds the search terms,
  fields read and source scope of the plan.
- `lynxdb explain --analyze '<query>'` also runs the query and reports its
  time and row count; REST responses report the time as `meta.took_ms`.

Server-side settings that affect queries are described in
[Query configuration](../configuration/query.md).

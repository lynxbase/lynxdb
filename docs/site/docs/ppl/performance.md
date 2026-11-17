---
title: Performance considerations
sidebar_label: Performance
description: Which parts of a PPL query LynxDB pushes to storage, and how to write fast queries.
---

# Performance considerations

A query's cost is mostly the data its scans read. The optimizer records on
each scan what storage may use to read less (the `time=`, `prune=`,
`columns=` and `limit=` annotations of an optimized plan), and the server
turns them into storage hints. Everything else runs over the rows the scan
returns.

## What reaches storage

| Query part | Hint | Effect on the server |
| --- | --- | --- |
| `earliest=`/`latest=`, `where _time >= '...'` directly after the source, request `from`/`to` | time range | segments outside the range are skipped |
| free-text words and quoted phrases (`error`, `"connection refused"`) | search terms | bloom filters skip segments; the term index selects rows |
| prefix and wildcard words (`conn*`, `*err*`) | token globs | the term index selects rows |
| `error OR warn` | term tree | the term index selects rows |
| a column equal to a string (`level=ERROR`, `where host = 'web-01'`) | field predicate | column statistics skip segments; rows are filtered while reading |
| the columns the plan uses | required columns | only those columns are read |
| `head N` over `fields`, `eval`, `rename` only | row limit | reading stops after N rows |
| `sort ... \| head N` | TopN | a bounded heap replaces the full sort |

## What does not reach storage

These are evaluated on every row the scan returns:

- numeric comparisons and ranges: `status=500`, `status >= 500`,
  `response_time > 800`;
- `IN` lists, `LIKE`, `REGEXP`, `CONTAINS`, wildcard field values
  (`host=web-*`), negations (`NOT error`, `status != 200`);
- field searches with `match`, `match_phrase` and `match_phrase_prefix`;
- predicates on computed values (`lower(host) = 'web-01'`);
- relative time comparisons in expressions (`where _time > now() - INTERVAL
  1 DAY`, `where earliest('-1h', _time)`); use `earliest=-1d` in the source
  command instead;
- a `where` that follows `eval`, `stats` or any command other than the
  source: the optimizer hands only the filters directly above a scan to it;
- `head` after a filter: `where status >= 500 | head 10` reads until ten rows
  pass, without a scan limit;
- in a plan with several scans (`join`, `lookup`, `append`, subqueries),
  everything except each scan's sources and the union of their time ranges.

## Writing fast queries

- **Bound the time.** Put `earliest=` in the query or pass `from`/`to`
  (`--since` in the CLI). Time is the cheapest filter: whole segments are
  skipped.
- **Search the raw text for rare words.** `source=main "timeout"` uses bloom
  filters and the term index; `where like(msg, '%timeout%')` reads every
  row.
- **Compare strings exactly.** `host="web-01"` reaches storage; `host=web-*`
  and `lower(host) = 'web-01'` do not.
- **Filter first.** Write `where` before `eval`, `stats` and joins, directly
  after the source, so its indexable parts reach the scan.
- **Aggregate before sorting.** `stats` reduces rows before `sort`;
  `sort ... | head N` becomes a TopN.
- **Keep join right sides small.** The right side of `join` and `lookup` and
  every subquery result are held in memory; filter them inside the
  subsearch.
- **Expect estimates where they apply.** `dc` is exact up to 10,000 distinct
  values per group; percentiles use a t-digest.

## Measuring

- `lynxdb query --explain '<query>'` shows the optimized plan and the scan
  annotations without running.
- `lynxdb explain '<query>'` adds the server's view: search terms, fields
  read, source scope, estimated cost.
- `lynxdb query --analyze` and `lynxdb explain --analyze` run the query and
  report rows scanned, segments scanned and skipped (by time, index, column
  statistics, bloom filter), and time per stage.
- REST responses carry the same numbers under `meta.stats`.

Server-side settings that affect query speed and memory are described in
[Performance tuning](../operations/performance-tuning.md) and
[Query configuration](../configuration/query.md).

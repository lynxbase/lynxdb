---
sidebar_position: 3
title: explain
description: Show how the server compiles and plans a PPL query, with optional execution profile.
---

# explain

Ask the server how it plans a [PPL](../ppl/overview.md) query, without running it.

```
lynxdb explain '<PPL query>' [flags]
```

The CLI calls `GET /api/v1/query/explain` on the server named by `--server`. The server compiles the query and returns:

- the **logical plan**: the IR tree the compiler produced from the PPL text;
- the **optimized plan**: the tree after the rule-based optimizer, with the scan's column list and pruning predicate;
- the **optimizer rules** that fired, with how often each fired;
- **scan hints**: the result type (`events` or `aggregate`), a cost class, the free-text search terms, the fields read, and the sources in scope. A plan that reads every event of its sources says so.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--analyze` | `false` | Run the query as well, and print its execution profile after the plan |

`--format json` prints the server's explain response, and with `--analyze` the profile, as one JSON document.

## Example

```bash
lynxdb explain 'source=main timeout | stats count() by host'
```

```
Logical plan:
Project(`count()`, host)
└─ Aggregate(`count()` = count() BY host)
   └─ Filter(SEARCH_WORD(_raw, "timeout"))
      └─ Scan(main)

Optimized plan:
Project(`count()`, host)
└─ Aggregate(`count()` = count() BY host)
   └─ Filter(SEARCH_WORD(_raw, "timeout"))
      └─ Scan(main) columns=[_raw, _time, host] prune=SEARCH_WORD(_raw, "timeout")

Optimizer rules:
  prune-scan x1

Scan:
  result type   aggregate
  cost          medium
  search terms  timeout
  fields read   _raw, _time, host
  sources       single main
```

The free-text term `timeout` becomes a `SEARCH_WORD` filter on `_raw`, and the scan reads three columns of the index `main`.

## With `--analyze`

```bash
lynxdb explain --analyze 'source=main level=ERROR | stats count()'
```

After the plan, the CLI runs the query and prints its profile: the time spent in each phase and the total.

```
  ══════ Query Profile ══════

  Phase Breakdown:
    exec            102µs     6%   ████████████████████
                 ──────────
    total             1ms
```

The query runs over all time; `explain` has no time-range flags.

## Invalid queries

A query that does not compile prints the server's error code and message, the query with a caret under the position and, when the compiler has one, a suggestion. The command then exits with code 1.

## `explain` compared with `query --explain`

| | `lynxdb explain` | `lynxdb query --explain` |
|---|---|---|
| Where it compiles | On the server | In the CLI |
| Needs a server | Yes | No |
| Logical and optimized plans, optimizer rules | Yes | Yes |
| Scan hints: result type, cost, search terms, fields read, sources | Yes | No |
| Execution profile | With `--analyze` | No |

## See also

- [PPL optimizer](../ppl/optimizer.md) for the rules that appear in the plan
- [PPL IR](../ppl/ir.md) for the plan operators
- [Pipeline execution](../ppl/pipeline-execution.md)
- [query --analyze](./query.md#profiles---analyze)

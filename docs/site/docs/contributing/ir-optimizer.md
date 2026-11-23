---
title: IR optimizer
sidebar_label: IR optimizer
description: The rewrite rules pkg/ir/opt applies to IR plans.
---

# IR optimizer

`pkg/ir/opt` rewrites a validated IR plan into a cheaper plan with the same
result. `opt.Optimize` applies its rules bottom-up over the whole plan,
repeats until no rule fires (at most 8 passes), then records required
columns on every scan. It returns the rules that fired with their counts;
`explain` lists them, and `GET /api/v1/query/explain` reports them as
`optimizer_rules`.

Every rule preserves the result. Rules that hand predicates to a scan keep
the predicates in the filter above it, so an executor may use any subset of
the scan's annotations, or none.

## Rules

| Rule | Rewrite |
| --- | --- |
| `fold-constants` | replaces a deterministic call or operator over literals with its value |
| `merge-filters` | `Filter(a)` over `Filter(b)` becomes `Filter(b AND a)` |
| `remove-true-filter` | drops `Filter(TRUE)` |
| `prune-scan` | copies the indexable conjuncts of a filter directly above a scan into `Scan.Prune` |
| `time-range-to-scan` | narrows `Scan.Time` by comparisons of `_time` with constant timestamps in a filter directly above the scan |
| `sort-limit-to-topn` | `Limit` over `Sort` becomes `TopN` |
| `limit-into-scan` | sets `Scan.Limit` to `count + offset` when only `Compute`, `Project` and `RenameColumns` separate a `Limit` from the scan |
| column pruning | records on each scan the columns the operators above it read (`Scan.Columns`), when they are known |

### fold-constants

Folding evaluates the expression with the VM through `Options.Fold`
(`query.Fold`); without it, folding is skipped. Volatile and query-constant
functions (`rand`, `now`) are not folded, nor are field references,
searches, subqueries and aggregate calls. `'2025-01-15 08:00:00'` compared
with `_time` folds to a timestamp literal, which then lets
`time-range-to-scan` fire.

### prune-scan

A conjunct is indexable when it is one of:

- a free-text search other than regex and substring searches
  (`SEARCH_WORD`, `SEARCH_PHRASE`, `SEARCH_PREFIX`, `SEARCH_GLOB` on `_raw`);
- a column equal to a string literal (`host = "web-01"`);
- a disjunction of free-text words (`SEARCH_WORD(_raw, "a") OR
  SEARCH_WORD(_raw, "b")`).

`GET /api/v1/query/explain` reports these predicates as the plan's search
terms. When the columnar executor runs the filter above the scan, it
derives what to skip from the whole filter; when it does not, the scan
that hands events to the row operators applies the pruning predicates
first.

```
source=main error host="web-01" status>=500
```

```
Filter((SEARCH_WORD(_raw, "error") AND (host = "web-01")) AND (status >= 500))
└─ Scan(main) prune=SEARCH_WORD(_raw, "error") prune=host = "web-01"
```

### time-range-to-scan

`>` and `>=` raise the earliest bound, `<` and `<=` lower the latest bound
(`<=` becomes an exclusive bound one nanosecond later). Only absolute bounds
narrow an existing range; relative bounds from `earliest=` stay as written.

```
source=main | where _time >= '2025-01-15 08:00:00' AND _time < '2025-01-15 09:00:00'
```

```
Filter((_time >= timestamp("2025-01-15T08:00:00Z")) AND (_time < timestamp("2025-01-15T09:00:00Z")))
└─ Scan(main) time=[2025-01-15T08:00:00Z, 2025-01-15T09:00:00Z)
```

### sort-limit-to-topn and limit-into-scan

```
source=main | sort - n | head 5          ->  TopN(5, n DESC NULLS LAST)
source=main | fields host | head 10      ->  Scan(main) limit=10 columns=[_time, host]
```

A `Limit` above a filter, an aggregation or any other operator that drops,
merges or reorders rows does not reach the scan.

### Column pruning

The pass walks down from the root with the set of columns each operator
needs. `Project` without `*` patterns (`fields a, b`) and `Aggregate`
(`stats`, `top`, `timechart`, ...) start a set; `Filter`, `Compute`, `Sort`,
`TopN`, `Dedup`, `RenameColumns` and `Extract` add the columns they read and
remove the ones they produce; `Limit`, `Reverse` and `fields -` pass the set
through. `_time` is always kept. Any other operator (`Window`, `Unnest`,
`FlattenObject`, `Patterns`, `Join`, `Union`, ...) needs all columns of its
input, so a scan below it reads every column, as does a plan whose output
is not narrowed by `fields` or `stats`.

## What the optimizer does not do

There is no predicate pushdown through other operators (a `where` after
`eval` stays above it), no join reordering and no cost model. Commands are
executed in the order written, apart from the rewrites above.

## Adding a rule

Add a function `func(ir.Node, *Options) (ir.Node, bool)` to `pkg/ir/opt`
and an entry in the `rules` table of `opt.go`. A rule sees one node at a
time and returns a replacement and whether it changed anything. It must
keep the plan valid (`ir.Validate` runs after optimization) and preserve
results for every input, including rows with NULL and absent columns.

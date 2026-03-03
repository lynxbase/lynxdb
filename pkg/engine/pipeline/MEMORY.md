# Memory Management in LynxDB Query Pipeline

## Architecture
- Each query gets a BudgetMonitor with a per-query limit (default: 1GB)
- Operators create BoundAccounts for their buffers (sort rows, aggregate groups, etc.)
- Scan operators track only the CURRENT batch (Shrink previous, Grow new)
- When Grow fails, operators with spill support write data to disk and retry

## Critical Invariants
1. Scan offset must NOT advance until Grow succeeds (prevents data loss on retry)
2. Sort catches child BudgetExceededError, spills its buffer, then retries child.Next()
3. Server queries MUST use streaming scan (SegmentStreamIterator) so operators can spill
4. Batch materialization (buildColumnarStore) is only for test injection paths

## Spill Support Matrix
| Operator    | Spill? | Strategy                          |
|-------------|--------|-----------------------------------|
| Sort        | Yes    | External k-way merge sort         |
| Aggregate   | Yes    | Grace hash partitioning           |
| Dedup       | Yes    | External hash table               |
| EventStats  | Yes    | External row buffer               |
| Join        | Yes    | Grace hash join                   |
| Scan        | No     | Streams one batch at a time       |
| Tail        | No     | O(N) ring buffer (bounded by count)|

## Data Flow

```
SegmentStreamIterator (scan)
  -> yields 1024-event batches
  -> Shrinks previous batch, Grows new batch in BoundAccount
  -> if Grow fails: returns BudgetExceededError (offset NOT advanced)

Sort (or other spill-capable operator)
  -> catches BudgetExceededError from child.Next()
  -> spills its accumulated buffer to disk via SpillManager
  -> retries child.Next() (scan replays the same batch since offset unchanged)
  -> continues accumulating into fresh in-memory buffer
```

## Why Streaming Matters for Non-Streamable Queries

Counterintuitively, non-streamable queries (sort, tail, join) benefit MORE from
the streaming scan path than streamable queries. The batch materialization path
(buildColumnarStore) loads ALL matching data into memory before the pipeline is
constructed. If data exceeds the memory budget, it fails with OOM before sort's
spill-to-disk logic ever gets a chance to run.

The streaming path reads one row group at a time (~65K events), letting sort
accumulate rows and spill to disk when the budget is exceeded. This is why
server-mode queries always use the streaming path regardless of IsStreamable().

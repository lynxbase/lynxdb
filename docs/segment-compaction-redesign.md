# Segment Compaction

LynxDB uses direct-to-part writes: accepted batches are buffered, flushed into immutable `.lsg` parts, and exposed after atomic rename. This keeps recovery simple, but sustained small-batch ingest can create L0 parts faster than background compaction removes them. Compaction therefore treats part pressure as debt local to each partition, not as a global count.

## Design

- A single normalized compaction config wires `storage.l0_threshold`, `storage.l1_threshold`, `storage.l2_target_size`, `storage.flush_threshold`, and `storage.row_group_size` into the batcher, writer, compactor, and scheduler.
- Ingest pressure is scoped to `(index, partition)`. Delay and reject decisions use only the hot partition's L0 parts plus pending flushes for that partition.
- L0 compaction stays size-tiered for write-heavy ingest. L1 and L2 use leveled byte targets so scan fan-out converges toward larger parts.
- Compaction jobs carry a score. L0 score is the larger of count pressure and byte pressure. L1/L2 scores use bytes over target, excluding inputs already queued or active.
- The scheduler deduplicates jobs by `(index, partition, output level, input IDs)` and exposes queued/active input IDs to the planner so repeated reactive ticks do not enqueue identical work.
- Queries read buffered events through the in-memory scan path instead of forcing a durable flush. Explicit flush and shutdown still write buffered events to parts.
- Debt observability is exposed through scheduler snapshots and metrics: queued jobs, active jobs, scores, pending bytes, input IDs, output level, last scheduler error, ingest delays, and ingest rejects.

## Production Model

The target shape follows the same constraints as MergeTree and LSM engines:

- Create fewer, larger fresh parts where possible.
- Keep L0 bounded by count and bytes.
- Prefer the highest compaction score, with L0 pressure ahead of maintenance.
- Merge within a single partition only.
- Preserve `.lsg` format compatibility and the no-WAL direct-to-part recovery model.

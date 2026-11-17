---
sidebar_position: 5
title: Indexing
description: How LynxDB skips data -- per-segment term indexes (FST + roaring bitmaps), per-row-group bloom filters, column statistics and time bounds, and how a query's storage hints use them.
---

# Indexing

Every `.lsg` segment carries the structures the server uses to avoid reading data a query does not need:

| Structure | Granularity | Built from | Answers |
|-----------|-------------|-----------|---------|
| Time range | Segment and row group | `_time` | Can any event fall inside the query's time bounds? |
| Bloom filters | Row group, per string column | Tokens of `_raw`, `_source`, `_sourcetype`, `host`, `index` and every string field | Can this row group contain a token? |
| Term index (FST + roaring bitmaps) | Segment | Tokens of `_raw` | Which rows contain a token? |
| Column statistics | Segment and row group | Min/max, counts, constant columns | Can a column hold a value? |
| Range bitmaps (format 2) | Row group, selected numeric columns | Column values | Which rows have a value in a range? |

A query reaches these structures through the **storage hints** the planner derives from the optimized plan (see [Query Engine](/docs/architecture/query-engine#planner-and-storage-hints)). The predicates stay in the plan's `Filter` above the scan, so index lookups only narrow the rows read; every row the scan returns is still checked by the filter.

## From Query to Hints

The optimizer's `prune-scan` rule copies the indexable conjuncts of the `Filter` directly above a `Scan` into the scan's pruning predicates, and `time-range-to-scan` narrows the scan's time range. `physical.ScanHints` (`pkg/engine/physical/source.go`) then turns them into hints:

| Query | Pruning predicate | Hint |
|-------|-------------------|------|
| `source=main timeout` | word search on `_raw` | `SearchTerms: [timeout]` |
| `source=main "connection refused"` | phrase search on `_raw` | `SearchTerms: [connection, refused]` |
| `source=main timeout OR refused` | disjunction of words | `SearchTermTree: AND(OR(timeout, refused))` |
| `source=main time*` | prefix search | `TokenGlobs: [time*]` |
| `source=main host=web-01` | `host = "web-01"` | `FieldPredicates: host = web-01` |
| `source=main earliest=-1h` | scan time range | `TimeBounds` resolved against the current time |
| `... \| fields host, status` | columns read | `RequiredCols` |

Search text is split into tokens the same way the index splits `_raw`: maximal runs of letters and digits, lower-cased. A phrase therefore becomes an AND of its tokens at the index level, and the filter checks the exact phrase afterwards.

What does not become a hint: comparisons other than string equality (`status>=500`, `status=500` with a number), searches on a named field with wildcards (`host=web-*`), regular expressions, `LIKE`, and predicates after a `stats`, `eval` of the compared field, or any other operator between the filter and the scan. These run as row filters over the rows the scan returns. A plan with several scans (`join`, `append`, `multisearch`, subsearches) keeps only the union of their sources and time ranges.

`lynxdb query --explain` shows the pruning predicates on the `Scan` line:

```
Scan(main) time=[now-1h, +inf) columns=[_raw, _time, host, status] prune=SEARCH_WORD(_raw, "timeout") OR SEARCH_WORD(_raw, "refused") prune=host = "web-01"
```

for

```
source=main earliest=-1h timeout OR refused host=web-01 status>=500 | fields host, status | head 20
```

## How the Server Applies Hints

`pkg/server/store.go` and `pkg/engine/pipeline/segment_stream.go` apply the hints in this order.

### 1. Segment Selection

`shouldSkipSegment` drops a segment before it is opened for reading when:

- its index is outside the query's source scope
- its `[min_time, max_time]` does not overlap the time bounds
- its column min/max excludes a string equality predicate (`host = "web-01"` when the segment's `host` range is `api-01`..`api-02`)
- its bloom filter (the union of its row groups' `_raw` blooms) lacks one of the `SearchTerms`

The remaining segments are read newest first, after the events still buffered in the ingest batcher.

### 2. Term Index Lookup

For each remaining segment, `SegmentStreamIterator` resolves the search hints against the segment's term index:

- `SearchTermTree` is evaluated as bitmap algebra (AND of OR groups); otherwise each term in `SearchTerms` is looked up and the posting lists intersected.
- Each `TokenGlobs` pattern is expanded against the FST term dictionary (up to 4096 terms) and the union of the matching posting lists is intersected in. A pattern that expands to more terms is ignored for pruning.

An empty result skips the segment. A bitmap that covers more than `query.bitmap_selectivity_threshold` of the segment's rows (default 0.9) is dropped, because a sequential read of the needed columns is cheaper than reading row by row.

```
source=main "connection refused"

FST lookup "connection" → {0, 5, 12, 33, 847, 1203, 5891}
FST lookup "refused"    → {12, 847, 1203, 5891, 7002}
intersection            → {12, 847, 1203, 5891}   rows to read
```

### 3. Row Group Filtering

Segments are stored in row groups (`storage.row_group_size`, default 65536 rows). Before a row group is read, it is skipped when:

- its time range is outside the time bounds
- the `_raw` bloom filter of the row group lacks a search term
- for an equality predicate: the column is constant with another value, absent from the row group, outside the row group's min/max, or its bloom filter lacks the value's tokens

### 4. Reading Rows

The row group is read with the term-index bitmap, the field predicates and the required columns: only the listed columns are decoded, and only rows in the bitmap that satisfy the field predicates become events. Events on the edge of the time bounds are filtered by time. The rows then flow into the `Filter` above the scan, which evaluates the full condition.

`--analyze` reports each step: segments skipped by index, time, column statistics and bloom filter, rows scanned, and the bitmap hits.

## Bloom Filters

The segment writer builds one bloom filter per row group for each string column, from the tokens of the column's values, with a 1% false-positive rate (`github.com/bits-and-blooms/bloom/v3`). Columns that hold one value in a row group are stored as constants and get no bloom filter. Blooms live in the `LSBL` region of the segment and are loaded on demand per row group; the union of a segment's `_raw` blooms is loaded once when the segment is opened.

A bloom filter answers "definitely absent" or "possibly present". A token absent from every row group of a segment skips the segment without reading its columns.

## Term Index

The term index maps each token of `_raw` to the rows of the segment that contain it:

- **Term dictionary**: a finite state transducer (`github.com/blevesearch/vellum`) maps each token to the offset of its posting list. Exact lookups cost O(token length); prefix and glob patterns are answered by iterating the FST.
- **Posting lists**: roaring bitmaps (`github.com/RoaringBitmap/roaring`) of row numbers. Roaring stores each 65536-row block as a sorted array, a bitmap or runs, whichever is smallest, and intersects and unions them block by block.

The index is built when a segment is written: every event's `_raw` is tokenized, each token's rows are collected into a bitmap, the sorted tokens are inserted into the FST, and both are written to the `LSIX` region. Compaction rebuilds the index for the merged segment.

## Range Bitmaps

Format 2 segments can carry bit-sliced range bitmaps (Range BSI) for selected numeric columns, per row group, in a section the footer points to. The row group filter evaluates range predicates (`>`, `>=`, `<`, `<=`) on those columns with them. PPL plans produce only equality hints, so range comparisons in a query run as row filters and do not consult these bitmaps. `storage.compaction_disable_bsi_on_output` stops compaction from writing them.

## Related

- [Segment Format](/docs/architecture/segment-format) -- where blooms, the term index and statistics live in the `.lsg` file
- [Query Engine](/docs/architecture/query-engine) -- how hints are derived from the plan
- [Storage Engine](/docs/architecture/storage-engine) -- when segments and their indexes are written
- [Performance Tuning](/docs/operations/performance-tuning#query-shape) -- writing queries that skip data

---
sidebar_position: 5
title: Indexing
description: Full-text search indexing in LynxDB -- FST-based inverted indexes, roaring bitmap posting lists, bloom filters, and how search queries use the index.
---

# Indexing

LynxDB provides full-text search through two index structures embedded in every segment: bloom filters for fast segment-level skipping, and FST-based inverted indexes with roaring bitmap posting lists for row-level precision. Together, they enable sub-millisecond search across millions of events without brute-force scanning.

## Index Architecture

```
Query: search "connection refused"
                    │
                    ▼
    ┌───────────────────────────────┐
    │      Bloom Filter Check       │
    │  (per-segment, ~100ns)        │
    │                               │
    │  seg_001: "connection" → YES  │
    │  seg_002: "connection" → NO   │  ← skip entirely
    │  seg_003: "connection" → YES  │
    └───────────────┬───────────────┘
                    │ (segments that pass)
                    ▼
    ┌───────────────────────────────┐
    │    Inverted Index Lookup      │
    │  (per-segment, ~1-10us)       │
    │                               │
    │  FST: "connection" → offset   │
    │  FST: "refused" → offset      │
    │                               │
    │  Roaring bitmap intersection: │
    │  events(connection) ∩         │
    │  events(refused)              │
    │  = {12, 847, 1203, 5891}     │
    └───────────────┬───────────────┘
                    │ (matching event IDs)
                    ▼
    ┌───────────────────────────────┐
    │      Column Data Read         │
    │  (only matching rows)         │
    │                               │
    │  Read _raw[12], _raw[847],   │
    │  _raw[1203], _raw[5891]      │
    └───────────────────────────────┘
```

## Bloom Filters

Every V2 segment contains a bloom filter that records all unique terms present in the segment. Bloom filters are probabilistic: they can tell you "definitely not present" (no false negatives) or "possibly present" (with a configurable false positive rate).

### Construction

During segment flush, the segment writer:

1. Tokenizes the `_raw` column of every event (splitting on whitespace, punctuation, and common delimiters).
2. Inserts each unique token into a bloom filter sized for the expected number of unique terms.
3. Serializes the bloom filter to the segment file between the column data and the inverted index.

The bloom filter is implemented using `github.com/bits-and-blooms/bloom/v3`.

### Query-Time Usage

When the query optimizer detects search terms in the query (e.g., `search "connection refused"` or `WHERE _raw LIKE "%timeout%"`), it extracts the literal strings and checks them against each segment's bloom filter before scanning:

```
for each segment:
    for each search term:
        if bloom_filter.Test(term) == false:
            skip this segment entirely
            break
    if not skipped:
        scan this segment
```

For selective queries where the search term appears in a small fraction of segments, bloom filter skipping eliminates 80-95% of segment reads. This is the single biggest optimization for full-text search.

### Caching

The bloom filter is loaded once when the segment is opened and cached on the `segmentHandle` struct. It is never re-read from disk. The in-memory size of a bloom filter is typically 1-10 KB per segment, so caching all bloom filters for thousands of segments costs only a few MB.

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| False positive rate | 1% | Higher rate = smaller filter, more false scans |
| Bit array size | Auto-scaled | Based on estimated unique terms per segment |
| Hash function count | Optimal | Calculated from bit array size and expected terms |

## FST-Based Inverted Index

The inverted index maps terms to the specific event indices within a segment that contain each term. It uses a Finite State Transducer (FST) as the term dictionary and roaring bitmaps as posting lists.

### Finite State Transducer (FST)

The FST is a compact automaton that maps byte strings (terms) to integer values (offsets into the posting list section). Built with `github.com/blevesearch/vellum`, it provides:

- **Exact lookup**: Given a term, retrieve its posting list offset in O(term length) time.
- **Prefix iteration**: Iterate all terms matching a prefix (e.g., all terms starting with `error`).
- **Fuzzy matching**: Find terms within a given edit distance (for typo-tolerant search).
- **Compression**: An FST shares common prefixes and suffixes between terms. For log data with many repeated field names and values, an FST is 5-10x smaller than a hash map.

### Roaring Bitmap Posting Lists

Each term in the FST points to a roaring bitmap that records which event indices (row numbers within the segment) contain that term. Roaring bitmaps (`github.com/RoaringBitmap/roaring`) are compressed sorted integer sets optimized for set operations:

- **Containers**: Values are split into 65536-element containers. Each container uses the best representation for its density:
  - **Array container**: For sparse sets (< 4096 elements), a sorted array of uint16.
  - **Bitmap container**: For dense sets, a 8 KB bitmap.
  - **Run container**: For contiguous ranges, a run-length encoding.
- **Set operations**: AND (intersection), OR (union), and NOT (complement) are implemented with container-level specialization and operate at memory bandwidth speed.
- **Compression**: Typically 10-50x smaller than an uncompressed bitmap for log data posting lists.

### Boolean Query Resolution

For multi-term queries, the inverted index resolves boolean operations using bitmap algebra:

```
Query: search "connection" AND "refused"

1. FST lookup "connection" → posting list A = {0, 5, 12, 33, 847, 1203, 5891}
2. FST lookup "refused"    → posting list B = {12, 847, 1203, 5891, 7002}
3. A ∩ B (AND)             → result = {12, 847, 1203, 5891}

Query: search "error" OR "warning"

1. FST lookup "error"   → posting list A = {1, 2, 5, 9, 12}
2. FST lookup "warning" → posting list B = {3, 7, 14, 20}
3. A ∪ B (OR)           → result = {1, 2, 3, 5, 7, 9, 12, 14, 20}

Query: search "error" AND NOT "timeout"

1. FST lookup "error"   → posting list A = {1, 2, 5, 9, 12}
2. FST lookup "timeout" → posting list B = {2, 9}
3. A \ B (AND NOT)      → result = {1, 5, 12}
```

Roaring bitmap operations are extremely fast -- intersecting two posting lists with millions of entries takes microseconds.

### Index Construction

During segment flush:

1. **Tokenization**: Each event's `_raw` field is tokenized into terms. The tokenizer splits on whitespace and punctuation, lowercases terms, and deduplicates.
2. **Dictionary building**: Unique terms are collected and sorted lexicographically.
3. **Posting list construction**: For each term, a roaring bitmap is built recording the event indices where the term appears.
4. **FST construction**: The sorted terms are inserted into an FST builder, which produces a compact automaton.
5. **Serialization**: The FST bytes and the serialized posting lists are written to the segment file.

### Index Size

The inverted index typically adds 10-20% to the segment file size. For a 500 MB segment with 1M events:

| Component | Typical Size |
|-----------|-------------|
| FST | 2-5 MB |
| Posting lists (roaring bitmaps) | 10-50 MB |
| Bloom filter | 100 KB - 1 MB |
| **Total index overhead** | **~15% of segment size** |

The overhead is well worth it: indexed full-text search can be 100-1000x faster than brute-force scanning for selective queries.

## How the Optimizer Uses Indexes

The 23-rule query optimizer integrates index usage across multiple rules:

### 1. Time Range Pruning

The segment header contains `min_timestamp` and `max_timestamp`. Queries with time bounds skip segments entirely:

```spl
level=error | where _time >= "2026-01-15" AND _time < "2026-01-16"
```

Any segment whose `max_timestamp < 2026-01-15` or `min_timestamp >= 2026-01-16` is pruned.

### 2. Bloom Filter Pruning

Literal string terms extracted from the query are tested against segment bloom filters:

```spl
search "connection refused"
```

Terms `"connection"` and `"refused"` are tested. Segments where either bloom filter test returns false are skipped.

### 3. Inverted Index Scan

For segments that pass bloom filter checks, the inverted index resolves the exact matching events:

```spl
search "connection refused" | where level="error"
```

The inverted index resolves `"connection" AND "refused"` to a set of event IDs. The `WHERE level="error"` filter is then applied only to those events.

### 4. Regex Literal Extraction

When a query uses `REX` or `MATCH` with a regex pattern, the optimizer extracts any literal prefix or substring:

```spl
| rex field=_raw "host=(?P<host>\S+)"
```

The literal `"host="` is extracted and used for bloom filter pruning, even though the full pattern is a regex.

### 5. Combined Pruning

All pruning strategies stack. A typical full-text search query benefits from all of them:

```
Segments on disk:              1,000
After time range pruning:        200  (80% pruned)
After bloom filter pruning:       15  (92.5% of remaining pruned)
After inverted index:            ~50  events read (from 15 segments)
```

## Comparison with Other Approaches

| Feature | LynxDB (FST + Roaring) | Lucene (Elasticsearch) | tsidx (Splunk) | Loki (No Index) |
|---------|------------------------|------------------------|----------------|-----------------|
| Term lookup | O(term length) | O(term length) | O(1) hash | N/A (grep) |
| Prefix search | Native FST iteration | Automaton intersection | Not supported | N/A |
| Fuzzy search | Levenshtein automaton | Levenshtein automaton | Not supported | N/A |
| Posting list compression | Roaring bitmaps | Frame of Reference + bitmaps | Compressed bitmaps | N/A |
| Memory footprint | Mmap (OS managed) | Heap (JVM managed) | Mmap | None |
| Index build speed | Single-pass | Multi-pass merge | Single-pass | None |

## Related

- [Segment Format](/docs/architecture/segment-format) -- where bloom filters and inverted indexes live in the `.lsg` file
- [Query Engine](/docs/architecture/query-engine) -- how the optimizer integrates index lookups
- [Storage Engine](/docs/architecture/storage-engine) -- when indexes are built (during segment flush)
- [Design Decisions](/docs/architecture/design-decisions) -- why FST + roaring instead of Lucene

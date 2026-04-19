---
sidebar_position: 3
title: Segment Format
description: The columnar .lsg V2 segment format -- binary layout, per-column encoding strategies (delta-varint, dictionary, Gorilla, LZ4), bloom filters, and inverted indexes.
---

# Segment Format (`.lsg` V2)

LynxDB stores log data in columnar segment files with the `.lsg` extension. Each segment is a self-contained, immutable file that holds events for a contiguous time range. The V2 format adds bloom filters and an FST-based inverted index to the V1 baseline.

## Format Versions

| Version | Contents | Writer | Reader |
|---------|----------|--------|--------|
| **V1** | Header + columns + footer | Legacy (no longer produced) | Supported (read-only) |
| **V2** | Header + columns + bloom filter + inverted index + footer | Current | Supported |

The segment writer always produces V2 files. The reader accepts both V1 and V2, detecting the version from the footer. The `decodeFooter` function returns `(*Footer, uint16, error)` where the `uint16` is the format version.

## Binary Layout

```
┌─────────────────────────────────────────┐  offset 0
│                 Header                   │
│  magic: "LSG\x00"  (4 bytes)           │
│  version: uint16                         │
│  event_count: uint64                     │
│  min_timestamp: int64 (unix nano)        │
│  max_timestamp: int64 (unix nano)        │
│  column_count: uint16                    │
│  column_descriptors[]:                   │
│    name: length-prefixed string          │
│    type: uint8 (string/int/float/bool)   │
│    encoding: uint8                       │
│    offset: uint64                        │
│    size: uint64                          │
├─────────────────────────────────────────┤
│             Column Data                  │
│                                          │
│  ┌─────────────────────────────────┐     │
│  │  _timestamp column              │     │
│  │  encoding: delta-varint         │     │
│  └─────────────────────────────────┘     │
│  ┌─────────────────────────────────┐     │
│  │  _raw column                    │     │
│  │  encoding: LZ4 compressed       │     │
│  └─────────────────────────────────┘     │
│  ┌─────────────────────────────────┐     │
│  │  string field columns           │     │
│  │  encoding: dictionary           │     │
│  └─────────────────────────────────┘     │
│  ┌─────────────────────────────────┐     │
│  │  numeric field columns          │     │
│  │  encoding: Gorilla / varint     │     │
│  └─────────────────────────────────┘     │
├─────────────────────────────────────────┤
│           Bloom Filter (V2)              │
│  serialized bloom filter (msgpack)       │
├─────────────────────────────────────────┤
│        Inverted Index (V2)               │
│  FST (vellum) + roaring bitmap postings  │
├─────────────────────────────────────────┤
│                Footer                    │
│  column_data_offset: uint64              │
│  bloom_filter_offset: uint64 (V2)        │
│  inverted_index_offset: uint64 (V2)      │
│  footer_offset: uint64                   │
│  checksum: uint32 (CRC32)               │
│  version: uint16                         │
│  magic: "LSG\x00"                       │
└─────────────────────────────────────────┘
```

The reader locates the footer by reading the last N bytes of the file (fixed footer size), validates the magic and checksum, then uses the offsets to jump directly to any section.

## Column Encodings

LynxDB uses different encoding strategies per column type. The goal is to minimize on-disk size while supporting fast columnar scans.

### Delta-Varint (Timestamps)

The `_timestamp` column stores nanosecond Unix timestamps. Since log timestamps are monotonically increasing with small deltas, delta encoding is highly effective:

```
Raw:     1709251200000000000, 1709251200001000000, 1709251200003000000
Deltas:  1709251200000000000, 1000000, 2000000
Varint:  [10 bytes],          [3 bytes], [3 bytes]
```

1. Store the first timestamp as-is.
2. Store subsequent values as deltas from the previous value.
3. Encode each delta using variable-length integer encoding (smaller values use fewer bytes).

For typical log data with millisecond-resolution timestamps arriving in order, deltas compress to 1-3 bytes each, achieving 5-10x compression over raw int64.

### Dictionary Encoding (String Fields)

Low-cardinality string fields (like `level`, `source`, `host`) use dictionary encoding:

```
Dictionary:  {0: "INFO", 1: "ERROR", 2: "WARN"}
Values:      [0, 0, 1, 0, 2, 0, 0, 1, ...]
```

1. Build a dictionary of unique values seen in the column.
2. Replace each string value with its dictionary index (a small integer).
3. Encode the index array with bit-packing (if the dictionary has 4 entries, each index needs only 2 bits).

For a column like `level` with 3 unique values across 100K events, this reduces the column from ~500 KB of strings to ~25 KB of 2-bit indexes plus a ~20-byte dictionary.

### LZ4 Compression (Raw Text)

The `_raw` column stores the original log line as-is. Since raw log text has high entropy but significant local repetition (repeated field names, common phrases), LZ4 block compression provides a good balance of compression ratio and decompression speed:

- **Compression ratio**: Typically 3-5x on log data.
- **Decompression speed**: ~4 GB/s. The decompression overhead is negligible compared to I/O.

LZ4 was chosen over zstd for its decompression speed. Query latency is more important than on-disk size for a log analytics workload.

### Gorilla Encoding (Float Fields)

Float columns (like `duration_ms`, `response_time`) use Gorilla encoding, originally designed for time-series data at Facebook:

1. Store the first value as a raw IEEE 754 double (8 bytes).
2. For subsequent values, XOR with the previous value.
3. If the XOR is zero (same value), store a single `0` bit.
4. If the XOR is non-zero, store the number of leading zeros, trailing zeros, and the meaningful bits.

For float fields with low variance (common in metrics), Gorilla encoding achieves 10-15x compression. For high-variance data, it degrades gracefully to approximately the raw size.

### Varint (Integer Fields)

Integer fields (like `status`, `bytes`) use variable-length integer encoding:

- Small values (0-127) use 1 byte.
- Medium values use 2-3 bytes.
- Large values use up to 10 bytes.

For HTTP status codes (200, 301, 404, 500), every value fits in 2 bytes. Combined with delta encoding when values are sorted, compression ratios of 3-5x are typical.

## Bloom Filter

Each V2 segment contains a bloom filter that indexes all terms present in the segment. The bloom filter is a probabilistic data structure that can answer "is this term possibly in this segment?" with no false negatives.

### How It Works at Query Time

When a query includes a search term (e.g., `search "connection refused"`):

1. For each segment, check the bloom filter for the search terms.
2. If the bloom filter says "definitely not present," skip the entire segment.
3. If the bloom filter says "possibly present," scan the segment.

For selective searches where the term appears in only a few segments, bloom filter skipping eliminates 90%+ of segment reads.

### Parameters

- **Bit array size**: Scaled to the number of unique terms in the segment.
- **Hash functions**: Optimal count based on expected terms and desired false positive rate.
- **Target false positive rate**: 1% (configurable).
- **Serialization**: msgpack via `github.com/bits-and-blooms/bloom/v3`.

The bloom filter is cached on the `segmentHandle` struct, so it is loaded once on segment open and never re-read.

## Inverted Index

The V2 inverted index provides exact and prefix-based term lookups with document-level posting lists.

### Structure

```
┌──────────────────┐     ┌─────────────────────┐
│   FST (vellum)   │────→│  Roaring Bitmap      │
│                  │     │  Posting Lists       │
│  "connection" →  │     │                     │
│    offset: 4096  │     │  term "connection":  │
│  "error" →       │     │    events: {0, 5,    │
│    offset: 8192  │     │     12, 847, 1203}   │
│  "refused" →     │     │                     │
│    offset: 12288 │     │  term "error":       │
│                  │     │    events: {1, 2,    │
│                  │     │     5, 9, 12, ...}   │
└──────────────────┘     └─────────────────────┘
```

- **FST (Finite State Transducer)**: Maps terms to offsets in the posting list section. Built with `github.com/blevesearch/vellum`. Supports exact lookup, prefix iteration, and fuzzy matching. Compressed representation -- significantly smaller than a hash map.
- **Roaring bitmaps**: Each term's posting list is a roaring bitmap (`github.com/RoaringBitmap/roaring`) recording which event indices within the segment contain that term. Roaring bitmaps are compressed sorted integer sets that support fast intersection, union, and iteration.

### Query Integration

When the query optimizer detects a full-text search predicate, it:

1. Looks up each search term in the FST.
2. Loads the corresponding roaring bitmap posting lists.
3. Intersects the bitmaps (for AND queries) or unions them (for OR queries).
4. Passes the resulting event index set to the scan operator, which reads only the matching rows from the columnar data.

This avoids scanning the `_raw` column entirely for selective queries. See [Indexing](/docs/architecture/indexing) for the full search pipeline.

## Memory-Mapped I/O

Segments are accessed via `mmap` (`github.com/blevesearch/mmap-go`). The OS virtual memory system handles paging segment data in and out of memory:

- **Cold segments** that have not been queried recently are paged out by the OS, consuming no physical memory.
- **Hot segments** that are frequently queried stay in the page cache, providing near-memory-speed access.
- **No manual cache management** is needed for the segment data itself (the OS does it). LynxDB's segment cache is only for warm-tier segments fetched from S3.

The `MmapSegment` type wraps the mmap handle and provides methods to read column data at specific offsets without copying.

## Segment Lifecycle

```
1. Memtable flush       →  New L0 segment written
2. Compaction (L0→L1)   →  Multiple L0 segments merged into L1
3. Compaction (L1→L2)   →  Multiple L1 segments merged into L2 (~1 GB target)
4. Tiering (warm)       →  L2 segment uploaded to S3, local copy evicted
5. Tiering (cold)       →  S3 segment moved to Glacier storage class
6. Retention            →  Segment deleted when it exceeds the retention period
```

At every stage, the segment is immutable -- compaction and tiering create new segments and delete old ones, they never modify a segment in place.

## Related

- [Storage Engine](/docs/architecture/storage-engine) -- async buffering, part flush, compaction overview
- [Indexing](/docs/architecture/indexing) -- how bloom filters and inverted indexes are used during queries
- [Query Engine](/docs/architecture/query-engine) -- how the scan operator reads columnar segments
- [Design Decisions](/docs/architecture/design-decisions) -- why columnar format, why these encodings

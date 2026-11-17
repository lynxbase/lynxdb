---
sidebar_position: 3
title: Segment Format
description: The columnar .lsg segment format, its format majors 1 and 2, and the data-directory FORMAT marker.
---

# Segment Format (`.lsg`)

LynxDB stores immutable columnar segments in the `.lsg` format. Each file starts with a magic that names its format major: `LSG1` for format 1, `LSG2` for format 2. LynxDB reads both and writes format 2.

## Layout

| Region | Magic | Purpose |
|---|---|---|
| Header | `LSG1` / `LSG2` | 24-byte file header with the format major and capability summaries |
| Column chunks | none | Encoded column data per row group, referenced from the footer |
| Bloom region | `LSBL` | Per-row-group, per-column bloom filters |
| Range section | none | Format 2: per-row-group range bitmaps (Range BSI) for selected numeric columns |
| Inverted index | `LSIX` | FST term dictionary plus roaring bitmap postings over `_raw` tokens |
| Primary index | `LSPK` | Optional sparse sort-key index, written when the writer is given a sort key |
| Footer | `LSGE` | Event count, row-group metadata, column catalog, region offsets |

The footer trailer is 12 bytes: footer payload length, capability summary, and a CRC32-IEEE over the footer payload plus the trailer fields before the CRC.

Row groups hold up to `storage.row_group_size` rows (default 65536). For each row group the footer records per-column statistics (min, max, count, null count), constant columns, and the location of its bloom section. Format 2 adds the location of each row group's range section and an index profile byte per catalog column.

## Versioning

The data-directory root contains a `FORMAT` marker:

```text
LSGFMT v2
```

On startup, LynxDB validates the marker and every segment header before loading segments:

- A data directory without segments and without a marker gets a marker for the newest format the binary supports.
- Segments without a marker, a marker newer than the binary supports, an unknown format major, an unknown required capability bit, or invalid magic bytes refuse startup.
- Physical corruption such as a footer checksum failure is a per-file warning, so the rest of the directory still loads.

A data directory written with format 1 keeps its `LSGFMT v1` marker; new parts and compaction outputs are format 2, and compaction reads both majors.

## Capabilities

| Bit | Name | Class | Meaning |
|---|---|---|---|
| 0 | `ColumnZSTD` | required | At least one column chunk uses ZSTD layer-2 compression |
| 1 | `RangeBSI` | optional | The segment carries range bitmaps |

A reader must understand every required bit set in a segment; optional bits can be ignored.

See `docs/storage-format.md` in the repository for the compatibility table and the magic and capability registries.

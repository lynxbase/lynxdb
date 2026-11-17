# LSG Storage Format

## 1. Compatibility Table

| LynxDB binary | Reads majors | Writes major | Marker accepts | Notes |
|---|---|---|---|---|
| v0.1.8 and later | 1, 2 | 2 | 1, 2 | A fresh data directory gets `LSGFMT v2`; an existing `LSGFMT v1` marker is kept |
| v0.1.6, v0.1.7 | 1 | 1 | 1 | |
| before v0.1.6 | older layouts | older layouts | none | No `FORMAT` marker; not readable by later releases |

## 2. Magic Bytes Registry

| Magic | Role |
|---|---|
| `LSG1` | Segment file magic for format-major 1 |
| `LSG2` | Segment file magic for format-major 2 |
| `LSGE` | Footer payload magic |
| `LSIX` | Inverted-index region magic |
| `LSPK` | Sparse primary-index region magic |
| `LSBL` | Per-row-group bloom region magic |

## 3. Major Version Table

| Major | Features |
|---|---|
| 1 | 24-byte `LSG1` header, column chunks, const columns, per-row-group `LSBL` blooms, `LSIX` inverted index, optional `LSPK` primary index, `LSGE` footer, 12-byte trailer |
| 2 | Major 1 layout with an `LSG2` header, plus per-row-group range sections (Range BSI) referenced from the footer and an index-profile byte per catalog column |

## 4. Capability Bits Table

| Bit | Name | Class | Description |
|---|---|---|---|
| 0 | `ColumnZSTD` | required | A column chunk uses ZSTD layer-2 compression. |
| 1 | `RangeBSI` | optional | The segment carries range bitmap sections. |

## 5. Upgrade

Upgrading needs no format step: replace the binary and restart. A binary that reads the data directory's marker major and every segment major starts normally; new parts and compaction outputs use the binary's write major. A binary refuses to start when the directory holds segments without a marker, when the marker is above `LSG_BINARY_MAX_MAJOR` or below `LSG_BINARY_MIN_MAJOR`, or when a segment has an unknown major or required capability bit.

## 6. Downgrade Runbook

See [format-downgrade-runbook.md](operator/format-downgrade-runbook.md).

## 7. Deprecation Policy

A LynxDB binary at format-major `N` reads `{N-2, N-1, N}` and writes `N`. Major bumps target at most one per 12 calendar months. A binary refuses to start when a data directory marker is below `LSG_BINARY_MIN_MAJOR` or above `LSG_BINARY_MAX_MAJOR`.

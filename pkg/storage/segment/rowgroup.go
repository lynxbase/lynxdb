package segment

import "encoding/binary"

// DefaultRowGroupSize is the number of rows per row group.
// 8192 rows provides fine-grained bloom filter pruning and effective
// ConstColumn detection. Matches part.DefaultRowGroupSize.
const DefaultRowGroupSize = 8192

// PrimaryIndexInterval is the number of rows between sparse primary index entries.
const PrimaryIndexInterval = 8192

// CompressionType identifies the block-level compression (layer 2).
type CompressionType uint8

const (
	CompressionNone CompressionType = 0
	CompressionLZ4  CompressionType = 1
	CompressionZSTD CompressionType = 2
)

// ColumnChunkMeta describes a single column chunk within a row group.
type ColumnChunkMeta struct {
	Name         string
	EncodingType uint8           // layer 1 encoding (delta, dict, gorilla, lz4)
	Compression  CompressionType // layer 2 block compression
	Offset       int64           // byte offset from file start
	Length       int64           // compressed size on disk
	RawSize      int64           // uncompressed size (after layer 1, before layer 2)
	CRC32        uint32          // CRC32 of on-disk data (layer 2 output)
	MinValue     string          // zone map: min value (string repr)
	MaxValue     string          // zone map: max value (string repr)
	Count        int64           // number of values
	NullCount    int64           // number of null values
}

// ConstColumnEntry describes a column whose value is identical across all rows
// in a row group. Stored once in the footer instead of writing a full column
// data chunk — enables O(1) match/skip at the row-group level.
type ConstColumnEntry struct {
	Name         string
	EncodingType uint8  // column.EncodingDict8, etc. — for type inference
	Value        string // the constant value (string representation)
}

// RowGroupMeta describes a single row group in the segment footer.
type RowGroupMeta struct {
	RowCount             int
	Columns              []ColumnChunkMeta
	ColumnPresenceBits   uint64             // bitmap: bit i = column i in catalog is present (chunk or const)
	ConstColumns         []ConstColumnEntry // columns with identical value across all rows in this RG
	PerColumnBloomOffset int64              // byte offset of this row group's per-column bloom section
	PerColumnBloomLength int64              // byte length of this row group's per-column bloom section
}

// CatalogEntry describes a column in the column catalog.
type CatalogEntry struct {
	Name         string
	DominantType uint8 // encoding type that dominates across row groups
}

// PrimaryIndexEntry is a single sample in the sparse primary index.
type PrimaryIndexEntry struct {
	RowOffset     uint32   // absolute row offset in the segment
	SortKeyValues []string // sort key field values at this row
}

// PrimaryIndex is a sparse index over sort key values, sampled every PrimaryIndexInterval rows.
type PrimaryIndex struct {
	Interval   int                 // sampling interval (e.g. 8192)
	SortFields []string            // sort key field names
	Entries    []PrimaryIndexEntry // sampled entries
}

// EncodePrimaryIndex serializes a PrimaryIndex to binary.
// Format:
//
//	uint32 interval
//	uint16 fieldCount
//	  for each field: uint16 nameLen + name bytes
//	uint32 entryCount
//	  for each entry:
//	    uint32 rowOffset
//	    for each field: uint16 valueLen + value bytes
func EncodePrimaryIndex(idx *PrimaryIndex) []byte {
	buf := make([]byte, 0, 256)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(idx.Interval))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(idx.SortFields)))
	for _, name := range idx.SortFields {
		b := []byte(name)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(b)))
		buf = append(buf, b...)
	}

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(idx.Entries)))
	for _, entry := range idx.Entries {
		buf = binary.LittleEndian.AppendUint32(buf, entry.RowOffset)
		for _, val := range entry.SortKeyValues {
			b := []byte(val)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(b)))
			buf = append(buf, b...)
		}
	}

	return buf
}

// DecodePrimaryIndex deserializes a PrimaryIndex from binary data.
func DecodePrimaryIndex(data []byte) (*PrimaryIndex, error) {
	pos := 0

	if pos+4 > len(data) {
		return nil, ErrCorruptSegment
	}
	interval := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	if pos+2 > len(data) {
		return nil, ErrCorruptSegment
	}
	fieldCount := binary.LittleEndian.Uint16(data[pos : pos+2])
	pos += 2

	fields := make([]string, fieldCount)
	for i := uint16(0); i < fieldCount; i++ {
		if pos+2 > len(data) {
			return nil, ErrCorruptSegment
		}
		nameLen := binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2
		if pos+int(nameLen) > len(data) {
			return nil, ErrCorruptSegment
		}
		fields[i] = string(data[pos : pos+int(nameLen)])
		pos += int(nameLen)
	}

	if pos+4 > len(data) {
		return nil, ErrCorruptSegment
	}
	entryCount := binary.LittleEndian.Uint32(data[pos : pos+4])
	pos += 4

	entries := make([]PrimaryIndexEntry, entryCount)
	for i := uint32(0); i < entryCount; i++ {
		if pos+4 > len(data) {
			return nil, ErrCorruptSegment
		}
		entries[i].RowOffset = binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4

		vals := make([]string, fieldCount)
		for f := uint16(0); f < fieldCount; f++ {
			if pos+2 > len(data) {
				return nil, ErrCorruptSegment
			}
			valLen := binary.LittleEndian.Uint16(data[pos : pos+2])
			pos += 2
			if pos+int(valLen) > len(data) {
				return nil, ErrCorruptSegment
			}
			vals[f] = string(data[pos : pos+int(valLen)])
			pos += int(valLen)
		}
		entries[i].SortKeyValues = vals
	}

	return &PrimaryIndex{
		Interval:   int(interval),
		SortFields: fields,
		Entries:    entries,
	}, nil
}

package segment

import (
	"encoding/binary"
	"hash/crc32"
)

// .lsg file format constants.
const (
	MagicBytes  = "LSEG"    // 4-byte magic at file start
	FooterMagic = "LSGF"    // 4-byte magic at footer start
	FormatV4    = uint16(4) // format version 4 (per-column blooms, const columns, presence bitmap)
	HeaderSize  = 16        // magic (4) + version (2) + flags (2) + rowGroupSize (4) + rowGroupCount (4)
)

// Footer holds the segment file footer data for V4.
type Footer struct {
	EventCount         int64
	RowGroups          []RowGroupMeta
	InvertedOffset     int64
	InvertedLength     int64
	PrimaryIndexOffset int64
	PrimaryIndexLength int64
	Catalog            []CatalogEntry
}

// ColumnStats holds aggregated column statistics across all row groups.
type ColumnStats struct {
	Name      string
	MinValue  string
	MaxValue  string
	Count     int64
	NullCount int64
}

// Stats returns aggregated column stats across all row groups.
// Complexity: O(row_group_count * columns_per_group) with map lookups.
// This is cold-path only (metadata/status endpoints), not query hot-path.
// If it becomes a bottleneck, pre-compute during segment write and store in footer.
func (f *Footer) Stats() []ColumnStats {
	if len(f.RowGroups) == 0 {
		return nil
	}
	// Aggregate stats across row groups (chunks + const columns).
	statMap := make(map[string]*ColumnStats)
	for _, rg := range f.RowGroups {
		for _, c := range rg.Columns {
			s, ok := statMap[c.Name]
			if !ok {
				s = &ColumnStats{
					Name:     c.Name,
					MinValue: c.MinValue,
					MaxValue: c.MaxValue,
				}
				statMap[c.Name] = s
			} else {
				if c.MinValue < s.MinValue {
					s.MinValue = c.MinValue
				}
				if c.MaxValue > s.MaxValue {
					s.MaxValue = c.MaxValue
				}
			}
			s.Count += c.Count
			s.NullCount += c.NullCount
		}
		// Include const columns in stats with their single value as min=max.
		for _, cc := range rg.ConstColumns {
			s, ok := statMap[cc.Name]
			if !ok {
				s = &ColumnStats{
					Name:     cc.Name,
					MinValue: cc.Value,
					MaxValue: cc.Value,
				}
				statMap[cc.Name] = s
			} else {
				if cc.Value < s.MinValue {
					s.MinValue = cc.Value
				}
				if cc.Value > s.MaxValue {
					s.MaxValue = cc.Value
				}
			}
			s.Count += int64(rg.RowCount)
		}
	}

	result := make([]ColumnStats, 0, len(statMap))
	// Use first row group column order for deterministic output.
	for _, c := range f.RowGroups[0].Columns {
		if s, ok := statMap[c.Name]; ok {
			result = append(result, *s)
			delete(statMap, c.Name)
		}
	}
	// Append const columns that weren't in Columns.
	for _, cc := range f.RowGroups[0].ConstColumns {
		if s, ok := statMap[cc.Name]; ok {
			result = append(result, *s)
			delete(statMap, cc.Name)
		}
	}

	return result
}

// encodeFooter serializes a V4 footer to binary.
//
// V4 footer layout:
//
//	"LSGF" magic (4B)
//	uint64 eventCount
//	uint32 rgCount
//	For each RG:
//	  uint32 rowCount
//	  uint64 columnPresenceBits
//	  uint16 constColumnCount
//	  For each const column:
//	    uint16 nameLen + name bytes
//	    uint8  encodingType
//	    uint16 valueLen + value bytes
//	  uint32 columnCount
//	  For each column chunk:
//	    (name, encoding, compression, offset, length, rawSize, CRC32, min, max, count, nullCount)
//	  uint64 perColumnBloomOffset
//	  uint64 perColumnBloomLength
//	uint32 catalogCount
//	For each catalog entry:
//	  uint16 nameLen + name bytes
//	  uint8  dominantType
//	uint64 invertedOffset
//	uint64 invertedLength
//	uint64 primaryIndexOffset
//	uint64 primaryIndexLength
func encodeFooter(f *Footer) []byte {
	buf := make([]byte, 0, 4096)

	// Footer magic.
	buf = append(buf, FooterMagic...)

	// Event count.
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.EventCount))

	// Row group count.
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(f.RowGroups)))

	// Row group metadata.
	for _, rg := range f.RowGroups {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(rg.RowCount))

		// Column presence bitmap.
		buf = binary.LittleEndian.AppendUint64(buf, rg.ColumnPresenceBits)

		// Const columns.
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(rg.ConstColumns)))
		for _, cc := range rg.ConstColumns {
			nameBytes := []byte(cc.Name)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(nameBytes)))
			buf = append(buf, nameBytes...)
			buf = append(buf, cc.EncodingType)
			valBytes := []byte(cc.Value)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(valBytes)))
			buf = append(buf, valBytes...)
		}

		// Column chunks.
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(rg.Columns)))

		for _, c := range rg.Columns {
			nameBytes := []byte(c.Name)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(nameBytes)))
			buf = append(buf, nameBytes...)
			buf = append(buf, c.EncodingType, byte(c.Compression))
			buf = binary.LittleEndian.AppendUint64(buf, uint64(c.Offset))
			buf = binary.LittleEndian.AppendUint64(buf, uint64(c.Length))
			buf = binary.LittleEndian.AppendUint64(buf, uint64(c.RawSize))
			buf = binary.LittleEndian.AppendUint32(buf, c.CRC32)

			minBytes := []byte(c.MinValue)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(minBytes)))
			buf = append(buf, minBytes...)
			maxBytes := []byte(c.MaxValue)
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(maxBytes)))
			buf = append(buf, maxBytes...)
			buf = binary.LittleEndian.AppendUint64(buf, uint64(c.Count))
			buf = binary.LittleEndian.AppendUint64(buf, uint64(c.NullCount))
		}

		// Per-column bloom section location.
		buf = binary.LittleEndian.AppendUint64(buf, uint64(rg.PerColumnBloomOffset))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(rg.PerColumnBloomLength))
	}

	// Column catalog.
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(f.Catalog)))
	for _, cat := range f.Catalog {
		nameBytes := []byte(cat.Name)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(nameBytes)))
		buf = append(buf, nameBytes...)
		buf = append(buf, cat.DominantType)
	}

	// Inverted index offsets.
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.InvertedOffset))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.InvertedLength))

	// Primary index offsets.
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.PrimaryIndexOffset))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.PrimaryIndexLength))

	// Footer size (so reader can locate footer start).
	footerPayloadLen := uint32(len(buf))
	buf = binary.LittleEndian.AppendUint32(buf, footerPayloadLen)

	// CRC32 of everything above.
	crc := crc32.ChecksumIEEE(buf)
	buf = binary.LittleEndian.AppendUint32(buf, crc)

	return buf
}

// DecodeFooter parses a footer from the tail of a data buffer.
// Used by the lazy fetcher to parse footers from range-read data
// without opening a full segment.
func DecodeFooter(data []byte) (*Footer, error) {
	return decodeFooter(data)
}

func decodeFooter(data []byte) (*Footer, error) {
	if len(data) < HeaderSize+8 {
		return nil, ErrCorruptSegment
	}

	// Read footer size and CRC from the last 8 bytes.
	footerSize := binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4])
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])

	totalFooterLen := int(footerSize) + 8
	if totalFooterLen > len(data) {
		return nil, ErrCorruptSegment
	}

	footerStart := len(data) - totalFooterLen
	footerPayload := data[footerStart : len(data)-4]

	calcCRC := crc32.ChecksumIEEE(footerPayload)
	if calcCRC != storedCRC {
		return nil, ErrChecksumMismatch
	}

	payload := data[footerStart : footerStart+int(footerSize)]
	pos := 0

	// Magic.
	if pos+4 > len(payload) || string(payload[pos:pos+4]) != FooterMagic {
		return nil, ErrCorruptSegment
	}
	pos += 4

	f := &Footer{}

	// Event count.
	if pos+8 > len(payload) {
		return nil, ErrCorruptSegment
	}
	f.EventCount = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
	pos += 8

	// Row group count.
	if pos+4 > len(payload) {
		return nil, ErrCorruptSegment
	}
	rgCount := binary.LittleEndian.Uint32(payload[pos : pos+4])
	pos += 4

	f.RowGroups = make([]RowGroupMeta, rgCount)
	for rg := uint32(0); rg < rgCount; rg++ {
		// Row count.
		if pos+4 > len(payload) {
			return nil, ErrCorruptSegment
		}
		rowCount := binary.LittleEndian.Uint32(payload[pos : pos+4])
		pos += 4
		f.RowGroups[rg].RowCount = int(rowCount)

		// Column presence bitmap.
		if pos+8 > len(payload) {
			return nil, ErrCorruptSegment
		}
		f.RowGroups[rg].ColumnPresenceBits = binary.LittleEndian.Uint64(payload[pos : pos+8])
		pos += 8

		// Const columns.
		if pos+2 > len(payload) {
			return nil, ErrCorruptSegment
		}
		constCount := binary.LittleEndian.Uint16(payload[pos : pos+2])
		pos += 2

		if constCount > 0 {
			f.RowGroups[rg].ConstColumns = make([]ConstColumnEntry, constCount)
			for i := uint16(0); i < constCount; i++ {
				// Name.
				if pos+2 > len(payload) {
					return nil, ErrCorruptSegment
				}
				nameLen := binary.LittleEndian.Uint16(payload[pos : pos+2])
				pos += 2
				if pos+int(nameLen) > len(payload) {
					return nil, ErrCorruptSegment
				}
				f.RowGroups[rg].ConstColumns[i].Name = string(payload[pos : pos+int(nameLen)])
				pos += int(nameLen)

				// Encoding type.
				if pos >= len(payload) {
					return nil, ErrCorruptSegment
				}
				f.RowGroups[rg].ConstColumns[i].EncodingType = payload[pos]
				pos++

				// Value.
				if pos+2 > len(payload) {
					return nil, ErrCorruptSegment
				}
				valLen := binary.LittleEndian.Uint16(payload[pos : pos+2])
				pos += 2
				if pos+int(valLen) > len(payload) {
					return nil, ErrCorruptSegment
				}
				f.RowGroups[rg].ConstColumns[i].Value = string(payload[pos : pos+int(valLen)])
				pos += int(valLen)
			}
		}

		// Column chunks.
		if pos+4 > len(payload) {
			return nil, ErrCorruptSegment
		}
		colCount := binary.LittleEndian.Uint32(payload[pos : pos+4])
		pos += 4

		f.RowGroups[rg].Columns = make([]ColumnChunkMeta, colCount)

		for c := uint32(0); c < colCount; c++ {
			cc := &f.RowGroups[rg].Columns[c]

			// Name.
			if pos+2 > len(payload) {
				return nil, ErrCorruptSegment
			}
			nameLen := binary.LittleEndian.Uint16(payload[pos : pos+2])
			pos += 2
			if pos+int(nameLen) > len(payload) {
				return nil, ErrCorruptSegment
			}
			cc.Name = string(payload[pos : pos+int(nameLen)])
			pos += int(nameLen)

			// Encoding, compression, offset, length, rawSize, CRC.
			if pos+2+8+8+8+4 > len(payload) {
				return nil, ErrCorruptSegment
			}
			cc.EncodingType = payload[pos]
			pos++
			cc.Compression = CompressionType(payload[pos])
			pos++
			cc.Offset = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
			cc.Length = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
			cc.RawSize = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
			cc.CRC32 = binary.LittleEndian.Uint32(payload[pos : pos+4])
			pos += 4

			// Min.
			if pos+2 > len(payload) {
				return nil, ErrCorruptSegment
			}
			minLen := binary.LittleEndian.Uint16(payload[pos : pos+2])
			pos += 2
			if pos+int(minLen) > len(payload) {
				return nil, ErrCorruptSegment
			}
			cc.MinValue = string(payload[pos : pos+int(minLen)])
			pos += int(minLen)

			// Max.
			if pos+2 > len(payload) {
				return nil, ErrCorruptSegment
			}
			maxLen := binary.LittleEndian.Uint16(payload[pos : pos+2])
			pos += 2
			if pos+int(maxLen) > len(payload) {
				return nil, ErrCorruptSegment
			}
			cc.MaxValue = string(payload[pos : pos+int(maxLen)])
			pos += int(maxLen)

			// Count and NullCount.
			if pos+16 > len(payload) {
				return nil, ErrCorruptSegment
			}
			cc.Count = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
			cc.NullCount = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
		}

		// Per-column bloom section location.
		if pos+16 > len(payload) {
			return nil, ErrCorruptSegment
		}
		f.RowGroups[rg].PerColumnBloomOffset = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
		pos += 8
		f.RowGroups[rg].PerColumnBloomLength = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
		pos += 8
	}

	// Column catalog.
	if pos+4 > len(payload) {
		return nil, ErrCorruptSegment
	}
	catCount := binary.LittleEndian.Uint32(payload[pos : pos+4])
	pos += 4

	f.Catalog = make([]CatalogEntry, catCount)
	for i := uint32(0); i < catCount; i++ {
		if pos+2 > len(payload) {
			return nil, ErrCorruptSegment
		}
		nameLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		// Bounds check using int64 to prevent overflow on 32-bit architectures.
		if int64(pos)+int64(nameLen) > int64(len(payload)) {
			return nil, ErrCorruptSegment
		}
		f.Catalog[i].Name = string(payload[pos : pos+nameLen])
		pos += nameLen
		// Explicit bounds check for DominantType byte.
		if pos >= len(payload) {
			return nil, ErrCorruptSegment
		}
		f.Catalog[i].DominantType = payload[pos]
		pos++
	}

	// Inverted index offsets.
	if pos+16 > len(payload) {
		return nil, ErrCorruptSegment
	}
	f.InvertedOffset = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
	pos += 8
	f.InvertedLength = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
	pos += 8

	// Primary index offsets (optional; zero means no index).
	if pos+16 <= len(payload) {
		f.PrimaryIndexOffset = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
		pos += 8
		f.PrimaryIndexLength = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
	}

	return f, nil
}

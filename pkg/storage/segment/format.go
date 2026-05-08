package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/bits"
	"sync"
)

// Footer holds the segment file footer data for LSG major v1.
type Footer struct {
	EventCount         int64
	RequiredCaps       uint64
	OptionalCaps       uint64
	RowGroups          []RowGroupMeta
	InvertedOffset     int64
	InvertedLength     int64
	PrimaryIndexOffset int64
	PrimaryIndexLength int64
	Catalog            []CatalogEntry

	cachedStats []ColumnStats // lazy-computed by Stats()
	statsOnce   sync.Once
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
// The result is computed once and cached for subsequent calls.
func (f *Footer) Stats() []ColumnStats {
	f.statsOnce.Do(func() {
		f.cachedStats = f.computeStats()
	})
	return f.cachedStats
}

// computeStats aggregates column stats across all row groups.
func (f *Footer) computeStats() []ColumnStats {
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

func encodeFooter(f *Footer) []byte {
	return encodeFooterForMajor(f, defaultFormatMajor)
}

func encodeFooterForMajor(f *Footer, major uint16) []byte {
	switch major {
	case LSG_FORMAT_MAJOR_V1:
		return encodeFooterV1(f)
	case LSG_FORMAT_MAJOR_V2:
		return encodeFooterV2(f)
	default:
		return encodeFooterV2(f)
	}
}

func encodeFooterV1(f *Footer) []byte {
	return encodeFooterWithLayout(f, false)
}

func encodeFooterV2(f *Footer) []byte {
	return encodeFooterWithLayout(f, true)
}

func encodeFooterWithLayout(f *Footer, includeRange bool) []byte {
	buf := make([]byte, 0, 4096)

	buf = append(buf, LSG_FOOTER_MAGIC...)
	buf = append(buf, 0, 0, 0, 0)

	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.EventCount))
	buf = binary.LittleEndian.AppendUint64(buf, f.RequiredCaps)
	buf = binary.LittleEndian.AppendUint64(buf, f.OptionalCaps)

	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(f.RowGroups)))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(f.Catalog)))

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
		if includeRange {
			buf = binary.LittleEndian.AppendUint64(buf, uint64(rg.PerColumnRangeOffset))
			buf = binary.LittleEndian.AppendUint64(buf, uint64(rg.PerColumnRangeLength))
		}
		buf = binary.LittleEndian.AppendUint64(buf, rg.RequiredCapabilities)
	}

	for _, cat := range f.Catalog {
		nameBytes := []byte(cat.Name)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(nameBytes)))
		buf = append(buf, nameBytes...)
		buf = append(buf, cat.DominantType)
		if includeRange {
			buf = append(buf, byte(cat.IndexProfile))
		}
	}

	// Inverted index offsets.
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.InvertedOffset))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.InvertedLength))

	// Primary index offsets.
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.PrimaryIndexOffset))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(f.PrimaryIndexLength))

	footerPayloadLen := uint32(len(buf))
	buf = binary.LittleEndian.AppendUint32(buf, footerPayloadLen)
	buf = binary.LittleEndian.AppendUint32(buf, footerCapsSummary(f.RequiredCaps, f.OptionalCaps))

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
	if len(data) >= LSG_HEADER_SIZE {
		if major, ok := magicMajor(data[:4]); ok && binary.LittleEndian.Uint16(data[4:6]) == major {
			return decodeFooterForMajor(data, major)
		}
	}

	footer, err := decodeFooterForMajor(data, defaultFormatMajor)
	if err == nil {
		return footer, nil
	}
	if defaultFormatMajor != LSG_FORMAT_MAJOR_V1 {
		if footer, v1Err := decodeFooterForMajor(data, LSG_FORMAT_MAJOR_V1); v1Err == nil {
			return footer, nil
		}
	}
	return nil, err
}

func decodeFooterForMajor(data []byte, major uint16) (*Footer, error) {
	switch major {
	case LSG_FORMAT_MAJOR_V1:
		return decodeFooterV1(data)
	case LSG_FORMAT_MAJOR_V2:
		return decodeFooterV2(data)
	default:
		return nil, fmt.Errorf("%w: unsupported format major version %d (this binary supports %d..%d)",
			ErrUnsupportedMajor, major, LSG_BINARY_MIN_MAJOR, LSG_BINARY_MAX_MAJOR)
	}
}

func decodeFooterV1(data []byte) (*Footer, error) {
	return decodeFooterWithLayout(data, false)
}

func decodeFooterV2(data []byte) (*Footer, error) {
	return decodeFooterWithLayout(data, true)
}

func decodeFooterWithLayout(data []byte, includeRange bool) (*Footer, error) {
	if len(data) < LSG_FOOTER_TRAILER_SIZE {
		return nil, fmt.Errorf("%w: truncated trailer (file size %d, expected >= %d)", ErrCorruptSegment, len(data), LSG_FOOTER_TRAILER_SIZE)
	}

	trailerStart := len(data) - LSG_FOOTER_TRAILER_SIZE
	footerSize := binary.LittleEndian.Uint32(data[trailerStart : trailerStart+4])
	storedCapsSummary := binary.LittleEndian.Uint32(data[trailerStart+4 : trailerStart+8])
	storedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])

	totalFooterLen := int(footerSize) + LSG_FOOTER_TRAILER_SIZE
	if totalFooterLen > len(data) {
		return nil, fmt.Errorf("%w: truncated trailer (file size %d, expected >= %d)", ErrCorruptSegment, len(data), totalFooterLen)
	}

	footerStart := len(data) - totalFooterLen
	footerPayload := data[footerStart : len(data)-4]

	calcCRC := crc32.ChecksumIEEE(footerPayload)
	if calcCRC != storedCRC {
		return nil, ErrChecksumMismatch
	}

	payload := data[footerStart : footerStart+int(footerSize)]
	pos := 0

	if pos+8 > len(payload) || string(payload[pos:pos+4]) != LSG_FOOTER_MAGIC {
		return nil, ErrCorruptSegment
	}
	pos += 4
	if payload[pos] != 0 || payload[pos+1] != 0 || payload[pos+2] != 0 || payload[pos+3] != 0 {
		return nil, ErrCorruptSegment
	}
	pos += 4

	f := &Footer{}

	if pos+8 > len(payload) {
		return nil, ErrCorruptSegment
	}
	f.EventCount = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
	pos += 8
	if pos+16 > len(payload) {
		return nil, ErrCorruptSegment
	}
	f.RequiredCaps = binary.LittleEndian.Uint64(payload[pos : pos+8])
	pos += 8
	f.OptionalCaps = binary.LittleEndian.Uint64(payload[pos : pos+8])
	pos += 8
	if footerCapsSummary(f.RequiredCaps, f.OptionalCaps) != storedCapsSummary {
		return nil, ErrCorruptSegment
	}

	if pos+8 > len(payload) {
		return nil, ErrCorruptSegment
	}
	rgCount := binary.LittleEndian.Uint32(payload[pos : pos+4])
	pos += 4
	catCount := binary.LittleEndian.Uint32(payload[pos : pos+4])
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
		if includeRange {
			if pos+16 > len(payload) {
				return nil, ErrCorruptSegment
			}
			f.RowGroups[rg].PerColumnRangeOffset = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
			f.RowGroups[rg].PerColumnRangeLength = int64(binary.LittleEndian.Uint64(payload[pos : pos+8]))
			pos += 8
		}
		if pos+8 > len(payload) {
			return nil, ErrCorruptSegment
		}
		f.RowGroups[rg].RequiredCapabilities = binary.LittleEndian.Uint64(payload[pos : pos+8])
		pos += 8
	}

	f.Catalog = make([]CatalogEntry, catCount)
	for i := uint32(0); i < catCount; i++ {
		if pos+2 > len(payload) {
			return nil, ErrCorruptSegment
		}
		nameLen := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if nameLen > 1024 {
			return nil, ErrCorruptSegment
		}
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
		if includeRange {
			if pos >= len(payload) {
				return nil, ErrCorruptSegment
			}
			f.Catalog[i].IndexProfile = IndexProfile(payload[pos])
			pos++
		}
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
		pos += 8
	}
	if pos != len(payload) {
		return nil, ErrCorruptSegment
	}

	return f, nil
}

func footerCapsSummary(required, optional uint64) uint32 {
	return uint32(required) ^ uint32(optional>>32)
}

func aggregateCapabilities(rowGroups []RowGroupMeta) (uint64, uint64) {
	var required uint64
	var optional uint64
	for _, rg := range rowGroups {
		required |= rg.RequiredCapabilities
		if rg.PerColumnRangeLength > 0 {
			optional |= CapBit_RangeBSI
		}
	}
	return required, optional
}

func lowestSetBit(mask uint64) int {
	if mask == 0 {
		return -1
	}
	return bits.TrailingZeros64(mask)
}

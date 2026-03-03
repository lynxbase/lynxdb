package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"strconv"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment/column"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment/index"
)

// Pooled ZSTD encoder/decoder to avoid ~1ms + ~1MB allocation per call.
var zstdEncoderPool = sync.Pool{
	New: func() interface{} {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))

		return enc
	},
}

var zstdDecoderPool = sync.Pool{
	New: func() interface{} {
		dec, _ := zstd.NewReader(nil)

		return dec
	},
}

// countingWriter wraps an io.Writer and tracks total bytes written.
type countingWriter struct {
	w       io.Writer
	written int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.written += int64(n)

	return n, err
}

// Writer creates .lsg segment files from a batch of events.
type Writer struct {
	w           *countingWriter
	compression CompressionType // layer 2 compression (default: LZ4)
	sortKey     []string        // sort key fields for sparse primary index (MV segments)
	rgSize      int             // row group size override (0 = use DefaultRowGroupSize)
	maxColumns  int             // max user-defined columns per segment (0 = unlimited)
}

// SetSortKey configures the sort key fields used to build a sparse primary index.
// When set, the writer samples sort key values every PrimaryIndexInterval rows
// and stores the index in the segment file for efficient binary search.
func (sw *Writer) SetSortKey(fields []string) {
	sw.sortKey = fields
}

// SetRowGroupSize overrides the default row group size for this writer.
// Must be called before Write(). A value <= 0 reverts to DefaultRowGroupSize.
func (sw *Writer) SetRowGroupSize(size int) {
	sw.rgSize = size
}

// SetMaxColumns limits the number of user-defined columns written per segment.
// When the field count exceeds this limit, only the most frequent fields are
// kept as columns; the rest remain searchable via _raw full-text search.
// A value <= 0 disables the cap.
func (sw *Writer) SetMaxColumns(n int) {
	sw.maxColumns = n
}

// NewWriter creates a writer that outputs to w with default LZ4 layer 2 compression.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: &countingWriter{w: w}, compression: CompressionLZ4}
}

// NewWriterWithCompression creates a writer with a specific layer 2 compression.
func NewWriterWithCompression(w io.Writer, compression CompressionType) *Writer {
	return &Writer{w: &countingWriter{w: w}, compression: compression}
}

// builtinColumns is the ordered list of builtin column names.
// Used to compute catalog indices for presence bitmap.
var builtinColumns = []string{"_time", "_raw", "_source", "_sourcetype", "host", "index"}

// Write encodes the given events into .lsg V4 format and writes to the underlying writer.
// Events should be sorted by timestamp. Returns total bytes written.
func (sw *Writer) Write(events []*event.Event) (int64, error) {
	if len(events) == 0 {
		return 0, ErrNoEvents
	}

	// Determine row groups.
	rgSize := sw.rgSize
	if rgSize <= 0 {
		rgSize = DefaultRowGroupSize
	}
	rgCount := (len(events) + rgSize - 1) / rgSize

	// Write header (V4).
	header := makeHeader(rgSize, rgCount)
	if _, err := sw.w.Write(header); err != nil {
		return sw.w.written, fmt.Errorf("segment: write header: %w", err)
	}

	// Collect all field names across events with type promotion.
	fieldSet := make(map[string]event.FieldType)
	for _, e := range events {
		for _, name := range e.FieldNames() {
			v := e.GetField(name)
			if v.IsNull() {
				continue
			}
			newType := v.Type()
			if prev, exists := fieldSet[name]; exists {
				if (prev == event.FieldTypeInt && newType == event.FieldTypeFloat) ||
					(prev == event.FieldTypeFloat && newType == event.FieldTypeInt) {
					newType = event.FieldTypeFloat
				}
			}
			fieldSet[name] = newType
		}
	}

	fieldNames := make([]string, 0, len(fieldSet))
	for name := range fieldSet {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	// Cap user-defined columns when MaxColumns is set. Builtin columns
	// (_time, _raw, _source, _sourcetype, host, index) are always written
	// regardless of the cap. When the user field count exceeds the limit,
	// keep the top-N most frequent fields and drop the rest — they remain
	// searchable via _raw full-text search.
	if sw.maxColumns > 0 && len(fieldNames) > sw.maxColumns {
		fieldNames = topFieldsByFrequency(events, fieldNames, sw.maxColumns)
		// Rebuild fieldSet to match the capped field list.
		capped := make(map[string]event.FieldType, len(fieldNames))
		for _, name := range fieldNames {
			capped[name] = fieldSet[name]
		}
		fieldSet = capped
	}

	// Build column catalog early so we know bit positions for the presence bitmap.
	catalog := buildCatalog(fieldSet, fieldNames)
	catalogIndex := make(map[string]int, len(catalog))
	for i, cat := range catalog {
		catalogIndex[cat.Name] = i
	}

	var rowGroups []RowGroupMeta

	// Write each row group.
	for rg := 0; rg < rgCount; rg++ {
		start := rg * rgSize
		end := start + rgSize
		if end > len(events) {
			end = len(events)
		}
		rgEvents := events[start:end]

		rgMeta, err := sw.writeRowGroup(rgEvents, fieldSet, fieldNames, catalogIndex)
		if err != nil {
			return sw.w.written, fmt.Errorf("segment: row group %d: %w", rg, err)
		}
		rowGroups = append(rowGroups, rgMeta)
	}

	// Build per-column bloom filters.
	//
	// Which columns get blooms:
	//   - _raw: always (full-text search)
	//   - _source, _sourcetype, host, index: always (metadata filters)
	//   - User-defined string fields: yes
	//   - Numeric fields: no (zone maps suffice)
	//   - ConstColumns: no (value is known exactly, O(1) check)
	//
	// Per-column two-pass sizing:
	//   Within a single column, blooms across RGs are uniformly sized (same m/k)
	//   so Merge() works for the backward-compat BloomFilter() union method.
	//   Different columns can have different sizes.
	bloomColumnNames := collectBloomColumns(fieldSet, fieldNames)

	// Determine which columns are const in each RG (to skip blooms for them).
	constInRG := make([]map[string]bool, rgCount)
	for rgi := range rowGroups {
		constInRG[rgi] = make(map[string]bool, len(rowGroups[rgi].ConstColumns))
		for _, cc := range rowGroups[rgi].ConstColumns {
			constInRG[rgi][cc.Name] = true
		}
	}

	// Pass 1: count unique tokens per column per RG, track max per column.
	maxTokensPerCol := make(map[string]uint, len(bloomColumnNames))
	for rg := 0; rg < rgCount; rg++ {
		start := rg * rgSize
		end := start + rgSize
		if end > len(events) {
			end = len(events)
		}
		rgEvents := events[start:end]

		for _, colName := range bloomColumnNames {
			if constInRG[rg][colName] {
				continue
			}
			count := countColumnTokens(rgEvents, colName, fieldSet)
			if count > maxTokensPerCol[colName] {
				maxTokensPerCol[colName] = count
			}
		}
	}
	for col, max := range maxTokensPerCol {
		if max < 100 {
			maxTokensPerCol[col] = 100
		}
	}

	// Pass 2: build per-column blooms, write per-RG bloom sections.
	inv := index.NewInvertedIndex()
	for rg := 0; rg < rgCount; rg++ {
		start := rg * rgSize
		end := start + rgSize
		if end > len(events) {
			end = len(events)
		}
		rgEvents := events[start:end]

		bloomSectionOffset := sw.w.written

		// Build bloom for each column in this RG, encode into one section.
		var bloomSection []byte

		// Count how many column blooms we'll write.
		var bloomCount uint16
		for _, colName := range bloomColumnNames {
			if constInRG[rg][colName] {
				continue
			}
			if maxTokensPerCol[colName] == 0 {
				continue
			}
			bloomCount++
		}
		bloomSection = binary.LittleEndian.AppendUint16(bloomSection, bloomCount)

		for _, colName := range bloomColumnNames {
			if constInRG[rg][colName] {
				continue
			}
			maxTok := maxTokensPerCol[colName]
			if maxTok == 0 {
				continue
			}

			bf := index.NewBloomFilter(maxTok, 0.01)
			addColumnTokens(bf, rgEvents, colName, fieldSet)

			bloomData, err := bf.Encode()
			if err != nil {
				return sw.w.written, fmt.Errorf("segment: encode bloom column %q rg%d: %w", colName, rg, err)
			}

			nameBytes := []byte(colName)
			bloomSection = binary.LittleEndian.AppendUint16(bloomSection, uint16(len(nameBytes)))
			bloomSection = append(bloomSection, nameBytes...)
			bloomSection = binary.LittleEndian.AppendUint32(bloomSection, uint32(len(bloomData)))
			bloomSection = append(bloomSection, bloomData...)
		}

		if _, err := sw.w.Write(bloomSection); err != nil {
			return sw.w.written, fmt.Errorf("segment: write bloom section rg%d: %w", rg, err)
		}
		rowGroups[rg].PerColumnBloomOffset = bloomSectionOffset
		rowGroups[rg].PerColumnBloomLength = sw.w.written - bloomSectionOffset

		// Build inverted index (global, with absolute row offsets).
		for i, e := range rgEvents {
			inv.Add(uint32(start+i), e.Raw)
		}
	}

	// Write inverted index.
	invertedOffset := sw.w.written
	invertedData, err := inv.Encode()
	if err != nil {
		return sw.w.written, fmt.Errorf("segment: encode inverted index: %w", err)
	}
	if _, err := sw.w.Write(invertedData); err != nil {
		return sw.w.written, fmt.Errorf("segment: write inverted index: %w", err)
	}
	invertedLength := sw.w.written - invertedOffset

	// Build sparse primary index if sort key is configured.
	var primaryIndexOffset, primaryIndexLength int64
	if len(sw.sortKey) > 0 {
		idx := &PrimaryIndex{
			Interval:   PrimaryIndexInterval,
			SortFields: sw.sortKey,
		}
		for row := 0; row < len(events); row += PrimaryIndexInterval {
			entry := PrimaryIndexEntry{RowOffset: uint32(row)}
			for _, field := range sw.sortKey {
				entry.SortKeyValues = append(entry.SortKeyValues, events[row].GetField(field).String())
			}
			idx.Entries = append(idx.Entries, entry)
		}
		primaryIndexOffset = sw.w.written
		idxData := EncodePrimaryIndex(idx)
		if _, err := sw.w.Write(idxData); err != nil {
			return sw.w.written, fmt.Errorf("segment: write primary index: %w", err)
		}
		primaryIndexLength = sw.w.written - primaryIndexOffset
	}

	// Write footer.
	footer := &Footer{
		EventCount:         int64(len(events)),
		RowGroups:          rowGroups,
		InvertedOffset:     invertedOffset,
		InvertedLength:     invertedLength,
		PrimaryIndexOffset: primaryIndexOffset,
		PrimaryIndexLength: primaryIndexLength,
		Catalog:            catalog,
	}
	footerBytes := encodeFooter(footer)
	if _, err := sw.w.Write(footerBytes); err != nil {
		return sw.w.written, fmt.Errorf("segment: write footer: %w", err)
	}

	return sw.w.written, nil
}

// writeRowGroup writes a single row group and returns its metadata.
// The catalogIndex parameter maps column names to their catalog index for the presence bitmap.
func (sw *Writer) writeRowGroup(events []*event.Event, fieldSet map[string]event.FieldType, fieldNames []string, catalogIndex map[string]int) (RowGroupMeta, error) {
	rgMeta := RowGroupMeta{RowCount: len(events)}

	// Timestamp column (delta-varint encoded) — never const.
	timestamps := make([]int64, len(events))
	for i, e := range events {
		timestamps[i] = e.Time.UnixNano()
	}
	chunk, err := sw.writeInt64Chunk("_time", timestamps)
	if err != nil {
		return rgMeta, err
	}
	rgMeta.Columns = append(rgMeta.Columns, chunk)
	setPresenceBit(&rgMeta, "_time", catalogIndex)

	// Raw column (LZ4 encoded) — never const (log lines are unique).
	rawValues := make([]string, len(events))
	for i, e := range events {
		rawValues[i] = e.Raw
	}
	chunk, err = sw.writeStringChunk("_raw", rawValues, column.EncodingLZ4, true)
	if err != nil {
		return rgMeta, err
	}
	rgMeta.Columns = append(rgMeta.Columns, chunk)
	setPresenceBit(&rgMeta, "_raw", catalogIndex)

	// Built-in string columns (dict encoded) — check for ConstColumn.
	for _, bf := range []struct {
		name   string
		getter func(*event.Event) string
	}{
		{"_source", func(e *event.Event) string { return e.Source }},
		{"_sourcetype", func(e *event.Event) string { return e.SourceType }},
		{"host", func(e *event.Event) string { return e.Host }},
		{"index", func(e *event.Event) string { return e.Index }},
	} {
		values := make([]string, len(events))
		for i, e := range events {
			values[i] = bf.getter(e)
		}

		// ConstColumn detection: all values identical and non-empty.
		if isConstString(values) {
			rgMeta.ConstColumns = append(rgMeta.ConstColumns, ConstColumnEntry{
				Name:         bf.name,
				EncodingType: uint8(column.EncodingDict8),
				Value:        values[0],
			})
			setPresenceBit(&rgMeta, bf.name, catalogIndex)

			continue
		}

		chunk, err = sw.writeStringChunk(bf.name, values, column.EncodingDict8, false)
		if err != nil {
			return rgMeta, err
		}
		rgMeta.Columns = append(rgMeta.Columns, chunk)
		setPresenceBit(&rgMeta, bf.name, catalogIndex)
	}

	// User-defined field columns — check for ConstColumn on string fields.
	for _, name := range fieldNames {
		ft := fieldSet[name]
		switch ft {
		case event.FieldTypeString:
			values := make([]string, len(events))
			for i, e := range events {
				v := e.GetField(name)
				if !v.IsNull() {
					if v.Type() == event.FieldTypeString {
						values[i] = v.AsString()
					} else {
						values[i] = v.String()
					}
				}
			}

			// ConstColumn detection for string fields.
			if isConstString(values) {
				rgMeta.ConstColumns = append(rgMeta.ConstColumns, ConstColumnEntry{
					Name:         name,
					EncodingType: uint8(column.EncodingDict8),
					Value:        values[0],
				})
				setPresenceBit(&rgMeta, name, catalogIndex)

				continue
			}

			chunk, err = sw.writeStringChunk(name, values, column.EncodingDict8, false)
		case event.FieldTypeInt, event.FieldTypeTimestamp:
			values := make([]int64, len(events))
			for i, e := range events {
				v := e.GetField(name)
				if v.IsNull() {
					continue
				}
				switch v.Type() {
				case event.FieldTypeTimestamp:
					values[i] = v.AsTimestamp().UnixNano()
				case event.FieldTypeFloat:
					values[i] = int64(v.AsFloat())
				case event.FieldTypeInt:
					values[i] = v.AsInt()
				}
			}
			chunk, err = sw.writeInt64Chunk(name, values)
		case event.FieldTypeFloat:
			values := make([]float64, len(events))
			for i, e := range events {
				v := e.GetField(name)
				if v.IsNull() {
					continue
				}
				switch v.Type() {
				case event.FieldTypeInt:
					values[i] = float64(v.AsInt())
				case event.FieldTypeFloat:
					values[i] = v.AsFloat()
				}
			}
			chunk, err = sw.writeFloat64Chunk(name, values)
		case event.FieldTypeBool:
			values := make([]int64, len(events))
			for i, e := range events {
				v := e.GetField(name)
				if !v.IsNull() && v.Type() == event.FieldTypeBool && v.AsBool() {
					values[i] = 1
				}
			}
			chunk, err = sw.writeInt64Chunk(name, values)
		default:
			continue
		}
		if err != nil {
			return rgMeta, err
		}
		rgMeta.Columns = append(rgMeta.Columns, chunk)
		setPresenceBit(&rgMeta, name, catalogIndex)
	}

	return rgMeta, nil
}

// isConstString returns true if all values in the slice are identical and non-empty.
func isConstString(values []string) bool {
	if len(values) == 0 {
		return false
	}
	first := values[0]
	if first == "" {
		return false
	}
	for _, v := range values[1:] {
		if v != first {
			return false
		}
	}

	return true
}

// setPresenceBit sets the bit in ColumnPresenceBits for the named column.
// Columns at index >= 64 are not tracked (bitmap is uint64).
func setPresenceBit(rg *RowGroupMeta, name string, catalogIndex map[string]int) {
	idx, ok := catalogIndex[name]
	if !ok || idx >= 64 {
		return
	}
	rg.ColumnPresenceBits |= 1 << uint(idx)
}

// buildCatalog creates the column catalog from field information.
// Order: builtins first (_time, _raw, _source, _sourcetype, host, index),
// then user fields in alphabetical order.
func buildCatalog(fieldSet map[string]event.FieldType, fieldNames []string) []CatalogEntry {
	catalog := make([]CatalogEntry, 0, 6+len(fieldNames))
	for _, name := range builtinColumns {
		enc := uint8(column.EncodingDelta)
		if name != "_time" {
			enc = uint8(column.EncodingDict8)
			if name == "_raw" {
				enc = uint8(column.EncodingLZ4)
			}
		}
		catalog = append(catalog, CatalogEntry{Name: name, DominantType: enc})
	}
	for _, name := range fieldNames {
		ft := fieldSet[name]
		var enc uint8
		switch ft {
		case event.FieldTypeString:
			enc = uint8(column.EncodingDict8)
		case event.FieldTypeInt, event.FieldTypeTimestamp, event.FieldTypeBool:
			enc = uint8(column.EncodingDelta)
		case event.FieldTypeFloat:
			enc = uint8(column.EncodingGorilla)
		}
		catalog = append(catalog, CatalogEntry{Name: name, DominantType: enc})
	}

	return catalog
}

// collectBloomColumns returns the ordered list of column names that get per-column blooms.
// Includes: _raw, _source, _sourcetype, host, index, + user-defined string fields.
// Excludes: _time (numeric), numeric fields (zone maps suffice).
func collectBloomColumns(fieldSet map[string]event.FieldType, fieldNames []string) []string {
	// Stable order: builtins first, then user fields alphabetical.
	cols := []string{"_raw", "_source", "_sourcetype", "host", "index"}
	for _, name := range fieldNames {
		if fieldSet[name] == event.FieldTypeString {
			cols = append(cols, name)
		}
	}

	return cols
}

// countColumnTokens counts unique tokens for a specific column across events in a RG.
func countColumnTokens(events []*event.Event, colName string, fieldSet map[string]event.FieldType) uint {
	unique := make(map[string]struct{})
	for _, e := range events {
		var val string
		switch colName {
		case "_raw":
			val = e.Raw
		case "_source":
			val = e.Source
		case "_sourcetype":
			val = e.SourceType
		case "host":
			val = e.Host
		case "index":
			val = e.Index
		default:
			v := e.GetField(colName)
			if !v.IsNull() {
				val = v.String()
			}
		}
		if val == "" {
			continue
		}
		for _, tok := range index.TokenizeUnique(val) {
			unique[tok] = struct{}{}
		}
	}

	return uint(len(unique))
}

// addColumnTokens adds all tokens for a specific column to the bloom filter.
func addColumnTokens(bf *index.BloomFilter, events []*event.Event, colName string, fieldSet map[string]event.FieldType) {
	for _, e := range events {
		var val string
		switch colName {
		case "_raw":
			val = e.Raw
		case "_source":
			val = e.Source
		case "_sourcetype":
			val = e.SourceType
		case "host":
			val = e.Host
		case "index":
			val = e.Index
		default:
			v := e.GetField(colName)
			if !v.IsNull() {
				val = v.String()
			}
		}
		if val == "" {
			continue
		}
		for _, tok := range index.TokenizeUnique(val) {
			bf.Add(tok)
		}
	}
}

// writeStringChunk encodes and writes a string column chunk with optional layer 2 compression.
func (sw *Writer) writeStringChunk(name string, values []string, preferredEnc column.EncodingType, skipLayer2 bool) (ColumnChunkMeta, error) {
	var enc column.StringEncoder

	switch preferredEnc {
	case column.EncodingLZ4:
		enc = column.NewLZ4Encoder()
	default:
		enc = column.NewDictEncoder()
	}

	data, err := enc.EncodeStrings(values)
	if err != nil {
		enc = column.NewLZ4Encoder()
		data, err = enc.EncodeStrings(values)
		if err != nil {
			return ColumnChunkMeta{}, fmt.Errorf("segment: encode column %q: %w", name, err)
		}
		skipLayer2 = true // LZ4 layer 1 → skip layer 2
	}

	actualEncoding := data[0]
	// If dict encoding fell back to LZ4 internally, skip layer 2.
	if column.EncodingType(actualEncoding) == column.EncodingLZ4 {
		skipLayer2 = true
	}

	rawSize := int64(len(data))
	compression := CompressionNone

	// Layer 2: block compression (configurable).
	if !skipLayer2 && len(data) > 64 {
		compressed, compType := sw.compressLayer2(data)
		if compressed != nil {
			compression = compType
			data = compressed
		}
	}

	offset := sw.w.written
	checksum := crc32.ChecksumIEEE(data)

	stat := stringColumnStats(name, values)

	if _, err := sw.w.Write(data); err != nil {
		return ColumnChunkMeta{}, fmt.Errorf("segment: write column %q: %w", name, err)
	}

	return ColumnChunkMeta{
		Name:         name,
		EncodingType: actualEncoding,
		Compression:  compression,
		Offset:       offset,
		Length:       int64(len(data)),
		RawSize:      rawSize,
		CRC32:        checksum,
		MinValue:     stat.MinValue,
		MaxValue:     stat.MaxValue,
		Count:        stat.Count,
		NullCount:    stat.NullCount,
	}, nil
}

// writeInt64Chunk encodes and writes an int64 column chunk with layer 2 compression.
func (sw *Writer) writeInt64Chunk(name string, values []int64) (ColumnChunkMeta, error) {
	enc := column.NewDeltaEncoder()
	data, err := enc.EncodeInt64s(values)
	if err != nil {
		return ColumnChunkMeta{}, fmt.Errorf("segment: encode column %q: %w", name, err)
	}

	rawSize := int64(len(data))
	compression := CompressionNone

	// Layer 2: block compression (configurable).
	if len(data) > 64 {
		compressed, compType := sw.compressLayer2(data)
		if compressed != nil {
			compression = compType
			data = compressed
		}
	}

	offset := sw.w.written
	checksum := crc32.ChecksumIEEE(data)

	stat := int64ColumnStats(name, values)

	if _, err := sw.w.Write(data); err != nil {
		return ColumnChunkMeta{}, fmt.Errorf("segment: write column %q: %w", name, err)
	}

	return ColumnChunkMeta{
		Name:         name,
		EncodingType: uint8(column.EncodingDelta),
		Compression:  compression,
		Offset:       offset,
		Length:       int64(len(data)),
		RawSize:      rawSize,
		CRC32:        checksum,
		MinValue:     stat.MinValue,
		MaxValue:     stat.MaxValue,
		Count:        stat.Count,
		NullCount:    stat.NullCount,
	}, nil
}

// writeFloat64Chunk encodes and writes a float64 column chunk with layer 2 compression.
func (sw *Writer) writeFloat64Chunk(name string, values []float64) (ColumnChunkMeta, error) {
	enc := column.NewGorillaEncoder()
	data, err := enc.EncodeFloat64s(values)
	if err != nil {
		return ColumnChunkMeta{}, fmt.Errorf("segment: encode column %q: %w", name, err)
	}

	rawSize := int64(len(data))
	compression := CompressionNone

	// Layer 2: block compression (configurable).
	if len(data) > 64 {
		compressed, compType := sw.compressLayer2(data)
		if compressed != nil {
			compression = compType
			data = compressed
		}
	}

	offset := sw.w.written
	checksum := crc32.ChecksumIEEE(data)

	stat := float64ColumnStats(name, values)

	if _, err := sw.w.Write(data); err != nil {
		return ColumnChunkMeta{}, fmt.Errorf("segment: write column %q: %w", name, err)
	}

	return ColumnChunkMeta{
		Name:         name,
		EncodingType: uint8(column.EncodingGorilla),
		Compression:  compression,
		Offset:       offset,
		Length:       int64(len(data)),
		RawSize:      rawSize,
		CRC32:        checksum,
		MinValue:     stat.MinValue,
		MaxValue:     stat.MaxValue,
		Count:        stat.Count,
		NullCount:    stat.NullCount,
	}, nil
}

// compressLayer2 applies the configured layer 2 compression. Returns nil if not beneficial.
func (sw *Writer) compressLayer2(data []byte) ([]byte, CompressionType) {
	switch sw.compression {
	case CompressionZSTD:
		compressed := compressZSTDBlock(data)
		if compressed != nil {
			return compressed, CompressionZSTD
		}
	case CompressionLZ4:
		compressed := compressLZ4Block(data)
		if compressed != nil {
			return compressed, CompressionLZ4
		}
	}

	return nil, CompressionNone
}

// compressLZ4Block applies LZ4 block compression. Returns nil if compression is not beneficial.
func compressLZ4Block(data []byte) []byte {
	maxCompressed := lz4.CompressBlockBound(len(data))
	compressed := make([]byte, maxCompressed+8) // 4B uncompressed size + 4B compressed size + data

	n, err := lz4.CompressBlock(data, compressed[8:], nil)
	if err != nil || n == 0 || n >= len(data) {
		return nil // incompressible or no savings
	}

	binary.LittleEndian.PutUint32(compressed[0:4], uint32(len(data)))
	binary.LittleEndian.PutUint32(compressed[4:8], uint32(n))

	return compressed[:8+n]
}

// lz4DecompPool reuses decompression scratch buffers across queries.
// Column decoders retain the decompressed bytes long-term, so we copy out
// the result before returning the buffer to the pool.
var lz4DecompPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 256*1024) // 256KB initial capacity

		return &buf
	},
}

// decompressLZ4Block decompresses an LZ4 block. The first 8 bytes are uncompressed + compressed sizes.
func decompressLZ4Block(data []byte) ([]byte, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("segment: lz4 block too short")
	}
	uncompSize := binary.LittleEndian.Uint32(data[0:4])
	compSize := binary.LittleEndian.Uint32(data[4:8])
	if 8+int(compSize) > len(data) {
		return nil, fmt.Errorf("segment: lz4 block truncated")
	}

	// Use pooled scratch buffer for decompression, then copy out.
	bufPtr := lz4DecompPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < int(uncompSize) {
		buf = make([]byte, uncompSize)
	} else {
		buf = buf[:uncompSize]
	}

	n, err := lz4.UncompressBlock(data[8:8+compSize], buf)
	if err != nil {
		*bufPtr = buf
		lz4DecompPool.Put(bufPtr)

		return nil, fmt.Errorf("segment: lz4 decompress: %w", err)
	}
	if n != int(uncompSize) {
		*bufPtr = buf
		lz4DecompPool.Put(bufPtr)

		return nil, fmt.Errorf("segment: lz4 size mismatch: got %d, want %d", n, uncompSize)
	}

	// Copy out: callers (column decoders) retain the result long-term.
	result := make([]byte, n)
	copy(result, buf[:n])
	*bufPtr = buf
	lz4DecompPool.Put(bufPtr)

	return result, nil
}

// compressZSTDBlock applies ZSTD block compression. Returns nil if not beneficial.
// Format: [4B uncompressed size LE][ZSTD data].
func compressZSTDBlock(data []byte) []byte {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(data, make([]byte, 4, 4+len(data)))
	zstdEncoderPool.Put(enc)
	if len(compressed)-4 >= len(data) {
		return nil // no savings
	}
	binary.LittleEndian.PutUint32(compressed[0:4], uint32(len(data)))

	return compressed
}

// decompressZSTDBlock decompresses a ZSTD block. First 4 bytes are uncompressed size.
func decompressZSTDBlock(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("segment: zstd block too short")
	}
	uncompSize := binary.LittleEndian.Uint32(data[0:4])
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	out, err := dec.DecodeAll(data[4:], make([]byte, 0, uncompSize))
	zstdDecoderPool.Put(dec)
	if err != nil {
		return nil, fmt.Errorf("segment: zstd decompress: %w", err)
	}
	if uint32(len(out)) != uncompSize {
		return nil, fmt.Errorf("segment: zstd size mismatch: got %d, want %d", len(out), uncompSize)
	}

	return out, nil
}

func stringColumnStats(name string, values []string) ColumnStats {
	stat := ColumnStats{Name: name, Count: int64(len(values))}
	minVal, maxVal := "", ""
	initialized := false
	for _, v := range values {
		if v == "" {
			stat.NullCount++

			continue
		}
		if !initialized {
			minVal, maxVal = v, v
			initialized = true

			continue
		}
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	if initialized {
		stat.MinValue = minVal
		stat.MaxValue = maxVal
	}

	return stat
}

func int64ColumnStats(name string, values []int64) ColumnStats {
	stat := ColumnStats{Name: name, Count: int64(len(values))}
	if len(values) > 0 {
		minVal, maxVal := values[0], values[0]
		for _, v := range values[1:] {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
		stat.MinValue = strconv.FormatInt(minVal, 10)
		stat.MaxValue = strconv.FormatInt(maxVal, 10)
	}

	return stat
}

func float64ColumnStats(name string, values []float64) ColumnStats {
	stat := ColumnStats{Name: name, Count: int64(len(values))}
	if len(values) > 0 {
		minVal, maxVal := values[0], values[0]
		for _, v := range values[1:] {
			if v < minVal {
				minVal = v
			}
			if v > maxVal {
				maxVal = v
			}
		}
		stat.MinValue = strconv.FormatFloat(minVal, 'g', -1, 64)
		stat.MaxValue = strconv.FormatFloat(maxVal, 'g', -1, 64)
	}

	return stat
}

// topFieldsByFrequency returns the top-N field names by occurrence frequency
// across all events. Fields that appear in more events are preferred as columns
// because they have higher coverage and benefit more from columnar encoding.
func topFieldsByFrequency(events []*event.Event, fieldNames []string, maxColumns int) []string {
	// Count how many events each field appears in (non-null).
	freq := make(map[string]int, len(fieldNames))
	for _, name := range fieldNames {
		freq[name] = 0
	}
	for _, ev := range events {
		for _, name := range ev.FieldNames() {
			if _, ok := freq[name]; ok {
				freq[name]++
			}
		}
	}

	// Sort by frequency descending, then alphabetically for stability.
	sort.Slice(fieldNames, func(i, j int) bool {
		fi, fj := freq[fieldNames[i]], freq[fieldNames[j]]
		if fi != fj {
			return fi > fj
		}

		return fieldNames[i] < fieldNames[j]
	})

	return fieldNames[:maxColumns]
}

func makeHeader(rowGroupSize, rowGroupCount int) []byte {
	header := make([]byte, HeaderSize)
	copy(header[0:4], MagicBytes)
	binary.LittleEndian.PutUint16(header[4:6], FormatV4)
	binary.LittleEndian.PutUint16(header[6:8], 0) // flags (reserved)
	binary.LittleEndian.PutUint32(header[8:12], uint32(rowGroupSize))
	binary.LittleEndian.PutUint32(header[12:16], uint32(rowGroupCount))

	return header
}

package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

// ErrFinalized is returned when WriteRowGroup is called after Finalize.
var ErrFinalized = errors.New("segment: stream writer already finalized")

// StreamWriter writes .lsg segment files incrementally, one row group at a time.
// This bounds memory usage to O(1 row group) instead of O(all events).
//
// Usage:
//
//	sw := NewStreamWriter(w, CompressionLZ4)
//	sw.WriteRowGroup(batch1)
//	sw.WriteRowGroup(batch2)
//	totalBytes, err := sw.Finalize()
type StreamWriter struct {
	w          *Writer // reuses Writer's writeRowGroup, chunk methods
	rgSize     int
	maxColumns int

	// Progressive state
	headerWritten bool
	totalEvents   int64
	rowGroups     []RowGroupMeta
	fieldSet      map[string]event.FieldType // union of all fields seen
	fieldNames    []string                   // sorted field names (rebuilt on each RG)

	// Per-row-group tracking for field presence (needed to rebuild presence bitmaps
	// at Finalize time when the catalog may have shifted due to new fields).
	rgFieldPresence []map[string]bool // per-RG: set of column names present (chunk or const)

	// Bloom accumulator: per-RG bloom data stored for footer write.
	bloomSections []bloomSectionData

	// Inverted index accumulated across all RGs.
	inv *index.InvertedIndex

	// Row group event tracking for inverted index absolute offsets.
	rowOffset int

	finalized bool
}

// bloomSectionData tracks the byte range of a per-RG bloom section already written.
type bloomSectionData struct {
	offset int64
	length int64
}

// NewStreamWriter creates a streaming segment writer that outputs to w with
// default LZ4 layer 2 compression.
func NewStreamWriter(w io.Writer, compression CompressionType) *StreamWriter {
	return &StreamWriter{
		w:        NewWriterWithCompression(w, compression),
		fieldSet: make(map[string]event.FieldType),
		inv:      index.NewInvertedIndex(),
	}
}

// SetRowGroupSize overrides the default row group size for this writer.
// Must be called before the first WriteRowGroup().
func (sw *StreamWriter) SetRowGroupSize(size int) {
	sw.rgSize = size
}

// SetMaxColumns limits the number of user-defined columns written per segment.
// Must be called before the first WriteRowGroup().
func (sw *StreamWriter) SetMaxColumns(n int) {
	sw.maxColumns = n
}

// WriteRowGroup writes a single row group of events to the segment.
// Events within the row group should be sorted by timestamp.
// The field set is accumulated progressively across row groups.
func (sw *StreamWriter) WriteRowGroup(events []*event.Event) error {
	if sw.finalized {
		return ErrFinalized
	}
	if len(events) == 0 {
		return ErrNoEvents
	}

	// Determine effective row group size.
	rgSize := sw.rgSize
	if rgSize <= 0 {
		rgSize = DefaultRowGroupSize
	}

	// Union new fields into fieldSet with type promotion.
	for _, e := range events {
		for _, name := range e.FieldNames() {
			v := e.GetField(name)
			if v.IsNull() {
				continue
			}
			newType := v.Type()
			if prev, exists := sw.fieldSet[name]; exists {
				if (prev == event.FieldTypeInt && newType == event.FieldTypeFloat) ||
					(prev == event.FieldTypeFloat && newType == event.FieldTypeInt) {
					newType = event.FieldTypeFloat
				}
			}
			sw.fieldSet[name] = newType
		}
	}

	// Rebuild sorted field names from fieldSet.
	sw.fieldNames = make([]string, 0, len(sw.fieldSet))
	for name := range sw.fieldSet {
		sw.fieldNames = append(sw.fieldNames, name)
	}
	sort.Strings(sw.fieldNames)

	// Apply maxColumns cap if set.
	fieldNames := sw.fieldNames
	fieldSet := sw.fieldSet
	if sw.maxColumns > 0 && len(fieldNames) > sw.maxColumns {
		fieldNames = topFieldsByFrequency(events, fieldNames, sw.maxColumns)
		capped := make(map[string]event.FieldType, len(fieldNames))
		for _, name := range fieldNames {
			capped[name] = sw.fieldSet[name]
		}
		fieldSet = capped
	}

	// Build catalog and catalogIndex from current fieldSet.
	catalog := buildCatalog(fieldSet, fieldNames)
	catalogIndex := make(map[string]int, len(catalog))
	for i, cat := range catalog {
		catalogIndex[cat.Name] = i
	}

	// Write header on first call with placeholder rgCount=0.
	if !sw.headerWritten {
		header := makeHeader(rgSize, 0) // placeholder count; reader uses footer
		if _, err := sw.w.w.Write(header); err != nil {
			return fmt.Errorf("segment: write header: %w", err)
		}
		sw.headerWritten = true
	}

	// Write the row group data using the underlying Writer's method.
	rgMeta, err := sw.w.writeRowGroup(events, fieldSet, fieldNames, catalogIndex)
	if err != nil {
		return fmt.Errorf("segment: row group %d: %w", len(sw.rowGroups), err)
	}

	// Track which column names are present in this row group (for Finalize rebuild).
	presence := make(map[string]bool, len(rgMeta.Columns)+len(rgMeta.ConstColumns))
	for _, c := range rgMeta.Columns {
		presence[c.Name] = true
	}
	for _, cc := range rgMeta.ConstColumns {
		presence[cc.Name] = true
	}
	sw.rgFieldPresence = append(sw.rgFieldPresence, presence)

	constInRG := make(map[string]bool, len(rgMeta.ConstColumns))
	for _, cc := range rgMeta.ConstColumns {
		constInRG[cc.Name] = true
	}

	bloomColumnNames := collectBloomColumns(fieldSet, fieldNames)

	// Pass 1: count max tokens per column for this RG.
	maxTokensPerCol := make(map[string]uint, len(bloomColumnNames))
	for _, colName := range bloomColumnNames {
		if constInRG[colName] {
			continue
		}
		count := countColumnTokens(events, colName, fieldSet)
		if count < 100 {
			count = 100
		}
		maxTokensPerCol[colName] = count
	}

	// Pass 2: build per-column blooms for this RG.
	bloomSectionOffset := sw.w.w.written

	var bloomSection []byte

	var bloomCount uint16
	for _, colName := range bloomColumnNames {
		if constInRG[colName] {
			continue
		}
		if maxTokensPerCol[colName] == 0 {
			continue
		}
		bloomCount++
	}
	bloomSection = binary.LittleEndian.AppendUint16(bloomSection, bloomCount)

	for _, colName := range bloomColumnNames {
		if constInRG[colName] {
			continue
		}
		maxTok := maxTokensPerCol[colName]
		if maxTok == 0 {
			continue
		}

		bf := index.NewBloomFilter(maxTok, 0.01)
		addColumnTokens(bf, events, colName, fieldSet)

		bloomData, err := bf.Encode()
		if err != nil {
			return fmt.Errorf("segment: encode bloom column %q rg%d: %w", colName, len(sw.rowGroups), err)
		}

		nameBytes := []byte(colName)
		bloomSection = binary.LittleEndian.AppendUint16(bloomSection, uint16(len(nameBytes)))
		bloomSection = append(bloomSection, nameBytes...)
		bloomSection = binary.LittleEndian.AppendUint32(bloomSection, uint32(len(bloomData)))
		bloomSection = append(bloomSection, bloomData...)
	}

	if _, err := sw.w.w.Write(bloomSection); err != nil {
		return fmt.Errorf("segment: write bloom section rg%d: %w", len(sw.rowGroups), err)
	}
	rgMeta.PerColumnBloomOffset = bloomSectionOffset
	rgMeta.PerColumnBloomLength = sw.w.w.written - bloomSectionOffset

	sw.bloomSections = append(sw.bloomSections, bloomSectionData{
		offset: bloomSectionOffset,
		length: sw.w.w.written - bloomSectionOffset,
	})

	// Add events to the global inverted index with absolute row offsets.
	for i, e := range events {
		sw.inv.Add(uint32(sw.rowOffset+i), e.Raw)
	}

	sw.rowGroups = append(sw.rowGroups, rgMeta)
	sw.totalEvents += int64(len(events))
	sw.rowOffset += len(events)

	return nil
}

// Finalize writes the inverted index and footer, completing the segment file.
// Returns the total bytes written. After Finalize, no more row groups can be written.
func (sw *StreamWriter) Finalize() (int64, error) {
	if sw.finalized {
		return sw.w.w.written, ErrFinalized
	}
	sw.finalized = true

	if len(sw.rowGroups) == 0 {
		return sw.w.w.written, ErrNoEvents
	}

	// Build the FINAL catalog from the accumulated fieldSet.
	finalFieldNames := make([]string, 0, len(sw.fieldSet))
	for name := range sw.fieldSet {
		finalFieldNames = append(finalFieldNames, name)
	}
	sort.Strings(finalFieldNames)

	// Apply maxColumns cap to the final catalog if set.
	finalFieldSet := sw.fieldSet
	if sw.maxColumns > 0 && len(finalFieldNames) > sw.maxColumns {
		// For the final catalog, we need the most frequent fields across all events.
		// Since we don't have all events anymore, use the field names that were
		// actually written (they were already capped per-RG). Collect all fields
		// that appear in any RG's presence set.
		allPresent := make(map[string]int, len(finalFieldNames))
		for _, presence := range sw.rgFieldPresence {
			for name := range presence {
				allPresent[name]++
			}
		}
		// Remove builtins from the count.
		builtinSet := make(map[string]bool, len(builtinColumns))
		for _, b := range builtinColumns {
			builtinSet[b] = true
		}
		userFields := make([]string, 0, len(allPresent))
		for name := range allPresent {
			if !builtinSet[name] {
				userFields = append(userFields, name)
			}
		}
		sort.Slice(userFields, func(i, j int) bool {
			ci, cj := allPresent[userFields[i]], allPresent[userFields[j]]
			if ci != cj {
				return ci > cj
			}
			return userFields[i] < userFields[j]
		})
		if len(userFields) > sw.maxColumns {
			userFields = userFields[:sw.maxColumns]
		}
		sort.Strings(userFields)
		finalFieldNames = userFields

		capped := make(map[string]event.FieldType, len(finalFieldNames))
		for _, name := range finalFieldNames {
			capped[name] = sw.fieldSet[name]
		}
		finalFieldSet = capped
	}

	finalCatalog := buildCatalog(finalFieldSet, finalFieldNames)
	finalCatalogIndex := make(map[string]int, len(finalCatalog))
	for i, cat := range finalCatalog {
		finalCatalogIndex[cat.Name] = i
	}

	// Rebuild ColumnPresenceBits for all row groups using the final catalog.
	for rgi := range sw.rowGroups {
		sw.rowGroups[rgi].ColumnPresenceBits = 0
		presence := sw.rgFieldPresence[rgi]
		for name := range presence {
			setPresenceBit(&sw.rowGroups[rgi], name, finalCatalogIndex)
		}
	}

	// Write inverted index.
	invertedOffset := sw.w.w.written
	invertedData, err := sw.inv.Encode()
	if err != nil {
		return sw.w.w.written, fmt.Errorf("segment: encode inverted index: %w", err)
	}
	if _, err := sw.w.w.Write(invertedData); err != nil {
		return sw.w.w.written, fmt.Errorf("segment: write inverted index: %w", err)
	}
	invertedLength := sw.w.w.written - invertedOffset

	// Write footer.
	footer := &Footer{
		EventCount:     sw.totalEvents,
		RowGroups:      sw.rowGroups,
		InvertedOffset: invertedOffset,
		InvertedLength: invertedLength,
		Catalog:        finalCatalog,
	}
	footerBytes := encodeFooter(footer)
	if _, err := sw.w.w.Write(footerBytes); err != nil {
		return sw.w.w.written, fmt.Errorf("segment: write footer: %w", err)
	}

	return sw.w.w.written, nil
}

package column

import (
	"encoding/binary"
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

// DictFilter provides fast equality filtering on dict-encoded columns
// by operating on dictionary codes instead of full string comparison.
type DictFilter struct {
	dict    []string
	dictMap map[string]int // reverse index: value → dictionary code (O(1) lookup)
	indices []int          // per-row dictionary index
	use8bit bool
	count   int
}

// NewDictFilterFromEncoded creates a DictFilter by parsing dict-encoded column data.
// Only parses the dictionary and index arrays, not full string decode.
func NewDictFilterFromEncoded(data []byte) (*DictFilter, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("%w: header too short for dict filter", ErrCorruptData)
	}

	encType := EncodingType(data[0])
	use8bit := encType == EncodingDict8
	if encType != EncodingDict8 && encType != EncodingDict16 {
		return nil, fmt.Errorf("%w: not dict encoded", ErrInvalidEncoding)
	}

	count := int(binary.LittleEndian.Uint32(data[1:5]))
	dictSize := int(binary.LittleEndian.Uint32(data[5:9]))
	pos := 9

	// Read dictionary.
	dict := make([]string, dictSize)
	for i := 0; i < dictSize; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("%w: truncated dict", ErrCorruptData)
		}
		sLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+sLen > len(data) {
			return nil, fmt.Errorf("%w: truncated dict entry", ErrCorruptData)
		}
		dict[i] = string(data[pos : pos+sLen])
		pos += sLen
	}

	// Read indices.
	indices := make([]int, count)
	if use8bit {
		for i := 0; i < count; i++ {
			if pos >= len(data) {
				return nil, fmt.Errorf("%w: truncated indices", ErrCorruptData)
			}
			indices[i] = int(data[pos])
			pos++
		}
	} else {
		for i := 0; i < count; i++ {
			if pos+2 > len(data) {
				return nil, fmt.Errorf("%w: truncated indices", ErrCorruptData)
			}
			indices[i] = int(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2
		}
	}

	// Build reverse index for O(1) value→code lookups.
	dictMap := make(map[string]int, dictSize)
	for i, d := range dict {
		dictMap[d] = i
	}

	return &DictFilter{
		dict:    dict,
		dictMap: dictMap,
		indices: indices,
		use8bit: use8bit,
		count:   count,
	}, nil
}

// FilterEquality returns a bitmap of rows where the column value equals the target.
// If the target is not in the dictionary, returns an empty bitmap immediately.
func (df *DictFilter) FilterEquality(value string) *roaring.Bitmap {
	targetCode, ok := df.dictMap[value]

	bm := roaring.New()
	if !ok {
		return bm // value not in dictionary -> empty result
	}

	// Scan indices comparing uint8/uint16 codes (no string comparison).
	for i, idx := range df.indices {
		if idx == targetCode {
			bm.Add(uint32(i))
		}
	}

	return bm
}

// FilterNotEquality returns a bitmap of rows where the column value != target.
func (df *DictFilter) FilterNotEquality(value string) *roaring.Bitmap {
	targetCode, ok := df.dictMap[value]

	bm := roaring.New()
	if !ok {
		// Value not in dictionary -> all rows match.
		bm.AddRange(0, uint64(df.count))

		return bm
	}

	for i, idx := range df.indices {
		if idx != targetCode {
			bm.Add(uint32(i))
		}
	}

	return bm
}

// DictSize returns the number of unique values in the dictionary.
func (df *DictFilter) DictSize() int {
	return len(df.dict)
}

// RowCount returns the total number of rows.
func (df *DictFilter) RowCount() int {
	return df.count
}

// ContainsValue returns true if the value exists in the dictionary.
func (df *DictFilter) ContainsValue(value string) bool {
	_, ok := df.dictMap[value]

	return ok
}

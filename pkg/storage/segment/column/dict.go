package column

import (
	"encoding/binary"
	"fmt"
)

// DictEncoder implements dictionary encoding for low-cardinality string columns.
// It builds a dictionary of unique values and stores indices into that dictionary.
// Uses 8-bit indices when <=256 unique values, 16-bit when <=65536.
//
// Wire format:
//
//	[1B encoding type] [4B count] [4B dict size] [dict entries: 4B len + data ...] [indices ...]
type DictEncoder struct{}

var _ StringEncoder = (*DictEncoder)(nil)

func NewDictEncoder() *DictEncoder {
	return &DictEncoder{}
}

func (d *DictEncoder) EncodeStrings(values []string) ([]byte, error) {
	if len(values) == 0 {
		return nil, ErrEmptyInput
	}

	// Build dictionary: map value -> index.
	dict := make(map[string]int)
	order := make([]string, 0)
	for _, v := range values {
		if _, ok := dict[v]; !ok {
			dict[v] = len(order)
			order = append(order, v)
		}
	}

	if len(order) > 65536 {
		return nil, fmt.Errorf("%w: got %d unique values", ErrTooManyUnique, len(order))
	}

	use8bit := len(order) <= 256

	// Estimate buffer size.
	var encType EncodingType
	if use8bit {
		encType = EncodingDict8
	} else {
		encType = EncodingDict16
	}

	// Calculate dict data size.
	dictDataSize := 0
	for _, s := range order {
		dictDataSize += 4 + len(s) // 4B length prefix + data
	}

	indexSize := len(values)
	if !use8bit {
		indexSize = len(values) * 2
	}

	buf := make([]byte, 0, 1+4+4+dictDataSize+indexSize)

	// Encoding type.
	buf = append(buf, byte(encType))

	// Value count.
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(values)))

	// Dict size (number of unique entries).
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(order)))

	// Dict entries.
	for _, s := range order {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s)))
		buf = append(buf, s...)
	}

	// Indices.
	if use8bit {
		for _, v := range values {
			buf = append(buf, byte(dict[v]))
		}
	} else {
		for _, v := range values {
			buf = binary.LittleEndian.AppendUint16(buf, uint16(dict[v]))
		}
	}

	return buf, nil
}

func (d *DictEncoder) DecodeStrings(data []byte) ([]string, error) {
	if len(data) < 9 { // 1 type + 4 count + 4 dict size
		return nil, fmt.Errorf("%w: header too short", ErrCorruptData)
	}

	encType := EncodingType(data[0])
	if encType != EncodingDict8 && encType != EncodingDict16 {
		return nil, fmt.Errorf("%w: expected dict encoding, got %d", ErrInvalidEncoding, encType)
	}

	count := binary.LittleEndian.Uint32(data[1:5])
	dictSize := binary.LittleEndian.Uint32(data[5:9])

	pos := 9

	// Read dictionary entries.
	dict := make([]string, dictSize)
	for i := uint32(0); i < dictSize; i++ {
		if pos+4 > len(data) {
			return nil, fmt.Errorf("%w: truncated dict entry length", ErrCorruptData)
		}
		sLen := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		if pos+int(sLen) > len(data) {
			return nil, fmt.Errorf("%w: truncated dict entry data", ErrCorruptData)
		}
		dict[i] = string(data[pos : pos+int(sLen)])
		pos += int(sLen)
	}

	// Read indices.
	result := make([]string, count)
	if encType == EncodingDict8 {
		if pos+int(count) > len(data) {
			return nil, fmt.Errorf("%w: truncated indices", ErrCorruptData)
		}
		for i := uint32(0); i < count; i++ {
			idx := data[pos]
			pos++
			if int(idx) >= len(dict) {
				return nil, fmt.Errorf("%w: index %d out of range (dict size %d)", ErrCorruptData, idx, len(dict))
			}
			result[i] = dict[idx]
		}
	} else {
		if pos+int(count)*2 > len(data) {
			return nil, fmt.Errorf("%w: truncated indices", ErrCorruptData)
		}
		for i := uint32(0); i < count; i++ {
			idx := binary.LittleEndian.Uint16(data[pos : pos+2])
			pos += 2
			if int(idx) >= len(dict) {
				return nil, fmt.Errorf("%w: index %d out of range (dict size %d)", ErrCorruptData, idx, len(dict))
			}
			result[i] = dict[idx]
		}
	}

	return result, nil
}

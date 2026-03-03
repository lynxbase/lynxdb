package column

import (
	"encoding/binary"
	"fmt"
)

// DeltaEncoder implements delta encoding with zigzag varints for int64 columns.
// Ideal for monotonic values like timestamps. Stores the first value as a fixed
// 8-byte int64, then encodes successive deltas as zigzag-encoded unsigned varints.
//
// Wire format:
//
//	[1B encoding type] [4B count] [8B first value] [zigzag varint deltas ...]
type DeltaEncoder struct{}

var _ Int64Encoder = (*DeltaEncoder)(nil)

func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{}
}

func (d *DeltaEncoder) EncodeInt64s(values []int64) ([]byte, error) {
	if len(values) == 0 {
		return nil, ErrEmptyInput
	}

	// Estimate: header + varints average ~2 bytes each.
	buf := make([]byte, 0, 13+len(values)*2)

	buf = append(buf, byte(EncodingDelta))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(values)))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(values[0]))

	prev := values[0]
	var varintBuf [binary.MaxVarintLen64]byte
	for i := 1; i < len(values); i++ {
		delta := values[i] - prev
		prev = values[i]
		// Zigzag encode: (delta << 1) ^ (delta >> 63)
		zigzag := uint64((delta << 1) ^ (delta >> 63))
		n := binary.PutUvarint(varintBuf[:], zigzag)
		buf = append(buf, varintBuf[:n]...)
	}

	return buf, nil
}

func (d *DeltaEncoder) DecodeInt64s(data []byte) ([]int64, error) {
	if len(data) < 13 { // 1 type + 4 count + 8 first
		return nil, fmt.Errorf("%w: header too short", ErrCorruptData)
	}

	if EncodingType(data[0]) != EncodingDelta {
		return nil, fmt.Errorf("%w: expected delta encoding, got %d", ErrInvalidEncoding, data[0])
	}

	count := binary.LittleEndian.Uint32(data[1:5])
	first := int64(binary.LittleEndian.Uint64(data[5:13]))

	result := make([]int64, count)
	result[0] = first

	pos := 13
	prev := first
	for i := uint32(1); i < count; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("%w: truncated varint at index %d", ErrCorruptData, i)
		}
		zigzag, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("%w: invalid varint at index %d", ErrCorruptData, i)
		}
		pos += n
		// Zigzag decode: (zigzag >> 1) ^ -(zigzag & 1)
		delta := int64(zigzag>>1) ^ -int64(zigzag&1)
		prev += delta
		result[i] = prev
	}

	return result, nil
}

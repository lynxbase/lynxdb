package column

import (
	"encoding/binary"
	"fmt"

	"github.com/pierrec/lz4/v4"
)

// LZ4Encoder implements LZ4 block compression for high-cardinality string columns.
// It length-prefixes each string, concatenates them, then LZ4-compresses the result.
//
// Wire format:
//
//	[1B encoding type] [4B count] [4B uncompressed size] [4B compressed size] [LZ4 data]
type LZ4Encoder struct{}

var _ StringEncoder = (*LZ4Encoder)(nil)

func NewLZ4Encoder() *LZ4Encoder {
	return &LZ4Encoder{}
}

func (e *LZ4Encoder) EncodeStrings(values []string) ([]byte, error) {
	if len(values) == 0 {
		return nil, ErrEmptyInput
	}

	// Build uncompressed payload: [4B len][data] for each string.
	uncompSize := 0
	for _, s := range values {
		uncompSize += 4 + len(s)
	}

	uncompressed := make([]byte, 0, uncompSize)
	for _, s := range values {
		uncompressed = binary.LittleEndian.AppendUint32(uncompressed, uint32(len(s)))
		uncompressed = append(uncompressed, s...)
	}

	// Compress with LZ4.
	maxCompressed := lz4.CompressBlockBound(len(uncompressed))
	compressed := make([]byte, maxCompressed)
	n, err := lz4.CompressBlock(uncompressed, compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("column: lz4 compress: %w", err)
	}
	// If lz4 returns 0, data is incompressible; store uncompressed.
	if n == 0 {
		n = len(uncompressed)
		compressed = uncompressed
	} else {
		compressed = compressed[:n]
	}

	// Header: type + count + uncompressed size + compressed size.
	header := make([]byte, 0, 13)
	header = append(header, byte(EncodingLZ4))
	header = binary.LittleEndian.AppendUint32(header, uint32(len(values)))
	header = binary.LittleEndian.AppendUint32(header, uint32(len(uncompressed)))
	header = binary.LittleEndian.AppendUint32(header, uint32(n))

	result := make([]byte, 0, len(header)+n)
	result = append(result, header...)
	result = append(result, compressed[:n]...)

	return result, nil
}

func (e *LZ4Encoder) DecodeStrings(data []byte) ([]string, error) {
	if len(data) < 13 { // 1 type + 4 count + 4 uncomp + 4 comp
		return nil, fmt.Errorf("%w: header too short", ErrCorruptData)
	}

	if EncodingType(data[0]) != EncodingLZ4 {
		return nil, fmt.Errorf("%w: expected LZ4 encoding, got %d", ErrInvalidEncoding, data[0])
	}

	count := binary.LittleEndian.Uint32(data[1:5])
	uncompSize := binary.LittleEndian.Uint32(data[5:9])
	compSize := binary.LittleEndian.Uint32(data[9:13])

	if 13+int(compSize) > len(data) {
		return nil, fmt.Errorf("%w: truncated compressed data", ErrCorruptData)
	}

	compressed := data[13 : 13+compSize]

	var uncompressed []byte
	if compSize == uncompSize {
		// Data was stored uncompressed.
		uncompressed = compressed
	} else {
		uncompressed = make([]byte, uncompSize)
		n, err := lz4.UncompressBlock(compressed, uncompressed)
		if err != nil {
			return nil, fmt.Errorf("column: lz4 decompress: %w", err)
		}
		if n != int(uncompSize) {
			return nil, fmt.Errorf("%w: decompressed size mismatch: got %d, want %d", ErrCorruptData, n, uncompSize)
		}
	}

	// Parse length-prefixed strings.
	result := make([]string, 0, count)
	pos := 0
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(uncompressed) {
			return nil, fmt.Errorf("%w: truncated string length at index %d", ErrCorruptData, i)
		}
		sLen := binary.LittleEndian.Uint32(uncompressed[pos : pos+4])
		pos += 4
		if pos+int(sLen) > len(uncompressed) {
			return nil, fmt.Errorf("%w: truncated string data at index %d", ErrCorruptData, i)
		}
		result = append(result, string(uncompressed[pos:pos+int(sLen)]))
		pos += int(sLen)
	}

	return result, nil
}

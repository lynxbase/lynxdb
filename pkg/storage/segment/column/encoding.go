package column

import "errors"

// EncodingType identifies the column encoding algorithm.
type EncodingType uint8

const (
	EncodingDict8   EncodingType = 1 // Dictionary with 8-bit indices (<=256 unique values)
	EncodingDict16  EncodingType = 2 // Dictionary with 16-bit indices (<=65536 unique values)
	EncodingLZ4     EncodingType = 3 // LZ4 block compression for strings
	EncodingDelta   EncodingType = 4 // Delta + zigzag varint for monotonic int64s
	EncodingGorilla EncodingType = 5 // Gorilla XOR encoding for float64s

	// Typed column encodings; tags are append-only, never renumber.
	// An old binary that encounters these tags returns ErrUnsupportedCapability
	// from readFieldColumn, which is the safe "I cannot decode this column"
	// path. No format version bump needed; the encoding byte in each column
	// chunk is the discriminator.

	EncodingDeltaDuration EncodingType = 6 // Delta int64 (nanoseconds), reconstructed as DurationValue
	EncodingMsgpackArray  EncodingType = 7 // Per-cell msgpack, stored via string/LZ4 machinery, reconstructed as ArrayValue
	EncodingMsgpackObject EncodingType = 8 // Per-cell msgpack, stored via string/LZ4 machinery, reconstructed as ObjectValue
)

func (e EncodingType) String() string {
	switch e {
	case EncodingDict8:
		return "dict8"
	case EncodingDict16:
		return "dict16"
	case EncodingLZ4:
		return "lz4"
	case EncodingDelta:
		return "delta"
	case EncodingGorilla:
		return "gorilla"
	case EncodingDeltaDuration:
		return "delta_duration"
	case EncodingMsgpackArray:
		return "msgpack_array"
	case EncodingMsgpackObject:
		return "msgpack_object"
	default:
		return "unknown"
	}
}

// StringEncoder encodes and decodes a column of string values.
type StringEncoder interface {
	EncodeStrings(values []string) ([]byte, error)
	DecodeStrings(data []byte) ([]string, error)
}

// Int64Encoder encodes and decodes a column of int64 values.
type Int64Encoder interface {
	EncodeInt64s(values []int64) ([]byte, error)
	DecodeInt64s(data []byte) ([]int64, error)
}

// Float64Encoder encodes and decodes a column of float64 values.
type Float64Encoder interface {
	EncodeFloat64s(values []float64) ([]byte, error)
	DecodeFloat64s(data []byte) ([]float64, error)
}

var (
	ErrEmptyInput       = errors.New("column: empty input")
	ErrCorruptData      = errors.New("column: corrupt data")
	ErrTooManyUnique    = errors.New("column: too many unique values for dict encoding")
	ErrInvalidEncoding  = errors.New("column: invalid encoding type marker")
	ErrInsufficientData = errors.New("column: insufficient data")
)

// maxDecodedColumnBytes caps the decompressed size of a single column block.
// A row-group column never approaches this; the bound exists to reject a
// malformed header that claims an implausible size before allocating for it.
const maxDecodedColumnBytes = 1 << 30 // 1 GiB

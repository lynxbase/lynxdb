package column

import (
	"encoding/binary"
	"fmt"
	"math"
)

// GorillaEncoder implements Gorilla XOR compression for float64 columns.
// Based on Facebook's Gorilla paper: stores the first value verbatim, then
// XORs each value with the previous and encodes the XOR result efficiently.
//
// Since we need bit-level packing, we implement a simple bitstream.
//
// Wire format:
//
//	[1B encoding type] [4B count] [8B first value] [packed XOR bits ...]
//
// XOR encoding per value:
//   - If XOR == 0: write 0 bit (same as previous)
//   - If XOR != 0: write 1 bit, then:
//   - [6 bits: leading zeros count] [6 bits: meaningful bits count] [meaningful bits]
type GorillaEncoder struct{}

var _ Float64Encoder = (*GorillaEncoder)(nil)

func NewGorillaEncoder() *GorillaEncoder {
	return &GorillaEncoder{}
}

func (g *GorillaEncoder) EncodeFloat64s(values []float64) ([]byte, error) {
	if len(values) == 0 {
		return nil, ErrEmptyInput
	}

	bw := newBitWriter(13 + len(values)*2) // estimate

	// Header.
	bw.writeByte(byte(EncodingGorilla))
	bw.writeUint32(uint32(len(values)))
	bw.writeUint64(math.Float64bits(values[0]))

	prev := math.Float64bits(values[0])
	for i := 1; i < len(values); i++ {
		curr := math.Float64bits(values[i])
		xor := prev ^ curr

		if xor == 0 {
			bw.writeBit(0) // Same as previous.
		} else {
			bw.writeBit(1) // Different.
			leading := countLeadingZeros64(xor)
			trailing := countTrailingZeros64(xor)
			meaningful := 64 - leading - trailing

			bw.writeBits(uint64(leading), 6)
			bw.writeBits(uint64(meaningful), 6)
			bw.writeBits(xor>>uint(trailing), meaningful)
		}
		prev = curr
	}

	return bw.bytes(), nil
}

func (g *GorillaEncoder) DecodeFloat64s(data []byte) ([]float64, error) {
	if len(data) < 13 {
		return nil, fmt.Errorf("%w: header too short", ErrCorruptData)
	}

	if EncodingType(data[0]) != EncodingGorilla {
		return nil, fmt.Errorf("%w: expected gorilla encoding, got %d", ErrInvalidEncoding, data[0])
	}

	count := binary.LittleEndian.Uint32(data[1:5])
	first := binary.LittleEndian.Uint64(data[5:13])

	result := make([]float64, count)
	result[0] = math.Float64frombits(first)

	if count == 1 {
		return result, nil
	}

	br := newBitReader(data[13:])
	prev := first

	for i := uint32(1); i < count; i++ {
		bit, err := br.readBit()
		if err != nil {
			return nil, fmt.Errorf("%w: reading control bit at index %d: %w", ErrCorruptData, i, err)
		}

		if bit == 0 {
			// Same as previous.
			result[i] = math.Float64frombits(prev)
		} else {
			leading, err := br.readBits(6)
			if err != nil {
				return nil, fmt.Errorf("%w: reading leading zeros at index %d: %w", ErrCorruptData, i, err)
			}
			meaningful, err := br.readBits(6)
			if err != nil {
				return nil, fmt.Errorf("%w: reading meaningful bits at index %d: %w", ErrCorruptData, i, err)
			}
			if meaningful == 0 {
				meaningful = 64 // Edge case: 0 means all 64 bits are meaningful.
			}
			trailing := 64 - int(leading) - int(meaningful)
			xorBits, err := br.readBits(int(meaningful))
			if err != nil {
				return nil, fmt.Errorf("%w: reading xor bits at index %d: %w", ErrCorruptData, i, err)
			}

			xor := xorBits << uint(trailing)
			curr := prev ^ xor
			prev = curr
			result[i] = math.Float64frombits(curr)
		}
	}

	return result, nil
}

func countLeadingZeros64(v uint64) int {
	if v == 0 {
		return 64
	}
	n := 0
	for v&(1<<63) == 0 {
		n++
		v <<= 1
	}

	return n
}

func countTrailingZeros64(v uint64) int {
	if v == 0 {
		return 64
	}
	n := 0
	for v&1 == 0 {
		n++
		v >>= 1
	}

	return n
}

// bitWriter writes individual bits to a byte buffer.
type bitWriter struct {
	buf     []byte
	current byte
	bitPos  uint8 // bits written in current byte (0-7)
}

func newBitWriter(estimatedBytes int) *bitWriter {
	return &bitWriter{buf: make([]byte, 0, estimatedBytes)}
}

func (w *bitWriter) writeBit(bit byte) {
	w.current |= (bit & 1) << (7 - w.bitPos)
	w.bitPos++
	if w.bitPos == 8 {
		w.buf = append(w.buf, w.current)
		w.current = 0
		w.bitPos = 0
	}
}

func (w *bitWriter) writeBits(value uint64, nbits int) {
	for i := nbits - 1; i >= 0; i-- {
		w.writeBit(byte((value >> uint(i)) & 1))
	}
}

func (w *bitWriter) writeByte(b byte) {
	w.writeBits(uint64(b), 8)
}

func (w *bitWriter) writeUint32(v uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	for _, b := range buf {
		w.writeByte(b)
	}
}

func (w *bitWriter) writeUint64(v uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	for _, b := range buf {
		w.writeByte(b)
	}
}

func (w *bitWriter) bytes() []byte {
	if w.bitPos > 0 {
		return append(w.buf, w.current)
	}

	return w.buf
}

// bitReader reads individual bits from a byte buffer.
type bitReader struct {
	data   []byte
	pos    int   // byte position
	bitPos uint8 // bit position in current byte (0-7)
}

func newBitReader(data []byte) *bitReader {
	return &bitReader{data: data}
}

func (r *bitReader) readBit() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("unexpected end of data")
	}
	bit := (r.data[r.pos] >> (7 - r.bitPos)) & 1
	r.bitPos++
	if r.bitPos == 8 {
		r.pos++
		r.bitPos = 0
	}

	return bit, nil
}

func (r *bitReader) readBits(nbits int) (uint64, error) {
	var value uint64
	for i := 0; i < nbits; i++ {
		bit, err := r.readBit()
		if err != nil {
			return 0, err
		}
		value = (value << 1) | uint64(bit)
	}

	return value, nil
}

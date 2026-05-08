package segment

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestUnit_Header_V2EmptyCaps_RoundTrips(t *testing.T) {
	header := makeHeader(LSG_FORMAT_MAJOR_V2, 0, 0)

	got, err := validateHeader(header)
	if err != nil {
		t.Fatalf("validateHeader: %v", err)
	}

	if got.major != LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("major = %d, want %d", got.major, LSG_FORMAT_MAJOR_V2)
	}
	if got.requiredCaps != 0 {
		t.Fatalf("requiredCaps = %#x, want 0", got.requiredCaps)
	}
	if got.optionalCaps != 0 {
		t.Fatalf("optionalCaps = %#x, want 0", got.optionalCaps)
	}
	if string(header[:4]) != MagicForMajor(LSG_FORMAT_MAJOR_V2) {
		t.Fatalf("magic = %q, want %q", header[:4], MagicForMajor(LSG_FORMAT_MAJOR_V2))
	}
	if binary.LittleEndian.Uint16(header[4:6]) != LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("encoded major = %d, want %d", binary.LittleEndian.Uint16(header[4:6]), LSG_FORMAT_MAJOR_V2)
	}
}

func TestUnit_Header_V2RangeBSIOptionalCaps_RoundTrips(t *testing.T) {
	header := makeHeader(LSG_FORMAT_MAJOR_V2, CapBit_ColumnZSTD, CapBit_RangeBSI)

	got, err := validateHeader(header)
	if err != nil {
		t.Fatalf("validateHeader: %v", err)
	}

	if got.major != LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("major = %d, want %d", got.major, LSG_FORMAT_MAJOR_V2)
	}
	if got.requiredCaps != CapBit_ColumnZSTD {
		t.Fatalf("requiredCaps = %#x, want %#x", got.requiredCaps, CapBit_ColumnZSTD)
	}
	if got.optionalCaps != CapBit_RangeBSI {
		t.Fatalf("optionalCaps = %#x, want %#x", got.optionalCaps, CapBit_RangeBSI)
	}
}

func TestUnit_Header_UnsupportedMajor_ReturnsUnsupportedMajor(t *testing.T) {
	data := make([]byte, LSG_MIN_FILE_SIZE)
	copy(data, makeHeader(9, 0, 0))

	err := ValidateSegmentHeader(data[:LSG_HEADER_SIZE], int64(len(data)))
	if !errors.Is(err, ErrUnsupportedMajor) {
		t.Fatalf("ValidateSegmentHeader error = %v, want ErrUnsupportedMajor", err)
	}
}

func TestUnit_Header_UnknownOptionalCapability_ReturnsUnsupportedCapability(t *testing.T) {
	data := make([]byte, LSG_MIN_FILE_SIZE)
	copy(data, makeHeader(LSG_FORMAT_MAJOR_V2, 0, 1<<63))

	err := ValidateSegmentHeader(data[:LSG_HEADER_SIZE], int64(len(data)))
	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("ValidateSegmentHeader error = %v, want ErrUnsupportedCapability", err)
	}
}

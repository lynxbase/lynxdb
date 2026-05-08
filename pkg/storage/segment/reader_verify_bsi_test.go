package segment

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

func TestIntegration_Reader_VerifyAllRangeBSIs_HealthySegment_ReturnsNil(t *testing.T) {
	events := makeRangeBSIEvents(t, 2048)
	data := writeRangeBSISegment(t, events, nil)
	r, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if err := r.VerifyAllRangeBSIs(); err != nil {
		t.Fatalf("VerifyAllRangeBSIs: %v", err)
	}
}

func TestIntegration_Reader_VerifyAllRangeBSIs_CorruptSection_ReturnsCorruptError(t *testing.T) {
	events := makeRangeBSIEvents(t, 2048)
	data := writeRangeBSISegment(t, events, nil)
	mutateFirstRangeBSIPayloadByteForReaderTest(t, data)

	r, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if err := r.VerifyAllRangeBSIs(); !errors.Is(err, index.ErrRangeSectionCorrupt) {
		t.Fatalf("VerifyAllRangeBSIs err = %v, want ErrRangeSectionCorrupt", err)
	}
}

func TestIntegration_Reader_VerifyAllRangeBSIs_V1Segment_ReturnsNil(t *testing.T) {
	data := writeSyntheticV1SegmentForRangeBSITest(t)
	r, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if err := r.VerifyAllRangeBSIs(); err != nil {
		t.Fatalf("VerifyAllRangeBSIs(V1): %v", err)
	}
}

func mutateFirstRangeBSIPayloadByteForReaderTest(t *testing.T, data []byte) {
	t.Helper()
	footer, err := DecodeFooter(data)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	for rgIdx, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength <= index.RangeSectionHeaderSize {
			continue
		}
		start := int(rg.PerColumnRangeOffset)
		end := start + int(rg.PerColumnRangeLength)
		if start < 0 || end > len(data) {
			t.Fatalf("row group %d range section [%d,%d) outside segment", rgIdx, start, end)
		}
		payloadOffset, payloadLen := firstRangeEntryPayloadForReaderTest(t, data[start:end])
		if payloadLen == 0 {
			t.Fatalf("row group %d first range entry payload is empty", rgIdx)
		}
		data[start+payloadOffset+payloadLen/2] ^= 0xff
		return
	}
	t.Fatal("no non-empty range BSI section found")
}

func firstRangeEntryPayloadForReaderTest(t *testing.T, section []byte) (int, int) {
	t.Helper()
	if len(section) < index.RangeSectionHeaderSize {
		t.Fatalf("range section length = %d, want at least %d", len(section), index.RangeSectionHeaderSize)
	}
	pos := index.RangeSectionHeaderSize
	if pos+2 > len(section) {
		t.Fatal("range entry truncated before name length")
	}
	nameLen := int(binary.LittleEndian.Uint16(section[pos : pos+2]))
	pos += 2
	const fixedAfterNameBeforePayloadLen = 1 + 1 + 8 + 8 + 1
	if pos+nameLen+fixedAfterNameBeforePayloadLen+4 > len(section) {
		t.Fatal("range entry truncated before payload length")
	}
	pos += nameLen + fixedAfterNameBeforePayloadLen
	payloadLen := int(binary.LittleEndian.Uint32(section[pos : pos+4]))
	pos += 4
	if pos+payloadLen+4 > len(section) {
		t.Fatal("range entry payload exceeds section")
	}
	return pos, payloadLen
}

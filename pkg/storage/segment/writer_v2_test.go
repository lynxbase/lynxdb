package segment

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestIntegration_Writer_V2BSIDisabled_EmitsLSG2WithEmptyRangeMetadata(t *testing.T) {
	data := writeTinyV2Segment(t)

	assertV2SegmentMetadataDefaults(t, data)
}

func TestIntegration_StreamWriter_V2Default_EmitsLSG2WithEmptyRangeMetadata(t *testing.T) {
	events := generateTestEvents(12)

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)
	sw.SetRowGroupSize(6)
	if err := sw.WriteRowGroup(events[:6]); err != nil {
		t.Fatalf("WriteRowGroup(0): %v", err)
	}
	if err := sw.WriteRowGroup(events[6:]); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}
	if _, err := sw.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	assertV2SegmentMetadataDefaults(t, buf.Bytes())
}

func assertV2SegmentMetadataDefaults(t *testing.T, data []byte) {
	t.Helper()

	if string(data[:4]) != MagicForMajor(LSG_FORMAT_MAJOR_V2) {
		t.Fatalf("magic = %q, want %q", data[:4], MagicForMajor(LSG_FORMAT_MAJOR_V2))
	}
	if got := binary.LittleEndian.Uint16(data[4:6]); got != LSG_FORMAT_MAJOR_V2 {
		t.Fatalf("header major = %d, want %d", got, LSG_FORMAT_MAJOR_V2)
	}
	if data[6] != 0 {
		t.Fatalf("header revision = %d, want 0", data[6])
	}

	footer, err := DecodeFooter(data)
	if err != nil {
		t.Fatalf("DecodeFooter: %v", err)
	}
	if footer.OptionalCaps != 0 {
		t.Fatalf("OptionalCaps = %#x, want 0", footer.OptionalCaps)
	}
	if footer.RequiredCaps&CapBit_RangeBSI != 0 {
		t.Fatalf("RequiredCaps has RangeBSI bit set: %#x", footer.RequiredCaps)
	}
	if len(footer.RowGroups) != 2 {
		t.Fatalf("row groups = %d, want 2", len(footer.RowGroups))
	}
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeOffset != 0 {
			t.Fatalf("RowGroups[%d].PerColumnRangeOffset = %d, want 0", i, rg.PerColumnRangeOffset)
		}
		if rg.PerColumnRangeLength != 0 {
			t.Fatalf("RowGroups[%d].PerColumnRangeLength = %d, want 0", i, rg.PerColumnRangeLength)
		}
	}
	for i, cat := range footer.Catalog {
		if cat.IndexProfile != IndexProfileDefault {
			t.Fatalf("Catalog[%d].IndexProfile = %d, want %d", i, cat.IndexProfile, IndexProfileDefault)
		}
	}
}

func writeTinyV2Segment(t *testing.T) []byte {
	t.Helper()

	events := generateTestEvents(12)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetRowGroupSize(6)
	w.SetIndexConfig(IndexConfig{DisableBSI: true})
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

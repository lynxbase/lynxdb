package segment

import (
	"bytes"
	"errors"
	"testing"
)

func TestIntegration_Reader_LoadRangeBSI_V1Segment_ReturnsNoIndex(t *testing.T) {
	data := writeSyntheticV1SegmentForRangeBSITest(t)
	r, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.HasRangeBSI() {
		t.Fatal("HasRangeBSI() = true, want false for V1 segment")
	}
	idx, err := r.LoadRangeBSI(0, "anything")
	if err != nil {
		t.Fatalf("LoadRangeBSI(V1): %v", err)
	}
	if idx != nil {
		t.Fatalf("LoadRangeBSI(V1) = %+v, want nil", idx)
	}
	if err := r.VerifyAllRangeBSIs(); err != nil {
		t.Fatalf("VerifyAllRangeBSIs(V1): %v", err)
	}
}

func TestIntegration_Reader_LoadRangeBSI_InvalidRowGroup_ReturnsInvalidRGIndex(t *testing.T) {
	data := writeSyntheticV1SegmentForRangeBSITest(t)
	r, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if _, err := r.LoadRangeBSI(-1, "anything"); !errors.Is(err, ErrInvalidRGIndex) {
		t.Fatalf("LoadRangeBSI(-1) err = %v, want ErrInvalidRGIndex", err)
	}
	if _, err := r.LoadRangeBSI(r.RowGroupCount(), "anything"); !errors.Is(err, ErrInvalidRGIndex) {
		t.Fatalf("LoadRangeBSI(out of range) err = %v, want ErrInvalidRGIndex", err)
	}
	if _, _, err := r.LoadRangeMeta(r.RowGroupCount(), "anything"); !errors.Is(err, ErrInvalidRGIndex) {
		t.Fatalf("LoadRangeMeta(out of range) err = %v, want ErrInvalidRGIndex", err)
	}
}

func writeSyntheticV1SegmentForRangeBSITest(t *testing.T) []byte {
	t.Helper()
	restore := defaultFormatMajor
	defaultFormatMajor = LSG_FORMAT_MAJOR_V1
	t.Cleanup(func() { defaultFormatMajor = restore })

	events := generateTestEvents(12)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SetRowGroupSize(6)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

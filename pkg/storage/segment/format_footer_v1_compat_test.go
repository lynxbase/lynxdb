package segment

import (
	"errors"
	"testing"
)

func TestUnit_FooterV1_DispatchDecode_DefaultsV2Fields(t *testing.T) {
	want := makeFooterV2Fixture(2, 3)

	got, err := decodeFooterForMajor(encodeFooterV1(want), LSG_FORMAT_MAJOR_V1)
	if err != nil {
		t.Fatalf("decodeFooterForMajor(v1): %v", err)
	}

	if got.EventCount != want.EventCount {
		t.Fatalf("EventCount = %d, want %d", got.EventCount, want.EventCount)
	}
	for i, rg := range got.RowGroups {
		if rg.PerColumnRangeOffset != 0 {
			t.Fatalf("RowGroups[%d].PerColumnRangeOffset = %d, want 0", i, rg.PerColumnRangeOffset)
		}
		if rg.PerColumnRangeLength != 0 {
			t.Fatalf("RowGroups[%d].PerColumnRangeLength = %d, want 0", i, rg.PerColumnRangeLength)
		}
	}
	for i, cat := range got.Catalog {
		if cat.IndexProfile != IndexProfileDefault {
			t.Fatalf("Catalog[%d].IndexProfile = %d, want %d", i, cat.IndexProfile, IndexProfileDefault)
		}
	}
}

func TestUnit_FooterV1_FooterOnlyDecode_FallsBackFromDefaultV2(t *testing.T) {
	want := makeFooterV2Fixture(1, 2)

	got, err := DecodeFooter(encodeFooterV1(want))
	if err != nil {
		t.Fatalf("DecodeFooter(v1 footer-only): %v", err)
	}

	if got.EventCount != want.EventCount {
		t.Fatalf("EventCount = %d, want %d", got.EventCount, want.EventCount)
	}
	for i, rg := range got.RowGroups {
		if rg.PerColumnRangeOffset != 0 || rg.PerColumnRangeLength != 0 {
			t.Fatalf("RowGroups[%d] range fields = (%d,%d), want zeroes",
				i, rg.PerColumnRangeOffset, rg.PerColumnRangeLength)
		}
	}
	for i, cat := range got.Catalog {
		if cat.IndexProfile != IndexProfileDefault {
			t.Fatalf("Catalog[%d].IndexProfile = %d, want %d", i, cat.IndexProfile, IndexProfileDefault)
		}
	}
}

func TestUnit_FooterV1_DecodeAsV2Rejected(t *testing.T) {
	footer := makeFooterV2Fixture(1, 2)

	_, err := decodeFooterV2(encodeFooterV1(footer))
	if !errors.Is(err, ErrCorruptSegment) {
		t.Fatalf("decodeFooterV2(v1 bytes) error = %v, want ErrCorruptSegment", err)
	}
}

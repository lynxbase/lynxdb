package segment

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

func TestUnit_FooterV2_EmptyRangeFields_RoundTrips(t *testing.T) {
	want := makeFooterV2Fixture(3, 4)

	got, err := decodeFooterV2(encodeFooterV2(want))
	if err != nil {
		t.Fatalf("decodeFooterV2: %v", err)
	}

	assertFooterEqual(t, want, got)
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

func TestUnit_FooterV2_RangeFieldsAndIndexProfile_RoundTrips(t *testing.T) {
	want := makeFooterV2Fixture(3, 4)
	want.RowGroups[1].PerColumnRangeOffset = 4096
	want.RowGroups[1].PerColumnRangeLength = 256
	want.Catalog[2].IndexProfile = IndexProfileRangeBSI
	want.OptionalCaps = CapBit_RangeBSI

	got, err := decodeFooterV2(encodeFooterV2(want))
	if err != nil {
		t.Fatalf("decodeFooterV2: %v", err)
	}

	assertFooterEqual(t, want, got)
}

func TestUnit_AggregateCapabilities_RealRangeSectionSetsOptionalRangeBSI(t *testing.T) {
	rowGroups := []RowGroupMeta{
		{RequiredCapabilities: CapBit_ColumnZSTD},
		{PerColumnRangeOffset: 4096, PerColumnRangeLength: 128},
	}

	required, optional := aggregateCapabilities(rowGroups)
	if required != CapBit_ColumnZSTD {
		t.Fatalf("required = %#x, want %#x", required, CapBit_ColumnZSTD)
	}
	if optional != CapBit_RangeBSI {
		t.Fatalf("optional = %#x, want %#x", optional, CapBit_RangeBSI)
	}
}

func TestUnit_AggregateCapabilities_HeaderOnlyRangeSectionDoesNotSetOptionalRangeBSI(t *testing.T) {
	rowGroups := []RowGroupMeta{
		{PerColumnRangeOffset: 4096, PerColumnRangeLength: index.RangeSectionHeaderSize},
	}

	required, optional := aggregateCapabilities(rowGroups)
	if required != 0 {
		t.Fatalf("required = %#x, want 0", required)
	}
	if optional != 0 {
		t.Fatalf("optional = %#x, want 0", optional)
	}
}

func TestUnit_RequiredCapsForRowGroup_RangeBSINotRequired(t *testing.T) {
	rg := RowGroupMeta{
		PerColumnRangeOffset: 4096,
		PerColumnRangeLength: 128,
		Columns: []ColumnChunkMeta{{
			Name:        "status",
			Compression: CompressionNone,
		}},
	}

	if got := requiredCapsForRowGroup(rg); got != 0 {
		t.Fatalf("requiredCapsForRowGroup = %#x, want 0", got)
	}
}

func makeFooterV2Fixture(rgCount, catalogCount int) *Footer {
	f := &Footer{
		EventCount:         int64(rgCount * 10),
		RequiredCaps:       0,
		OptionalCaps:       0,
		RowGroups:          make([]RowGroupMeta, rgCount),
		InvertedOffset:     8192,
		InvertedLength:     512,
		PrimaryIndexOffset: 8704,
		PrimaryIndexLength: 96,
		Catalog:            make([]CatalogEntry, catalogCount),
	}
	for i := range f.RowGroups {
		f.RowGroups[i] = RowGroupMeta{
			RowCount:             10 + i,
			ColumnPresenceBits:   uint64(1 << uint(i%8)),
			PerColumnBloomOffset: int64(1024 + i*100),
			PerColumnBloomLength: int64(40 + i),
			Columns: []ColumnChunkMeta{{
				Name:         "field_a",
				EncodingType: 1,
				Compression:  CompressionLZ4,
				Offset:       int64(128 + i*16),
				Length:       int64(12 + i),
				RawSize:      int64(20 + i),
				CRC32:        uint32(1000 + i),
				MinValue:     "1",
				MaxValue:     "9",
				Count:        int64(10 + i),
				NullCount:    int64(i % 2),
			}},
			ConstColumns: []ConstColumnEntry{{
				Name:         "host",
				EncodingType: 1,
				Value:        "web-01",
			}},
		}
	}
	for i := range f.Catalog {
		f.Catalog[i] = CatalogEntry{
			Name:         "catalog_field_" + string(rune('a'+i)),
			DominantType: uint8(i + 1),
			IndexProfile: IndexProfileDefault,
		}
	}
	return f
}

func assertFooterEqual(t *testing.T, want, got *Footer) {
	t.Helper()
	if got.EventCount != want.EventCount {
		t.Fatalf("EventCount = %d, want %d", got.EventCount, want.EventCount)
	}
	if got.RequiredCaps != want.RequiredCaps {
		t.Fatalf("RequiredCaps = %#x, want %#x", got.RequiredCaps, want.RequiredCaps)
	}
	if got.OptionalCaps != want.OptionalCaps {
		t.Fatalf("OptionalCaps = %#x, want %#x", got.OptionalCaps, want.OptionalCaps)
	}
	if got.InvertedOffset != want.InvertedOffset || got.InvertedLength != want.InvertedLength {
		t.Fatalf("inverted range = (%d,%d), want (%d,%d)",
			got.InvertedOffset, got.InvertedLength, want.InvertedOffset, want.InvertedLength)
	}
	if got.PrimaryIndexOffset != want.PrimaryIndexOffset || got.PrimaryIndexLength != want.PrimaryIndexLength {
		t.Fatalf("primary range = (%d,%d), want (%d,%d)",
			got.PrimaryIndexOffset, got.PrimaryIndexLength, want.PrimaryIndexOffset, want.PrimaryIndexLength)
	}
	assertCatalogEqual(t, want.Catalog, got.Catalog)
	assertRowGroupsEqual(t, want.RowGroups, got.RowGroups)
}

func assertCatalogEqual(t *testing.T, want, got []CatalogEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("catalog len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("catalog[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func assertRowGroupsEqual(t *testing.T, want, got []RowGroupMeta) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row group len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		wrg, grg := want[i], got[i]
		if grg.RowCount != wrg.RowCount ||
			grg.ColumnPresenceBits != wrg.ColumnPresenceBits ||
			grg.PerColumnBloomOffset != wrg.PerColumnBloomOffset ||
			grg.PerColumnBloomLength != wrg.PerColumnBloomLength ||
			grg.PerColumnRangeOffset != wrg.PerColumnRangeOffset ||
			grg.PerColumnRangeLength != wrg.PerColumnRangeLength ||
			grg.RequiredCapabilities != wrg.RequiredCapabilities {
			t.Fatalf("row group[%d] metadata = %+v, want %+v", i, grg, wrg)
		}
		assertColumnChunksEqual(t, i, wrg.Columns, grg.Columns)
		assertConstColumnsEqual(t, i, wrg.ConstColumns, grg.ConstColumns)
	}
}

func assertColumnChunksEqual(t *testing.T, rg int, want, got []ColumnChunkMeta) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row group[%d] columns len = %d, want %d", rg, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row group[%d] column[%d] = %+v, want %+v", rg, i, got[i], want[i])
		}
	}
}

func assertConstColumnsEqual(t *testing.T, rg int, want, got []ConstColumnEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row group[%d] const columns len = %d, want %d", rg, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row group[%d] const column[%d] = %+v, want %+v", rg, i, got[i], want[i])
		}
	}
}

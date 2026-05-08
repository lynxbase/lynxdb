package segment

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

func TestIntegration_Writer_DisableBSI_WritesNoRangeSectionsAndClearsCap(t *testing.T) {
	events := makeRangeBSIEvents(t, 1024)
	data := writeRangeBSISegment(t, events, func(w *Writer) {
		w.SetIndexConfig(IndexConfig{DisableBSI: true})
	})

	footer, _ := rangeSectionsFromFooter(t, data)
	if footer.OptionalCaps&CapBit_RangeBSI != 0 {
		t.Fatalf("OptionalCaps = %#x, want RangeBSI clear", footer.OptionalCaps)
	}
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength != 0 {
			t.Fatalf("row group %d PerColumnRangeLength = %d, want 0", i, rg.PerColumnRangeLength)
		}
	}
}

func TestIntegration_Writer_ProfileOverride_InvalidStringColumnDemotesToDefault(t *testing.T) {
	events := makeRangeBSIEvents(t, 1024)
	data := writeRangeBSISegment(t, events, func(w *Writer) {
		w.SetIndexConfig(IndexConfig{
			ProfileOverrides: map[string]IndexProfile{
				"_time": IndexProfileDefault,
				"_raw":  IndexProfileRangeBSI,
			},
			BSIMaxBitCount: 64,
		})
	})

	footer, sections := rangeSectionsFromFooter(t, data)
	rawCatalog := catalogEntryByNameSegmentTest(t, footer, "_raw")
	if rawCatalog.IndexProfile != IndexProfileDefault {
		t.Fatalf("_raw IndexProfile = %d, want default", rawCatalog.IndexProfile)
	}
	for i, sectionBytes := range sections {
		section := parseRangeSectionSegmentTest(t, sectionBytes)
		for _, entry := range section.Entries {
			if entry.Name == "_raw" {
				t.Fatalf("row group %d unexpectedly wrote _raw BSI entry", i)
			}
		}
	}
}

func TestIntegration_Writer_BSIMaxBitCount_SkipsTooWideColumns(t *testing.T) {
	events := makeRangeBSIEvents(t, 1024)
	data := writeRangeBSISegment(t, events, func(w *Writer) {
		w.SetIndexConfig(IndexConfig{
			ProfileOverrides: map[string]IndexProfile{
				"_time":       IndexProfileRangeBSI,
				"duration_ms": IndexProfileRangeBSI,
				"latency":     IndexProfileDefault,
			},
			BSIMaxBitCount: 1,
		})
	})

	footer, sections := rangeSectionsFromFooter(t, data)
	if footer.OptionalCaps&CapBit_RangeBSI != 0 {
		t.Fatalf("OptionalCaps = %#x, want RangeBSI clear when all candidates exceed bit cap", footer.OptionalCaps)
	}
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength != index.RangeSectionHeaderSize {
			t.Fatalf("row group %d range length = %d, want header-only %d",
				i, rg.PerColumnRangeLength, index.RangeSectionHeaderSize)
		}
		if section := parseRangeSectionSegmentTest(t, sections[i]); section.Count != 0 {
			t.Fatalf("row group %d bsiCount = %d, want 0", i, section.Count)
		}
	}
}

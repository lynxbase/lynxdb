package segment

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

func TestIntegration_Writer_BSICapability_ClearWhenDisabled(t *testing.T) {
	events := makeRangeBSIEvents(t, 1024)
	data := writeRangeBSISegment(t, events, func(w *Writer) {
		w.SetIndexConfig(IndexConfig{DisableBSI: true})
	})

	footer, _ := rangeSectionsFromFooter(t, data)
	if footer.OptionalCaps&CapBit_RangeBSI != 0 {
		t.Fatalf("OptionalCaps = %#x, want RangeBSI clear", footer.OptionalCaps)
	}
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeOffset != 0 || rg.PerColumnRangeLength != 0 {
			t.Fatalf("row group %d range metadata = (%d,%d), want (0,0)",
				i, rg.PerColumnRangeOffset, rg.PerColumnRangeLength)
		}
	}
}

func TestIntegration_Writer_BSICapability_ClearForHeaderOnlySections(t *testing.T) {
	events := makeRangeBSIEvents(t, 1024)
	data := writeRangeBSISegment(t, events, func(w *Writer) {
		w.SetIndexConfig(IndexConfig{
			ProfileOverrides: map[string]IndexProfile{
				"_time":       IndexProfileDefault,
				"duration_ms": IndexProfileDefault,
				"latency":     IndexProfileDefault,
			},
			BSIMaxBitCount: 64,
		})
	})

	footer, sections := rangeSectionsFromFooter(t, data)
	if footer.OptionalCaps&CapBit_RangeBSI != 0 {
		t.Fatalf("OptionalCaps = %#x, want RangeBSI clear", footer.OptionalCaps)
	}
	for i, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength != index.RangeSectionHeaderSize {
			t.Fatalf("row group %d range length = %d, want header-only %d",
				i, rg.PerColumnRangeLength, index.RangeSectionHeaderSize)
		}
		section := parseRangeSectionSegmentTest(t, sections[i])
		if section.Count != 0 {
			t.Fatalf("row group %d bsiCount = %d, want 0", i, section.Count)
		}
	}
}

func TestIntegration_Writer_BSICapability_SetWhenAnyRealBSIExists(t *testing.T) {
	events := makeRangeBSIEvents(t, 1024)
	data := writeRangeBSISegment(t, events, nil)

	footer, _ := rangeSectionsFromFooter(t, data)
	if footer.OptionalCaps&CapBit_RangeBSI == 0 {
		t.Fatalf("OptionalCaps = %#x, want RangeBSI set", footer.OptionalCaps)
	}
	realSections := 0
	for _, rg := range footer.RowGroups {
		if rg.PerColumnRangeLength > index.RangeSectionHeaderSize {
			realSections++
		}
		if rg.RequiredCapabilities&CapBit_RangeBSI != 0 {
			t.Fatalf("row group required caps include RangeBSI: %#x", rg.RequiredCapabilities)
		}
	}
	if realSections == 0 {
		t.Fatal("no real range BSI sections found")
	}
}

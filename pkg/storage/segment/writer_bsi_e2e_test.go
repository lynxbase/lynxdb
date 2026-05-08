package segment

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage/segment/index"
)

func TestIntegration_Writer_BSISections_DecodeToSourceValues(t *testing.T) {
	events := makeRangeBSIEvents(t, 2048)
	data := writeRangeBSISegment(t, events, nil)

	footer, sections := rangeSectionsFromFooter(t, data)
	if footer.OptionalCaps&CapBit_RangeBSI == 0 {
		t.Fatalf("OptionalCaps = %#x, want RangeBSI bit set", footer.OptionalCaps)
	}
	if got := catalogEntryByNameSegmentTest(t, footer, "_time").IndexProfile; got != IndexProfileRangeBSI {
		t.Fatalf("_time IndexProfile = %d, want %d", got, IndexProfileRangeBSI)
	}
	if got := catalogEntryByNameSegmentTest(t, footer, "duration_ms").IndexProfile; got != IndexProfileRangeBSI {
		t.Fatalf("duration_ms IndexProfile = %d, want %d", got, IndexProfileRangeBSI)
	}

	for rgIdx, sectionBytes := range sections {
		if len(sectionBytes) <= index.RangeSectionHeaderSize {
			t.Fatalf("row group %d range section length = %d, want > %d",
				rgIdx, len(sectionBytes), index.RangeSectionHeaderSize)
		}
		section := parseRangeSectionSegmentTest(t, sectionBytes)
		if section.Count < 2 {
			t.Fatalf("row group %d bsiCount = %d, want at least _time and duration_ms", rgIdx, section.Count)
		}

		for _, entry := range section.Entries {
			assertRangeEntryCRCValidSegmentTest(t, entry)
			if entry.Layout != 0 {
				t.Fatalf("row group %d entry %q layout = %d, want 0", rgIdx, entry.Name, entry.Layout)
			}
			_ = decodeRangeBSIEntrySegmentTest(t, entry)
		}

		start := rgIdx * 512
		timeEntry := rangeEntryByNameSegmentTest(t, section, "_time")
		timeBSI := decodeRangeBSIEntrySegmentTest(t, timeEntry)
		durationEntry := rangeEntryByNameSegmentTest(t, section, "duration_ms")
		durationBSI := decodeRangeBSIEntrySegmentTest(t, durationEntry)

		for _, localRow := range []uint64{0, 1, 17, 128, 255, 511} {
			if start+int(localRow) >= len(events) {
				continue
			}
			gotTimeOffset, ok := timeBSI.GetValue(localRow)
			if !ok {
				t.Fatalf("row group %d _time row %d missing", rgIdx, localRow)
			}
			wantTime := events[start+int(localRow)].Time.UnixNano()
			if gotTime := timeEntry.MinValue + gotTimeOffset; gotTime != wantTime {
				t.Fatalf("row group %d _time row %d = %d, want %d", rgIdx, localRow, gotTime, wantTime)
			}

			gotDurationOffset, ok := durationBSI.GetValue(localRow)
			if !ok {
				t.Fatalf("row group %d duration_ms row %d missing", rgIdx, localRow)
			}
			wantDuration, _ := events[start+int(localRow)].GetField("duration_ms").TryAsInt()
			if gotDuration := durationEntry.MinValue + gotDurationOffset; gotDuration != wantDuration {
				t.Fatalf("row group %d duration_ms row %d = %d, want %d", rgIdx, localRow, gotDuration, wantDuration)
			}
		}
	}
}

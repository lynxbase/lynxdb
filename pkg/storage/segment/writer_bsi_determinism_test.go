package segment

import (
	"bytes"
	"testing"
)

func TestIntegration_Writer_BSISections_AreByteStableAcrossWrites(t *testing.T) {
	events := makeRangeBSIEvents(t, 2048)
	first := writeRangeBSISegment(t, events, nil)
	second := writeRangeBSISegment(t, events, nil)

	firstFooter, firstSections := rangeSectionsFromFooter(t, first)
	secondFooter, secondSections := rangeSectionsFromFooter(t, second)
	if len(firstFooter.RowGroups) != len(secondFooter.RowGroups) {
		t.Fatalf("row groups = %d and %d", len(firstFooter.RowGroups), len(secondFooter.RowGroups))
	}
	for rg := range firstSections {
		if !bytes.Equal(firstSections[rg], secondSections[rg]) {
			t.Fatalf("row group %d LSRB sections differ across identical writes", rg)
		}
	}
}

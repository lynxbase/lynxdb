package segment

import (
	"testing"

	bsi "github.com/RoaringBitmap/roaring/BitSliceIndexing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestIntegration_Reader_LoadRangeBSI_DecodesSourceValuesAndReusesCache(t *testing.T) {
	events := makeRangeBSIEvents(t, 32*1024)
	data := writeRangeBSISegment(t, events, nil)

	r, err := OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if !r.HasRangeBSI() {
		t.Fatal("HasRangeBSI() = false, want true")
	}

	columns := rangeBSICatalogColumnsForReaderTest(t, r)
	if len(columns) < 2 {
		t.Fatalf("range BSI catalog columns = %v, want at least two", columns)
	}

	globalStart := 0
	for rgIdx, rg := range r.footer.RowGroups {
		for _, col := range columns {
			idx, err := r.LoadRangeBSI(rgIdx, col)
			if err != nil {
				t.Fatalf("LoadRangeBSI(%d, %q): %v", rgIdx, col, err)
			}
			if idx == nil {
				t.Fatalf("LoadRangeBSI(%d, %q) = nil, want BSI", rgIdx, col)
			}
			second, err := r.LoadRangeBSI(rgIdx, col)
			if err != nil {
				t.Fatalf("LoadRangeBSI(%d, %q) second call: %v", rgIdx, col, err)
			}
			if second != idx {
				t.Fatalf("LoadRangeBSI(%d, %q) did not reuse cached pointer", rgIdx, col)
			}

			meta, ok, err := r.LoadRangeMeta(rgIdx, col)
			if err != nil {
				t.Fatalf("LoadRangeMeta(%d, %q): %v", rgIdx, col, err)
			}
			if !ok {
				t.Fatalf("LoadRangeMeta(%d, %q) ok = false, want true", rgIdx, col)
			}
			assertReaderRangeBSISamples(t, events, rgIdx, globalStart, rg.RowCount, col, meta, idx)
		}
		globalStart += rg.RowCount
	}
}

func assertReaderRangeBSISamples(t *testing.T, events []*event.Event, rgIdx, globalStart, rowCount int, col string, meta rangeMeta, idx *bsi.BSI) {
	t.Helper()
	for _, localRow := range sampleLocalRowsForReaderTest(rowCount, 32) {
		globalRow := globalStart + localRow
		if globalRow >= len(events) {
			continue
		}
		gotOffset, ok := idx.GetValue(uint64(localRow))
		if !ok {
			t.Fatalf("rg %d col %q local row %d missing BSI value", rgIdx, col, localRow)
		}
		wantRaw, ok := rawRangeBSIValue(events[globalRow], col, meta.ValueKind)
		if !ok {
			t.Fatalf("source row %d col %q has no raw range value", globalRow, col)
		}
		if gotRaw := meta.MinValue + gotOffset; gotRaw != wantRaw {
			t.Fatalf("rg %d col %q local row %d raw value = %d, want %d",
				rgIdx, col, localRow, gotRaw, wantRaw)
		}
	}
}

func rangeBSICatalogColumnsForReaderTest(t *testing.T, r *Reader) []string {
	t.Helper()
	var columns []string
	for _, cat := range r.footer.Catalog {
		if cat.IndexProfile == IndexProfileRangeBSI {
			columns = append(columns, cat.Name)
		}
	}
	return columns
}

func sampleLocalRowsForReaderTest(rowCount, samples int) []int {
	if rowCount <= 0 || samples <= 0 {
		return nil
	}
	if rowCount <= samples {
		rows := make([]int, rowCount)
		for i := range rows {
			rows[i] = i
		}
		return rows
	}

	rows := make([]int, 0, samples)
	seen := make(map[int]struct{}, samples)
	for i := 0; i < samples; i++ {
		row := i * (rowCount - 1) / (samples - 1)
		if _, ok := seen[row]; ok {
			continue
		}
		seen[row] = struct{}{}
		rows = append(rows, row)
	}
	return rows
}

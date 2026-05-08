package optimizer

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

type testSegmentSet struct {
	readers []*segment.Reader
}

func (s testSegmentSet) Segments() []*segment.Reader {
	return s.readers
}

func TestUnit_Optimizer_LowerRangeToBSI_AllSegmentsHaveRangeBSI_TagsPredicate(t *testing.T) {
	query, cmp := rangeBSITestQuery()
	readers := []*segment.Reader{
		openRangeToBSITestSegment(t, true),
		openRangeToBSITestSegment(t, true),
	}

	got, changed := LowerRangeToBSI(query, testSegmentSet{readers: readers})
	if !changed {
		t.Fatal("LowerRangeToBSI changed = false, want true")
	}
	preds := rangeBSIAnnotation(t, got)
	if len(preds) != 1 {
		t.Fatalf("range predicate count = %d, want 1", len(preds))
	}
	if !preds[0].LoweredToBSI {
		t.Fatalf("LoweredToBSI = false, want true for all-BSI scan set")
	}
	if !cmp.LoweredToBSI {
		t.Fatalf("CompareExpr.LoweredToBSI = false, want true")
	}
}

func TestUnit_Optimizer_LowerRangeToBSI_MixedV1AndV2_LeavesPredicateUntagged(t *testing.T) {
	query, cmp := rangeBSITestQuery()
	v1 := openRangeToBSIV1Fixture(t)
	v2 := openRangeToBSITestSegment(t, true)

	got, changed := LowerRangeToBSI(query, testSegmentSet{readers: []*segment.Reader{v2, v1}})
	if changed {
		t.Fatal("LowerRangeToBSI changed = true, want false for mixed V1/V2 scan set")
	}
	preds := rangeBSIAnnotation(t, got)
	if preds[0].LoweredToBSI {
		t.Fatalf("LoweredToBSI = true, want false when one segment is V1")
	}
	if cmp.LoweredToBSI {
		t.Fatalf("CompareExpr.LoweredToBSI = true, want false")
	}
}

func TestUnit_Optimizer_LowerRangeToBSI_OneV2SegmentDefaultProfile_LeavesPredicateUntagged(t *testing.T) {
	query, cmp := rangeBSITestQuery()
	withBSI := openRangeToBSITestSegment(t, true)
	withoutBSI := openRangeToBSITestSegment(t, false)

	got, changed := LowerRangeToBSI(query, testSegmentSet{readers: []*segment.Reader{withBSI, withoutBSI}})
	if changed {
		t.Fatal("LowerRangeToBSI changed = true, want false when one V2 segment lacks status BSI")
	}
	preds := rangeBSIAnnotation(t, got)
	if preds[0].LoweredToBSI {
		t.Fatalf("LoweredToBSI = true, want false when one V2 segment has default profile")
	}
	if cmp.LoweredToBSI {
		t.Fatalf("CompareExpr.LoweredToBSI = true, want false")
	}
}

func rangeBSITestQuery() (*spl2.Query, *spl2.CompareExpr) {
	cmp := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "status"},
		Op:    ">=",
		Right: &spl2.LiteralExpr{Value: "500"},
	}
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.WhereCommand{Expr: cmp},
		},
	}
	q.Annotate(rangePredicatesAnnotation, []spl2.RangePredicate{{Field: "status", Min: "500"}})

	return q, cmp
}

func rangeBSIAnnotation(t *testing.T, q *spl2.Query) []spl2.RangePredicate {
	t.Helper()
	ann, ok := q.GetAnnotation(rangePredicatesAnnotation)
	if !ok {
		t.Fatal("missing rangePredicates annotation")
	}
	preds, ok := ann.([]spl2.RangePredicate)
	if !ok {
		t.Fatalf("rangePredicates annotation = %T, want []spl2.RangePredicate", ann)
	}

	return preds
}

func openRangeToBSITestSegment(t *testing.T, statusBSI bool) *segment.Reader {
	t.Helper()
	var buf bytes.Buffer
	w := segment.NewWriter(&buf)
	w.SetRowGroupSize(128)
	if statusBSI {
		w.SetIndexConfig(segment.IndexConfig{
			ProfileOverrides: map[string]segment.IndexProfile{
				"status": segment.IndexProfileRangeBSI,
			},
			BSIMaxBitCount: 64,
		})
	} else {
		w.SetIndexConfig(segment.IndexConfig{
			ProfileOverrides: map[string]segment.IndexProfile{
				"status": segment.IndexProfileDefault,
			},
			BSIMaxBitCount: 64,
		})
	}
	if _, err := w.Write(rangeToBSIEvents(t, 256)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := segment.OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	if statusBSI && r.IndexProfile("status") != segment.IndexProfileRangeBSI {
		t.Fatalf("status IndexProfile = %d, want RangeBSI", r.IndexProfile("status"))
	}
	if !statusBSI && r.IndexProfile("status") == segment.IndexProfileRangeBSI {
		t.Fatalf("status IndexProfile = RangeBSI, want default")
	}

	return r
}

func openRangeToBSIV1Fixture(t *testing.T) *segment.Reader {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "segments", "v1.lsg")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	major, err := segment.SegmentHeaderMajor(data, int64(len(data)))
	if err != nil {
		t.Fatalf("SegmentHeaderMajor: %v", err)
	}
	if major != segment.LSG_FORMAT_MAJOR_V1 {
		t.Fatalf("fixture major = %d, want V1", major)
	}
	r, err := segment.OpenSegment(data)
	if err != nil {
		t.Fatalf("OpenSegment(V1 fixture): %v", err)
	}
	if r.HasRangeBSI() {
		t.Fatal("V1 fixture HasRangeBSI() = true, want false")
	}

	return r
}

func rangeToBSIEvents(t *testing.T, n int) []*event.Event {
	t.Helper()
	base := time.Date(2026, 5, 8, 16, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		status := int64(200 + i%500)
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), "range-to-bsi")
		e.Index = "main"
		e.Source = "/var/log/range-to-bsi.log"
		e.SourceType = "json"
		e.Host = "range-to-bsi-host"
		e.SetField("status", event.IntValue(status))
		events[i] = e
	}

	return events
}

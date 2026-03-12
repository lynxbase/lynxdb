package pipeline

import (
	"fmt"
	"math"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestPartialAgg_CountOnly(t *testing.T) {
	events := makePartialAggEvents(
		map[string]event.Value{"host": event.StringValue("a")},
		map[string]event.Value{"host": event.StringValue("b")},
		map[string]event.Value{"host": event.StringValue("a")},
	)
	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs:   []PartialAggFunc{{Name: "count", Alias: "count"}},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(partials))
	}
	byHost := groupByKey(partials, "host")
	if byHost["a"].States[0].Count != 2 {
		t.Errorf("host=a count: expected 2, got %d", byHost["a"].States[0].Count)
	}
	if byHost["b"].States[0].Count != 1 {
		t.Errorf("host=b count: expected 1, got %d", byHost["b"].States[0].Count)
	}
}

func TestPartialAgg_Sum(t *testing.T) {
	events := makePartialAggEvents(
		map[string]event.Value{"host": event.StringValue("a"), "bytes": event.IntValue(100)},
		map[string]event.Value{"host": event.StringValue("a"), "bytes": event.IntValue(200)},
		map[string]event.Value{"host": event.StringValue("b"), "bytes": event.IntValue(50)},
	)
	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs:   []PartialAggFunc{{Name: "sum", Field: "bytes", Alias: "total"}},
	}
	partials := ComputePartialAgg(events, spec)
	byHost := groupByKey(partials, "host")
	if byHost["a"].States[0].Sum != 300 {
		t.Errorf("host=a sum: expected 300, got %f", byHost["a"].States[0].Sum)
	}
	if byHost["b"].States[0].Sum != 50 {
		t.Errorf("host=b sum: expected 50, got %f", byHost["b"].States[0].Sum)
	}
}

func TestPartialAgg_MinMax(t *testing.T) {
	events := makePartialAggEvents(
		map[string]event.Value{"v": event.IntValue(10)},
		map[string]event.Value{"v": event.IntValue(3)},
		map[string]event.Value{"v": event.IntValue(7)},
	)
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{
			{Name: "min", Field: "v", Alias: "min_v"},
			{Name: "max", Field: "v", Alias: "max_v"},
		},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 1 {
		t.Fatalf("expected 1 group, got %d", len(partials))
	}
	g := partials[0]
	if g.States[0].Min != event.IntValue(3) {
		t.Errorf("min: expected 3, got %v", g.States[0].Min)
	}
	if g.States[1].Max != event.IntValue(10) {
		t.Errorf("max: expected 10, got %v", g.States[1].Max)
	}
}

func TestPartialAgg_Avg(t *testing.T) {
	events := makePartialAggEvents(
		map[string]event.Value{"v": event.FloatValue(10)},
		map[string]event.Value{"v": event.FloatValue(20)},
		map[string]event.Value{"v": event.FloatValue(30)},
	)
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "avg", Field: "v", Alias: "avg_v"}},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 1 {
		t.Fatalf("expected 1 group, got %d", len(partials))
	}
	g := partials[0]
	if g.States[0].Count != 3 {
		t.Errorf("avg count: expected 3, got %d", g.States[0].Count)
	}
	if g.States[0].Sum != 60 {
		t.Errorf("avg sum: expected 60, got %f", g.States[0].Sum)
	}
}

func TestPartialAgg_EmptyInput(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "count", Alias: "count"}},
	}
	partials := ComputePartialAgg(nil, spec)
	if len(partials) != 0 {
		t.Errorf("expected 0 groups for empty input, got %d", len(partials))
	}
}

func TestPartialAgg_MultipleGroupBy(t *testing.T) {
	events := makePartialAggEvents(
		map[string]event.Value{"host": event.StringValue("a"), "method": event.StringValue("GET"), "v": event.IntValue(1)},
		map[string]event.Value{"host": event.StringValue("a"), "method": event.StringValue("POST"), "v": event.IntValue(2)},
		map[string]event.Value{"host": event.StringValue("a"), "method": event.StringValue("GET"), "v": event.IntValue(3)},
	)
	spec := &PartialAggSpec{
		GroupBy: []string{"host", "method"},
		Funcs:   []PartialAggFunc{{Name: "count", Alias: "count"}},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(partials))
	}
}

func TestMergePartialAggs_Count(t *testing.T) {
	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs:   []PartialAggFunc{{Name: "count", Alias: "count"}},
	}
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{"host": event.StringValue("a")}, States: []PartialAggState{{Count: 5}}},
		{Key: map[string]event.Value{"host": event.StringValue("b")}, States: []PartialAggState{{Count: 3}}},
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{"host": event.StringValue("a")}, States: []PartialAggState{{Count: 7}}},
		{Key: map[string]event.Value{"host": event.StringValue("c")}, States: []PartialAggState{{Count: 2}}},
	}
	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	byHost := rowsByKey(rows, "host")
	assertIntField(t, byHost["a"], "count", 12)
	assertIntField(t, byHost["b"], "count", 3)
	assertIntField(t, byHost["c"], "count", 2)
}

func TestMergePartialAggs_Sum(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "sum", Field: "v", Alias: "total"}},
	}
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Sum: 100, Count: 5}}},
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Sum: 200, Count: 10}}},
	}
	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	assertFloatField(t, rows[0], "total", 300)
}

func TestMergePartialAggs_Avg(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "avg", Field: "v", Alias: "avg_v"}},
	}
	// Segment 1: values [10, 20] → sum=30, count=2
	// Segment 2: values [30] → sum=30, count=1
	// Correct avg = 60/3 = 20 (NOT (15+30)/2 = 22.5)
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Sum: 30, Count: 2}}},
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Sum: 30, Count: 1}}},
	}
	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	assertFloatField(t, rows[0], "avg_v", 20)
}

func TestMergePartialAggs_MinMax(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{
			{Name: "min", Field: "v", Alias: "min_v"},
			{Name: "max", Field: "v", Alias: "max_v"},
		},
	}
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{
			{Min: event.IntValue(5)},
			{Max: event.IntValue(50)},
		}},
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{
			{Min: event.IntValue(3)},
			{Max: event.IntValue(100)},
		}},
	}
	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["min_v"] != event.IntValue(3) {
		t.Errorf("min_v: expected 3, got %v", rows[0]["min_v"])
	}
	if rows[0]["max_v"] != event.IntValue(100) {
		t.Errorf("max_v: expected 100, got %v", rows[0]["max_v"])
	}
}

func TestMergePartialAggs_EmptyPartials(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "count", Alias: "count"}},
	}
	rows := MergePartialAggs([][]*PartialAggGroup{nil, nil}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for no-group-by empty, got %d", len(rows))
	}
	assertIntField(t, rows[0], "count", 0)
}

func TestMergePartialAggs_EmptyWithGroupBy(t *testing.T) {
	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs:   []PartialAggFunc{{Name: "count", Alias: "count"}},
	}
	rows := MergePartialAggs([][]*PartialAggGroup{nil, nil}, spec)
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for group-by empty, got %d", len(rows))
	}
}

func TestMergePartialAggs_MultipleAggs(t *testing.T) {
	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs: []PartialAggFunc{
			{Name: "count", Alias: "count"},
			{Name: "sum", Field: "bytes", Alias: "total_bytes"},
			{Name: "avg", Field: "latency", Alias: "avg_latency"},
		},
	}
	p1 := []*PartialAggGroup{
		{
			Key: map[string]event.Value{"host": event.StringValue("web1")},
			States: []PartialAggState{
				{Count: 10},
				{Sum: 5000, Count: 10},
				{Sum: 100, Count: 10},
			},
		},
	}
	p2 := []*PartialAggGroup{
		{
			Key: map[string]event.Value{"host": event.StringValue("web1")},
			States: []PartialAggState{
				{Count: 20},
				{Sum: 15000, Count: 20},
				{Sum: 400, Count: 20},
			},
		},
	}
	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]
	assertIntField(t, row, "count", 30)
	assertFloatField(t, row, "total_bytes", 20000)
	// avg = (100+400)/(10+20) = 500/30 ≈ 16.667
	assertFloatFieldApprox(t, row, "avg_latency", 500.0/30.0)
}

// -- helpers --

func makePartialAggEvents(fieldSets ...map[string]event.Value) []*event.Event {
	events := make([]*event.Event, len(fieldSets))
	for i, fields := range fieldSets {
		events[i] = &event.Event{Fields: fields}
	}

	return events
}

func groupByKey(groups []*PartialAggGroup, field string) map[string]*PartialAggGroup {
	m := make(map[string]*PartialAggGroup)
	for _, g := range groups {
		key := g.Key[field].String()
		m[key] = g
	}

	return m
}

func rowsByKey(rows []map[string]event.Value, field string) map[string]map[string]event.Value {
	m := make(map[string]map[string]event.Value)
	for _, row := range rows {
		key := row[field].String()
		m[key] = row
	}

	return m
}

func assertIntField(t *testing.T, row map[string]event.Value, field string, expected int64) {
	t.Helper()
	v, ok := row[field]
	if !ok {
		t.Errorf("field %q not found in row", field)

		return
	}
	if v.Type() != event.FieldTypeInt {
		t.Errorf("field %q: expected int, got %s (%v)", field, v.Type(), v)

		return
	}
	if v.AsInt() != expected {
		t.Errorf("field %q: expected %d, got %d", field, expected, v.AsInt())
	}
}

func assertFloatField(t *testing.T, row map[string]event.Value, field string, expected float64) {
	t.Helper()
	v, ok := row[field]
	if !ok {
		t.Errorf("field %q not found in row", field)

		return
	}
	if v.Type() != event.FieldTypeFloat {
		t.Errorf("field %q: expected float, got %s (%v)", field, v.Type(), v)

		return
	}
	if v.AsFloat() != expected {
		t.Errorf("field %q: expected %f, got %f", field, expected, v.AsFloat())
	}
}

func assertFloatFieldApprox(t *testing.T, row map[string]event.Value, field string, expected float64) {
	t.Helper()
	v, ok := row[field]
	if !ok {
		t.Errorf("field %q not found in row", field)

		return
	}
	if v.Type() != event.FieldTypeFloat {
		t.Errorf("field %q: expected float, got %s (%v)", field, v.Type(), v)

		return
	}
	if math.Abs(v.AsFloat()-expected) > 1e-9 {
		t.Errorf("field %q: expected %f, got %f", field, expected, v.AsFloat())
	}
}

func TestMergePartialAggs_DC(t *testing.T) {
	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs:   []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}

	// Segment 1: {alice, bob}.
	p1 := []*PartialAggGroup{
		{
			Key: map[string]event.Value{"host": event.StringValue("web1")},
			States: []PartialAggState{{
				DistinctSet: map[string]bool{"alice": true, "bob": true},
			}},
		},
	}
	// Segment 2: {bob, charlie} — bob overlaps.
	p2 := []*PartialAggGroup{
		{
			Key: map[string]event.Value{"host": event.StringValue("web1")},
			States: []PartialAggState{{
				DistinctSet: map[string]bool{"bob": true, "charlie": true},
			}},
		},
	}

	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	// dc = |{alice, bob, charlie}| = 3 (NOT 2+2=4).
	assertIntField(t, rows[0], "dc_user", 3)
}

func TestMergePartialAggs_DCCountFallback(t *testing.T) {
	// When DistinctSet is empty but Count is set (backfill path),
	// finalizePartialState should return Count as the dc value.
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}
	p1 := []*PartialAggGroup{
		{
			Key: map[string]event.Value{},
			States: []PartialAggState{{
				Count:       5,
				DistinctSet: nil,
			}},
		},
	}

	rows := MergePartialAggs([][]*PartialAggGroup{p1}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	assertIntField(t, rows[0], "dc_user", 5)
}

func TestMergePartialAggs_DCCountFallbackMerge(t *testing.T) {
	// Two groups with only Count (no DistinctSet): counts should accumulate.
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Count: 5}}},
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Count: 3}}},
	}

	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	// Upper-bound fallback: 5+3=8.
	assertIntField(t, rows[0], "dc_user", 8)
}

func TestMergePartialAggs_DistinctSetNotMutated(t *testing.T) {
	// Bug #4: MergePartialAggs should deep-copy DistinctSet so the original
	// group is not mutated when subsequent merges modify the clone.
	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs:   []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}

	original := &PartialAggGroup{
		Key: map[string]event.Value{"host": event.StringValue("web1")},
		States: []PartialAggState{{
			DistinctSet: map[string]bool{"alice": true, "bob": true},
		}},
	}
	addition := &PartialAggGroup{
		Key: map[string]event.Value{"host": event.StringValue("web1")},
		States: []PartialAggState{{
			DistinctSet: map[string]bool{"charlie": true},
		}},
	}

	// The first partial has only 'original', which gets cloned.
	// The second partial has 'addition', which merges into the clone.
	_ = MergePartialAggs([][]*PartialAggGroup{{original}, {addition}}, spec)

	// Original's DistinctSet must NOT contain "charlie".
	if original.States[0].DistinctSet["charlie"] {
		t.Error("original DistinctSet was mutated: contains 'charlie' after merge")
	}
	if len(original.States[0].DistinctSet) != 2 {
		t.Errorf("original DistinctSet: got %d entries, want 2", len(original.States[0].DistinctSet))
	}
}

// --- HLL dc() tests ---

func TestPartialAgg_DC_HLLPromotion(t *testing.T) {
	// Generate enough events to trigger HLL promotion (> dcHLLThreshold = 10,000).
	var fieldSets []map[string]event.Value
	for i := 0; i < dcHLLThreshold+100; i++ {
		fieldSets = append(fieldSets, map[string]event.Value{
			"user": event.StringValue(fmt.Sprintf("user-%d", i)),
		})
	}
	events := makePartialAggEvents(fieldSets...)

	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 1 {
		t.Fatalf("expected 1 group, got %d", len(partials))
	}

	g := partials[0]
	// After promotion, DistinctSet should be nil and DistinctHLL should be set.
	if g.States[0].DistinctSet != nil {
		t.Error("expected DistinctSet to be nil after HLL promotion")
	}
	if g.States[0].DistinctHLL == nil {
		t.Fatal("expected DistinctHLL to be non-nil after promotion")
	}

	// Finalize — should be within ~1% of exact count.
	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]["dc_user"].AsInt()
	expected := int64(dcHLLThreshold + 100)
	errorPct := math.Abs(float64(got-expected)) / float64(expected) * 100
	if errorPct > 2.0 {
		t.Errorf("HLL dc() error too high: got %d, expected %d (%.2f%% error)", got, expected, errorPct)
	}
}

func TestMergePartialAggs_DC_ExactPlusHLL(t *testing.T) {
	// Cross-mode merge: one shard has exact set, other has HLL.
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}

	// Shard 1: exact set with 100 distinct values.
	exactSet := make(map[string]bool, 100)
	for i := 0; i < 100; i++ {
		exactSet[fmt.Sprintf("user-%d", i)] = true
	}
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{DistinctSet: exactSet}}},
	}

	// Shard 2: HLL with 15,000 distinct values.
	hll := NewHyperLogLog()
	for i := 0; i < 15000; i++ {
		hll.Add(fmt.Sprintf("user-%d", i)) // first 100 overlap with shard 1
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{DistinctHLL: hll}}},
	}

	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]["dc_user"].AsInt()
	// The exact answer is 15,000 (100 from shard 1 are a subset of shard 2's 15,000).
	// HLL estimate should be within ~2% of 15,000.
	expected := int64(15000)
	errorPct := math.Abs(float64(got-expected)) / float64(expected) * 100
	if errorPct > 3.0 {
		t.Errorf("cross-mode merge dc() error too high: got %d, expected %d (%.2f%% error)", got, expected, errorPct)
	}
}

func TestMergePartialAggs_DC_HLLPlusHLL(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}

	// Shard 1: HLL with users 0-9999.
	hll1 := NewHyperLogLog()
	for i := 0; i < 10000; i++ {
		hll1.Add(fmt.Sprintf("user-%d", i))
	}

	// Shard 2: HLL with users 5000-14999 (50% overlap).
	hll2 := NewHyperLogLog()
	for i := 5000; i < 15000; i++ {
		hll2.Add(fmt.Sprintf("user-%d", i))
	}

	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{DistinctHLL: hll1}}},
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{DistinctHLL: hll2}}},
	}

	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]["dc_user"].AsInt()
	// Exact: 15,000 unique users (0-14999).
	expected := int64(15000)
	errorPct := math.Abs(float64(got-expected)) / float64(expected) * 100
	if errorPct > 3.0 {
		t.Errorf("HLL+HLL merge dc() error too high: got %d, expected %d (%.2f%% error)", got, expected, errorPct)
	}
}

func TestMergePartialAggs_DC_HLLNotMutated(t *testing.T) {
	// Verify that merging doesn't mutate the original HLL.
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}

	hll := NewHyperLogLog()
	for i := 0; i < 1000; i++ {
		hll.Add(fmt.Sprintf("user-%d", i))
	}
	countBefore := hll.Count()

	original := &PartialAggGroup{
		Key:    map[string]event.Value{},
		States: []PartialAggState{{DistinctHLL: hll}},
	}
	addition := &PartialAggGroup{
		Key: map[string]event.Value{},
		States: []PartialAggState{{
			DistinctSet: map[string]bool{"extra-1": true, "extra-2": true},
		}},
	}

	_ = MergePartialAggs([][]*PartialAggGroup{{original}, {addition}}, spec)

	// Original HLL count should not change.
	countAfter := hll.Count()
	if countBefore != countAfter {
		t.Errorf("original HLL was mutated: count before=%d, after=%d", countBefore, countAfter)
	}
}

func TestPartialAgg_DC_BelowThreshold_Exact(t *testing.T) {
	// Below threshold, dc should use exact counting.
	var fieldSets []map[string]event.Value
	for i := 0; i < 50; i++ {
		fieldSets = append(fieldSets, map[string]event.Value{
			"user": event.StringValue(fmt.Sprintf("user-%d", i%25)), // 25 distinct
		})
	}
	events := makePartialAggEvents(fieldSets...)

	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 1 {
		t.Fatalf("expected 1 group, got %d", len(partials))
	}

	g := partials[0]
	if g.States[0].DistinctHLL != nil {
		t.Error("expected exact mode (no HLL) below threshold")
	}
	if len(g.States[0].DistinctSet) != 25 {
		t.Errorf("expected 25 distinct values, got %d", len(g.States[0].DistinctSet))
	}

	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	assertIntField(t, rows[0], "dc_user", 25)
}

// --- Percentile partial aggregation tests ---

func TestIsPushableAgg_Percentiles(t *testing.T) {
	for _, name := range []string{"perc50", "perc75", "perc90", "perc95", "perc99", "stdev"} {
		if !IsPushableAgg(name) {
			t.Errorf("IsPushableAgg(%q) = false, want true", name)
		}
	}
	// Non-pushable should still return false.
	for _, name := range []string{"values", "first", "last"} {
		if IsPushableAgg(name) {
			t.Errorf("IsPushableAgg(%q) = true, want false", name)
		}
	}
}

func TestPartialAgg_Perc99(t *testing.T) {
	// Create 100 events with values 1..100.
	var fieldSets []map[string]event.Value
	for i := 1; i <= 100; i++ {
		fieldSets = append(fieldSets, map[string]event.Value{
			"latency": event.FloatValue(float64(i)),
		})
	}
	events := makePartialAggEvents(fieldSets...)

	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "perc99", Field: "latency", Alias: "p99"}},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 1 {
		t.Fatalf("expected 1 group, got %d", len(partials))
	}
	if partials[0].States[0].Digest == nil {
		t.Fatal("expected Digest to be non-nil")
	}

	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]["p99"].AsFloat()
	// p99 of [1..100] should be ~99.
	if got < 95 || got > 100 {
		t.Errorf("perc99: expected ~99, got %f", got)
	}
}

func TestMergePartialAggs_Perc99(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "perc99", Field: "latency", Alias: "p99"}},
	}

	// Segment 1: values 1..50.
	td1 := NewTDigest(100)
	for i := 1; i <= 50; i++ {
		td1.Add(float64(i))
	}
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Digest: td1}}},
	}

	// Segment 2: values 51..100.
	td2 := NewTDigest(100)
	for i := 51; i <= 100; i++ {
		td2.Add(float64(i))
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{{Digest: td2}}},
	}

	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]["p99"].AsFloat()
	// p99 of [1..100] should be ~99.
	if got < 95 || got > 100 {
		t.Errorf("merged perc99: expected ~99, got %f", got)
	}
}

func TestPartialAgg_Perc50_EmptyInput(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "perc50", Field: "v", Alias: "p50"}},
	}
	partials := ComputePartialAgg(nil, spec)
	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if !rows[0]["p50"].IsNull() {
		t.Errorf("expected null for empty perc50, got %v", rows[0]["p50"])
	}
}

func TestPartialAgg_Perc50_GroupBy(t *testing.T) {
	var fieldSets []map[string]event.Value
	for i := 1; i <= 10; i++ {
		fieldSets = append(fieldSets, map[string]event.Value{
			"host": event.StringValue("a"), "v": event.FloatValue(float64(i)),
		})
	}
	for i := 91; i <= 100; i++ {
		fieldSets = append(fieldSets, map[string]event.Value{
			"host": event.StringValue("b"), "v": event.FloatValue(float64(i)),
		})
	}
	events := makePartialAggEvents(fieldSets...)

	spec := &PartialAggSpec{
		GroupBy: []string{"host"},
		Funcs:   []PartialAggFunc{{Name: "perc50", Field: "v", Alias: "p50"}},
	}
	partials := ComputePartialAgg(events, spec)
	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	byHost := rowsByKey(rows, "host")
	// Median of [1..10] ≈ 5.5; median of [91..100] ≈ 95.5.
	p50a := byHost["a"]["p50"].AsFloat()
	p50b := byHost["b"]["p50"].AsFloat()
	if p50a < 4 || p50a > 7 {
		t.Errorf("host=a p50: expected ~5.5, got %f", p50a)
	}
	if p50b < 93 || p50b > 97 {
		t.Errorf("host=b p50: expected ~95.5, got %f", p50b)
	}
}

// --- Stdev partial aggregation tests ---

func TestPartialAgg_Stdev(t *testing.T) {
	events := makePartialAggEvents(
		map[string]event.Value{"v": event.FloatValue(2)},
		map[string]event.Value{"v": event.FloatValue(4)},
		map[string]event.Value{"v": event.FloatValue(4)},
		map[string]event.Value{"v": event.FloatValue(4)},
		map[string]event.Value{"v": event.FloatValue(5)},
		map[string]event.Value{"v": event.FloatValue(5)},
		map[string]event.Value{"v": event.FloatValue(7)},
		map[string]event.Value{"v": event.FloatValue(9)},
	)
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "stdev", Field: "v", Alias: "sd"}},
	}
	partials := ComputePartialAgg(events, spec)
	if len(partials) != 1 {
		t.Fatalf("expected 1 group, got %d", len(partials))
	}
	if partials[0].States[0].Count != 8 {
		t.Errorf("expected count=8, got %d", partials[0].States[0].Count)
	}

	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]["sd"].AsFloat()
	// Sample stdev of [2,4,4,4,5,5,7,9]: mean=5, sum_sq_diff=32, s=sqrt(32/7)≈2.138
	if math.Abs(got-math.Sqrt(32.0/7.0)) > 0.01 {
		t.Errorf("stdev: expected ~2.138, got %f", got)
	}
}

func TestMergePartialAggs_Stdev(t *testing.T) {
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "stdev", Field: "v", Alias: "sd"}},
	}

	// Segment 1: values [2, 4, 4, 4].
	s1 := PartialAggState{}
	for _, v := range []float64{2, 4, 4, 4} {
		s1.Count++
		delta := v - s1.StdevMean
		s1.StdevMean += delta / float64(s1.Count)
		delta2 := v - s1.StdevMean
		s1.StdevM2 += delta * delta2
	}
	p1 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{s1}},
	}

	// Segment 2: values [5, 5, 7, 9].
	s2 := PartialAggState{}
	for _, v := range []float64{5, 5, 7, 9} {
		s2.Count++
		delta := v - s2.StdevMean
		s2.StdevMean += delta / float64(s2.Count)
		delta2 := v - s2.StdevMean
		s2.StdevM2 += delta * delta2
	}
	p2 := []*PartialAggGroup{
		{Key: map[string]event.Value{}, States: []PartialAggState{s2}},
	}

	rows := MergePartialAggs([][]*PartialAggGroup{p1, p2}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	got := rows[0]["sd"].AsFloat()
	// Sample stdev of [2,4,4,4,5,5,7,9]: mean=5, sum_sq_diff=32, s=sqrt(32/7)≈2.138
	if math.Abs(got-math.Sqrt(32.0/7.0)) > 0.01 {
		t.Errorf("merged stdev: expected ~2.138, got %f", got)
	}
}

func TestPartialAgg_Stdev_SingleValue(t *testing.T) {
	events := makePartialAggEvents(
		map[string]event.Value{"v": event.FloatValue(42)},
	)
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "stdev", Field: "v", Alias: "sd"}},
	}
	partials := ComputePartialAgg(events, spec)
	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if !rows[0]["sd"].IsNull() {
		t.Errorf("expected null for stdev of single value, got %v", rows[0]["sd"])
	}
}

func TestPartialAgg_MixedPushable(t *testing.T) {
	// Verify count, perc99, stdev all work together in one spec.
	var fieldSets []map[string]event.Value
	for i := 1; i <= 100; i++ {
		fieldSets = append(fieldSets, map[string]event.Value{
			"latency": event.FloatValue(float64(i)),
		})
	}
	events := makePartialAggEvents(fieldSets...)

	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{
			{Name: "count", Alias: "count"},
			{Name: "perc99", Field: "latency", Alias: "p99"},
			{Name: "stdev", Field: "latency", Alias: "sd"},
		},
	}
	partials := ComputePartialAgg(events, spec)
	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]
	assertIntField(t, row, "count", 100)
	p99 := row["p99"].AsFloat()
	if p99 < 95 || p99 > 100 {
		t.Errorf("p99: expected ~99, got %f", p99)
	}
	sd := row["sd"].AsFloat()
	// stdev of uniform [1..100] = ~29.01
	if sd < 28 || sd > 30 {
		t.Errorf("stdev: expected ~29, got %f", sd)
	}
}

func TestMergePartialAggs_TDigestNotMutated(t *testing.T) {
	// Verify that merging doesn't mutate the original TDigest.
	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "perc50", Field: "v", Alias: "p50"}},
	}

	td := NewTDigest(100)
	for i := 0; i < 100; i++ {
		td.Add(float64(i))
	}
	countBefore := td.Count()

	original := &PartialAggGroup{
		Key:    map[string]event.Value{},
		States: []PartialAggState{{Digest: td}},
	}
	addition := &PartialAggGroup{
		Key:    map[string]event.Value{},
		States: []PartialAggState{{Digest: NewTDigest(100)}},
	}
	addition.States[0].Digest.Add(999)

	_ = MergePartialAggs([][]*PartialAggGroup{{original}, {addition}}, spec)

	// Original TDigest count should not change.
	if td.Count() != countBefore {
		t.Errorf("original TDigest was mutated: count before=%.0f, after=%.0f", countBefore, td.Count())
	}
}

func TestPartialAgg_DC_HighCardinality_Accuracy(t *testing.T) {
	// Accuracy test: 50K events with 20K distinct values.
	// HLL should return within ~1% of 20,000.
	distinctCount := 20_000
	totalEvents := 50_000

	var fieldSets []map[string]event.Value
	for i := 0; i < totalEvents; i++ {
		fieldSets = append(fieldSets, map[string]event.Value{
			"user": event.StringValue(fmt.Sprintf("user-%d", i%distinctCount)),
		})
	}
	events := makePartialAggEvents(fieldSets...)

	spec := &PartialAggSpec{
		Funcs: []PartialAggFunc{{Name: "dc", Field: "user", Alias: "dc_user"}},
	}

	partials := ComputePartialAgg(events, spec)
	rows := MergePartialAggs([][]*PartialAggGroup{partials}, spec)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	got := rows[0]["dc_user"].AsInt()
	expected := int64(distinctCount)
	errorPct := math.Abs(float64(got-expected)) / float64(expected) * 100
	t.Logf("HLL accuracy: got=%d, expected=%d, error=%.2f%%", got, expected, errorPct)
	if errorPct > 2.0 {
		t.Errorf("HLL dc() error too high for 20K distinct values: %.2f%%", errorPct)
	}
}

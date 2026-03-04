package pipeline

import (
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

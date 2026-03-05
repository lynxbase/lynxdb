package segment

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// helpers

// makeSingleSourceSegment creates a 1-RG segment where all events have the same
// source, sourcetype, host, level, and status. String fields with uniform values
// become const columns; int fields become chunk columns with min=max zone maps.
func makeSingleSourceSegment(t *testing.T, source, level string, status int64, n int) *Reader {
	t.Helper()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		raw := fmt.Sprintf("ts=%s source=%s level=%s status=%d msg=ok",
			ts.Format(time.RFC3339Nano), source, level, status)
		e := event.NewEvent(ts, raw)
		e.Source = source
		e.SourceType = "json"
		e.Index = "main"
		e.Host = "web-01"
		e.SetField("level", event.StringValue(level))
		e.SetField("status", event.IntValue(status))
		events[i] = e
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	return r
}

// makeMultiValueSegment creates a segment where field values vary across events
// so that the column is stored as a real chunk (not const).
func makeMultiValueSegment(t *testing.T, levels []string, statuses []int64, n int) *Reader {
	t.Helper()

	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	rng := rand.New(rand.NewSource(99))

	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		lvl := levels[rng.Intn(len(levels))]
		status := statuses[rng.Intn(len(statuses))]
		raw := fmt.Sprintf("level=%s status=%d", lvl, status)
		e := event.NewEvent(ts, raw)
		e.Source = "nginx"
		e.SourceType = "access"
		e.Host = "web-01"
		e.Index = "main"
		e.SetField("level", event.StringValue(lvl))
		e.SetField("status", event.IntValue(status))
		events[i] = e
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		t.Fatalf("Write: %v", err)
	}
	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	return r
}

// Tests

func TestRGFilter_NilEvaluator(t *testing.T) {
	eval := NewRGFilterEvaluator(nil, nil)
	if eval != nil {
		t.Fatal("expected nil evaluator for nil root")
	}
}

func TestRGFilter_ConstColumn_Eq_Skip(t *testing.T) {
	// Single-source segment: _source is const "nginx" in the one RG.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	// Query for _source=redis → const mismatch → skip.
	node := &RGFilterNode{
		Op:    RGFilterFieldEq,
		Field: "_source",
		Value: "redis",
	}
	eval := NewRGFilterEvaluator(node, r)
	if eval == nil {
		t.Fatal("expected non-nil evaluator")
	}

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for _source=redis (const nginx), got %d", v)
	}
	if stats.ConstSkips != 1 {
		t.Errorf("ConstSkips: got %d, want 1", stats.ConstSkips)
	}
	if stats.TotalSkipped != 1 {
		t.Errorf("TotalSkipped: got %d, want 1", stats.TotalSkipped)
	}
	if stats.TotalChecked != 1 {
		t.Errorf("TotalChecked: got %d, want 1", stats.TotalChecked)
	}
}

func TestRGFilter_ConstColumn_Eq_Match(t *testing.T) {
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	// Query for _source=nginx → const matches → must scan (RGMaybe).
	node := &RGFilterNode{
		Op:    RGFilterFieldEq,
		Field: "_source",
		Value: "nginx",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for matching const, got %d", v)
	}
	if stats.TotalSkipped != 0 {
		t.Errorf("TotalSkipped: got %d, want 0", stats.TotalSkipped)
	}
}

func TestRGFilter_ConstColumn_Neq_Skip(t *testing.T) {
	// level is const "info" in all rows.
	// field != "info" → const matches excluded value → skip.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:    RGFilterFieldNeq,
		Field: "level",
		Value: "info",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for level!=info (all info), got %d", v)
	}
	if stats.ConstSkips != 1 {
		t.Errorf("ConstSkips: got %d, want 1", stats.ConstSkips)
	}
}

func TestRGFilter_ConstColumn_Neq_NoSkip(t *testing.T) {
	// level is const "info"; querying !=error → const doesn't match excluded → no skip.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:    RGFilterFieldNeq,
		Field: "level",
		Value: "error",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for level!=error (all info), got %d", v)
	}
}

func TestRGFilter_ColumnPresence_Skip(t *testing.T) {
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	// Field "nonexistent" is not in any row group.
	node := &RGFilterNode{
		Op:    RGFilterFieldEq,
		Field: "nonexistent",
		Value: "anything",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for absent column, got %d", v)
	}
	if stats.PresenceSkips != 1 {
		t.Errorf("PresenceSkips: got %d, want 1", stats.PresenceSkips)
	}
}

func TestRGFilter_ZoneMap_IntRange_Skip(t *testing.T) {
	// Status values [200, 300, 404] — all < 500.
	r := makeMultiValueSegment(t, []string{"info"}, []int64{200, 300, 404}, 100)

	// Predicate: status >= 500. Zone map max is 404 → should skip.
	node := &RGFilterNode{
		Op:       RGFilterFieldRange,
		Field:    "status",
		RangeOp:  ">=",
		RangeVal: "500",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for status>=500 with max=404, got %d", v)
	}
	if stats.ZoneMapSkips != 1 {
		t.Errorf("ZoneMapSkips: got %d, want 1", stats.ZoneMapSkips)
	}
}

func TestRGFilter_ZoneMap_IntRange_NoSkip(t *testing.T) {
	// Status values [200, 500] — range overlaps with >= 300.
	r := makeMultiValueSegment(t, []string{"info"}, []int64{200, 500}, 100)

	node := &RGFilterNode{
		Op:       RGFilterFieldRange,
		Field:    "status",
		RangeOp:  ">=",
		RangeVal: "300",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for overlapping range, got %d", v)
	}
}

func TestRGFilter_ZoneMap_IntRange_LessThan_Skip(t *testing.T) {
	// Status values [500, 502] — min=500.
	r := makeMultiValueSegment(t, []string{"info"}, []int64{500, 502}, 100)

	// Predicate: status < 200 → min=500 >= 200 → skip.
	node := &RGFilterNode{
		Op:       RGFilterFieldRange,
		Field:    "status",
		RangeOp:  "<",
		RangeVal: "200",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for status<200 with min=500, got %d", v)
	}
}

func TestRGFilter_ZoneMap_IntRange_ConstColumn_Skip(t *testing.T) {
	// Status is uniform 200 → stored as chunk with min=max="200" (int is not const).
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	// Predicate: status > 300 — zone map max=200 <= 300 → skip.
	node := &RGFilterNode{
		Op:       RGFilterFieldRange,
		Field:    "status",
		RangeOp:  ">",
		RangeVal: "300",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for status>300 with max=200, got %d", v)
	}
	if stats.ZoneMapSkips != 1 {
		t.Errorf("ZoneMapSkips: got %d, want 1", stats.ZoneMapSkips)
	}
}

func TestRGFilter_ZoneMap_StringEq_Skip(t *testing.T) {
	// Level values [error, warn] → zone map [error, warn].
	// "debug" < "error" → outside zone map → skip.
	r := makeMultiValueSegment(t, []string{"error", "warn"}, []int64{200}, 100)

	node := &RGFilterNode{
		Op:    RGFilterFieldEq,
		Field: "level",
		Value: "debug",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for zone map string exclusion, got %d", v)
	}
	if stats.ZoneMapSkips != 1 {
		t.Errorf("ZoneMapSkips: got %d, want 1", stats.ZoneMapSkips)
	}
}

func TestRGFilter_BloomTerm_Skip(t *testing.T) {
	// _raw never contains "CRITICAL".
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:    RGFilterTerm,
		Terms: []string{"critical"}, // bloom is lowercased
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for bloom miss on 'critical', got %d", v)
	}
	if stats.BloomSkips != 1 {
		t.Errorf("BloomSkips: got %d, want 1", stats.BloomSkips)
	}
}

func TestRGFilter_BloomTerm_Maybe(t *testing.T) {
	// _raw contains "info" (from our test data).
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:    RGFilterTerm,
		Terms: []string{"info"},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for bloom hit on 'info', got %d", v)
	}
}

func TestRGFilter_AND_ShortCircuit(t *testing.T) {
	// _source=nginx (const). Query: _source=redis AND <bloom term>.
	// First child skips via const → AND short-circuits.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op: RGFilterAnd,
		Children: []RGFilterNode{
			{Op: RGFilterFieldEq, Field: "_source", Value: "redis"},
			{Op: RGFilterTerm, Terms: []string{"info"}},
		},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip from AND short-circuit, got %d", v)
	}
	if stats.ConstSkips != 1 {
		t.Errorf("ConstSkips: got %d, want 1 (short-circuit on first child)", stats.ConstSkips)
	}
	// BloomSkips should be 0 because AND short-circuited before checking bloom.
	if stats.BloomSkips != 0 {
		t.Errorf("BloomSkips: got %d, want 0 (short-circuit)", stats.BloomSkips)
	}
}

func TestRGFilter_AND_BothCheck(t *testing.T) {
	// _source=nginx (const match), but bloom term "critical" is absent.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op: RGFilterAnd,
		Children: []RGFilterNode{
			{Op: RGFilterFieldEq, Field: "_source", Value: "nginx"},
			{Op: RGFilterTerm, Terms: []string{"critical"}},
		},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip from second AND child bloom miss, got %d", v)
	}
	if stats.ConstSkips != 0 {
		t.Errorf("ConstSkips: got %d, want 0 (first child passed)", stats.ConstSkips)
	}
	if stats.BloomSkips != 1 {
		t.Errorf("BloomSkips: got %d, want 1", stats.BloomSkips)
	}
}

func TestRGFilter_OR_AllSkip(t *testing.T) {
	// _source=nginx (const). OR: _source=postgres OR _source=mysql → both miss → skip.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op: RGFilterOr,
		Children: []RGFilterNode{
			{Op: RGFilterFieldEq, Field: "_source", Value: "postgres"},
			{Op: RGFilterFieldEq, Field: "_source", Value: "mysql"},
		},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip when all OR branches skip, got %d", v)
	}
}

func TestRGFilter_OR_OneMaybe(t *testing.T) {
	// _source=nginx (const). OR: _source=nginx OR _source=postgres → first matches.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op: RGFilterOr,
		Children: []RGFilterNode{
			{Op: RGFilterFieldEq, Field: "_source", Value: "nginx"},
			{Op: RGFilterFieldEq, Field: "_source", Value: "postgres"},
		},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe when one OR branch matches, got %d", v)
	}
}

func TestRGFilter_NOT_AlwaysMaybe(t *testing.T) {
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op: RGFilterNot,
		Children: []RGFilterNode{
			{Op: RGFilterFieldEq, Field: "_source", Value: "nginx"},
		},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for NOT (conservative), got %d", v)
	}
}

func TestRGFilter_FieldIn_ConstMiss(t *testing.T) {
	// _source=nginx (const). IN {postgres, mysql} → const not in set → skip.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:     RGFilterFieldIn,
		Field:  "_source",
		Values: []string{"postgres", "mysql"},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for IN miss, got %d", v)
	}
	if stats.ConstSkips != 1 {
		t.Errorf("ConstSkips: got %d, want 1", stats.ConstSkips)
	}
}

func TestRGFilter_FieldIn_ConstHit(t *testing.T) {
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:     RGFilterFieldIn,
		Field:  "_source",
		Values: []string{"nginx", "postgres"},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for IN hit, got %d", v)
	}
}

func TestRGFilter_FieldIn_AbsentColumn(t *testing.T) {
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:     RGFilterFieldIn,
		Field:  "nonexistent",
		Values: []string{"a", "b"},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for absent column IN, got %d", v)
	}
	if stats.PresenceSkips != 1 {
		t.Errorf("PresenceSkips: got %d, want 1", stats.PresenceSkips)
	}
}

func TestRGFilter_FieldIn_ZoneMap_Skip(t *testing.T) {
	// Level [error, warn]. IN {aaa, debug} — both < "error" (min) → outside zone map.
	r := makeMultiValueSegment(t, []string{"error", "warn"}, []int64{200}, 100)

	node := &RGFilterNode{
		Op:     RGFilterFieldIn,
		Field:  "level",
		Values: []string{"aaa", "debug"},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for IN zone map miss, got %d", v)
	}
	if stats.ZoneMapSkips != 1 {
		t.Errorf("ZoneMapSkips: got %d, want 1", stats.ZoneMapSkips)
	}
}

func TestRGFilter_FieldIn_BloomSkip(t *testing.T) {
	// Multi-value segment with levels [error, warn]. Source is "nginx" (const).
	// IN {_source: "postgres", "mysql"} with bloom terms that don't match.
	// After const check passes (source is non-const for this field),
	// bloom should skip if terms don't appear.
	r := makeMultiValueSegment(t, []string{"error", "warn"}, []int64{200, 500}, 100)

	// level column has values [error, warn]. Query for level IN {debug, critical}
	// with bloom terms ["debug", "critical"] that shouldn't appear in the bloom.
	node := &RGFilterNode{
		Op:     RGFilterFieldIn,
		Field:  "level",
		Values: []string{"debug", "critical"},
		Terms:  []string{"debug", "critical"},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	v := eval.EvaluateRowGroup(0, &stats)
	// Either zone map or bloom should skip this.
	if v != RGSkip {
		t.Errorf("expected RGSkip for IN with non-matching bloom terms, got %d", v)
	}
}

func TestRGFilter_FieldIn_BloomMaybe(t *testing.T) {
	// _source is const "nginx" in makeSingleSourceSegment.
	// IN {_source: "nginx", "redis"} with bloom terms ["nginx"].
	// Const check should pass (nginx is in the set), so we get RGMaybe.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:     RGFilterFieldIn,
		Field:  "_source",
		Values: []string{"nginx", "redis"},
		Terms:  []string{"nginx"},
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for IN with matching const, got %d", v)
	}
}

func TestRGFilter_FieldIn_BloomStats(t *testing.T) {
	// Multi-value segment. Query for level IN with terms that are in the bloom.
	r := makeMultiValueSegment(t, []string{"error", "warn"}, []int64{200}, 100)

	node := &RGFilterNode{
		Op:     RGFilterFieldIn,
		Field:  "level",
		Values: []string{"error", "info"},
		Terms:  []string{"error"}, // error is in the segment
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	v := eval.EvaluateRowGroup(0, &stats)
	// "error" is within zone map [error, warn] and in bloom → RGMaybe.
	if v != RGMaybe {
		t.Errorf("expected RGMaybe when bloom hits, got %d", v)
	}
	// Bloom should have been checked.
	if stats.BloomsChecked != 1 {
		t.Errorf("BloomsChecked: got %d, want 1", stats.BloomsChecked)
	}
}

func TestRGFilter_Neq_ZoneMap_AllSameValue(t *testing.T) {
	// All statuses are 200 → zone map min=max="200".
	// Predicate: status != 200 → min==max==200 → skip via zone map.
	r := makeMultiValueSegment(t, []string{"info"}, []int64{200}, 100)

	node := &RGFilterNode{
		Op:    RGFilterFieldNeq,
		Field: "status",
		Value: "200",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for != with all-same-value, got %d", v)
	}
}

func TestRGFilter_FieldRange_ConstStringColumn(t *testing.T) {
	// level is const "info". Predicate: level > "warn" → "info" > "warn" is false → skip.
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	node := &RGFilterNode{
		Op:       RGFilterFieldRange,
		Field:    "level",
		RangeOp:  ">",
		RangeVal: "warn",
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGSkip {
		t.Errorf("expected RGSkip for level>'warn' (const info), got %d", v)
	}
	if stats.ConstSkips != 1 {
		t.Errorf("ConstSkips: got %d, want 1", stats.ConstSkips)
	}
}

func TestRGFilter_ColumnChunkInRowGroup(t *testing.T) {
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	// _time is always a chunk column.
	cc := r.ColumnChunkInRowGroup(0, "_time")
	if cc == nil {
		t.Fatal("expected _time chunk in rg 0")
	}
	if cc.Name != "_time" {
		t.Errorf("Name: got %q, want _time", cc.Name)
	}

	// Out of range.
	if r.ColumnChunkInRowGroup(999, "_time") != nil {
		t.Error("expected nil for out-of-range rgIdx")
	}
	if r.ColumnChunkInRowGroup(-1, "_time") != nil {
		t.Error("expected nil for negative rgIdx")
	}

	// Non-existent column.
	if r.ColumnChunkInRowGroup(0, "nonexistent") != nil {
		t.Error("expected nil for nonexistent column")
	}
}

func TestRGFilter_EmptyTerms(t *testing.T) {
	r := makeSingleSourceSegment(t, "nginx", "info", 200, 100)

	// Empty terms → always maybe.
	node := &RGFilterNode{
		Op:    RGFilterTerm,
		Terms: nil,
	}
	eval := NewRGFilterEvaluator(node, r)

	var stats RGFilterStats
	if v := eval.EvaluateRowGroup(0, &stats); v != RGMaybe {
		t.Errorf("expected RGMaybe for empty terms, got %d", v)
	}
}

// Benchmarks

func BenchmarkRGFilter_SingleFieldEq(b *testing.B) {
	events := make([]*event.Event, 0, 100)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		e := event.NewEvent(ts, fmt.Sprintf("ts=%s source=nginx level=info", ts.Format(time.RFC3339Nano)))
		e.Source = "nginx"
		e.SourceType = "json"
		e.Host = "web-01"
		e.Index = "main"
		e.SetField("level", event.StringValue("info"))
		e.SetField("status", event.IntValue(200))
		events = append(events, e)
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		b.Fatalf("Write: %v", err)
	}
	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		b.Fatalf("OpenSegment: %v", err)
	}

	// _source is const "nginx"; query for "redis" → const skip.
	node := &RGFilterNode{
		Op:    RGFilterFieldEq,
		Field: "_source",
		Value: "redis",
	}
	eval := NewRGFilterEvaluator(node, r)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eval.EvaluateRowGroup(0, nil)
	}
}

func BenchmarkRGFilter_AND_TwoFields(b *testing.B) {
	events := make([]*event.Event, 0, 100)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		ts := base.Add(time.Duration(i) * time.Millisecond)
		e := event.NewEvent(ts, fmt.Sprintf("ts=%s source=nginx level=info status=200", ts.Format(time.RFC3339Nano)))
		e.Source = "nginx"
		e.SourceType = "json"
		e.Host = "web-01"
		e.Index = "main"
		e.SetField("level", event.StringValue("info"))
		e.SetField("status", event.IntValue(200))
		events = append(events, e)
	}

	var buf bytes.Buffer
	w := NewWriter(&buf)
	if _, err := w.Write(events); err != nil {
		b.Fatalf("Write: %v", err)
	}
	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		b.Fatalf("OpenSegment: %v", err)
	}

	// AND(_source=redis, level=error) — first child const skip short-circuits.
	node := &RGFilterNode{
		Op: RGFilterAnd,
		Children: []RGFilterNode{
			{Op: RGFilterFieldEq, Field: "_source", Value: "redis"},
			{Op: RGFilterFieldEq, Field: "level", Value: "error"},
		},
	}
	eval := NewRGFilterEvaluator(node, r)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eval.EvaluateRowGroup(0, nil)
	}
}

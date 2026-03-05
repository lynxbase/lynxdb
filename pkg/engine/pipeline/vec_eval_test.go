package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// makeBatchWithInts creates a batch with a single int column.
func makeBatchWithInts(name string, vals []int64) *Batch {
	col := make([]event.Value, len(vals))
	for i, v := range vals {
		col[i] = event.IntValue(v)
	}
	return &Batch{
		Columns: map[string][]event.Value{name: col},
		Len:     len(vals),
	}
}

// makeBatchWithStrings creates a batch with a single string column.
func makeBatchWithStrings(name string, vals []string) *Batch {
	col := make([]event.Value, len(vals))
	for i, v := range vals {
		col[i] = event.StringValue(v)
	}
	return &Batch{
		Columns: map[string][]event.Value{name: col},
		Len:     len(vals),
	}
}

// makeBatchWithNulls creates a batch with a column containing some null values.
func makeBatchWithNulls(name string, vals []int64, nullIndices []int) *Batch {
	col := make([]event.Value, len(vals))
	nullSet := make(map[int]bool)
	for _, idx := range nullIndices {
		nullSet[idx] = true
	}
	for i, v := range vals {
		if nullSet[i] {
			col[i] = event.NullValue()
		} else {
			col[i] = event.IntValue(v)
		}
	}
	return &Batch{
		Columns: map[string][]event.Value{name: col},
		Len:     len(vals),
	}
}

func TestVecCompareNode_IntGTE(t *testing.T) {
	batch := makeBatchWithInts("status", []int64{200, 301, 404, 500, 502})
	node := &vecCompareNode{field: "status", op: ">=", value: "500"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{false, false, false, true, true}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecCompareNode_StringEQ(t *testing.T) {
	batch := makeBatchWithStrings("level", []string{"INFO", "ERROR", "WARN", "ERROR", "INFO"})
	node := &vecCompareNode{field: "level", op: "=", value: "ERROR"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{false, true, false, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecCompareNode_MissingColumn(t *testing.T) {
	batch := makeBatchWithInts("other", []int64{1, 2, 3})
	node := &vecCompareNode{field: "status", op: ">=", value: "500"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	// Missing column → all false.
	for i, v := range bitmap {
		if v {
			t.Errorf("bitmap[%d] should be false for missing column", i)
		}
	}
}

func TestVecAndNode(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"status": {event.IntValue(500), event.IntValue(200), event.IntValue(503), event.IntValue(200)},
			"level":  {event.StringValue("ERROR"), event.StringValue("ERROR"), event.StringValue("INFO"), event.StringValue("INFO")},
		},
		Len: 4,
	}

	node := &vecAndNode{
		left:  &vecCompareNode{field: "status", op: ">=", value: "500"},
		right: &vecCompareNode{field: "level", op: "=", value: "ERROR"},
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	// status >= 500: [T, F, T, F]
	// level = "ERROR": [T, T, F, F]
	// AND: [T, F, F, F]
	want := []bool{true, false, false, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("AND bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecOrNode(t *testing.T) {
	batch := &Batch{
		Columns: map[string][]event.Value{
			"status": {event.IntValue(500), event.IntValue(200), event.IntValue(503), event.IntValue(200)},
			"level":  {event.StringValue("ERROR"), event.StringValue("ERROR"), event.StringValue("INFO"), event.StringValue("INFO")},
		},
		Len: 4,
	}

	node := &vecOrNode{
		left:  &vecCompareNode{field: "status", op: ">=", value: "500"},
		right: &vecCompareNode{field: "level", op: "=", value: "ERROR"},
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	// status >= 500: [T, F, T, F]
	// level = "ERROR": [T, T, F, F]
	// OR: [T, T, T, F]
	want := []bool{true, true, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("OR bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecNotNode(t *testing.T) {
	batch := makeBatchWithInts("status", []int64{200, 500, 200})
	node := &vecNotNode{
		child: &vecCompareNode{field: "status", op: "=", value: "500"},
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, false, true}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("NOT bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecInNode_Int(t *testing.T) {
	batch := makeBatchWithInts("status", []int64{200, 404, 500, 301})
	node := &vecInNode{
		field:  "status",
		intSet: map[int64]struct{}{200: {}, 404: {}, 500: {}},
		strSet: map[string]struct{}{"200": {}, "404": {}, "500": {}},
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, true, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("IN bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecInNode_String(t *testing.T) {
	batch := makeBatchWithStrings("level", []string{"ERROR", "INFO", "WARN", "DEBUG"})
	node := &vecInNode{
		field:  "level",
		strSet: map[string]struct{}{"ERROR": {}, "WARN": {}},
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, false, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("IN string bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecInNode_Negated(t *testing.T) {
	batch := makeBatchWithInts("status", []int64{200, 404, 500})
	node := &vecInNode{
		field:   "status",
		negated: true,
		intSet:  map[int64]struct{}{200: {}},
		strSet:  map[string]struct{}{"200": {}},
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	// NOT IN (200): [false, true, true]
	want := []bool{false, true, true}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("NOT IN bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecInNode_MissingColumn(t *testing.T) {
	batch := makeBatchWithInts("other", []int64{1, 2})
	node := &vecInNode{
		field:  "status",
		strSet: map[string]struct{}{"200": {}},
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	// Missing column with IN → all false.
	for i, v := range bitmap {
		if v {
			t.Errorf("missing col IN bitmap[%d] should be false", i)
		}
	}

	// Missing column with NOT IN → all true.
	node.negated = true
	bitmap, ok = node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	for i, v := range bitmap {
		if !v {
			t.Errorf("missing col NOT IN bitmap[%d] should be true", i)
		}
	}
}

func TestVecNullCheckNode_Isnull(t *testing.T) {
	batch := makeBatchWithNulls("status", []int64{200, 0, 500}, []int{1})
	node := &vecNullCheckNode{field: "status", wantNull: true}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{false, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("isnull bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecNullCheckNode_Isnotnull(t *testing.T) {
	batch := makeBatchWithNulls("status", []int64{200, 0, 500}, []int{1})
	node := &vecNullCheckNode{field: "status", wantNull: false}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, false, true}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("isnotnull bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecNullCheckNode_MissingColumn(t *testing.T) {
	batch := makeBatchWithInts("other", []int64{1, 2, 3})

	// isnull on missing column → all true (all values are null).
	node := &vecNullCheckNode{field: "status", wantNull: true}
	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	for i, v := range bitmap {
		if !v {
			t.Errorf("isnull missing col bitmap[%d] should be true", i)
		}
	}

	// isnotnull on missing column → all false.
	node2 := &vecNullCheckNode{field: "status", wantNull: false}
	bitmap, ok = node2.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	for i, v := range bitmap {
		if v {
			t.Errorf("isnotnull missing col bitmap[%d] should be false", i)
		}
	}
}

func TestVecLikeNode_Prefix(t *testing.T) {
	batch := makeBatchWithStrings("uri", []string{"/api/users", "/web/index", "/api/health", "/other"})
	node := &vecLikeNode{field: "uri", pattern: "/api/%", kind: "prefix", literal: "/api/"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, false, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("LIKE prefix bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecLikeNode_Suffix(t *testing.T) {
	batch := makeBatchWithStrings("file", []string{"test.log", "data.csv", "app.log", "readme.md"})
	node := &vecLikeNode{field: "file", pattern: "%.log", kind: "suffix", literal: ".log"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, false, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("LIKE suffix bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecLikeNode_Contains(t *testing.T) {
	batch := makeBatchWithStrings("msg", []string{"connection error", "success", "timeout error occurred", "ok"})
	node := &vecLikeNode{field: "msg", pattern: "%error%", kind: "contains", literal: "error"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, false, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("LIKE contains bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecLikeNode_General(t *testing.T) {
	batch := makeBatchWithStrings("path", []string{"/a/b/c", "/a/x/c", "/b/b/c", "/a/b/d"})
	node := &vecLikeNode{field: "path", pattern: "/a/%/c", kind: "general", literal: "/a/%/c"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{true, true, false, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("LIKE general bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecRangeNode_Int(t *testing.T) {
	batch := makeBatchWithInts("status", []int64{200, 400, 404, 500, 599, 600})
	node := &vecRangeNode{
		field: "status", minVal: "400", maxVal: "599", minOp: ">=", maxOp: "<=",
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	want := []bool{false, true, true, true, true, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("range bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecRangeNode_ExclusiveBounds(t *testing.T) {
	batch := makeBatchWithInts("x", []int64{1, 2, 3, 4, 5})
	node := &vecRangeNode{
		field: "x", minVal: "2", maxVal: "4", minOp: ">", maxOp: "<",
	}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	// > 2 AND < 4 → only 3
	want := []bool{false, false, true, false, false}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("exclusive range bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

func TestVecCompareNode_NullSemantics(t *testing.T) {
	// NULL compared to anything should be false.
	batch := makeBatchWithNulls("status", []int64{200, 0, 500}, []int{1})
	node := &vecCompareNode{field: "status", op: ">=", value: "0"}

	bitmap, ok := node.evalBitmap(batch)
	if !ok {
		t.Fatal("evalBitmap failed")
	}
	// 200 >= 0: true, null >= 0: false (SQL null semantics), 500 >= 0: true
	want := []bool{true, false, true}
	for i, v := range want {
		if bitmap[i] != v {
			t.Errorf("null semantics bitmap[%d]: got %v, want %v", i, bitmap[i], v)
		}
	}
}

// TestFilterIterator_CompoundVectorized tests the full FilterIterator integration
// with compound vectorized expressions.
func TestFilterIterator_CompoundVectorized(t *testing.T) {
	// Build events: status varies [200, 300, 400, 500, 600], level alternates.
	events := make([]*event.Event, 10)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	levels := []string{"INFO", "ERROR"}
	for i := range events {
		events[i] = &event.Event{
			Time: base.Add(time.Duration(i) * time.Second),
			Raw:  "test",
			Fields: map[string]event.Value{
				"status": event.IntValue(int64(200 + (i%5)*100)),
				"level":  event.StringValue(levels[i%2]),
			},
		}
	}

	// WHERE status >= 500 AND level = "ERROR"
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "level"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "ERROR"},
		},
	}

	prog := &vm.Program{} // dummy program (compound plan takes priority)
	scan := NewScanIterator(events, 1024)
	filter := NewFilterIteratorWithExpr(scan, prog, expr)

	ctx := context.Background()
	if err := filter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	batch, err := filter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if batch == nil {
		t.Fatal("expected non-nil batch")
	}

	// Verify vectorized path was used.
	if !filter.WasVectorized() {
		t.Error("expected vectorized path to be used")
	}

	// Verify all matching rows have status >= 500 AND level = "ERROR".
	for i := 0; i < batch.Len; i++ {
		status := batch.Columns["status"][i].AsInt()
		level := batch.Columns["level"][i].AsString()
		if status < 500 || level != "ERROR" {
			t.Errorf("row %d: status=%d level=%q does not match filter", i, status, level)
		}
	}
}

// TestFilterIterator_InExpression tests IN with the vectorized path.
func TestFilterIterator_InExpression(t *testing.T) {
	events := make([]*event.Event, 5)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range events {
		events[i] = &event.Event{
			Time: base.Add(time.Duration(i) * time.Second),
			Raw:  "test",
			Fields: map[string]event.Value{
				"status": event.IntValue(int64(200 + i*100)),
			},
		}
	}

	expr := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "400"},
		},
	}

	prog := &vm.Program{}
	scan := NewScanIterator(events, 1024)
	filter := NewFilterIteratorWithExpr(scan, prog, expr)

	ctx := context.Background()
	_ = filter.Init(ctx)
	batch, err := filter.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil {
		t.Fatal("expected non-nil batch")
	}
	if batch.Len != 2 {
		t.Errorf("expected 2 matching rows, got %d", batch.Len)
	}
}

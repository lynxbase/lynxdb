package pipeline

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestBatchAppendBatch_Basic(t *testing.T) {
	b1 := &Batch{
		Columns: map[string][]event.Value{
			"a": {event.IntValue(1), event.IntValue(2)},
			"b": {event.StringValue("x"), event.StringValue("y")},
		},
		Len: 2,
	}
	b2 := &Batch{
		Columns: map[string][]event.Value{
			"a": {event.IntValue(3)},
			"b": {event.StringValue("z")},
		},
		Len: 1,
	}

	b1.AppendBatch(b2)

	if b1.Len != 3 {
		t.Fatalf("Len: got %d, want 3", b1.Len)
	}
	// Verify column "a".
	aCol := b1.Columns["a"]
	if len(aCol) != 3 {
		t.Fatalf("col a: len=%d, want 3", len(aCol))
	}
	for i, want := range []int64{1, 2, 3} {
		if aCol[i].AsInt() != want {
			t.Errorf("a[%d]: got %d, want %d", i, aCol[i].AsInt(), want)
		}
	}
	// Verify column "b".
	bCol := b1.Columns["b"]
	if len(bCol) != 3 {
		t.Fatalf("col b: len=%d, want 3", len(bCol))
	}
	for i, want := range []string{"x", "y", "z"} {
		if bCol[i].String() != want {
			t.Errorf("b[%d]: got %q, want %q", i, bCol[i].String(), want)
		}
	}
}

func TestBatchAppendBatch_SparseColumns(t *testing.T) {
	// b1 has columns "a" and "b"; b2 has columns "b" and "c".
	b1 := &Batch{
		Columns: map[string][]event.Value{
			"a": {event.IntValue(1)},
			"b": {event.StringValue("x")},
		},
		Len: 1,
	}
	b2 := &Batch{
		Columns: map[string][]event.Value{
			"b": {event.StringValue("y")},
			"c": {event.IntValue(99)},
		},
		Len: 1,
	}

	b1.AppendBatch(b2)

	if b1.Len != 2 {
		t.Fatalf("Len: got %d, want 2", b1.Len)
	}

	// Column "a": present in b1 only → row 1 should be null.
	aCol := b1.Columns["a"]
	if len(aCol) != 2 {
		t.Fatalf("col a: len=%d, want 2", len(aCol))
	}
	if aCol[0].AsInt() != 1 {
		t.Errorf("a[0]: got %d, want 1", aCol[0].AsInt())
	}
	if !aCol[1].IsNull() {
		t.Errorf("a[1]: expected null, got %v", aCol[1])
	}

	// Column "b": present in both.
	bCol := b1.Columns["b"]
	if len(bCol) != 2 {
		t.Fatalf("col b: len=%d, want 2", len(bCol))
	}
	if bCol[0].String() != "x" || bCol[1].String() != "y" {
		t.Errorf("b: got [%q, %q], want [x, y]", bCol[0].String(), bCol[1].String())
	}

	// Column "c": present in b2 only → row 0 should be null.
	cCol := b1.Columns["c"]
	if len(cCol) != 2 {
		t.Fatalf("col c: len=%d, want 2", len(cCol))
	}
	if !cCol[0].IsNull() {
		t.Errorf("c[0]: expected null, got %v", cCol[0])
	}
	if cCol[1].AsInt() != 99 {
		t.Errorf("c[1]: got %d, want 99", cCol[1].AsInt())
	}
}

func TestBatchAppendBatch_EmptyOther(t *testing.T) {
	b := &Batch{
		Columns: map[string][]event.Value{
			"a": {event.IntValue(1)},
		},
		Len: 1,
	}
	b.AppendBatch(nil)
	if b.Len != 1 {
		t.Fatalf("nil append: Len should remain 1, got %d", b.Len)
	}

	b.AppendBatch(&Batch{Columns: map[string][]event.Value{}, Len: 0})
	if b.Len != 1 {
		t.Fatalf("empty append: Len should remain 1, got %d", b.Len)
	}
}

func TestBatchAppendBatch_IntoEmpty(t *testing.T) {
	b := NewBatch(0)
	other := &Batch{
		Columns: map[string][]event.Value{
			"x": {event.IntValue(42)},
		},
		Len: 1,
	}
	b.AppendBatch(other)
	if b.Len != 1 {
		t.Fatalf("Len: got %d, want 1", b.Len)
	}
	if b.Columns["x"][0].AsInt() != 42 {
		t.Errorf("x[0]: got %d, want 42", b.Columns["x"][0].AsInt())
	}
}

func TestBatchPermuteSlice_Basic(t *testing.T) {
	b := &Batch{
		Columns: map[string][]event.Value{
			"name": {event.StringValue("alice"), event.StringValue("bob"), event.StringValue("charlie")},
			"age":  {event.IntValue(30), event.IntValue(25), event.IntValue(35)},
		},
		Len: 3,
	}

	// Reverse order.
	result := b.PermuteSlice([]int{2, 1, 0})
	if result.Len != 3 {
		t.Fatalf("Len: got %d, want 3", result.Len)
	}

	expectedNames := []string{"charlie", "bob", "alice"}
	expectedAges := []int64{35, 25, 30}
	for i := 0; i < 3; i++ {
		if result.Columns["name"][i].String() != expectedNames[i] {
			t.Errorf("name[%d]: got %q, want %q", i, result.Columns["name"][i].String(), expectedNames[i])
		}
		if result.Columns["age"][i].AsInt() != expectedAges[i] {
			t.Errorf("age[%d]: got %d, want %d", i, result.Columns["age"][i].AsInt(), expectedAges[i])
		}
	}
}

func TestBatchPermuteSlice_Subset(t *testing.T) {
	b := &Batch{
		Columns: map[string][]event.Value{
			"v": {event.IntValue(10), event.IntValue(20), event.IntValue(30), event.IntValue(40)},
		},
		Len: 4,
	}

	// Pick middle two in reverse.
	result := b.PermuteSlice([]int{2, 1})
	if result.Len != 2 {
		t.Fatalf("Len: got %d, want 2", result.Len)
	}
	if result.Columns["v"][0].AsInt() != 30 || result.Columns["v"][1].AsInt() != 20 {
		t.Errorf("v: got [%d, %d], want [30, 20]",
			result.Columns["v"][0].AsInt(), result.Columns["v"][1].AsInt())
	}
}

func TestBatchPermuteSlice_Empty(t *testing.T) {
	b := &Batch{
		Columns: map[string][]event.Value{
			"v": {event.IntValue(1)},
		},
		Len: 1,
	}

	result := b.PermuteSlice([]int{})
	if result.Len != 0 {
		t.Fatalf("Len: got %d, want 0", result.Len)
	}
}

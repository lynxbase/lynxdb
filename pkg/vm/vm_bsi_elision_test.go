package vm

import (
	"testing"

	"github.com/RoaringBitmap/roaring"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestUnit_VM_BSIHandledRangePredicate_RuntimeContextShortCircuitsComparison(t *testing.T) {
	prog := compileBSIHandledStatusPredicate(t)
	rows := roaring.BitmapOf(0, 1, 2)
	vm := &VM{}

	for row := uint32(0); row < 3; row++ {
		got, err := vm.ExecuteWithContext(prog,
			map[string]event.Value{"status": event.IntValue(200)},
			&PredicateContext{RowIndex: row, BSIHandledRows: rows},
		)
		if err != nil {
			t.Fatalf("ExecuteWithContext(row=%d): %v", row, err)
		}
		if !got.AsBool() {
			t.Fatalf("row %d result = false, want true because BSI handled the predicate", row)
		}
	}
}

func TestUnit_VM_BSIHandledRangePredicate_NoRuntimeContext_EvaluatesPerRow(t *testing.T) {
	prog := compileBSIHandledStatusPredicate(t)
	vm := &VM{}

	for _, tt := range []struct {
		name   string
		status int64
		want   bool
	}{
		{name: "below threshold", status: 200, want: false},
		{name: "at threshold", status: 500, want: true},
		{name: "above threshold", status: 503, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := vm.ExecuteWithContext(prog,
				map[string]event.Value{"status": event.IntValue(tt.status)},
				nil,
			)
			if err != nil {
				t.Fatalf("ExecuteWithContext: %v", err)
			}
			if got.AsBool() != tt.want {
				t.Fatalf("status %d result = %v, want %v", tt.status, got.AsBool(), tt.want)
			}
		})
	}
}

func TestUnit_VM_BSIHandledRangePredicate_FieldScopedContextDoesNotMaskOtherFields(t *testing.T) {
	prog := compileBSIHandledStatusPredicate(t)
	fields := map[string]interface{ Contains(uint32) bool }{
		"duration_ms": roaring.BitmapOf(0),
	}

	got, err := (&VM{}).ExecuteWithContext(prog,
		map[string]event.Value{"status": event.IntValue(200)},
		&PredicateContext{RowIndex: 0, BSIHandledFields: fields},
	)
	if err != nil {
		t.Fatalf("ExecuteWithContext: %v", err)
	}
	if got.AsBool() {
		t.Fatal("result = true, want false because BSI proof belongs to a different field")
	}
}

func TestUnit_VM_BSIHandledRangePredicate_FieldScopedContextHandlesOnlyMatchingRows(t *testing.T) {
	prog := compileBSIHandledStatusPredicate(t)
	fields := map[string]interface{ Contains(uint32) bool }{
		"status": roaring.BitmapOf(0, 2),
	}
	vm := &VM{}

	for _, tt := range []struct {
		row  uint32
		want bool
	}{
		{row: 0, want: true},
		{row: 1, want: false},
		{row: 2, want: true},
	} {
		got, err := vm.ExecuteWithContext(prog,
			map[string]event.Value{"status": event.IntValue(200)},
			&PredicateContext{RowIndex: tt.row, BSIHandledFields: fields},
		)
		if err != nil {
			t.Fatalf("ExecuteWithContext(row=%d): %v", tt.row, err)
		}
		if got.AsBool() != tt.want {
			t.Fatalf("row %d result = %v, want %v", tt.row, got.AsBool(), tt.want)
		}
	}
}

func compileBSIHandledStatusPredicate(t *testing.T) *Program {
	t.Helper()
	prog, err := CompilePredicate(&spl2.CompareExpr{
		Left:         &spl2.FieldExpr{Name: "status"},
		Op:           ">=",
		Right:        &spl2.LiteralExpr{Value: "500"},
		LoweredToBSI: true,
	})
	if err != nil {
		t.Fatalf("CompilePredicate: %v", err)
	}
	if prog.BSIHandledComparisons != 1 {
		t.Fatalf("BSIHandledComparisons = %d, want 1", prog.BSIHandledComparisons)
	}

	return prog
}

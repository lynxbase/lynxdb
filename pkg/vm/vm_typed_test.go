package vm

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// OpConstDuration

// OpArrayBuild

func TestVMArrayBuild_Empty(t *testing.T) {
	p := &Program{}
	p.EmitOp(OpArrayBuild, 0)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.Type() != event.FieldTypeArray {
		t.Fatalf("expected array, got %s", result.Type())
	}
	if len(result.AsArray()) != 0 {
		t.Errorf("expected empty array, got len %d", len(result.AsArray()))
	}
}

// OpObjectBuild

func TestVMObjectBuild_Empty(t *testing.T) {
	p := &Program{}
	p.EmitOp(OpObjectBuild, 0)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.Type() != event.FieldTypeObject {
		t.Fatalf("expected object, got %s", result.Type())
	}
	if len(result.AsObject()) != 0 {
		t.Errorf("expected empty object, got len %d", len(result.AsObject()))
	}
}

// OpIndex

// OpMember

// OpLen

// Duration arithmetic matrix

// Timestamp +/- duration

// Deep equality

func TestVMDeepEquality_Arrays(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		want bool
	}{
		{
			name: "equal arrays",
			a:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
			b:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
			want: true,
		},
		{
			name: "different elements",
			a:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
			b:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(3)}),
			want: false,
		},
		{
			name: "different lengths",
			a:    event.ArrayValue([]event.Value{event.IntValue(1)}),
			b:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
			want: false,
		},
		{
			name: "empty arrays",
			a:    event.ArrayValue([]event.Value{}),
			b:    event.ArrayValue([]event.Value{}),
			want: true,
		},
		{
			name: "array vs non-array",
			a:    event.ArrayValue([]event.Value{event.IntValue(1)}),
			b:    event.IntValue(1),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{}
			p.AddConstant(tt.a)
			p.AddConstant(tt.b)
			// Use OpConstStr as the const loading opcode; it just loads from
			// the constant pool by index regardless of value type.
			p.EmitOp(OpConstStr, 0)
			p.EmitOp(OpConstStr, 1)
			p.EmitOp(OpEq)
			p.EmitOp(OpReturn)

			result := runProgram(t, p, nil)
			if result.AsBool() != tt.want {
				t.Errorf("got %v, want %v", result.AsBool(), tt.want)
			}
		})
	}
}

func TestVMDeepEquality_Objects(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		want bool
	}{
		{
			name: "equal objects",
			a:    event.ObjectValue(map[string]event.Value{"x": event.IntValue(1), "y": event.IntValue(2)}),
			b:    event.ObjectValue(map[string]event.Value{"x": event.IntValue(1), "y": event.IntValue(2)}),
			want: true,
		},
		{
			name: "different values",
			a:    event.ObjectValue(map[string]event.Value{"x": event.IntValue(1)}),
			b:    event.ObjectValue(map[string]event.Value{"x": event.IntValue(2)}),
			want: false,
		},
		{
			name: "different keys",
			a:    event.ObjectValue(map[string]event.Value{"x": event.IntValue(1)}),
			b:    event.ObjectValue(map[string]event.Value{"y": event.IntValue(1)}),
			want: false,
		},
		{
			name: "empty objects",
			a:    event.ObjectValue(map[string]event.Value{}),
			b:    event.ObjectValue(map[string]event.Value{}),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{}
			p.AddConstant(tt.a)
			p.AddConstant(tt.b)
			p.EmitOp(OpConstStr, 0)
			p.EmitOp(OpConstStr, 1)
			p.EmitOp(OpEq)
			p.EmitOp(OpReturn)

			result := runProgram(t, p, nil)
			if result.AsBool() != tt.want {
				t.Errorf("got %v, want %v", result.AsBool(), tt.want)
			}
		})
	}
}

func TestVMDeepEquality_Duration(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		want bool
	}{
		{
			name: "equal durations",
			a:    event.DurationValue(5 * time.Second),
			b:    event.DurationValue(5 * time.Second),
			want: true,
		},
		{
			name: "different durations",
			a:    event.DurationValue(5 * time.Second),
			b:    event.DurationValue(3 * time.Second),
			want: false,
		},
		{
			name: "duration vs int (cross-type)",
			a:    event.DurationValue(5 * time.Second),
			b:    event.IntValue(5),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{}
			p.AddConstant(tt.a)
			p.AddConstant(tt.b)
			p.EmitOp(OpConstStr, 0)
			p.EmitOp(OpConstStr, 1)
			p.EmitOp(OpEq)
			p.EmitOp(OpReturn)

			result := runProgram(t, p, nil)
			if result.AsBool() != tt.want {
				t.Errorf("got %v, want %v", result.AsBool(), tt.want)
			}
		})
	}
}

// Duration comparison (ordering)

// Array comparison (ordering)

func TestVMArray_Compare(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		want int
	}{
		{
			name: "equal",
			a:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
			b:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
			want: 0,
		},
		{
			name: "first element differs",
			a:    event.ArrayValue([]event.Value{event.IntValue(1)}),
			b:    event.ArrayValue([]event.Value{event.IntValue(2)}),
			want: -1,
		},
		{
			name: "shorter is less",
			a:    event.ArrayValue([]event.Value{event.IntValue(1)}),
			b:    event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareValues(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("CompareValues = %d, want %d", got, tt.want)
			}
		})
	}
}

// Truthiness for new types

func TestVMTruthiness_Duration(t *testing.T) {
	tests := []struct {
		name string
		v    event.Value
		want bool
	}{
		{"non-zero", event.DurationValue(1 * time.Second), true},
		{"zero", event.DurationValue(0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if IsTruthy(tt.v) != tt.want {
				t.Errorf("IsTruthy(%v) = %v, want %v", tt.v, IsTruthy(tt.v), tt.want)
			}
		})
	}
}

func TestVMTruthiness_Array(t *testing.T) {
	tests := []struct {
		name string
		v    event.Value
		want bool
	}{
		{"non-empty", event.ArrayValue([]event.Value{event.IntValue(1)}), true},
		{"empty", event.ArrayValue([]event.Value{}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if IsTruthy(tt.v) != tt.want {
				t.Errorf("IsTruthy(%v) = %v, want %v", tt.v, IsTruthy(tt.v), tt.want)
			}
		})
	}
}

func TestVMTruthiness_Object(t *testing.T) {
	tests := []struct {
		name string
		v    event.Value
		want bool
	}{
		{"non-empty", event.ObjectValue(map[string]event.Value{"a": event.IntValue(1)}), true},
		{"empty", event.ObjectValue(map[string]event.Value{}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if IsTruthy(tt.v) != tt.want {
				t.Errorf("IsTruthy(%v) = %v, want %v", tt.v, IsTruthy(tt.v), tt.want)
			}
		})
	}
}

// Nesting: array of objects

// IsArray / IsObject with native types

// Duration + Add commutative (duration + timestamp)

// Opcode encoding tests for new opcodes

// Cross-type: duration vs non-matching types yield null/false

// Opcode definitions completeness: every new opcode has a definition entry

// TypeOf for new types

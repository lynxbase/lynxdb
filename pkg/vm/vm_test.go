package vm

import (
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func runProgram(t *testing.T, prog *Program, fields map[string]event.Value) event.Value {
	t.Helper()
	vm := &VM{}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatalf("VM.Execute error: %v", err)
	}

	return result
}

func TestVMStringConcat(t *testing.T) {
	// "hello" + " " + "world"
	p := &Program{}
	p.AddConstant(event.StringValue("hello"))
	p.AddConstant(event.StringValue(" "))
	p.AddConstant(event.StringValue("world"))
	p.EmitOp(OpConstStr, 0)
	p.EmitOp(OpConstStr, 1)
	p.EmitOp(OpAdd) // generic add handles string concat
	p.EmitOp(OpConstStr, 2)
	p.EmitOp(OpAdd)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsString() != "hello world" {
		t.Errorf("got %q, want %q", result.AsString(), "hello world")
	}
}

func TestVMCoalesce(t *testing.T) {
	// coalesce(null, null, "found") → "found"
	p := &Program{}
	p.AddConstant(event.StringValue("found"))
	p.EmitOp(OpConstNull)
	p.EmitOp(OpConstNull)
	p.EmitOp(OpConstStr, 0)
	p.EmitOp(OpCoalesce, 3)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsString() != "found" {
		t.Errorf("got %v, want 'found'", result)
	}
}

func TestVMIsNull(t *testing.T) {
	t.Run("isnull(null)→true", func(t *testing.T) {
		p := &Program{}
		p.EmitOp(OpConstNull)
		p.EmitOp(OpIsNull)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.AsBool() {
			t.Errorf("expected true")
		}
	})

	t.Run("isnull('x')→false", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("x"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpIsNull)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsBool() {
			t.Errorf("expected false")
		}
	})
}

func TestVMStackOverflow(t *testing.T) {
	// Push 300 values onto a 256-size stack
	p := &Program{}
	for i := 0; i < 300; i++ {
		p.EmitOp(OpConstTrue)
	}
	p.EmitOp(OpReturn)

	vm := &VM{}
	_, err := vm.Execute(p, nil)
	if !errors.Is(err, ErrStackOverflow) {
		t.Errorf("expected ErrStackOverflow, got %v", err)
	}
}

func TestCompareValues_NumericStrings(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		want int // -1, 0, 1
	}{
		// Numeric string comparison — the core bug fix.
		// "98" vs "786": lexicographic says "98" > "786", numeric says "98" < "786".
		{name: "numeric_string_98_vs_786", a: event.StringValue("98"), b: event.StringValue("786"), want: -1},
		{name: "numeric_string_786_vs_98", a: event.StringValue("786"), b: event.StringValue("98"), want: 1},
		{name: "numeric_string_equal", a: event.StringValue("42"), b: event.StringValue("42"), want: 0},
		{name: "numeric_string_negative", a: event.StringValue("-5"), b: event.StringValue("3"), want: -1},
		{name: "numeric_string_floats", a: event.StringValue("3.14"), b: event.StringValue("2.71"), want: 1},
		{name: "numeric_string_large", a: event.StringValue("1000000"), b: event.StringValue("999999"), want: 1},

		// Non-numeric strings — fall back to lexicographic.
		{name: "alpha_strings", a: event.StringValue("apple"), b: event.StringValue("banana"), want: -1},
		{name: "alpha_equal", a: event.StringValue("hello"), b: event.StringValue("hello"), want: 0},

		// Mixed: one numeric, one not — fall back to lexicographic.
		{name: "mixed_numeric_alpha", a: event.StringValue("42"), b: event.StringValue("abc"), want: -1}, // "4" < "a"

		// Int vs int.
		{name: "int_comparison", a: event.IntValue(98), b: event.IntValue(786), want: -1},

		// Null handling.
		{name: "null_vs_null", a: event.NullValue(), b: event.NullValue(), want: 0},
		{name: "null_vs_value", a: event.NullValue(), b: event.StringValue("42"), want: -1},
		{name: "value_vs_null", a: event.StringValue("42"), b: event.NullValue(), want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareValues(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("CompareValues(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

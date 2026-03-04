package vm

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// helper: build and run a program, return result
func runProgram(t *testing.T, prog *Program, fields map[string]event.Value) event.Value {
	t.Helper()
	vm := &VM{}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatalf("VM.Execute error: %v", err)
	}

	return result
}

func TestVMIntegerArithmetic(t *testing.T) {
	tests := []struct {
		name string
		prog *Program
		want int64
	}{
		{
			name: "1+2",
			prog: buildArith(OpConstInt, 0, OpConstInt, 1, OpAddInt,
				event.IntValue(1), event.IntValue(2)),
			want: 3,
		},
		{
			name: "10-3",
			prog: buildArith(OpConstInt, 0, OpConstInt, 1, OpSubInt,
				event.IntValue(10), event.IntValue(3)),
			want: 7,
		},
		{
			name: "4*5",
			prog: buildArith(OpConstInt, 0, OpConstInt, 1, OpMulInt,
				event.IntValue(4), event.IntValue(5)),
			want: 20,
		},
		{
			name: "10/3",
			prog: buildArith(OpConstInt, 0, OpConstInt, 1, OpDivInt,
				event.IntValue(10), event.IntValue(3)),
			want: 3, // integer division
		},
		{
			name: "10%3",
			prog: buildArith(OpConstInt, 0, OpConstInt, 1, OpModInt,
				event.IntValue(10), event.IntValue(3)),
			want: 1,
		},
		{
			name: "1+2*3 (manually ordered as 2*3+1)",
			prog: func() *Program {
				p := &Program{}
				p.AddConstant(event.IntValue(2))
				p.AddConstant(event.IntValue(3))
				p.AddConstant(event.IntValue(1))
				p.EmitOp(OpConstInt, 0)
				p.EmitOp(OpConstInt, 1)
				p.EmitOp(OpMulInt)
				p.EmitOp(OpConstInt, 2)
				p.EmitOp(OpAddInt)
				p.EmitOp(OpReturn)

				return p
			}(),
			want: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runProgram(t, tt.prog, nil)
			if result.Type() != event.FieldTypeInt || result.AsInt() != tt.want {
				t.Errorf("got %v, want %d", result, tt.want)
			}
		})
	}
}

func TestVMNegate(t *testing.T) {
	p := &Program{}
	p.AddConstant(event.IntValue(5))
	p.EmitOp(OpConstInt, 0)
	p.EmitOp(OpNegInt)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsInt() != -5 {
		t.Errorf("got %v, want -5", result)
	}
}

func TestVMFloatArithmetic(t *testing.T) {
	tests := []struct {
		name string
		a, b float64
		op   Opcode
		want float64
	}{
		{"1.5+2.5", 1.5, 2.5, OpAddFloat, 4.0},
		{"10.0-3.5", 10.0, 3.5, OpSubFloat, 6.5},
		{"2.5*4.0", 2.5, 4.0, OpMulFloat, 10.0},
		{"10.0/4.0", 10.0, 4.0, OpDivFloat, 2.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{}
			p.AddConstant(event.FloatValue(tt.a))
			p.AddConstant(event.FloatValue(tt.b))
			p.EmitOp(OpConstFloat, 0)
			p.EmitOp(OpConstFloat, 1)
			p.EmitOp(tt.op)
			p.EmitOp(OpReturn)

			result := runProgram(t, p, nil)
			if result.Type() != event.FieldTypeFloat {
				t.Fatalf("expected float, got %s", result.Type())
			}
			if math.Abs(result.AsFloat()-tt.want) > 1e-10 {
				t.Errorf("got %v, want %v", result.AsFloat(), tt.want)
			}
		})
	}
}

func TestVMMixedArithmetic(t *testing.T) {
	// int(1) + float(2.5) = float(3.5) via OpAdd (generic)
	p := &Program{}
	p.AddConstant(event.IntValue(1))
	p.AddConstant(event.FloatValue(2.5))
	p.EmitOp(OpConstInt, 0)
	p.EmitOp(OpConstFloat, 1)
	p.EmitOp(OpAdd)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.Type() != event.FieldTypeFloat {
		t.Fatalf("expected float, got %s", result.Type())
	}
	if math.Abs(result.AsFloat()-3.5) > 1e-10 {
		t.Errorf("got %v, want 3.5", result.AsFloat())
	}
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

func TestVMComparisons(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		op   Opcode
		want bool
	}{
		// Int comparisons
		{"5==5", event.IntValue(5), event.IntValue(5), OpEq, true},
		{"5==3", event.IntValue(5), event.IntValue(3), OpEq, false},
		{"5!=3", event.IntValue(5), event.IntValue(3), OpNeq, true},
		{"3<5", event.IntValue(3), event.IntValue(5), OpLt, true},
		{"5<3", event.IntValue(5), event.IntValue(3), OpLt, false},
		{"3<=3", event.IntValue(3), event.IntValue(3), OpLte, true},
		{"5>3", event.IntValue(5), event.IntValue(3), OpGt, true},
		{"3>=3", event.IntValue(3), event.IntValue(3), OpGte, true},
		// Float
		{"1.5<2.5", event.FloatValue(1.5), event.FloatValue(2.5), OpLt, true},
		// String
		{`"abc"=="abc"`, event.StringValue("abc"), event.StringValue("abc"), OpEq, true},
		{`"abc"<"def"`, event.StringValue("abc"), event.StringValue("def"), OpLt, true},
		// Mixed int/float
		{"int5==float5", event.IntValue(5), event.FloatValue(5.0), OpEq, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{}
			p.AddConstant(tt.a)
			p.AddConstant(tt.b)
			p.EmitOp(OpConstInt, 0) // works for any const
			p.EmitOp(OpConstInt, 1)
			p.EmitOp(tt.op)
			p.EmitOp(OpReturn)

			result := runProgram(t, p, nil)
			if result.Type() != event.FieldTypeBool || result.AsBool() != tt.want {
				t.Errorf("got %v, want %v", result, tt.want)
			}
		})
	}
}

func TestVMLogicOperators(t *testing.T) {
	tests := []struct {
		name string
		a, b event.Value
		op   Opcode
		want bool
	}{
		{"true AND true", event.BoolValue(true), event.BoolValue(true), OpAnd, true},
		{"true AND false", event.BoolValue(true), event.BoolValue(false), OpAnd, false},
		{"true OR false", event.BoolValue(true), event.BoolValue(false), OpOr, true},
		{"false OR false", event.BoolValue(false), event.BoolValue(false), OpOr, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Program{}
			p.AddConstant(tt.a)
			p.AddConstant(tt.b)
			p.EmitOp(OpConstInt, 0)
			p.EmitOp(OpConstInt, 1)
			p.EmitOp(tt.op)
			p.EmitOp(OpReturn)

			result := runProgram(t, p, nil)
			if result.AsBool() != tt.want {
				t.Errorf("got %v, want %v", result, tt.want)
			}
		})
	}

	// NOT
	t.Run("NOT true", func(t *testing.T) {
		p := &Program{}
		p.EmitOp(OpConstTrue)
		p.EmitOp(OpNot)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsBool() != false {
			t.Errorf("got %v, want false", result)
		}
	})
}

func TestVMFieldAccess(t *testing.T) {
	fields := map[string]event.Value{
		"status": event.IntValue(200),
		"host":   event.StringValue("web-01"),
	}

	// Load existing field
	t.Run("load existing", func(t *testing.T) {
		p := &Program{}
		p.AddFieldName("status")
		p.EmitOp(OpLoadField, 0)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, fields)
		if result.AsInt() != 200 {
			t.Errorf("got %v, want 200", result)
		}
	})

	// Load missing field → null
	t.Run("load missing → null", func(t *testing.T) {
		p := &Program{}
		p.AddFieldName("missing")
		p.EmitOp(OpLoadField, 0)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, fields)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})

	// Store field
	t.Run("store field", func(t *testing.T) {
		mutableFields := map[string]event.Value{
			"x": event.IntValue(1),
		}
		p := &Program{}
		p.AddFieldName("y")
		p.AddConstant(event.IntValue(42))
		p.EmitOp(OpConstInt, 0)
		p.EmitOp(OpStoreField, 0)
		// push the stored value to check
		p.EmitOp(OpLoadField, 0)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, mutableFields)
		if result.AsInt() != 42 {
			t.Errorf("got %v, want 42", result)
		}
		if mutableFields["y"].AsInt() != 42 {
			t.Errorf("field not stored: %v", mutableFields["y"])
		}
	})

	// FieldExists
	t.Run("field exists", func(t *testing.T) {
		p := &Program{}
		p.AddFieldName("status")
		p.AddFieldName("missing")
		p.EmitOp(OpFieldExists, 0)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, fields)
		if !result.AsBool() {
			t.Errorf("expected true for existing field")
		}
	})

	t.Run("field not exists", func(t *testing.T) {
		p := &Program{}
		p.AddFieldName("missing")
		p.EmitOp(OpFieldExists, 0)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, fields)
		if result.AsBool() {
			t.Errorf("expected false for missing field")
		}
	})
}

func TestVMNullHandling(t *testing.T) {
	// null + 5 → null
	t.Run("null+5", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.IntValue(5))
		p.EmitOp(OpConstNull)
		p.EmitOp(OpConstInt, 0)
		p.EmitOp(OpAdd)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})

	// null == null → true
	t.Run("null==null", func(t *testing.T) {
		p := &Program{}
		p.EmitOp(OpConstNull)
		p.EmitOp(OpConstNull)
		p.EmitOp(OpEq)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.AsBool() {
			t.Errorf("expected true, got %v", result)
		}
	})

	// null > 5 → false (null compares as less than everything)
	t.Run("null>5", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.IntValue(5))
		p.EmitOp(OpConstNull)
		p.EmitOp(OpConstInt, 0)
		p.EmitOp(OpGt)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsBool() {
			t.Errorf("expected false, got %v", result)
		}
	})
}

func TestVMTypeConversion(t *testing.T) {
	// tonumber("42") → 42
	t.Run("tonumber string→int", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("42"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpToInt)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.Type() != event.FieldTypeInt || result.AsInt() != 42 {
			t.Errorf("got %v, want int(42)", result)
		}
	})

	// tostring(42) → "42"
	t.Run("tostring int→string", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.IntValue(42))
		p.EmitOp(OpConstInt, 0)
		p.EmitOp(OpToString)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.Type() != event.FieldTypeString || result.AsString() != "42" {
			t.Errorf("got %v, want string(42)", result)
		}
	})

	// tonumber("abc") → null
	t.Run("tonumber invalid→null", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("abc"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpToInt)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})

	// tofloat
	t.Run("tofloat string→float", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("3.14"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpToFloat)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.Type() != event.FieldTypeFloat || math.Abs(result.AsFloat()-3.14) > 1e-10 {
			t.Errorf("got %v, want float(3.14)", result)
		}
	})
}

func TestVMJumpIfFalse(t *testing.T) {
	// IF(status >= 500, "error", "ok")
	// Implementation: push status, push 500, OpGte, OpJumpIfFalse → else,
	//   push "error", OpJump → end, push "ok", return
	fields := map[string]event.Value{"status": event.IntValue(503)}

	p := &Program{}
	statusIdx := p.AddFieldName("status")
	c500 := p.AddConstant(event.IntValue(500))
	cError := p.AddConstant(event.StringValue("error"))
	cOk := p.AddConstant(event.StringValue("ok"))

	p.EmitOp(OpLoadField, statusIdx)
	p.EmitOp(OpConstInt, c500)
	p.EmitOp(OpGte)
	jumpFalse := p.EmitOp(OpJumpIfFalse, 0) // placeholder
	p.EmitOp(OpConstStr, cError)
	jumpEnd := p.EmitOp(OpJump, 0) // placeholder
	elsePos := p.Len()
	p.EmitOp(OpConstStr, cOk)
	endPos := p.Len()
	p.EmitOp(OpReturn)

	// Patch jumps
	p.PatchUint16(jumpFalse+1, uint16(elsePos))
	p.PatchUint16(jumpEnd+1, uint16(endPos))

	result := runProgram(t, p, fields)
	if result.AsString() != "error" {
		t.Errorf("got %q, want %q", result.AsString(), "error")
	}

	// Now test with status=200
	fields["status"] = event.IntValue(200)
	result = runProgram(t, p, fields)
	if result.AsString() != "ok" {
		t.Errorf("got %q, want %q", result.AsString(), "ok")
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

func TestVMIsNotNull(t *testing.T) {
	t.Run("isnotnull('x')→true", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("x"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpIsNotNull)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.AsBool() {
			t.Errorf("expected true")
		}
	})

	t.Run("isnotnull(null)→false", func(t *testing.T) {
		p := &Program{}
		p.EmitOp(OpConstNull)
		p.EmitOp(OpIsNotNull)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsBool() {
			t.Errorf("expected false")
		}
	})
}

func TestVMRegexMatch(t *testing.T) {
	p := &Program{}
	p.AddConstant(event.StringValue("error_code=42"))
	regIdx := p.AddRegex(`\d+`)
	p.EmitOp(OpConstStr, 0)
	p.EmitOp(OpStrMatch, regIdx)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if !result.AsBool() {
		t.Errorf("expected true for regex match")
	}

	// Non-matching
	p2 := &Program{}
	p2.AddConstant(event.StringValue("no-digits-here"))
	regIdx2 := p2.AddRegex(`^\d+$`)
	p2.EmitOp(OpConstStr, 0)
	p2.EmitOp(OpStrMatch, regIdx2)
	p2.EmitOp(OpReturn)

	result = runProgram(t, p2, nil)
	if result.AsBool() {
		t.Errorf("expected false for non-match")
	}
}

func TestVMSubstring(t *testing.T) {
	// substr("hello", 1, 3) → "hel" (1-indexed like SPL2)
	p := &Program{}
	p.AddConstant(event.StringValue("hello"))
	p.AddConstant(event.IntValue(1))
	p.AddConstant(event.IntValue(3))
	p.EmitOp(OpConstStr, 0)
	p.EmitOp(OpConstInt, 1)
	p.EmitOp(OpConstInt, 2)
	p.EmitOp(OpSubstr)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsString() != "hel" {
		t.Errorf("got %q, want %q", result.AsString(), "hel")
	}
}

func TestVMRound(t *testing.T) {
	// round(3.14159, 2) → 3.14
	p := &Program{}
	p.AddConstant(event.FloatValue(3.14159))
	p.AddConstant(event.IntValue(2))
	p.EmitOp(OpConstFloat, 0)
	p.EmitOp(OpConstInt, 1)
	p.EmitOp(OpRound)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if math.Abs(result.AsFloat()-3.14) > 1e-10 {
		t.Errorf("got %v, want 3.14", result.AsFloat())
	}
}

func TestVMLn(t *testing.T) {
	// ln(1) → 0
	t.Run("ln(1)=0", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.FloatValue(1))
		p.EmitOp(OpConstFloat, 0)
		p.EmitOp(OpLn)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if math.Abs(result.AsFloat()) > 1e-10 {
			t.Errorf("got %v, want 0", result.AsFloat())
		}
	})

	// ln(e) → 1.0
	t.Run("ln(e)=1", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.FloatValue(math.E))
		p.EmitOp(OpConstFloat, 0)
		p.EmitOp(OpLn)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if math.Abs(result.AsFloat()-1.0) > 1e-10 {
			t.Errorf("got %v, want 1.0", result.AsFloat())
		}
	})
}

func TestVMMathFunctions(t *testing.T) {
	t.Run("abs(-5)=5", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.IntValue(-5))
		p.EmitOp(OpConstInt, 0)
		p.EmitOp(OpAbs)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsInt() != 5 {
			t.Errorf("got %v, want 5", result)
		}
	})

	t.Run("ceil(2.3)=3", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.FloatValue(2.3))
		p.EmitOp(OpConstFloat, 0)
		p.EmitOp(OpCeil)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsFloat() != 3.0 {
			t.Errorf("got %v, want 3.0", result)
		}
	})

	t.Run("floor(2.7)=2", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.FloatValue(2.7))
		p.EmitOp(OpConstFloat, 0)
		p.EmitOp(OpFloor)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsFloat() != 2.0 {
			t.Errorf("got %v, want 2.0", result)
		}
	})

	t.Run("sqrt(25)=5", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.FloatValue(25))
		p.EmitOp(OpConstFloat, 0)
		p.EmitOp(OpSqrt)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if math.Abs(result.AsFloat()-5.0) > 1e-10 {
			t.Errorf("got %v, want 5.0", result)
		}
	})
}

func TestVMMvAppend(t *testing.T) {
	// mvappend("a", "b", "c") → "a|||b|||c"
	p := &Program{}
	p.AddConstant(event.StringValue("a"))
	p.AddConstant(event.StringValue("b"))
	p.AddConstant(event.StringValue("c"))
	p.EmitOp(OpConstStr, 0)
	p.EmitOp(OpConstStr, 1)
	p.EmitOp(OpConstStr, 2)
	p.EmitOp(OpMvAppend, 3)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsString() != "a|||b|||c" {
		t.Errorf("got %q, want %q", result.AsString(), "a|||b|||c")
	}
}

func TestVMMvJoin(t *testing.T) {
	// mvjoin("a|||b|||c", ",") → "a,b,c"
	p := &Program{}
	p.AddConstant(event.StringValue("a|||b|||c"))
	p.AddConstant(event.StringValue(","))
	p.EmitOp(OpConstStr, 0)
	p.EmitOp(OpConstStr, 1)
	p.EmitOp(OpMvJoin)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsString() != "a,b,c" {
		t.Errorf("got %q, want %q", result.AsString(), "a,b,c")
	}
}

func TestVMMvDedup(t *testing.T) {
	// mvdedup("a|||b|||a") → "a|||b"
	p := &Program{}
	p.AddConstant(event.StringValue("a|||b|||a"))
	p.EmitOp(OpConstStr, 0)
	p.EmitOp(OpMvDedup)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsString() != "a|||b" {
		t.Errorf("got %q, want %q", result.AsString(), "a|||b")
	}
}

func TestVMStrftime(t *testing.T) {
	ts := time.Date(2026, 2, 10, 8, 12, 3, 0, time.UTC)
	p := &Program{}
	p.AddConstant(event.TimestampValue(ts))
	p.AddConstant(event.StringValue("%H:%M:%S"))
	p.EmitOp(OpConstInt, 0) // timestamps use same const loading
	p.EmitOp(OpConstStr, 1)
	p.EmitOp(OpStrftime)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsString() != "08:12:03" {
		t.Errorf("got %q, want %q", result.AsString(), "08:12:03")
	}
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

func TestVMStackUnderflow(t *testing.T) {
	p := &Program{}
	p.EmitOp(OpPop)
	p.EmitOp(OpReturn)

	vm := &VM{}
	_, err := vm.Execute(p, nil)
	if !errors.Is(err, ErrStackUnderflow) {
		t.Errorf("expected ErrStackUnderflow, got %v", err)
	}
}

func TestVMDup(t *testing.T) {
	p := &Program{}
	p.AddConstant(event.IntValue(42))
	p.EmitOp(OpConstInt, 0)
	p.EmitOp(OpDup)
	p.EmitOp(OpAddInt)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if result.AsInt() != 84 {
		t.Errorf("got %v, want 84", result)
	}
}

func TestVMDivByZero(t *testing.T) {
	p := &Program{}
	p.AddConstant(event.IntValue(10))
	p.AddConstant(event.IntValue(0))
	p.EmitOp(OpConstInt, 0)
	p.EmitOp(OpConstInt, 1)
	p.EmitOp(OpDivInt)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, nil)
	if !result.IsNull() {
		t.Errorf("expected null for div by zero, got %v", result)
	}
}

func TestVMConcatNullPropagation(t *testing.T) {
	// null . "hello" → null
	t.Run("null+string→null", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("hello"))
		p.EmitOp(OpConstNull)
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpConcat)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})

	// "hello" . null → null
	t.Run("string+null→null", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("hello"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpConstNull)
		p.EmitOp(OpConcat)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})

	// null . null → null
	t.Run("null+null→null", func(t *testing.T) {
		p := &Program{}
		p.EmitOp(OpConstNull)
		p.EmitOp(OpConstNull)
		p.EmitOp(OpConcat)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})

	// tonumber(null) → null
	t.Run("tonumber(null)→null", func(t *testing.T) {
		p := &Program{}
		p.EmitOp(OpConstNull)
		p.EmitOp(OpToInt)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})

	// tofloat(null) → null
	t.Run("tofloat(null)→null", func(t *testing.T) {
		p := &Program{}
		p.EmitOp(OpConstNull)
		p.EmitOp(OpToFloat)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if !result.IsNull() {
			t.Errorf("expected null, got %v", result)
		}
	})
}

func TestVMStringOps(t *testing.T) {
	t.Run("strlen", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("hello"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpStrLen)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsInt() != 5 {
			t.Errorf("got %v, want 5", result)
		}
	})

	t.Run("lower", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("HELLO"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpToLower)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsString() != "hello" {
			t.Errorf("got %q, want %q", result.AsString(), "hello")
		}
	})

	t.Run("upper", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("hello"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpToUpper)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsString() != "HELLO" {
			t.Errorf("got %q, want %q", result.AsString(), "HELLO")
		}
	})

	t.Run("concat", func(t *testing.T) {
		p := &Program{}
		p.AddConstant(event.StringValue("foo"))
		p.AddConstant(event.StringValue("bar"))
		p.EmitOp(OpConstStr, 0)
		p.EmitOp(OpConstStr, 1)
		p.EmitOp(OpConcat)
		p.EmitOp(OpReturn)

		result := runProgram(t, p, nil)
		if result.AsString() != "foobar" {
			t.Errorf("got %q, want %q", result.AsString(), "foobar")
		}
	})
}

func TestVMContextual(t *testing.T) {
	// Complex expression: status >= 500 AND host = "web-01"
	fields := map[string]event.Value{
		"status": event.IntValue(503),
		"host":   event.StringValue("web-01"),
	}

	p := &Program{}
	statusIdx := p.AddFieldName("status")
	hostIdx := p.AddFieldName("host")
	c500 := p.AddConstant(event.IntValue(500))
	cWeb01 := p.AddConstant(event.StringValue("web-01"))

	// status >= 500
	p.EmitOp(OpLoadField, statusIdx)
	p.EmitOp(OpConstInt, c500)
	p.EmitOp(OpGte)
	// host == "web-01"
	p.EmitOp(OpLoadField, hostIdx)
	p.EmitOp(OpConstStr, cWeb01)
	p.EmitOp(OpEq)
	// AND
	p.EmitOp(OpAnd)
	p.EmitOp(OpReturn)

	result := runProgram(t, p, fields)
	if !result.AsBool() {
		t.Errorf("expected true, got %v", result)
	}
}

func TestVMInList(t *testing.T) {
	// status IN (200, 201, 204)
	p := &Program{}
	statusIdx := p.AddFieldName("status")
	c200 := p.AddConstant(event.IntValue(200))
	c201 := p.AddConstant(event.IntValue(201))
	c204 := p.AddConstant(event.IntValue(204))

	p.EmitOp(OpLoadField, statusIdx)
	p.EmitOp(OpConstInt, c200)
	p.EmitOp(OpConstInt, c201)
	p.EmitOp(OpConstInt, c204)
	p.EmitOp(OpInList, 3)
	p.EmitOp(OpReturn)

	fields := map[string]event.Value{"status": event.IntValue(201)}
	result := runProgram(t, p, fields)
	if !result.AsBool() {
		t.Errorf("expected true for 201 IN (200,201,204)")
	}

	fields["status"] = event.IntValue(404)
	result = runProgram(t, p, fields)
	if result.AsBool() {
		t.Errorf("expected false for 404 IN (200,201,204)")
	}
}

func TestVMReuse(t *testing.T) {
	// Verify VM can be reused across multiple Execute calls
	p := &Program{}
	fIdx := p.AddFieldName("x")
	p.EmitOp(OpLoadField, fIdx)
	p.EmitOp(OpReturn)

	vm := &VM{}
	for i := 0; i < 100; i++ {
		fields := map[string]event.Value{"x": event.IntValue(int64(i))}
		result, err := vm.Execute(p, fields)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if result.AsInt() != int64(i) {
			t.Fatalf("iteration %d: got %v, want %d", i, result, i)
		}
	}
}

// Opcode encoding tests

func TestMakeAndReadOperands(t *testing.T) {
	tests := []struct {
		op       Opcode
		operands []int
		want     []byte
	}{
		{OpConstInt, []int{256}, []byte{byte(OpConstInt), 1, 0}},
		{OpConstTrue, nil, []byte{byte(OpConstTrue)}},
		{OpJump, []int{100}, []byte{byte(OpJump), 0, 100}},
		{OpLoadField, []int{5}, []byte{byte(OpLoadField), 0, 5}},
	}

	for _, tt := range tests {
		t.Run(tt.op.String(), func(t *testing.T) {
			got := Make(tt.op, tt.operands...)
			if len(got) != len(tt.want) {
				t.Fatalf("length: got %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("byte[%d]: got 0x%02x, want 0x%02x", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// Benchmarks

func BenchmarkVMSimplePredicate(b *testing.B) {
	// status >= 500
	p := &Program{}
	statusIdx := p.AddFieldName("status")
	c500 := p.AddConstant(event.IntValue(500))
	p.EmitOp(OpLoadField, statusIdx)
	p.EmitOp(OpConstInt, c500)
	p.EmitOp(OpGte)
	p.EmitOp(OpReturn)

	fields := map[string]event.Value{"status": event.IntValue(503)}
	vm := &VM{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		vm.Execute(p, fields)
	}
}

func BenchmarkVMArithmetic(b *testing.B) {
	// (x + 1) * 2
	p := &Program{}
	xIdx := p.AddFieldName("x")
	c1 := p.AddConstant(event.IntValue(1))
	c2 := p.AddConstant(event.IntValue(2))
	p.EmitOp(OpLoadField, xIdx)
	p.EmitOp(OpConstInt, c1)
	p.EmitOp(OpAddInt)
	p.EmitOp(OpConstInt, c2)
	p.EmitOp(OpMulInt)
	p.EmitOp(OpReturn)

	fields := map[string]event.Value{"x": event.IntValue(10)}
	vm := &VM{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		vm.Execute(p, fields)
	}
}

func BenchmarkVMComplexPredicate(b *testing.B) {
	// status >= 500 AND host = "web-01"
	p := &Program{}
	statusIdx := p.AddFieldName("status")
	hostIdx := p.AddFieldName("host")
	c500 := p.AddConstant(event.IntValue(500))
	cWeb01 := p.AddConstant(event.StringValue("web-01"))

	p.EmitOp(OpLoadField, statusIdx)
	p.EmitOp(OpConstInt, c500)
	p.EmitOp(OpGte)
	p.EmitOp(OpLoadField, hostIdx)
	p.EmitOp(OpConstStr, cWeb01)
	p.EmitOp(OpEq)
	p.EmitOp(OpAnd)
	p.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"status": event.IntValue(503),
		"host":   event.StringValue("web-01"),
	}
	vm := &VM{}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		vm.Execute(p, fields)
	}
}

// helpers

func buildArith(ops ...interface{}) *Program {
	p := &Program{}
	constants := []event.Value{}
	var instructions []interface{}

	// Separate constants from instructions
	for _, op := range ops {
		switch v := op.(type) {
		case event.Value:
			constants = append(constants, v)
		default:
			instructions = append(instructions, v)
		}
	}

	for _, c := range constants {
		p.AddConstant(c)
	}

	i := 0
	for i < len(instructions) {
		if v, ok := instructions[i].(Opcode); ok {
			def := definitions[v]
			if def != nil && len(def.OperandWidths) > 0 {
				i++
				operand := instructions[i].(int)
				p.EmitOp(v, operand)
			} else {
				p.EmitOp(v)
			}
		}
		i++
	}
	p.EmitOp(OpReturn)

	return p
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

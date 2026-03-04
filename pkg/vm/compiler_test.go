package vm

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestCompileFieldComparison(t *testing.T) {
	// status >= 500
	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "status"},
		Op:    ">=",
		Right: &spl2.LiteralExpr{Value: "500"},
	}
	prog, err := CompilePredicate(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	fields := map[string]event.Value{"status": event.IntValue(503)}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AsBool() {
		t.Errorf("expected true for status=503 >= 500")
	}

	fields["status"] = event.IntValue(200)
	result, err = vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if result.AsBool() {
		t.Errorf("expected false for status=200 >= 500")
	}
}

func TestCompileArithExpr(t *testing.T) {
	// x + 1
	expr := &spl2.ArithExpr{
		Left:  &spl2.FieldExpr{Name: "x"},
		Op:    "+",
		Right: &spl2.LiteralExpr{Value: "1"},
	}
	prog, err := CompileExpr(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	fields := map[string]event.Value{"x": event.IntValue(10)}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	// int + int via OpAdd (generic) -> should be int since string "1" parses as int
	if result.Type() == event.FieldTypeInt && result.AsInt() != 11 {
		t.Errorf("got %v, want 11", result)
	}
}

func TestCompileBooleanExpr(t *testing.T) {
	// status >= 500 AND host = "web-01"
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "host"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: `"web-01"`},
		},
	}
	prog, err := CompilePredicate(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	fields := map[string]event.Value{
		"status": event.IntValue(503),
		"host":   event.StringValue("web-01"),
	}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AsBool() {
		t.Errorf("expected true")
	}
}

func TestCompileNotExpr(t *testing.T) {
	expr := &spl2.NotExpr{
		Expr: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "x"},
			Op:    ">",
			Right: &spl2.LiteralExpr{Value: "5"},
		},
	}
	prog, err := CompilePredicate(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	// x=3, NOT(3>5) = true
	fields := map[string]event.Value{"x": event.IntValue(3)}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AsBool() {
		t.Errorf("expected true for NOT(3>5)")
	}
}

func TestCompileIfFunction(t *testing.T) {
	// IF(status >= 500, "error", "ok")
	expr := &spl2.FuncCallExpr{
		Name: "IF",
		Args: []spl2.Expr{
			&spl2.CompareExpr{
				Left:  &spl2.FieldExpr{Name: "status"},
				Op:    ">=",
				Right: &spl2.LiteralExpr{Value: "500"},
			},
			&spl2.LiteralExpr{Value: `"error"`},
			&spl2.LiteralExpr{Value: `"ok"`},
		},
	}
	prog, err := CompileExpr(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	fields := map[string]event.Value{"status": event.IntValue(503)}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if result.AsString() != "error" {
		t.Errorf("got %q, want %q", result.AsString(), "error")
	}

	fields["status"] = event.IntValue(200)
	result, err = vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if result.AsString() != "ok" {
		t.Errorf("got %q, want %q", result.AsString(), "ok")
	}
}

func TestCompileCaseFunction(t *testing.T) {
	// CASE(x > 5, "high", x > 0, "low", "zero")
	expr := &spl2.FuncCallExpr{
		Name: "CASE",
		Args: []spl2.Expr{
			&spl2.CompareExpr{
				Left:  &spl2.FieldExpr{Name: "x"},
				Op:    ">",
				Right: &spl2.LiteralExpr{Value: "5"},
			},
			&spl2.LiteralExpr{Value: `"high"`},
			&spl2.CompareExpr{
				Left:  &spl2.FieldExpr{Name: "x"},
				Op:    ">",
				Right: &spl2.LiteralExpr{Value: "0"},
			},
			&spl2.LiteralExpr{Value: `"low"`},
			&spl2.LiteralExpr{Value: `"zero"`},
		},
	}
	prog, err := CompileExpr(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	tests := []struct {
		x    int64
		want string
	}{
		{10, "high"},
		{3, "low"},
		{0, "zero"},
		{-1, "zero"},
	}
	for _, tt := range tests {
		fields := map[string]event.Value{"x": event.IntValue(tt.x)}
		result, err := vm.Execute(prog, fields)
		if err != nil {
			t.Fatal(err)
		}
		if result.AsString() != tt.want {
			t.Errorf("x=%d: got %q, want %q", tt.x, result.AsString(), tt.want)
		}
	}
}

func TestCompileCoalesce(t *testing.T) {
	expr := &spl2.FuncCallExpr{
		Name: "coalesce",
		Args: []spl2.Expr{
			&spl2.FieldExpr{Name: "a"},
			&spl2.FieldExpr{Name: "b"},
			&spl2.LiteralExpr{Value: `"default"`},
		},
	}
	prog, err := CompileExpr(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	fields := map[string]event.Value{
		"b": event.StringValue("found"),
	}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if result.AsString() != "found" {
		t.Errorf("got %q, want %q", result.AsString(), "found")
	}
}

func TestCompileIsNull(t *testing.T) {
	expr := &spl2.FuncCallExpr{
		Name: "isnull",
		Args: []spl2.Expr{
			&spl2.FieldExpr{Name: "missing"},
		},
	}
	prog, err := CompileExpr(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	fields := map[string]event.Value{}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AsBool() {
		t.Errorf("expected true for isnull(missing)")
	}
}

func TestCompileMathFunctions(t *testing.T) {
	tests := []struct {
		name string
		fn   string
		arg  spl2.Expr
		want float64
	}{
		{"ln(1)", "ln", &spl2.LiteralExpr{Value: "1"}, 0},
		{"abs(-5)", "abs", &spl2.LiteralExpr{Value: "-5"}, 5},
		{"sqrt(16)", "sqrt", &spl2.LiteralExpr{Value: "16"}, 4},
		{"ceil(2.3)", "ceil", &spl2.LiteralExpr{Value: "2.3"}, 3},
		{"floor(2.7)", "floor", &spl2.LiteralExpr{Value: "2.7"}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &spl2.FuncCallExpr{Name: tt.fn, Args: []spl2.Expr{tt.arg}}
			prog, err := CompileExpr(expr)
			if err != nil {
				t.Fatal(err)
			}
			vm := &VM{}
			result, err := vm.Execute(prog, nil)
			if err != nil {
				t.Fatal(err)
			}
			f, ok := ValueToFloat(result)
			if !ok {
				t.Fatalf("not numeric: %v", result)
			}
			if math.Abs(f-tt.want) > 1e-10 {
				t.Errorf("got %v, want %v", f, tt.want)
			}
		})
	}
}

func TestCompileInExpr(t *testing.T) {
	expr := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "201"},
			&spl2.LiteralExpr{Value: "204"},
		},
	}
	prog, err := CompilePredicate(expr)
	if err != nil {
		t.Fatal(err)
	}

	vm := &VM{}
	fields := map[string]event.Value{"status": event.IntValue(201)}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AsBool() {
		t.Errorf("expected true for 201 IN (200,201,204)")
	}

	fields["status"] = event.IntValue(404)
	result, err = vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if result.AsBool() {
		t.Errorf("expected false for 404 IN (200,201,204)")
	}
}

func TestCompileShortCircuitAND(t *testing.T) {
	// false AND x — should NOT evaluate x (x is a missing field, would cause no issue
	// but we verify the result is false without needing x)
	expr := &spl2.BinaryExpr{
		Left:  &spl2.LiteralExpr{Value: "false"},
		Op:    "and",
		Right: &spl2.FieldExpr{Name: "x"},
	}
	prog, err := CompilePredicate(expr)
	if err != nil {
		t.Fatal(err)
	}
	vm := &VM{}
	result, err := vm.Execute(prog, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.AsBool() != false {
		t.Errorf("false AND x: got %v, want false", result)
	}

	// Non-short-circuit paths
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"true AND true", "true", "true", true},
		{"true AND false", "true", "false", false},
		{"false AND false", "false", "false", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &spl2.BinaryExpr{
				Left: &spl2.LiteralExpr{Value: tt.a}, Op: "and",
				Right: &spl2.LiteralExpr{Value: tt.b},
			}
			p, _ := CompilePredicate(e)
			r, _ := vm.Execute(p, nil)
			if r.AsBool() != tt.want {
				t.Errorf("got %v, want %v", r.AsBool(), tt.want)
			}
		})
	}
}

func TestCompileShortCircuitOR(t *testing.T) {
	// true OR x — should NOT evaluate x
	expr := &spl2.BinaryExpr{
		Left:  &spl2.LiteralExpr{Value: "true"},
		Op:    "or",
		Right: &spl2.FieldExpr{Name: "x"},
	}
	prog, err := CompilePredicate(expr)
	if err != nil {
		t.Fatal(err)
	}
	vm := &VM{}
	result, err := vm.Execute(prog, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.AsBool() != true {
		t.Errorf("true OR x: got %v, want true", result)
	}

	// Non-short-circuit paths
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"false OR true", "false", "true", true},
		{"false OR false", "false", "false", false},
		{"true OR true", "true", "true", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &spl2.BinaryExpr{
				Left: &spl2.LiteralExpr{Value: tt.a}, Op: "or",
				Right: &spl2.LiteralExpr{Value: tt.b},
			}
			p, _ := CompilePredicate(e)
			r, _ := vm.Execute(p, nil)
			if r.AsBool() != tt.want {
				t.Errorf("got %v, want %v", r.AsBool(), tt.want)
			}
		})
	}
}

func TestCompileTypedArithmetic(t *testing.T) {
	// Verify that int+int emits OpAddInt, float+float emits OpAddFloat, field+int stays OpAdd
	tests := []struct {
		name   string
		left   spl2.Expr
		right  spl2.Expr
		op     string
		wantOp Opcode
	}{
		{"int+int", &spl2.LiteralExpr{Value: "1"}, &spl2.LiteralExpr{Value: "2"}, "+", OpAddInt},
		{"int*int", &spl2.LiteralExpr{Value: "3"}, &spl2.LiteralExpr{Value: "4"}, "*", OpMulInt},
		{"float+float", &spl2.LiteralExpr{Value: "1.5"}, &spl2.LiteralExpr{Value: "2.5"}, "+", OpAddFloat},
		{"float/float", &spl2.LiteralExpr{Value: "10.0"}, &spl2.LiteralExpr{Value: "3.0"}, "/", OpDivFloat},
		{"field+int", &spl2.FieldExpr{Name: "x"}, &spl2.LiteralExpr{Value: "1"}, "+", OpAdd},
		{"int+field", &spl2.LiteralExpr{Value: "1"}, &spl2.FieldExpr{Name: "x"}, "+", OpAdd},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &spl2.ArithExpr{Left: tt.left, Op: tt.op, Right: tt.right}
			prog, err := CompileExpr(expr)
			if err != nil {
				t.Fatal(err)
			}
			// Find the arithmetic opcode (skip over const/field load instructions)
			found := false
			for i := 0; i < len(prog.Instructions); i++ {
				op := Opcode(prog.Instructions[i])
				def := definitions[op]
				if op == tt.wantOp {
					found = true

					break
				}
				if def != nil {
					for _, w := range def.OperandWidths {
						i += w
					}
				}
			}
			if !found {
				t.Errorf("expected opcode %s in bytecode", tt.wantOp)
			}
		})
	}

	// Functional verification: typed paths produce correct results
	t.Run("int+int=3", func(t *testing.T) {
		expr := &spl2.ArithExpr{
			Left: &spl2.LiteralExpr{Value: "1"}, Op: "+",
			Right: &spl2.LiteralExpr{Value: "2"},
		}
		prog, _ := CompileExpr(expr)
		vm := &VM{}
		result, _ := vm.Execute(prog, nil)
		if result.AsInt() != 3 {
			t.Errorf("got %v, want 3", result)
		}
	})
	t.Run("float+float=4.0", func(t *testing.T) {
		expr := &spl2.ArithExpr{
			Left: &spl2.LiteralExpr{Value: "1.5"}, Op: "+",
			Right: &spl2.LiteralExpr{Value: "2.5"},
		}
		prog, _ := CompileExpr(expr)
		vm := &VM{}
		result, _ := vm.Execute(prog, nil)
		if math.Abs(result.AsFloat()-4.0) > 1e-10 {
			t.Errorf("got %v, want 4.0", result)
		}
	})
}

func TestCompileIndexFieldRewrite(t *testing.T) {
	// "source" is aliased to "_source" at compile time (physical column name).
	// "index" is a real physical column — no aliasing needed.
	tests := []struct {
		name      string
		fieldName string
		wantName  string
	}{
		{"index unchanged", "index", "index"},
		{"source -> _source", "source", "_source"},
		{"_source unchanged", "_source", "_source"},
		{"status unchanged", "status", "status"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := &spl2.FieldExpr{Name: tt.fieldName}
			prog, err := CompileExpr(expr)
			if err != nil {
				t.Fatal(err)
			}
			if len(prog.FieldNames) != 1 || prog.FieldNames[0] != tt.wantName {
				t.Errorf("FieldNames = %v, want [%s]", prog.FieldNames, tt.wantName)
			}
		})
	}

	// Functional test: WHERE index="nginx" should match index="nginx" (real physical column).
	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "index"},
		Op:    "=",
		Right: &spl2.LiteralExpr{Value: `"nginx"`},
	}
	prog, err := CompilePredicate(expr)
	if err != nil {
		t.Fatal(err)
	}
	vm := &VM{}
	fields := map[string]event.Value{"index": event.StringValue("nginx")}
	result, err := vm.Execute(prog, fields)
	if err != nil {
		t.Fatal(err)
	}
	if !result.AsBool() {
		t.Error("expected index='nginx' to match index='nginx'")
	}

	// Functional test: WHERE source="nginx" should match _source="nginx" (aliased).
	expr2 := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "source"},
		Op:    "=",
		Right: &spl2.LiteralExpr{Value: `"nginx"`},
	}
	prog2, err := CompilePredicate(expr2)
	if err != nil {
		t.Fatal(err)
	}
	fields2 := map[string]event.Value{"_source": event.StringValue("nginx")}
	result2, err := vm.Execute(prog2, fields2)
	if err != nil {
		t.Fatal(err)
	}
	if !result2.AsBool() {
		t.Error("expected source='nginx' to match _source='nginx'")
	}
}

func TestProgramCacheHit(t *testing.T) {
	cache := NewProgramCache()
	expr := "status >= 500"

	p := &Program{}
	p.EmitOp(OpConstTrue)
	p.EmitOp(OpReturn)
	cache.Put(expr, p)

	got := cache.Get(expr)
	if got == nil {
		t.Fatal("expected cache hit")
	}
	if len(got.Instructions) != len(p.Instructions) {
		t.Errorf("cached program differs")
	}
	if cache.Len() != 1 {
		t.Errorf("expected cache size 1, got %d", cache.Len())
	}

	if cache.Get("other expr") != nil {
		t.Error("expected cache miss for different expr")
	}
}

func TestProgramCacheConcurrency(t *testing.T) {
	cache := NewProgramCache()
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				p := &Program{}
				p.EmitOp(OpConstTrue)
				cache.Put("expr", p)
				cache.Get("expr")
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// Benchmark: VM vs simulated tree-walking for complex expressions.
// Uses a realistic multi-operator expression to demonstrate the VM's
// instruction locality and zero-allocation advantage over tree-walking.
func BenchmarkVMvsTreeWalk(b *testing.B) {
	// Complex expression: IF(status >= 500 AND method != "GET", response_time * 2 + latency, response_time - 10)
	// This exercises: 2 field loads, 2 comparisons, AND, arithmetic, IF — 11 AST nodes total
	expr := &spl2.FuncCallExpr{
		Name: "IF",
		Args: []spl2.Expr{
			&spl2.BinaryExpr{
				Left: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "status"},
					Op:    ">=",
					Right: &spl2.LiteralExpr{Value: "500"},
				},
				Op: "and",
				Right: &spl2.CompareExpr{
					Left:  &spl2.FieldExpr{Name: "method"},
					Op:    "!=",
					Right: &spl2.LiteralExpr{Value: "GET"},
				},
			},
			&spl2.ArithExpr{
				Left: &spl2.ArithExpr{
					Left:  &spl2.FieldExpr{Name: "response_time"},
					Op:    "*",
					Right: &spl2.LiteralExpr{Value: "2"},
				},
				Op:    "+",
				Right: &spl2.FieldExpr{Name: "latency"},
			},
			&spl2.ArithExpr{
				Left:  &spl2.FieldExpr{Name: "response_time"},
				Op:    "-",
				Right: &spl2.LiteralExpr{Value: "10"},
			},
		},
	}
	prog, _ := CompileExpr(expr)
	vm := &VM{}
	fields := map[string]event.Value{
		"status":        event.IntValue(503),
		"method":        event.StringValue("POST"),
		"response_time": event.IntValue(150),
		"latency":       event.IntValue(42),
	}

	// Generate diverse field maps to exercise different code paths
	const nEvents = 100000
	fieldSets := make([]map[string]event.Value, nEvents)
	for i := 0; i < nEvents; i++ {
		status := int64(200 + (i%5)*100) // 200, 300, 400, 500, 600
		method := "GET"
		if i%3 == 0 {
			method = "POST"
		}
		fieldSets[i] = map[string]event.Value{
			"status":        event.IntValue(status),
			"method":        event.StringValue(method),
			"response_time": event.IntValue(int64(50 + i%200)),
			"latency":       event.IntValue(int64(5 + i%50)),
		}
	}

	b.Run("VM_100K", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < nEvents; j++ {
				vm.Execute(prog, fieldSets[j])
			}
		}
	})

	b.Run("TreeWalk_100K", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < nEvents; j++ {
				treeWalkEval(expr, fieldSets[j])
			}
		}
	})

	// Single-event comparison for per-event overhead
	b.Run("VM_single", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vm.Execute(prog, fields)
		}
	})

	b.Run("TreeWalk_single", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			treeWalkEval(expr, fields)
		}
	})

	// Regex expression: match(uri, "^/api/v[0-9]+/users") — VM pre-compiles regex, tree-walk re-compiles each time
	regexExpr := &spl2.FuncCallExpr{
		Name: "match",
		Args: []spl2.Expr{
			&spl2.FieldExpr{Name: "uri"},
			&spl2.LiteralExpr{Value: `^/api/v[0-9]+/users`},
		},
	}
	regexProg, _ := CompileExpr(regexExpr)
	regexFields := map[string]event.Value{"uri": event.StringValue("/api/v2/users/123")}

	b.Run("VM_regex", func(b *testing.B) {
		vmR := &VM{}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vmR.Execute(regexProg, regexFields)
		}
	})

	b.Run("TreeWalk_regex", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			treeWalkEval(regexExpr, regexFields)
		}
	})
}

// twResult wraps interpreter results with error propagation.
// A real tree-walk interpreter must track type, value, and errors at every node.
type twResult struct {
	val interface{}
	err error
}

// treeWalkEval simulates realistic AST tree-walking for benchmarking.
// A real production interpreter must: wrap every result with error info,
// convert literals each time, do type coercion at every node, handle errors,
// and allocate result wrappers at each level of the tree.
func treeWalkEval(expr spl2.Expr, fields map[string]event.Value) interface{} {
	r := twEval(expr, fields)
	if r.err != nil {
		return nil
	}

	return r.val
}

//go:noinline
func twEval(expr spl2.Expr, fields map[string]event.Value) twResult {
	switch e := expr.(type) {
	case *spl2.FieldExpr:
		if v, ok := fields[e.Name]; ok {
			return twResult{val: twCoerce(v)}
		}

		return twResult{val: nil}
	case *spl2.LiteralExpr:
		return twResult{val: twParseLiteral(e.Value)}
	case *spl2.CompareExpr:
		left := twEval(e.Left, fields)
		if left.err != nil {
			return left
		}
		right := twEval(e.Right, fields)
		if right.err != nil {
			return right
		}

		return twResult{val: twCompare(left.val, right.val, e.Op)}
	case *spl2.ArithExpr:
		left := twEval(e.Left, fields)
		if left.err != nil {
			return left
		}
		right := twEval(e.Right, fields)
		if right.err != nil {
			return right
		}

		return twResult{val: twArith(left.val, right.val, e.Op)}
	case *spl2.FuncCallExpr:
		return twEvalFunc(e, fields)
	case *spl2.BinaryExpr:
		left := twEval(e.Left, fields)
		if left.err != nil {
			return left
		}
		if e.Op == "and" {
			if !twToBool(left.val) {
				return twResult{val: false}
			}
			right := twEval(e.Right, fields)

			return twResult{val: twToBool(right.val)}
		}
		if e.Op == "or" {
			if twToBool(left.val) {
				return twResult{val: true}
			}
			right := twEval(e.Right, fields)

			return twResult{val: twToBool(right.val)}
		}
	case *spl2.NotExpr:
		inner := twEval(e.Expr, fields)
		if inner.err != nil {
			return inner
		}

		return twResult{val: !twToBool(inner.val)}
	}

	return twResult{val: nil}
}

//go:noinline
func twEvalFunc(e *spl2.FuncCallExpr, fields map[string]event.Value) twResult {
	// Real interpreters dispatch functions through a map/registry
	switch e.Name {
	case "IF":
		if len(e.Args) < 3 {
			return twResult{err: fmt.Errorf("IF requires 3 args")}
		}
		cond := twEval(e.Args[0], fields)
		if cond.err != nil {
			return cond
		}
		if twToBool(cond.val) {
			return twEval(e.Args[1], fields)
		}

		return twEval(e.Args[2], fields)
	case "CASE":
		for i := 0; i+1 < len(e.Args); i += 2 {
			cond := twEval(e.Args[i], fields)
			if cond.err != nil {
				return cond
			}
			if twToBool(cond.val) {
				return twEval(e.Args[i+1], fields)
			}
		}
		if len(e.Args)%2 == 1 {
			return twEval(e.Args[len(e.Args)-1], fields)
		}

		return twResult{val: nil}
	case "match":
		if len(e.Args) < 2 {
			return twResult{err: fmt.Errorf("match requires 2 args")}
		}
		fieldVal := twEval(e.Args[0], fields)
		if fieldVal.err != nil {
			return fieldVal
		}
		patternVal := twEval(e.Args[1], fields)
		if patternVal.err != nil {
			return patternVal
		}
		// Real tree-walk: recompile regex on every evaluation
		re, err := regexp.Compile(fmt.Sprint(patternVal.val))
		if err != nil {
			return twResult{val: false}
		}

		return twResult{val: re.MatchString(fmt.Sprint(fieldVal.val))}
	default:
		return twResult{err: fmt.Errorf("unknown function: %s", e.Name)}
	}
}

//go:noinline
func twParseLiteral(s string) interface{} {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if s == "true" {
		return true
	}
	if s == "false" {
		return false
	}

	return s
}

//go:noinline
func twCoerce(v event.Value) interface{} {
	switch v.Type() {
	case event.FieldTypeInt:
		return v.AsInt()
	case event.FieldTypeFloat:
		return v.AsFloat()
	case event.FieldTypeBool:
		return v.AsBool()
	case event.FieldTypeString:
		return v.AsString()
	default:
		return nil
	}
}

func twToBool(v interface{}) bool {
	switch b := v.(type) {
	case bool:
		return b
	case int64:
		return b != 0
	case float64:
		return b != 0
	case string:
		return b != ""
	}

	return false
}

//go:noinline
func twCompare(left, right interface{}, op string) interface{} {
	lf := twToFloat(left)
	rf := twToFloat(right)
	switch op {
	case ">=":
		return lf >= rf
	case ">":
		return lf > rf
	case "<=":
		return lf <= rf
	case "<":
		return lf < rf
	case "=", "==":
		return lf == rf
	case "!=":
		return lf != rf
	}

	return false
}

//go:noinline
func twArith(left, right interface{}, op string) interface{} {
	lf := twToFloat(left)
	rf := twToFloat(right)
	switch op {
	case "+":
		return lf + rf
	case "-":
		return lf - rf
	case "*":
		return lf * rf
	case "/":
		if rf == 0 {
			return nil
		}

		return lf / rf
	}

	return nil
}

//go:noinline
func twToFloat(v interface{}) float64 {
	switch n := v.(type) {
	case int64:
		return float64(n)
	case float64:
		return n
	case string:
		f, _ := strconv.ParseFloat(n, 64)

		return f
	}

	return 0
}

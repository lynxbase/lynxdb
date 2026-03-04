package vm

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// CompileExpr compiles a single SPL2 expression into a Program.
func CompileExpr(expr spl2.Expr) (*Program, error) {
	c := &compiler{prog: &Program{}}
	if err := c.compileExpr(expr); err != nil {
		return nil, err
	}
	c.prog.EmitOp(OpReturn)

	return c.prog, nil
}

// CompilePredicate compiles a WHERE/search predicate into a Program.
// The result on the stack will be a boolean.
func CompilePredicate(pred spl2.Expr) (*Program, error) {
	return CompileExpr(pred)
}

type compiler struct {
	prog *Program
}

func (c *compiler) compileExpr(expr spl2.Expr) error {
	switch e := expr.(type) {
	case *spl2.LiteralExpr:
		return c.compileLiteral(e)
	case *spl2.FieldExpr:
		// Resolve virtual field aliases at compile time so the VM
		// reads the correct physical column from segment data.
		// "source" is a Splunk-compat alias for "_source".
		// "index" is a real physical column — no aliasing needed.
		name := e.Name
		if name == "source" {
			name = "_source"
		}
		idx := c.prog.AddFieldName(name)
		c.prog.EmitOp(OpLoadField, idx)
	case *spl2.CompareExpr:
		return c.compileCompare(e)
	case *spl2.BinaryExpr:
		return c.compileBinary(e)
	case *spl2.ArithExpr:
		return c.compileArith(e)
	case *spl2.NotExpr:
		if err := c.compileExpr(e.Expr); err != nil {
			return err
		}
		c.prog.EmitOp(OpNot)
	case *spl2.FuncCallExpr:
		return c.compileFuncCall(e)
	case *spl2.InExpr:
		return c.compileIn(e)
	case *spl2.GlobExpr:
		// Glob used as standalone expression = boolean match on _raw
		idx := c.prog.AddFieldName("_raw")
		c.prog.EmitOp(OpLoadField, idx)
		regIdx := c.prog.AddRegex(e.Pattern)
		c.prog.EmitOp(OpGlobMatch, regIdx)
	default:
		return fmt.Errorf("unsupported expression type: %T", expr)
	}

	return nil
}

func (c *compiler) compileLiteral(lit *spl2.LiteralExpr) error {
	val := lit.Value
	// Try to detect quoted strings
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		s := val[1 : len(val)-1]
		idx := c.prog.AddConstant(event.StringValue(s))
		c.prog.EmitOp(OpConstStr, idx)

		return nil
	}
	// Try integer
	if n, err := strconv.ParseInt(val, 10, 64); err == nil {
		idx := c.prog.AddConstant(event.IntValue(n))
		c.prog.EmitOp(OpConstInt, idx)

		return nil
	}
	// Try float
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		idx := c.prog.AddConstant(event.FloatValue(f))
		c.prog.EmitOp(OpConstFloat, idx)

		return nil
	}
	// Boolean
	if strings.EqualFold(val, "true") {
		c.prog.EmitOp(OpConstTrue)

		return nil
	}
	if strings.EqualFold(val, "false") {
		c.prog.EmitOp(OpConstFalse)

		return nil
	}
	if strings.EqualFold(val, "null") {
		c.prog.EmitOp(OpConstNull)

		return nil
	}
	// Default to string
	idx := c.prog.AddConstant(event.StringValue(val))
	c.prog.EmitOp(OpConstStr, idx)

	return nil
}

func (c *compiler) compileCompare(e *spl2.CompareExpr) error {
	if err := c.compileExpr(e.Left); err != nil {
		return err
	}
	if err := c.compileExpr(e.Right); err != nil {
		return err
	}
	switch e.Op {
	case "=", "==":
		c.prog.EmitOp(OpEq)
	case "!=":
		c.prog.EmitOp(OpNeq)
	case "<":
		c.prog.EmitOp(OpLt)
	case "<=":
		c.prog.EmitOp(OpLte)
	case ">":
		c.prog.EmitOp(OpGt)
	case ">=":
		c.prog.EmitOp(OpGte)
	case "like":
		c.prog.EmitOp(OpLike)
	default:
		return fmt.Errorf("unknown comparison operator: %s", e.Op)
	}

	return nil
}

func (c *compiler) compileBinary(e *spl2.BinaryExpr) error {
	switch strings.ToLower(e.Op) {
	case "and":
		// Short-circuit AND: if left is false, skip right and push false
		if err := c.compileExpr(e.Left); err != nil {
			return err
		}
		jumpFalse := c.prog.EmitOp(OpJumpIfFalse, 0) // pops left
		if err := c.compileExpr(e.Right); err != nil {
			return err
		}
		jumpEnd := c.prog.EmitOp(OpJump, 0)
		falseLabel := c.prog.Len()
		c.prog.EmitOp(OpConstFalse)
		endLabel := c.prog.Len()
		c.prog.PatchUint16(jumpFalse+1, uint16(falseLabel))
		c.prog.PatchUint16(jumpEnd+1, uint16(endLabel))

	case "or":
		// Short-circuit OR: if left is true, skip right and push true
		if err := c.compileExpr(e.Left); err != nil {
			return err
		}
		jumpTrue := c.prog.EmitOp(OpJumpIfTrue, 0) // pops left
		if err := c.compileExpr(e.Right); err != nil {
			return err
		}
		jumpEnd := c.prog.EmitOp(OpJump, 0)
		trueLabel := c.prog.Len()
		c.prog.EmitOp(OpConstTrue)
		endLabel := c.prog.Len()
		c.prog.PatchUint16(jumpTrue+1, uint16(trueLabel))
		c.prog.PatchUint16(jumpEnd+1, uint16(endLabel))

	default:
		return fmt.Errorf("unknown binary operator: %s", e.Op)
	}

	return nil
}

func (c *compiler) compileArith(e *spl2.ArithExpr) error {
	if err := c.compileExpr(e.Left); err != nil {
		return err
	}
	if err := c.compileExpr(e.Right); err != nil {
		return err
	}

	lt, rt := literalType(e.Left), literalType(e.Right)
	if lt == "int" && rt == "int" {
		switch e.Op {
		case "+":
			c.prog.EmitOp(OpAddInt)
		case "-":
			c.prog.EmitOp(OpSubInt)
		case "*":
			c.prog.EmitOp(OpMulInt)
		case "/":
			c.prog.EmitOp(OpDivInt)
		case "%":
			c.prog.EmitOp(OpModInt)
		default:
			return fmt.Errorf("unknown arithmetic operator: %s", e.Op)
		}

		return nil
	}
	if lt == "float" && rt == "float" {
		switch e.Op {
		case "+":
			c.prog.EmitOp(OpAddFloat)
		case "-":
			c.prog.EmitOp(OpSubFloat)
		case "*":
			c.prog.EmitOp(OpMulFloat)
		case "/":
			c.prog.EmitOp(OpDivFloat)
		case "%":
			c.prog.EmitOp(OpMod) // no OpModFloat, fall back to generic
		default:
			return fmt.Errorf("unknown arithmetic operator: %s", e.Op)
		}

		return nil
	}

	switch e.Op {
	case "+":
		c.prog.EmitOp(OpAdd)
	case "-":
		c.prog.EmitOp(OpSub)
	case "*":
		c.prog.EmitOp(OpMul)
	case "/":
		c.prog.EmitOp(OpDiv)
	case "%":
		c.prog.EmitOp(OpMod)
	default:
		return fmt.Errorf("unknown arithmetic operator: %s", e.Op)
	}

	return nil
}

// literalType returns "int", "float", or "" for a literal expression.
func literalType(e spl2.Expr) string {
	lit, ok := e.(*spl2.LiteralExpr)
	if !ok {
		return ""
	}
	val := lit.Value
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		return "" // quoted string
	}
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return "int"
	}
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return "float"
	}

	return ""
}

func (c *compiler) compileFuncCall(e *spl2.FuncCallExpr) error {
	name := strings.ToLower(e.Name)
	switch name {
	case "if":
		return c.compileIf(e)
	case "case":
		return c.compileCase(e)
	case "coalesce":
		return c.compileCoalesce(e)
	case "null":
		if len(e.Args) != 0 {
			return fmt.Errorf("null expects 0 arguments, got %d", len(e.Args))
		}
		c.prog.EmitOp(OpConstNull)
	case "isnull":
		if len(e.Args) != 1 {
			return fmt.Errorf("isnull expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpIsNull)
	case "isnotnull":
		if len(e.Args) != 1 {
			return fmt.Errorf("isnotnull expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpIsNotNull)
	case "tonumber":
		if len(e.Args) != 1 {
			return fmt.Errorf("tonumber expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpToFloat)
	case "tostring":
		if len(e.Args) != 1 {
			return fmt.Errorf("tostring expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpToString)
	case "round":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return fmt.Errorf("round expects 1-2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if len(e.Args) == 2 {
			if err := c.compileExpr(e.Args[1]); err != nil {
				return err
			}
		} else {
			idx := c.prog.AddConstant(event.IntValue(0))
			c.prog.EmitOp(OpConstInt, idx)
		}
		c.prog.EmitOp(OpRound)
	case "ln":
		if len(e.Args) != 1 {
			return fmt.Errorf("ln expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpLn)
	case "abs":
		if len(e.Args) != 1 {
			return fmt.Errorf("abs expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpAbs)
	case "ceil", "ceiling":
		if len(e.Args) != 1 {
			return fmt.Errorf("ceil expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpCeil)
	case "floor":
		if len(e.Args) != 1 {
			return fmt.Errorf("floor expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpFloor)
	case "sqrt":
		if len(e.Args) != 1 {
			return fmt.Errorf("sqrt expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpSqrt)
	case "len":
		if len(e.Args) != 1 {
			return fmt.Errorf("len expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpStrLen)
	case "lower":
		if len(e.Args) != 1 {
			return fmt.Errorf("lower expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpToLower)
	case "upper":
		if len(e.Args) != 1 {
			return fmt.Errorf("upper expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpToUpper)
	case "substr":
		if len(e.Args) != 3 {
			return fmt.Errorf("substr expects 3 arguments, got %d", len(e.Args))
		}
		for _, arg := range e.Args {
			if err := c.compileExpr(arg); err != nil {
				return err
			}
		}
		c.prog.EmitOp(OpSubstr)
	case "match":
		if len(e.Args) != 2 {
			return fmt.Errorf("match expects 2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		// Second arg should be a regex pattern literal
		pattern := exprToString(e.Args[1])
		regIdx := c.prog.AddRegex(pattern)
		c.prog.EmitOp(OpStrMatch, regIdx)
	case "mvappend":
		for _, arg := range e.Args {
			if err := c.compileExpr(arg); err != nil {
				return err
			}
		}
		c.prog.EmitOp(OpMvAppend, len(e.Args))
	case "mvjoin":
		if len(e.Args) != 2 {
			return fmt.Errorf("mvjoin expects 2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if err := c.compileExpr(e.Args[1]); err != nil {
			return err
		}
		c.prog.EmitOp(OpMvJoin)
	case "mvdedup":
		if len(e.Args) != 1 {
			return fmt.Errorf("mvdedup expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpMvDedup)
	case "mvcount":
		if len(e.Args) != 1 {
			return fmt.Errorf("mvcount expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpMvCount)
	case "replace":
		if len(e.Args) != 3 {
			return fmt.Errorf("replace expects 3 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		// Second arg is a regex pattern
		pattern := exprToString(e.Args[1])
		regIdx := c.prog.AddRegex(pattern)
		if err := c.compileExpr(e.Args[2]); err != nil {
			return err
		}
		c.prog.EmitOp(OpReplace, regIdx)
	case "split":
		if len(e.Args) != 2 {
			return fmt.Errorf("split expects 2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if err := c.compileExpr(e.Args[1]); err != nil {
			return err
		}
		c.prog.EmitOp(OpSplit)
	case "strftime":
		if len(e.Args) != 2 {
			return fmt.Errorf("strftime expects 2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if err := c.compileExpr(e.Args[1]); err != nil {
			return err
		}
		c.prog.EmitOp(OpStrftime)
	case "max":
		if len(e.Args) != 2 {
			return fmt.Errorf("max expects 2 arguments, got %d", len(e.Args))
		}

		return c.compileMaxMin(e.Args[0], e.Args[1], true)
	case "min":
		if len(e.Args) != 2 {
			return fmt.Errorf("min expects 2 arguments, got %d", len(e.Args))
		}

		return c.compileMaxMin(e.Args[0], e.Args[1], false)
	case "json_extract":
		if len(e.Args) != 2 {
			return fmt.Errorf("json_extract expects 2 arguments, got %d", len(e.Args))
		}
		// Compile field (first arg), then path (second arg).
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if err := c.compileExpr(e.Args[1]); err != nil {
			return err
		}
		c.prog.EmitOp(OpJsonExtract)
	case "json_valid":
		if len(e.Args) != 1 {
			return fmt.Errorf("json_valid expects 1 argument, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		c.prog.EmitOp(OpJsonValid)
	case "json_keys":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return fmt.Errorf("json_keys expects 1-2 arguments, got %d", len(e.Args))
		}
		// Compile field (first arg).
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		// Compile path (second arg) or push empty string.
		if len(e.Args) == 2 {
			if err := c.compileExpr(e.Args[1]); err != nil {
				return err
			}
		} else {
			idx := c.prog.AddConstant(event.StringValue(""))
			c.prog.EmitOp(OpConstStr, idx)
		}
		c.prog.EmitOp(OpJsonKeys)
	case "json_array_length":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return fmt.Errorf("json_array_length expects 1-2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if len(e.Args) == 2 {
			if err := c.compileExpr(e.Args[1]); err != nil {
				return err
			}
		} else {
			idx := c.prog.AddConstant(event.StringValue(""))
			c.prog.EmitOp(OpConstStr, idx)
		}
		c.prog.EmitOp(OpJsonArrayLen)
	case "json_object":
		// json_object(k1, v1, k2, v2, ...) — even number of args.
		if len(e.Args)%2 != 0 {
			return fmt.Errorf("json_object expects an even number of arguments, got %d", len(e.Args))
		}
		for _, arg := range e.Args {
			if err := c.compileExpr(arg); err != nil {
				return err
			}
		}
		c.prog.EmitOp(OpJsonObject, len(e.Args))
	case "json_array":
		for _, arg := range e.Args {
			if err := c.compileExpr(arg); err != nil {
				return err
			}
		}
		c.prog.EmitOp(OpJsonArray, len(e.Args))
	case "json_type":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return fmt.Errorf("json_type expects 1-2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if len(e.Args) == 2 {
			if err := c.compileExpr(e.Args[1]); err != nil {
				return err
			}
		} else {
			idx := c.prog.AddConstant(event.StringValue(""))
			c.prog.EmitOp(OpConstStr, idx)
		}
		c.prog.EmitOp(OpJsonType)
	case "json_set":
		if len(e.Args) != 3 {
			return fmt.Errorf("json_set expects 3 arguments, got %d", len(e.Args))
		}
		for _, arg := range e.Args {
			if err := c.compileExpr(arg); err != nil {
				return err
			}
		}
		c.prog.EmitOp(OpJsonSet)
	case "json_remove":
		if len(e.Args) != 2 {
			return fmt.Errorf("json_remove expects 2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if err := c.compileExpr(e.Args[1]); err != nil {
			return err
		}
		c.prog.EmitOp(OpJsonRemove)
	case "json_merge":
		if len(e.Args) != 2 {
			return fmt.Errorf("json_merge expects 2 arguments, got %d", len(e.Args))
		}
		if err := c.compileExpr(e.Args[0]); err != nil {
			return err
		}
		if err := c.compileExpr(e.Args[1]); err != nil {
			return err
		}
		c.prog.EmitOp(OpJsonMerge)
	default:
		return fmt.Errorf("unknown function: %s", e.Name)
	}

	return nil
}

func (c *compiler) compileIf(e *spl2.FuncCallExpr) error {
	if len(e.Args) != 3 {
		return fmt.Errorf("IF expects 3 arguments, got %d", len(e.Args))
	}
	// Compile condition
	if err := c.compileExpr(e.Args[0]); err != nil {
		return err
	}
	jumpFalse := c.prog.EmitOp(OpJumpIfFalse, 0)
	// Then branch
	if err := c.compileExpr(e.Args[1]); err != nil {
		return err
	}
	jumpEnd := c.prog.EmitOp(OpJump, 0)
	// Else branch
	elsePos := c.prog.Len()
	if err := c.compileExpr(e.Args[2]); err != nil {
		return err
	}
	endPos := c.prog.Len()
	// Patch jumps
	c.prog.PatchUint16(jumpFalse+1, uint16(elsePos))
	c.prog.PatchUint16(jumpEnd+1, uint16(endPos))

	return nil
}

func (c *compiler) compileCase(e *spl2.FuncCallExpr) error {
	// CASE(cond1, val1, cond2, val2, ..., default)
	// Odd number of args = last is default; even = null default
	args := e.Args
	var jumpEnds []int

	pairs := len(args) / 2
	for i := 0; i < pairs; i++ {
		if err := c.compileExpr(args[i*2]); err != nil {
			return err
		}
		jumpFalse := c.prog.EmitOp(OpJumpIfFalse, 0)
		if err := c.compileExpr(args[i*2+1]); err != nil {
			return err
		}
		jumpEnd := c.prog.EmitOp(OpJump, 0)
		jumpEnds = append(jumpEnds, jumpEnd)
		nextCase := c.prog.Len()
		c.prog.PatchUint16(jumpFalse+1, uint16(nextCase))
	}

	// Default value
	if len(args)%2 == 1 {
		if err := c.compileExpr(args[len(args)-1]); err != nil {
			return err
		}
	} else {
		c.prog.EmitOp(OpConstNull)
	}

	endPos := c.prog.Len()
	for _, je := range jumpEnds {
		c.prog.PatchUint16(je+1, uint16(endPos))
	}

	return nil
}

func (c *compiler) compileCoalesce(e *spl2.FuncCallExpr) error {
	for _, arg := range e.Args {
		if err := c.compileExpr(arg); err != nil {
			return err
		}
	}
	c.prog.EmitOp(OpCoalesce, len(e.Args))

	return nil
}

func (c *compiler) compileMaxMin(a, b spl2.Expr, isMax bool) error {
	// max(a, b) = IF(a > b, a, b)
	if err := c.compileExpr(a); err != nil {
		return err
	}
	if err := c.compileExpr(b); err != nil {
		return err
	}
	if isMax {
		c.prog.EmitOp(OpGt)
	} else {
		c.prog.EmitOp(OpLt)
	}
	jumpFalse := c.prog.EmitOp(OpJumpIfFalse, 0)
	if err := c.compileExpr(a); err != nil {
		return err
	}
	jumpEnd := c.prog.EmitOp(OpJump, 0)
	elsePos := c.prog.Len()
	if err := c.compileExpr(b); err != nil {
		return err
	}
	endPos := c.prog.Len()
	c.prog.PatchUint16(jumpFalse+1, uint16(elsePos))
	c.prog.PatchUint16(jumpEnd+1, uint16(endPos))

	return nil
}

func (c *compiler) compileIn(e *spl2.InExpr) error {
	if err := c.compileExpr(e.Field); err != nil {
		return err
	}
	for _, val := range e.Values {
		if err := c.compileExpr(val); err != nil {
			return err
		}
	}
	c.prog.EmitOp(OpInList, len(e.Values))

	return nil
}

// exprToString extracts a string value from a literal expression.
func exprToString(e spl2.Expr) string {
	switch v := e.(type) {
	case *spl2.LiteralExpr:
		s := v.Value
		if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
			return s[1 : len(s)-1]
		}

		return s
	default:
		return e.String()
	}
}

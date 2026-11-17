package vm

import (
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// NativeFunc evaluates a function natively. args aliases the VM stack and
// must not be retained after the call returns.
type NativeFunc func(args []event.Value) event.Value

// CompileOptions configures CompileExpr.
type CompileOptions struct {
	// Now is the query's reference time; query-constant expressions such as
	// now() and relative time bounds evaluate against it.
	Now time.Time
	// Subqueries holds the rows each subquery of the expression returned.
	Subqueries map[*ir.Subquery]*SubqueryResult
}

// SubqueryResult is the evaluated result of an uncorrelated subquery: its
// rows, each holding the subquery's columns in schema order.
type SubqueryResult struct {
	Rows [][]event.Value
}

// CompileExpr compiles a canonical IR expression to a bytecode program.
// Subqueries must have been replaced by their results before compilation.
func CompileExpr(e ir.Expr, opts CompileOptions) (*Program, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	c := &irCompiler{prog: &Program{NumericStrings: true}, now: opts.Now.UTC(), subqueries: opts.Subqueries}
	if err := c.compile(e); err != nil {
		return nil, err
	}
	c.prog.EmitOp(OpReturn)
	return c.prog, nil
}

type irCompiler struct {
	prog       *Program
	now        time.Time
	params     []*ir.LambdaParam
	subqueries map[*ir.Subquery]*SubqueryResult
}

func (c *irCompiler) constant(v event.Value) {
	idx := c.prog.AddConstant(v)
	c.prog.EmitOp(OpConstStr, idx)
}

func (c *irCompiler) native(f NativeFunc, argc int) {
	idx := c.prog.AddNative(f)
	c.prog.EmitOp(OpCallNative, idx, argc)
}

func (c *irCompiler) compileAll(es []ir.Expr) error {
	for _, e := range es {
		if err := c.compile(e); err != nil {
			return err
		}
	}
	return nil
}

func (c *irCompiler) compile(e ir.Expr) error {
	switch n := e.(type) {
	case *ir.Literal:
		c.constant(LiteralValue(n.V))
	case *ir.FieldRef:
		idx := c.prog.AddConstant(event.StringValue(n.Name))
		c.prog.EmitOp(OpLoadPath, idx)
	case *ir.LambdaParam:
		for i := len(c.params) - 1; i >= 0; i-- {
			if c.params[i] == n || c.params[i].Name == n.Name {
				c.prog.EmitOp(OpLoadLambdaParam, len(c.params)-1-i)
				return nil
			}
		}
		return fmt.Errorf("vm: lambda parameter %s is not in scope", n.Name)
	case *ir.Unary:
		if err := c.compile(n.X); err != nil {
			return err
		}
		if n.Op == ir.OpNot {
			c.prog.EmitOp(OpNot3VL)
		} else {
			c.prog.EmitOp(OpNegStrict)
		}
	case *ir.Binary:
		return c.binary(n)
	case *ir.Call:
		return c.call(n)
	case *ir.Case:
		return c.caseExpr(n)
	case *ir.Cast:
		if err := c.compile(n.X); err != nil {
			return err
		}
		c.native(castFunc(n.To), 1)
	case *ir.ArrayExpr:
		if err := c.compileAll(n.Elems); err != nil {
			return err
		}
		c.prog.EmitOp(OpArrayBuild, len(n.Elems))
	case *ir.ObjectExpr:
		for i, k := range n.Keys {
			c.constant(event.StringValue(k))
			if err := c.compile(n.Values[i]); err != nil {
				return err
			}
		}
		c.prog.EmitOp(OpObjectBuild, len(n.Keys))
	case *ir.Index:
		if err := c.compile(n.X); err != nil {
			return err
		}
		if err := c.compile(n.Key); err != nil {
			return err
		}
		c.prog.EmitOp(OpIndex)
	case *ir.InList:
		if err := c.compile(n.X); err != nil {
			return err
		}
		if err := c.compileAll(n.List); err != nil {
			return err
		}
		c.prog.EmitOp(OpInStrict, len(n.List))
		if n.Not {
			c.prog.EmitOp(OpNot3VL)
		}
	case *ir.IsMissing:
		idx := c.prog.AddFieldName(n.Field.Name)
		c.prog.EmitOp(OpFieldMissing, idx)
	case *ir.Search:
		return c.search(n)
	case *ir.RelativeTime:
		c.constant(event.TimestampValue(n.Bound.Resolve(c.now)))
	case *ir.Lambda:
		return fmt.Errorf("vm: a lambda is only valid as a function argument")
	case *ir.Subquery:
		return c.subquery(n)
	case *ir.AggCall:
		return fmt.Errorf("vm: aggregate %s cannot be compiled as a scalar expression", n)
	default:
		return fmt.Errorf("vm: unsupported expression %T", e)
	}
	return nil
}

func (c *irCompiler) binary(n *ir.Binary) error {
	switch n.Op {
	case ir.OpAnd:
		return c.logical(n, true)
	case ir.OpOr:
		return c.logical(n, false)
	}
	if err := c.compile(n.L); err != nil {
		return err
	}
	if err := c.compile(n.R); err != nil {
		return err
	}
	switch n.Op {
	case ir.OpAdd:
		c.prog.EmitOp(OpAddStrict)
	case ir.OpSub:
		c.prog.EmitOp(OpSubStrict)
	case ir.OpMul:
		c.prog.EmitOp(OpMulStrict)
	case ir.OpDiv:
		c.prog.EmitOp(OpDivStrict)
	case ir.OpMod:
		c.prog.EmitOp(OpModStrict)
	case ir.OpEq:
		c.prog.EmitOp(OpEqStrict)
	case ir.OpNe:
		c.prog.EmitOp(OpNeqStrict)
	case ir.OpLt:
		c.prog.EmitOp(OpLtStrict)
	case ir.OpLe:
		c.prog.EmitOp(OpLteStrict)
	case ir.OpGt:
		c.prog.EmitOp(OpGtStrict)
	case ir.OpGe:
		c.prog.EmitOp(OpGteStrict)
	case ir.OpXor:
		c.native(nativeXor, 2)
	default:
		return fmt.Errorf("vm: unknown binary operator %s", n.Op)
	}
	return nil
}

// logical emits Kleene AND/OR with short-circuit evaluation.
func (c *irCompiler) logical(n *ir.Binary, and bool) error {
	if err := c.compile(n.L); err != nil {
		return err
	}
	jumpNull := c.prog.EmitOp(OpJumpIfNull3VL, 0)
	var jumpDecided int
	if and {
		jumpDecided = c.prog.EmitOp(OpJumpIfFalse, 0)
	} else {
		jumpDecided = c.prog.EmitOp(OpJumpIfTrue, 0)
	}
	if err := c.compile(n.R); err != nil {
		return err
	}
	jumpEnd1 := c.prog.EmitOp(OpJump, 0)
	nullLabel := c.prog.Len()
	if err := c.compile(n.R); err != nil {
		return err
	}
	if and {
		c.prog.EmitOp(OpAnd3VLNull)
	} else {
		c.prog.EmitOp(OpOr3VLNull)
	}
	jumpEnd2 := c.prog.EmitOp(OpJump, 0)
	decidedLabel := c.prog.Len()
	if and {
		c.prog.EmitOp(OpConstFalse)
	} else {
		c.prog.EmitOp(OpConstTrue)
	}
	end := c.prog.Len()
	c.prog.PatchUint16(jumpNull+1, uint16(nullLabel))
	c.prog.PatchUint16(jumpDecided+1, uint16(decidedLabel))
	c.prog.PatchUint16(jumpEnd1+1, uint16(end))
	c.prog.PatchUint16(jumpEnd2+1, uint16(end))
	return nil
}

// caseExpr evaluates branch conditions in order; a NULL condition does not
// select its branch.
func (c *irCompiler) caseExpr(n *ir.Case) error {
	var ends []int
	for _, w := range n.Whens {
		if err := c.compile(w.Cond); err != nil {
			return err
		}
		c.native(nativeIsTrue, 1)
		next := c.prog.EmitOp(OpJumpIfFalse, 0)
		if err := c.compile(w.Then); err != nil {
			return err
		}
		ends = append(ends, c.prog.EmitOp(OpJump, 0))
		c.prog.PatchUint16(next+1, uint16(c.prog.Len()))
	}
	if n.Else != nil {
		if err := c.compile(n.Else); err != nil {
			return err
		}
	} else {
		c.prog.EmitOp(OpConstNull)
	}
	end := c.prog.Len()
	for _, j := range ends {
		c.prog.PatchUint16(j+1, uint16(end))
	}
	return nil
}

func (c *irCompiler) search(n *ir.Search) error {
	field := n.Field
	if field == "" {
		field = ir.RawColumn
	}
	idx := c.prog.AddConstant(event.StringValue(field))
	c.prog.EmitOp(OpLoadPath, idx)
	switch n.Mode {
	case ir.SearchWord:
		c.constant(event.StringValue(n.Pattern))
		c.prog.EmitOp(OpHasToken)
	case ir.SearchPrefix:
		g := c.prog.AddGlob(strings.ToLower(escapeGlob(n.Pattern)) + "*")
		c.prog.EmitOp(OpHasTokenGlob, g)
	case ir.SearchGlob:
		g := c.prog.AddGlob(strings.ToLower(n.Pattern))
		c.prog.EmitOp(OpHasTokenGlob, g)
	case ir.SearchPhrase:
		c.native(phraseMatcher(n.Pattern, n.CaseSensitive), 1)
	case ir.SearchSubstring:
		if n.CaseSensitive {
			c.native(substringMatcher(n.Pattern), 1)
		} else {
			c.constant(event.StringValue(n.Pattern))
			c.prog.EmitOp(OpContainsCI)
		}
	case ir.SearchRegex:
		f, err := regexMatcher(n.Pattern, n.CaseSensitive)
		if err != nil {
			return err
		}
		c.native(f, 1)
	default:
		return fmt.Errorf("vm: unknown search mode %d", n.Mode)
	}
	return nil
}

func escapeGlob(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '*', '?', '[', ']', '{', '}', '\\':
			out = append(out, '\\')
		}
		out = append(out, s[i])
	}
	return string(out)
}

// compileLambda compiles a lambda body into a sub-program sharing the root
// program's pools and returns its index.
func (c *irCompiler) compileLambda(l *ir.Lambda) (int, error) {
	parent := c.prog.Instructions
	c.prog.Instructions = nil
	c.params = append(c.params, l.Params...)
	err := c.compile(l.Body)
	c.params = c.params[:len(c.params)-len(l.Params)]
	if err != nil {
		c.prog.Instructions = parent
		return 0, err
	}
	c.prog.EmitOp(OpReturn)
	sub := &Program{Instructions: c.prog.Instructions, NumericStrings: true, Params: len(l.Params)}
	c.prog.Instructions = parent
	return c.prog.AddSubProgram(sub), nil
}

// LiteralValue converts an IR literal value to an event value.
func LiteralValue(v any) event.Value {
	switch x := v.(type) {
	case nil:
		return event.NullValue()
	case bool:
		return event.BoolValue(x)
	case int64:
		return event.IntValue(x)
	case float64:
		return event.FloatValue(x)
	case string:
		return event.StringValue(x)
	case time.Time:
		return event.TimestampValue(x)
	case time.Duration:
		return event.DurationValue(x)
	case []any:
		out := make([]event.Value, len(x))
		for i, e := range x {
			out[i] = LiteralValue(e)
		}
		return event.ArrayValue(out)
	case map[string]any:
		out := make(map[string]event.Value, len(x))
		for k, e := range x {
			out[k] = LiteralValue(e)
		}
		return event.ObjectValue(out)
	}
	return event.StringValue(fmt.Sprint(v))
}

// subquery compiles an evaluated subquery: EXISTS and scalar subqueries
// become constants, IN becomes a hash-set membership test with IN-list NULL
// semantics.
func (c *irCompiler) subquery(n *ir.Subquery) error {
	res, ok := c.subqueries[n]
	if !ok {
		return fmt.Errorf("vm: subquery %s was not evaluated before compilation", n)
	}
	switch n.Mode {
	case ir.SubqueryExists:
		c.constant(event.BoolValue((len(res.Rows) > 0) != n.Not))
		return nil
	case ir.SubqueryScalar:
		switch len(res.Rows) {
		case 0:
			c.prog.EmitOp(OpConstNull)
		case 1:
			c.constant(res.Rows[0][0])
		default:
			return fmt.Errorf("scalar subquery returned %d rows; it must return at most one", len(res.Rows))
		}
		return nil
	}
	set := make(map[string]struct{}, len(res.Rows))
	hasNull := false
	for _, r := range res.Rows {
		key, null := tupleKey(r)
		if null {
			hasNull = true
			continue
		}
		set[key] = struct{}{}
	}
	if err := c.compileAll(n.Args); err != nil {
		return err
	}
	not := n.Not
	c.native(func(a []event.Value) event.Value {
		key, null := tupleKey(a)
		var r event.Value
		switch {
		case null:
			r = event.NullValue()
		default:
			if _, ok := set[key]; ok {
				r = event.BoolValue(true)
			} else if hasNull {
				r = event.NullValue()
			} else {
				r = event.BoolValue(false)
			}
		}
		if not && !r.IsNull() {
			r = event.BoolValue(!r.AsBool())
		}
		return r
	}, len(n.Args))
	return nil
}

// tupleKey encodes values for set membership; numbers compare by numeric
// value and numeric strings match numbers, as = does.
func tupleKey(vals []event.Value) (string, bool) {
	var b strings.Builder
	for _, v := range vals {
		if v.IsNull() {
			return "", true
		}
		switch v.Type() {
		case event.FieldTypeInt, event.FieldTypeFloat:
			f, _ := numFloat(v)
			b.WriteString("n" + formatFloat(f))
		case event.FieldTypeString:
			if n, ok := parseNumber(v.AsString()); ok {
				f, _ := numFloat(n)
				b.WriteString("n" + formatFloat(f))
			} else {
				b.WriteString("s" + v.AsString())
			}
		default:
			s, _ := str(v)
			b.WriteString("o" + s)
		}
		b.WriteByte(0)
	}
	return b.String(), false
}

package optimizer

import (
	"strconv"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

type constantFoldingRule struct{}

func (r *constantFoldingRule) Name() string { return "ConstantFolding" }
func (r *constantFoldingRule) Description() string {
	return "Folds constant arithmetic and string expressions at compile time"
}
func (r *constantFoldingRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false
	for i, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.WhereCommand:
			newExpr, folded := foldExpr(c.Expr)
			if folded {
				q.Commands[i] = &spl2.WhereCommand{Expr: newExpr}
				changed = true
			}
		case *spl2.EvalCommand:
			if c.Expr != nil {
				newExpr, folded := foldExpr(c.Expr)
				if folded {
					c.Expr = newExpr
					changed = true
				}
			}
		}
	}

	return q, changed
}

func foldExpr(expr spl2.Expr) (spl2.Expr, bool) {
	switch e := expr.(type) {
	case *spl2.ArithExpr:
		// Recursively fold children first
		left, lChanged := foldExpr(e.Left)
		right, rChanged := foldExpr(e.Right)
		if lChanged || rChanged {
			e = &spl2.ArithExpr{Left: left, Op: e.Op, Right: right}
		}
		lLit, lOk := e.Left.(*spl2.LiteralExpr)
		rLit, rOk := e.Right.(*spl2.LiteralExpr)
		if lOk && rOk {
			// String concatenation: "abc" + "def" → "abcdef"
			if e.Op == "+" {
				_, le := strconv.ParseFloat(lLit.Value, 64)
				_, re := strconv.ParseFloat(rLit.Value, 64)
				if le != nil || re != nil {
					// At least one side isn't numeric — string concat.
					return &spl2.LiteralExpr{Value: lLit.Value + rLit.Value}, true
				}
			}

			lv, le := strconv.ParseFloat(lLit.Value, 64)
			rv, re := strconv.ParseFloat(rLit.Value, 64)
			if le == nil && re == nil {
				var result float64
				switch e.Op {
				case "+":
					result = lv + rv
				case "-":
					result = lv - rv
				case "*":
					result = lv * rv
				case "/":
					if rv != 0 {
						result = lv / rv
					}
				}
				_, lie := strconv.ParseInt(lLit.Value, 10, 64)
				_, rie := strconv.ParseInt(rLit.Value, 10, 64)
				if lie == nil && rie == nil {
					return &spl2.LiteralExpr{Value: strconv.FormatInt(int64(result), 10)}, true
				}

				return &spl2.LiteralExpr{Value: strconv.FormatFloat(result, 'g', -1, 64)}, true
			}
		}
		if lChanged || rChanged {
			return e, true
		}
	case *spl2.CompareExpr:
		// Recursively fold children first
		left, lChanged := foldExpr(e.Left)
		right, rChanged := foldExpr(e.Right)
		if lChanged || rChanged {
			e = &spl2.CompareExpr{Left: left, Op: e.Op, Right: right}
		}
		lLit, lOk := e.Left.(*spl2.LiteralExpr)
		rLit, rOk := e.Right.(*spl2.LiteralExpr)
		if lOk && rOk {
			lv, le := strconv.ParseFloat(lLit.Value, 64)
			rv, re := strconv.ParseFloat(rLit.Value, 64)
			if le == nil && re == nil {
				var result bool
				switch e.Op {
				case "=", "==":
					result = lv == rv
				case "!=":
					result = lv != rv
				case ">":
					result = lv > rv
				case ">=":
					result = lv >= rv
				case "<":
					result = lv < rv
				case "<=":
					result = lv <= rv
				}
				if result {
					return &spl2.LiteralExpr{Value: "true"}, true
				}

				return &spl2.LiteralExpr{Value: "false"}, true
			}
		}
		if lChanged || rChanged {
			return e, true
		}
	case *spl2.FuncCallExpr:
		// Fold known pure functions on literal arguments.
		allLit := true
		args := make([]spl2.Expr, len(e.Args))
		anyChanged := false
		for i, arg := range e.Args {
			folded, changed := foldExpr(arg)
			args[i] = folded
			if changed {
				anyChanged = true
			}
			if _, ok := folded.(*spl2.LiteralExpr); !ok {
				allLit = false
			}
		}
		if allLit && len(args) == 1 {
			lit := args[0].(*spl2.LiteralExpr)
			switch strings.ToLower(e.Name) {
			case "len":
				return &spl2.LiteralExpr{Value: strconv.Itoa(len(lit.Value))}, true
			case "lower":
				return &spl2.LiteralExpr{Value: strings.ToLower(lit.Value)}, true
			case "upper":
				return &spl2.LiteralExpr{Value: strings.ToUpper(lit.Value)}, true
			}
		}
		if anyChanged {
			return &spl2.FuncCallExpr{Name: e.Name, Args: args}, true
		}
	}

	return expr, false
}

type constantPropagationRule struct{}

func (r *constantPropagationRule) Name() string { return "ConstantPropagation" }
func (r *constantPropagationRule) Description() string {
	return "Substitutes known constant values into downstream expressions"
}
func (r *constantPropagationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	// Track known constants from EVAL assignments
	constants := make(map[string]string) // field → literal value
	changed := false

	for i, cmd := range q.Commands {
		switch c := cmd.(type) {
		case *spl2.EvalCommand:
			if c.Expr != nil {
				if lit, ok := c.Expr.(*spl2.LiteralExpr); ok {
					constants[c.Field] = lit.Value
				}
			}
			// Multi-assignment eval: eval x=5, y=x+1
			for _, a := range c.Assignments {
				if lit, ok := a.Expr.(*spl2.LiteralExpr); ok {
					constants[a.Field] = lit.Value
				}
			}
		case *spl2.RenameCommand:
			// Propagate through rename: if rename a AS b, constant[a] → constant[b]
			for _, r := range c.Renames {
				if val, ok := constants[r.Old]; ok {
					constants[r.New] = val
				}
			}
		case *spl2.WhereCommand:
			newExpr, sub := substituteConstants(c.Expr, constants)
			if sub {
				q.Commands[i] = &spl2.WhereCommand{Expr: newExpr}
				changed = true
			}
		}
	}

	return q, changed
}

func substituteConstants(expr spl2.Expr, constants map[string]string) (spl2.Expr, bool) {
	switch e := expr.(type) {
	case *spl2.FieldExpr:
		if val, ok := constants[e.Name]; ok {
			return &spl2.LiteralExpr{Value: val}, true
		}
	case *spl2.CompareExpr:
		l, lc := substituteConstants(e.Left, constants)
		r, rc := substituteConstants(e.Right, constants)
		if lc || rc {
			return &spl2.CompareExpr{Left: l, Op: e.Op, Right: r}, true
		}
	case *spl2.BinaryExpr:
		l, lc := substituteConstants(e.Left, constants)
		r, rc := substituteConstants(e.Right, constants)
		if lc || rc {
			return &spl2.BinaryExpr{Left: l, Op: e.Op, Right: r}, true
		}
	}

	return expr, false
}

type predicateSimplificationRule struct{}

func (r *predicateSimplificationRule) Name() string { return "PredicateSimplification" }
func (r *predicateSimplificationRule) Description() string {
	return "Simplifies redundant boolean logic (x AND x, true OR x, etc.)"
}
func (r *predicateSimplificationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false
	for i, cmd := range q.Commands {
		if w, ok := cmd.(*spl2.WhereCommand); ok {
			newExpr, simplified := simplifyPredicate(w.Expr)
			if simplified {
				q.Commands[i] = &spl2.WhereCommand{Expr: newExpr}
				changed = true
			}
		}
	}

	return q, changed
}

func simplifyPredicate(expr spl2.Expr) (spl2.Expr, bool) {
	if e, ok := expr.(*spl2.BinaryExpr); ok {
		// x AND x → x
		if strings.EqualFold(e.Op, "and") && exprEqual(e.Left, e.Right) {
			return e.Left, true
		}
		// x OR x → x
		if strings.EqualFold(e.Op, "or") && exprEqual(e.Left, e.Right) {
			return e.Left, true
		}
		// true OR x → true; x OR true → true
		if strings.EqualFold(e.Op, "or") {
			if isLiteralTrue(e.Left) || isLiteralTrue(e.Right) {
				return &spl2.LiteralExpr{Value: "true"}, true
			}
		}
		// false AND x → false; x AND false → false
		if strings.EqualFold(e.Op, "and") {
			if isLiteralFalse(e.Left) || isLiteralFalse(e.Right) {
				return &spl2.LiteralExpr{Value: "false"}, true
			}
		}
		// true AND x → x
		if strings.EqualFold(e.Op, "and") && isLiteralTrue(e.Left) {
			return e.Right, true
		}
		if strings.EqualFold(e.Op, "and") && isLiteralTrue(e.Right) {
			return e.Left, true
		}
		// false OR x → x
		if strings.EqualFold(e.Op, "or") && isLiteralFalse(e.Left) {
			return e.Right, true
		}
		if strings.EqualFold(e.Op, "or") && isLiteralFalse(e.Right) {
			return e.Left, true
		}
	}

	return expr, false
}

type negationPushdownRule struct{}

func (r *negationPushdownRule) Name() string { return "NegationPushdown" }
func (r *negationPushdownRule) Description() string {
	return "Converts NOT(a > b) into a <= b for direct comparison"
}
func (r *negationPushdownRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	changed := false
	for i, cmd := range q.Commands {
		if w, ok := cmd.(*spl2.WhereCommand); ok {
			newExpr, pushed := pushNegation(w.Expr)
			if pushed {
				q.Commands[i] = &spl2.WhereCommand{Expr: newExpr}
				changed = true
			}
		}
	}

	return q, changed
}

func pushNegation(expr spl2.Expr) (spl2.Expr, bool) {
	not, ok := expr.(*spl2.NotExpr)
	if !ok {
		return expr, false
	}
	cmp, ok := not.Expr.(*spl2.CompareExpr)
	if !ok {
		return expr, false
	}
	negOp := negateOp(cmp.Op)
	if negOp == "" {
		return expr, false
	}

	return &spl2.CompareExpr{Left: cmp.Left, Op: negOp, Right: cmp.Right}, true
}

func negateOp(op string) string {
	switch op {
	case ">":
		return "<="
	case ">=":
		return "<"
	case "<":
		return ">="
	case "<=":
		return ">"
	case "=", "==":
		return "!="
	case "!=":
		return "="
	}

	return ""
}

type deadCodeEliminationRule struct{}

func (r *deadCodeEliminationRule) Name() string { return "DeadCodeElimination" }
func (r *deadCodeEliminationRule) Description() string {
	return "Removes unreachable pipeline stages after WHERE false"
}
func (r *deadCodeEliminationRule) Apply(q *spl2.Query) (*spl2.Query, bool) {
	for i, cmd := range q.Commands {
		if w, ok := cmd.(*spl2.WhereCommand); ok {
			if isLiteralFalse(w.Expr) {
				// Everything after WHERE false is dead code
				q.Commands = q.Commands[:i+1]

				return q, true
			}
			if isLiteralTrue(w.Expr) {
				// WHERE true is a no-op, remove it
				q.Commands = append(q.Commands[:i], q.Commands[i+1:]...)

				return q, true
			}
		}
	}

	return q, false
}

func collectFieldNames(expr spl2.Expr) map[string]bool {
	fields := make(map[string]bool)
	collectFieldNamesHelper(expr, fields)

	return fields
}

func collectFieldNamesHelper(expr spl2.Expr, fields map[string]bool) {
	switch e := expr.(type) {
	case *spl2.FieldExpr:
		fields[e.Name] = true
	case *spl2.CompareExpr:
		collectFieldNamesHelper(e.Left, fields)
		collectFieldNamesHelper(e.Right, fields)
	case *spl2.BinaryExpr:
		collectFieldNamesHelper(e.Left, fields)
		collectFieldNamesHelper(e.Right, fields)
	case *spl2.ArithExpr:
		collectFieldNamesHelper(e.Left, fields)
		collectFieldNamesHelper(e.Right, fields)
	case *spl2.NotExpr:
		collectFieldNamesHelper(e.Expr, fields)
	case *spl2.FuncCallExpr:
		for _, arg := range e.Args {
			collectFieldNamesHelper(arg, fields)
		}
	case *spl2.InExpr:
		collectFieldNamesHelper(e.Field, fields)
	}
}

func exprEqual(a, b spl2.Expr) bool {
	if a == nil || b == nil {
		return a == b
	}
	switch x := a.(type) {
	case *spl2.FieldExpr:
		y, ok := b.(*spl2.FieldExpr)

		return ok && x.Name == y.Name
	case *spl2.LiteralExpr:
		y, ok := b.(*spl2.LiteralExpr)

		return ok && x.Value == y.Value
	case *spl2.GlobExpr:
		y, ok := b.(*spl2.GlobExpr)

		return ok && x.Pattern == y.Pattern
	case *spl2.CompareExpr:
		y, ok := b.(*spl2.CompareExpr)

		return ok && x.Op == y.Op && exprEqual(x.Left, y.Left) && exprEqual(x.Right, y.Right)
	case *spl2.BinaryExpr:
		y, ok := b.(*spl2.BinaryExpr)

		return ok && x.Op == y.Op && exprEqual(x.Left, y.Left) && exprEqual(x.Right, y.Right)
	case *spl2.ArithExpr:
		y, ok := b.(*spl2.ArithExpr)

		return ok && x.Op == y.Op && exprEqual(x.Left, y.Left) && exprEqual(x.Right, y.Right)
	case *spl2.NotExpr:
		y, ok := b.(*spl2.NotExpr)

		return ok && exprEqual(x.Expr, y.Expr)
	case *spl2.FuncCallExpr:
		y, ok := b.(*spl2.FuncCallExpr)
		if !ok || x.Name != y.Name || len(x.Args) != len(y.Args) {
			return false
		}
		for i := range x.Args {
			if !exprEqual(x.Args[i], y.Args[i]) {
				return false
			}
		}

		return true
	case *spl2.InExpr:
		y, ok := b.(*spl2.InExpr)
		if !ok || !exprEqual(x.Field, y.Field) || len(x.Values) != len(y.Values) {
			return false
		}
		for i := range x.Values {
			if !exprEqual(x.Values[i], y.Values[i]) {
				return false
			}
		}

		return true
	default:
		return a.String() == b.String()
	}
}

func isLiteralTrue(expr spl2.Expr) bool {
	if lit, ok := expr.(*spl2.LiteralExpr); ok {
		return strings.EqualFold(lit.Value, "true")
	}

	return false
}

func isLiteralFalse(expr spl2.Expr) bool {
	if lit, ok := expr.(*spl2.LiteralExpr); ok {
		return strings.EqualFold(lit.Value, "false")
	}

	return false
}

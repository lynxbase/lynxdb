// Package query is the single entry point that turns query text into an
// optimized plan: it compiles PPL to the canonical IR and optimizes the plan.
package query

import (
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ir/opt"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/ppl/parser"
)

// Compiled is an optimized plan ready for execution.
type Compiled struct {
	// Source is the query text.
	Source string
	// Logical is the plan as the frontend produced it.
	Logical *ir.Plan
	// Plan is the optimized plan.
	Plan *ir.Plan
	// Applied lists the optimizer rules that fired.
	Applied []opt.Applied
	// Explain and ExplainMode carry an explain prefix of the query.
	Explain     bool
	ExplainMode string
	// Sources lists the source names the query reads.
	Sources []string
}

// Options changes how CompileWith reads query text.
type Options struct {
	// DefaultSource is the index a query reads when it names none; with it,
	// a query may start with a command or with search terms.
	DefaultSource string
}

// Compile parses, analyzes and optimizes a PPL query. Plans of recent
// queries are reused; each call returns its own Compiled, whose plans are
// immutable and may be shared.
func Compile(src string) (*Compiled, error) { return CompileWith(src, Options{}) }

// CompileWith is Compile with opts.
func CompileWith(src string, opts Options) (*Compiled, error) {
	key := planKey{src: src, opts: opts}
	c, ok := plans.get(key)
	if !ok {
		var err error
		if c, err = compile(src, opts); err != nil {
			return nil, err
		}
		c.Applied = c.Applied[:len(c.Applied):len(c.Applied)]
		c.Sources = c.Sources[:len(c.Sources):len(c.Sources)]
		plans.put(key, c)
	}
	return c.copy(), nil
}

// copy returns a Compiled that shares only immutable values with c.
func (c *Compiled) copy() *Compiled {
	cp := *c
	logical, plan := *c.Logical, *c.Plan
	cp.Logical, cp.Plan = &logical, &plan
	return &cp
}

func compile(src string, opts Options) (*Compiled, error) {
	res, err := ppl.CompileWith(src, parser.Options{DefaultSource: opts.DefaultSource})
	if err != nil {
		return nil, err
	}
	plan, applied := opt.Optimize(res.Plan, opt.Options{Fold: Fold})
	if err := ir.Validate(plan); err != nil {
		return nil, fmt.Errorf("optimizer produced an invalid plan: %w", err)
	}
	return &Compiled{Source: src, Logical: res.Plan, Plan: plan, Applied: applied,
		Explain: res.Explain, ExplainMode: res.ExplainMode, Sources: res.Sources}, nil
}

// Fold computes a constant expression as the executor computes it.
func Fold(e ir.Expr) (*ir.Literal, bool) {
	x, err := pipeline.NewExpr(e, time.Time{})
	if err != nil {
		return nil, false
	}
	v, present := x.Value(nil)
	if !present {
		return nil, false
	}
	lit, ok := Literal(v)
	if !ok {
		return nil, false
	}
	if !e.Type().IsDynamic() && lit.T.Kind != ir.KindNull && !e.Type().Accepts(lit.T) {
		return nil, false
	}
	return lit, true
}

// Literal converts a scalar event value to an IR literal.
func Literal(v event.Value) (*ir.Literal, bool) {
	switch v.Type() {
	case event.FieldTypeNull:
		return ir.NullLit(), true
	case event.FieldTypeBool:
		return ir.BoolLit(v.AsBool()), true
	case event.FieldTypeInt:
		return ir.IntLit(v.AsInt()), true
	case event.FieldTypeFloat:
		return ir.FloatLit(v.AsFloat()), true
	case event.FieldTypeString:
		return ir.StringLit(v.AsString()), true
	case event.FieldTypeTimestamp:
		return ir.TimestampLit(v.AsTimestamp()), true
	case event.FieldTypeDuration:
		return ir.DurationLit(v.AsDuration()), true
	}
	return nil, false
}

// Explain renders the logical and optimized plans of a compiled query and
// the optimizer rules that rewrote it.
func Explain(c *Compiled) string {
	var b strings.Builder
	b.WriteString("Logical plan:\n")
	b.WriteString(ir.Render(c.Logical))
	b.WriteString("\nOptimized plan:\n")
	b.WriteString(ir.Render(c.Plan))
	if len(c.Applied) > 0 {
		b.WriteString("\nOptimizer rules:\n")
		for _, a := range c.Applied {
			fmt.Fprintf(&b, "  %s x%d\n", a.Rule, a.Count)
		}
	}
	return b.String()
}

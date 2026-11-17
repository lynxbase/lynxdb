// Package query is the single entry point that turns query text into
// results: it compiles PPL to the canonical IR, optimizes the plan and runs
// it through the physical planner. The CLI's file and pipe modes and the
// server use the same Compile; they differ only in the scan source.
package query

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ir/opt"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/vm"
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

// Compile parses, analyzes and optimizes a PPL query.
func Compile(src string) (*Compiled, error) {
	res, err := ppl.Compile(src)
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

// Fold evaluates a constant expression with the VM.
func Fold(e ir.Expr) (*ir.Literal, bool) {
	prog, err := vm.CompileExpr(e, vm.CompileOptions{})
	if err != nil {
		return nil, false
	}
	var m vm.VM
	v, err := m.Execute(prog, nil)
	if err != nil {
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

// Options configures in-memory execution.
type Options struct {
	// DefaultSource is the index a scan without sources reads.
	DefaultSource string
	// Now is the query's reference time; zero means the current time.
	Now       time.Time
	BatchSize int
	// Coordinator and SpillManager enable memory accounting and disk spill.
	Coordinator  *pipeline.MemoryCoordinator
	SpillManager *pipeline.SpillManager
	// Collect, when set, receives per-operator runtime statistics.
	Collect map[ir.Node]*physical.NodeStats
}

// Run executes a compiled plan over an in-memory event store.
func Run(ctx context.Context, c *Compiled, store physical.EventStore, opts Options) ([]map[string]event.Value, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	defaultSource := opts.DefaultSource
	if defaultSource == "" {
		defaultSource = "main"
	}
	iter, err := physical.Build(c.Plan, physical.Options{
		Source:       physical.NewEventSource(store, defaultSource, opts.Now),
		Describe:     DescribeStore(store),
		Coordinator:  opts.Coordinator,
		SpillManager: opts.SpillManager,
		BatchSize:    opts.BatchSize,
		Now:          opts.Now,
		Context:      ctx,
		Collect:      opts.Collect,
	})
	if err != nil {
		return nil, err
	}
	return pipeline.CollectAll(ctx, iter)
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

// Execute compiles and runs a query over an in-memory event store.
func Execute(ctx context.Context, src string, store physical.EventStore, opts Options) ([]map[string]event.Value, error) {
	c, err := Compile(src)
	if err != nil {
		return nil, err
	}
	return Run(ctx, c, store, opts)
}

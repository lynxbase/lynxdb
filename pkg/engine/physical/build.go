// Package physical plans the execution of canonical IR plans as row
// operators: it turns each logical operator into pipeline iterators whose
// expressions, comparisons and aggregates are those of the columnar
// executor. A plan reaches it with its subqueries answered. Storage access
// goes through the Source hook, which receives the IR scan with its pruning
// hints.
package physical

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Options configures Build.
type Options struct {
	// Source opens an iterator over the events a scan reads.
	Source func(scan *ir.Scan) (pipeline.Iterator, error)
	// Describe returns one row per column of a source for SourceSchema.
	Describe func(src ir.Source) ([]map[string]event.Value, error)
	// BatchSize is the number of rows per batch; zero uses the default.
	BatchSize int
	// Now is the query's reference time; zero means the time of Build.
	Now time.Time
}

// UnsupportedError reports an IR construct this engine cannot execute.
type UnsupportedError struct {
	Feature string
}

func (e *UnsupportedError) Error() string {
	return "not supported by the execution engine: " + e.Feature
}

func unsupported(format string, args ...any) error {
	return &UnsupportedError{Feature: fmt.Sprintf(format, args...)}
}

// Build turns a plan into an iterator producing its rows.
func Build(plan *ir.Plan, opts Options) (pipeline.Iterator, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	b := &builder{opts: opts}
	return b.node(plan.Root)
}

type builder struct {
	opts  Options
	temps int
}

func (b *builder) batchSize() int {
	if b.opts.BatchSize > 0 {
		return b.opts.BatchSize
	}
	return pipeline.DefaultBatchSize
}

func (b *builder) temp() string {
	b.temps++
	return "__t" + strconv.Itoa(b.temps)
}

// compile prepares a value expression for the rows.
func (b *builder) compile(e ir.Expr) (*pipeline.Expr, error) {
	return pipeline.NewExpr(e, b.opts.Now)
}

// cond prepares a condition for the rows.
func (b *builder) cond(e ir.Expr) (*pipeline.Expr, error) {
	return pipeline.NewCond(e, b.opts.Now)
}

func (b *builder) input(n ir.Node) (pipeline.Iterator, error) {
	return b.node(n.Inputs()[0])
}

func (b *builder) node(n ir.Node) (pipeline.Iterator, error) {
	switch x := n.(type) {
	case *ir.Scan:
		if b.opts.Source == nil {
			return nil, fmt.Errorf("physical: no source for %s", x.Label())
		}
		it, err := b.opts.Source(x)
		if err != nil || x.Time.IsZero() {
			return it, err
		}
		return b.timeFilter(it, x.Time)
	case *ir.Values:
		return b.values(x)
	case *ir.SourceSchema:
		if b.opts.Describe == nil {
			return nil, unsupported("describe in this execution mode")
		}
		rows, err := b.opts.Describe(x.Source)
		if err != nil {
			return nil, err
		}
		return pipeline.NewRowScanIterator(rows, b.batchSize()), nil
	case *ir.Filter:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		cond, err := b.cond(x.Cond)
		if err != nil {
			return nil, err
		}
		return pipeline.NewFilterIterator(child, cond), nil
	case *ir.Compute:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return b.compute(child, x.Assigns)
	case *ir.Project:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return pipeline.NewProjectIterator(child, x.Names, x.Drop), nil
	case *ir.RenameColumns:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return newRenameIterator(child, x.Renames), nil
	case *ir.Aggregate:
		return b.aggregate(x)
	case *ir.Window:
		return b.window(x)
	case *ir.Sort:
		return b.sort(x.Input, x.Keys, 0, 0, false)
	case *ir.TopN:
		return b.sort(x.Input, x.Keys, x.Count, x.Offset, true)
	case *ir.Limit:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		var it pipeline.Iterator = child
		if x.Offset > 0 {
			it = pipeline.NewOffsetIterator(it, int(x.Offset))
		}
		if x.Count >= 0 {
			it = pipeline.NewLimitIterator(it, int(x.Count))
		}
		return it, nil
	case *ir.Reverse:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return pipeline.NewReverseIterator(child, b.batchSize()), nil
	case *ir.Dedup:
		return b.dedup(x)
	case *ir.Join:
		return b.join(x)
	case *ir.Union:
		iters := make([]pipeline.Iterator, len(x.Sources))
		for i, s := range x.Sources {
			it, err := b.node(s)
			if err != nil {
				return nil, err
			}
			iters[i] = it
		}
		if x.Mode == ir.UnionByTime {
			return newBlockingIterator(pipeline.NewUnionIterator(iters), b.batchSize(), sortByTimeDesc), nil
		}
		return pipeline.NewUnionIterator(iters), nil
	case *ir.ZipColumns:
		left, err := b.node(x.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.node(x.Right)
		if err != nil {
			return nil, err
		}
		return newZipIterator(left, right, x.Override, b.batchSize()), nil
	case *ir.Extract:
		return b.extract(x)
	case *ir.Patterns:
		return b.patterns(x)
	case *ir.Unnest:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return newMapIterator(child, b.batchSize(), unnestRow(x)), nil
	case *ir.FlattenObject:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return newMapIterator(child, b.batchSize(), flattenRow(x)), nil
	case *ir.FillNull:
		return b.fillNull(x)
	case *ir.Transpose:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return newBlockingIterator(child, b.batchSize(), transposeRows(x)), nil
	case *ir.Totals:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return newBlockingIterator(child, b.batchSize(), totalsRows(x)), nil
	case *ir.CombineValues:
		child, err := b.input(x)
		if err != nil {
			return nil, err
		}
		return newBlockingIterator(child, b.batchSize(), combineRows(x.Field)), nil
	}
	return nil, fmt.Errorf("physical: unknown operator %T", n)
}

// timeFilter enforces a scan's time range on its source's rows, so the
// range holds exactly whatever pruning the storage applied.
func (b *builder) timeFilter(it pipeline.Iterator, tr *ir.TimeRange) (pipeline.Iterator, error) {
	ts := &ir.FieldRef{Name: ir.TimeColumn, T: ir.Timestamp}
	var conds []ir.Expr
	if tr.Earliest != nil {
		conds = append(conds, &ir.Binary{Op: ir.OpGe, L: ts, R: &ir.RelativeTime{Bound: tr.Earliest}, T: ir.Bool})
	}
	if tr.Latest != nil {
		conds = append(conds, &ir.Binary{Op: ir.OpLt, L: ts, R: &ir.RelativeTime{Bound: tr.Latest}, T: ir.Bool})
	}
	cond, err := b.cond(ir.And(conds...))
	if err != nil {
		return nil, err
	}
	return pipeline.NewFilterIterator(it, cond), nil
}

func (b *builder) compute(child pipeline.Iterator, assigns []ir.Assign) (pipeline.Iterator, error) {
	out := make([]pipeline.EvalAssignment, len(assigns))
	for i, a := range assigns {
		e, err := b.compile(a.Expr)
		if err != nil {
			return nil, fmt.Errorf("eval %s: %w", a.Name, err)
		}
		out[i] = pipeline.EvalAssignment{Field: a.Name, Expr: e}
	}
	return pipeline.NewEvalIterator(child, out), nil
}

func (b *builder) values(x *ir.Values) (pipeline.Iterator, error) {
	names := x.Out.Names()
	rows := make([]map[string]event.Value, len(x.Rows))
	for i, r := range x.Rows {
		row := make(map[string]event.Value, len(r))
		for j, e := range r {
			lit, ok := e.(*ir.Literal)
			if !ok {
				return nil, fmt.Errorf("physical: values row %d holds a non-literal", i)
			}
			row[names[j]] = literalValue(lit.V)
		}
		rows[i] = row
	}
	return pipeline.NewRowScanIterator(rows, b.batchSize()), nil
}

// fieldOrTemp returns the column name holding e's value, adding an
// evaluation of e into a temporary column when e is not a plain column.
func (b *builder) fieldOrTemp(e ir.Expr, temps *[]ir.Assign) string {
	if f, ok := e.(*ir.FieldRef); ok {
		return f.Name
	}
	name := b.temp()
	*temps = append(*temps, ir.Assign{Name: name, Expr: e})
	return name
}

func (b *builder) sort(input ir.Node, keys []ir.SortKey, count, offset int64, top bool) (pipeline.Iterator, error) {
	child, err := b.node(input)
	if err != nil {
		return nil, err
	}
	var temps []ir.Assign
	fields := make([]pipeline.SortField, 0, len(keys))
	for _, k := range keys {
		fields = append(fields, pipeline.SortField{Name: b.fieldOrTemp(k.Expr, &temps), Desc: k.Desc, NullsFirst: k.NullsFirst})
	}
	var it pipeline.Iterator = child
	if len(temps) > 0 {
		if it, err = b.compute(it, temps); err != nil {
			return nil, err
		}
	}
	if top {
		// The first rows of groups break ties by their other columns, as
		// the columnar executor does: groups come in no order of their own.
		if aggregated(input) && !input.Schema().Open {
			for _, name := range input.Schema().Names() {
				if !slices.ContainsFunc(fields, func(f pipeline.SortField) bool { return f.Name == name }) {
					fields = append(fields, pipeline.SortField{Name: name})
				}
			}
		}
		it = pipeline.NewTopNIterator(it, fields, int(count+offset), b.batchSize())
		if offset > 0 {
			it = pipeline.NewOffsetIterator(it, int(offset))
		}
	} else {
		it = pipeline.NewSortIterator(it, fields, b.batchSize())
	}
	return dropTemps(it, temps), nil
}

func dropTemps(it pipeline.Iterator, temps []ir.Assign) pipeline.Iterator {
	if len(temps) == 0 {
		return it
	}
	names := make([]string, len(temps))
	for i, t := range temps {
		names[i] = t.Name
	}
	return pipeline.NewProjectIterator(it, names, true)
}

func (b *builder) fillNull(x *ir.FillNull) (pipeline.Iterator, error) {
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	if len(x.Fields) > 0 {
		assigns := make([]ir.Assign, len(x.Fields))
		for i, f := range x.Fields {
			cur := &ir.FieldRef{Name: f.Name, T: ir.Any}
			assigns[i] = ir.Assign{Name: f.Name, Expr: &ir.Call{Func: ir.MustFunc("coalesce"), Args: []ir.Expr{cur, f.Expr}, T: ir.Any}}
		}
		return b.compute(child, assigns)
	}
	lit, ok := x.Default.(*ir.Literal)
	if !ok {
		return nil, unsupported("fillnull over all fields with a non-constant value")
	}
	v := literalValue(lit.V)
	return newMapIterator(child, b.batchSize(), func(row map[string]event.Value) []map[string]event.Value {
		for k, val := range row {
			if val.IsNull() {
				row[k] = v
			}
		}
		return []map[string]event.Value{row}
	}), nil
}

// literalValue returns the value of a literal of a plan as one of a row.
func literalValue(v any) event.Value {
	switch v := v.(type) {
	case bool:
		return event.BoolValue(v)
	case int64:
		return event.IntValue(v)
	case float64:
		return event.FloatValue(v)
	case string:
		return event.StringValue(v)
	case time.Time:
		return event.TimestampValue(v)
	case time.Duration:
		return event.DurationValue(v)
	}
	return event.NullValue()
}

// aggregated reports whether the rows of n are groups of an aggregate,
// computed or chosen among after it.
func aggregated(n ir.Node) bool {
	for {
		switch n.(type) {
		case *ir.Aggregate:
			return true
		case *ir.Compute, *ir.Project, *ir.Filter, *ir.RenameColumns:
			n = n.Inputs()[0]
		default:
			return false
		}
	}
}

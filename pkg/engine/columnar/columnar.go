// Package columnar runs queries in the canonical IR over tables of the
// columnar store. The columnar executor runs every part of a plan that it
// can, from the scans upwards, and the row operators of the physical engine
// run what is above those parts over their rows. A scan that the executor
// cannot take with the operators above it gives its events as rows.
package columnar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irplan"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Tables gives the tables that the sources of a query name.
type Tables interface {
	// Snapshot returns the rows that the table name holds now, for the
	// caller to release, or false when there is no such table.
	Snapshot(name string) (*colstore.Snapshot, bool)
	// Names lists the tables.
	Names() []string
}

// Options sets how a query runs.
type Options struct {
	// DefaultSource is the table that a scan without sources reads.
	DefaultSource string
	// Now is the time that relative times count from; the zero time means
	// the time of the call.
	Now time.Time
	// Workers is the number of goroutines of a columnar scan; zero lets
	// the executor share the CPUs among the queries that run.
	Workers int
	// BatchSize is the number of rows the row operators take at a time;
	// zero means their default.
	BatchSize int
}

// Result is the rows of a query. Maps gives them as maps and AppendJSON as
// JSON objects; a result that the columnar executor produced whole is kept
// in its columns until one of them asks.
type Result struct {
	// Columns names the columns in their order when the plan fixes them.
	Columns []string
	// Columnar is set when the columnar executor ran the whole plan.
	Columnar bool

	rows   []map[string]event.Value
	table  *exec.Result
	schema ir.Schema
}

// pushedSource starts the name of the source that stands for a part of a
// plan the columnar executor ran.
const pushedSource = "\x00columnar"

// Run executes plan over tables and returns its rows.
func Run(ctx context.Context, plan *ir.Plan, tables Tables, opts Options) (*Result, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	r := &run{ctx: ctx, tables: tables, opts: opts}
	defer r.release()
	root, err := r.push(plan.Root, true)
	if err != nil {
		return nil, err
	}
	out := &Result{}
	if schema := plan.Root.Schema(); !schema.Open {
		out.Columns = schema.Names()
	}
	if scan, ok := root.(*ir.Scan); ok && len(scan.Sources) == 1 && r.ran[scan.Sources[0].Name] != nil {
		out.table, out.schema, out.Columnar = r.ran[scan.Sources[0].Name].table, plan.Root.Schema(), true

		return out, nil
	}
	iter, err := physical.Build(&ir.Plan{Root: root}, physical.Options{
		Source:    r.source,
		Describe:  r.describe,
		BatchSize: opts.BatchSize,
		Now:       opts.Now,
		Context:   ctx,
	})
	if err != nil {
		return nil, err
	}
	if out.rows, err = pipeline.CollectAll(ctx, iter); err != nil {
		return nil, err
	}

	return out, nil
}

// run is the state of one query.
type run struct {
	ctx    context.Context
	tables Tables
	opts   Options
	// ran holds the parts of the plan that the columnar executor runs, by
	// the name of the source that stands for each.
	ran map[string]*pushed
	// snaps are the tables' rows that the query reads until it ends.
	snaps []*colstore.Snapshot
}

// pushed is a part of the plan that the columnar executor runs: its rows,
// or for a part whose rows the row operators take block by block the plan
// that gives them and the rows of the tables it reads.
type pushed struct {
	table  *exec.Result
	plan   exec.Node
	snap   *colstore.Snapshot
	schema ir.Schema
}

func (r *run) release() {
	for _, snap := range r.snaps {
		snap.Release()
	}
}

// handsOver reports whether plan gives rows as it scans them: a projection
// of a scan, with at most a limit above it.
func handsOver(plan exec.Node) bool {
	if limit, ok := plan.(*exec.Sort); ok && len(limit.By) == 0 {
		plan = limit.Input
	}
	rows, ok := plan.(*exec.Project)
	if !ok {
		return false
	}
	_, scans := rows.Input.(*exec.Scan)

	return scans
}

// push returns the plan under n with every part that the columnar executor
// can run, from a scan upwards, replaced by a scan of that part's rows; root
// is set for the whole plan, whose rows are the result.
func (r *run) push(n ir.Node, root bool) (ir.Node, error) {
	if part, err := r.columnar(n, root); err != nil {
		return nil, err
	} else if part != nil {
		name := pushedSource + strconv.Itoa(len(r.ran))
		if r.ran == nil {
			r.ran = map[string]*pushed{}
		}
		r.ran[name] = part

		return &ir.Scan{Sources: []ir.Source{{Name: name}}, Out: n.Schema()}, nil
	}
	ins := n.Inputs()
	if len(ins) == 0 {
		return n, nil
	}
	outs := make([]ir.Node, len(ins))
	for i, in := range ins {
		out, err := r.push(in, false)
		if err != nil {
			return nil, err
		}
		outs[i] = out
	}

	return n.WithInputs(outs), nil
}

// columnar gives the plan under n to the columnar executor, or returns nil
// for a plan that the executor does not take. A part of a larger plan that
// gives rows as it scans them is left to hand them over block by block;
// anything else runs here.
func (r *run) columnar(n ir.Node, root bool) (*pushed, error) {
	var scan *ir.Scan
	for at := n; scan == nil; {
		ins := at.Inputs()
		switch s, isScan := at.(*ir.Scan); {
		case isScan:
			scan = s
		case len(ins) != 1:
			return nil, nil
		default:
			at = ins[0]
		}
	}
	plan, err := irplan.Plan(&ir.Plan{Root: n}, irplan.Options{Now: r.opts.Now, Opaque: opaque(r.opts.Now)})
	if err != nil {
		// What the planner does not take, or takes for an error, is for the
		// row operators to run or to refuse.
		return nil, nil
	}
	snap := r.snapshot(scan)
	if !root && handsOver(plan) {
		if exec.Supports(snap, plan) != nil {
			return nil, nil
		}

		return &pushed{plan: plan, snap: snap, schema: n.Schema()}, nil
	}
	res, err := exec.Run(r.ctx, snap, plan, exec.Options{Workers: r.opts.Workers})
	switch {
	case errors.Is(err, exec.ErrUnsupported):
		return nil, nil
	case err != nil:
		return nil, err
	}

	return &pushed{table: res, schema: n.Schema()}, nil
}

// snapshot returns the rows of the tables that a scan reads, which stay
// until the query ends. A table that is not there holds no rows.
func (r *run) snapshot(scan *ir.Scan) *colstore.Snapshot {
	var snaps []*colstore.Snapshot
	for _, name := range r.sources(scan) {
		if snap, ok := r.tables.Snapshot(name); ok {
			snaps = append(snaps, snap)
		}
	}
	all := colstore.Union(snaps...)
	for _, snap := range snaps {
		snap.Release()
	}
	r.snaps = append(r.snaps, all)

	return all
}

// sources returns the names of the tables that a scan reads.
func (r *run) sources(scan *ir.Scan) []string {
	if len(scan.Sources) == 0 {
		name := r.opts.DefaultSource
		if name == "" {
			name = "main"
		}

		return []string{name}
	}
	var out []string
	seen := map[string]bool{}
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	for _, s := range scan.Sources {
		if !s.Pattern && s.Name != "*" {
			add(s.Name)

			continue
		}
		for _, name := range r.tables.Names() {
			if s.Name == "*" || ir.MatchName(s.Name, name) {
				add(name)
			}
		}
	}

	return out
}

// source gives the row operators the rows of a scan: those of a part of the
// plan that the columnar executor runs, or the events of the scan's tables
// as its hints ask for them. Rows that need no order come block by block.
func (r *run) source(scan *ir.Scan) (pipeline.Iterator, error) {
	if len(scan.Sources) == 1 {
		if part, ok := r.ran[scan.Sources[0].Name]; ok {
			if part.plan != nil {
				return &stream{r: r, snap: part.snap, plan: part.plan, schema: part.schema}, nil
			}

			return pipeline.NewRowScanIterator(rowsOf(part.table, part.schema), r.opts.BatchSize), nil
		}
	}
	plan, err := irplan.Rows(scan, irplan.Options{Now: r.opts.Now, Opaque: opaque(r.opts.Now)})
	if err != nil {
		return nil, err
	}
	snap := r.snapshot(scan)
	if handsOver(plan) {
		return &stream{r: r, snap: snap, plan: plan, schema: ir.Schema{Open: true}}, nil
	}
	res, err := exec.Run(r.ctx, snap, plan, exec.Options{Workers: r.opts.Workers})
	if err != nil {
		return nil, err
	}

	return pipeline.NewRowScanIterator(rowsOf(res, ir.Schema{Open: true}), r.opts.BatchSize), nil
}

// stream hands the rows of a plan to the row operators as the columnar
// executor scans them.
type stream struct {
	r      *run
	snap   *colstore.Snapshot
	plan   exec.Node
	schema ir.Schema

	batches chan *pipeline.Batch
	cancel  context.CancelFunc
	done    chan struct{}
	err     error
}

// Init starts the scan; the row operators may call it again, and Close more
// than once.
func (s *stream) Init(ctx context.Context) error {
	if s.cancel != nil {
		return nil
	}
	ctx, s.cancel = context.WithCancel(ctx)
	s.batches, s.done = make(chan *pipeline.Batch, 2), make(chan struct{})
	size := s.r.opts.BatchSize
	if size <= 0 {
		size = pipeline.DefaultBatchSize
	}
	go func() {
		defer close(s.done)
		defer close(s.batches)
		s.err = exec.Each(ctx, s.snap, s.plan, exec.Options{Workers: s.r.opts.Workers}, func(res *exec.Result) error {
			batch := batchOf(res, s.schema)
			for from := 0; from < batch.Len; from += size {
				select {
				case s.batches <- batch.Slice(from, min(from+size, batch.Len)):
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		})
	}()

	return nil
}

func (s *stream) Next(ctx context.Context) (*pipeline.Batch, error) {
	if s.batches == nil {
		if err := s.Init(ctx); err != nil {
			return nil, err
		}
	}
	select {
	case batch, ok := <-s.batches:
		if ok {
			return batch, nil
		}
		<-s.done
		if s.err != nil && !errors.Is(s.err, context.Canceled) {
			return nil, s.err
		}

		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *stream) Close() error {
	if s.cancel == nil {
		// A scan that never started has nothing to end, and does not
		// start afterwards.
		s.cancel = func() {}
		s.batches, s.done = make(chan *pipeline.Batch), make(chan struct{})
		close(s.batches)
		close(s.done)

		return nil
	}
	s.cancel()
	for range s.batches {
	}
	<-s.done

	return nil
}

func (s *stream) Schema() []pipeline.FieldInfo { return nil }

// batchOf turns the columns of a result into a batch of the row operators,
// by the rules of rowsOf.
func batchOf(res *exec.Result, schema ir.Schema) *pipeline.Batch {
	batch := &pipeline.Batch{Columns: make(map[string][]event.Value, len(res.Columns)), Len: res.Rows}
	for c := range res.Columns {
		name := res.Columns[c].Name
		col, listed := schema.Lookup(name)
		at := name == ir.TimeColumn || (listed && col.Type.Kind == ir.KindTimestamp)
		vals := make([]event.Value, res.Rows)
		var present []bool
		for i := range vals {
			v := res.Value(i, c)
			switch {
			case v.Kind == exec.KindAny && !listed:
				if present == nil {
					present = make([]bool, res.Rows)
					for j := 0; j < i; j++ {
						present[j] = true
					}
				}

				continue
			case v.IsNull():
				vals[i] = event.NullValue()
			case at && v.Kind == vec.KindInt:
				vals[i] = event.TimestampValue(time.Unix(0, v.Int).UTC())
			default:
				vals[i] = valueOf(v)
			}
			if present != nil {
				present[i] = true
			}
		}
		batch.Columns[name] = vals
		if present != nil {
			if batch.Present == nil {
				batch.Present = map[string][]bool{}
			}
			batch.Present[name] = present
		}
	}

	return batch
}

// describe returns one row per field that the rows of a table hold.
func (r *run) describe(src ir.Source) ([]map[string]event.Value, error) {
	snap, ok := r.tables.Snapshot(src.Name)
	if !ok {
		return nil, fmt.Errorf("columnar: no table %q", src.Name)
	}
	defer snap.Release()
	seen := map[string]bool{}
	var names []string
	for _, h := range snap.Parts() {
		for _, path := range h.Paths() {
			if !seen[path] {
				seen[path] = true
				names = append(names, path)
			}
		}
	}
	sort.Strings(names)
	rows := make([]map[string]event.Value, len(names))
	for i, name := range names {
		rows[i] = map[string]event.Value{"column": event.StringValue(name)}
	}

	return rows, nil
}

// rowsOf turns the columns of a result into rows. A row has no entry for a
// field it does not hold; the columns that schema lists hold a null there,
// and those it types as times hold times.
func rowsOf(res *exec.Result, schema ir.Schema) []map[string]event.Value {
	rows := make([]map[string]event.Value, res.Rows)
	for i := range rows {
		rows[i] = make(map[string]event.Value, len(res.Columns))
	}
	for c := range res.Columns {
		name := res.Columns[c].Name
		col, listed := schema.Lookup(name)
		at := name == ir.TimeColumn || (listed && col.Type.Kind == ir.KindTimestamp)
		for i := range rows {
			v := res.Value(i, c)
			switch {
			case v.Kind == exec.KindAny && !listed:
			case v.IsNull():
				rows[i][name] = event.NullValue()
			case at && v.Kind == vec.KindInt:
				rows[i][name] = event.TimestampValue(time.Unix(0, v.Int).UTC())
			default:
				rows[i][name] = valueOf(v)
			}
		}
	}

	return rows
}

func valueOf(v exec.Value) event.Value {
	switch v.Kind {
	case vec.KindBool:
		return event.BoolValue(v.Bool)
	case vec.KindInt:
		return event.IntValue(v.Int)
	case vec.KindFloat:
		return event.FloatValue(v.Float)
	case vec.KindString:
		return event.StringValue(string(v.Bytes))
	case vec.KindVariant:
		text, err := variant.AppendJSON(nil, v.Bytes)
		if err != nil {
			return event.NullValue()
		}
		dec := json.NewDecoder(bytes.NewReader(text))
		dec.UseNumber()
		var decoded any
		if dec.Decode(&decoded) != nil {
			return event.NullValue()
		}

		return decodedValue(decoded)
	}

	return event.NullValue()
}

// decodedValue turns a decoded JSON value into a value of a row.
func decodedValue(v any) event.Value {
	switch v := v.(type) {
	case bool:
		return event.BoolValue(v)
	case string:
		return event.StringValue(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return event.IntValue(n)
		}
		if f, err := v.Float64(); err == nil {
			return event.FloatValue(f)
		}

		return event.StringValue(v.String())
	case []any:
		elems := make([]event.Value, len(v))
		for i, e := range v {
			elems[i] = decodedValue(e)
		}

		return event.ArrayValue(elems)
	case map[string]any:
		fields := make(map[string]event.Value, len(v))
		for k, e := range v {
			fields[k] = decodedValue(e)
		}

		return event.ObjectValue(fields)
	}

	return event.NullValue()
}

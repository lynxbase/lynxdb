// Package columnar runs queries in the canonical IR over tables of the
// columnar store. The columnar executor runs every part of a plan that it
// can, from the scans upwards, and the row operators of the physical engine
// run what is above those parts over their rows. A scan that the executor
// cannot take with the operators above it gives its events as rows.
package columnar

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irplan"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
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
	// rows leaves every operator above the scans to the row operators, for
	// the tests that require both ways of running a plan to agree.
	rows bool
}

// Result is the rows of a query. Maps gives them as maps and AppendJSON as
// JSON objects; a result that the columnar executor produced whole is kept
// in its columns until one of them asks.
type Result struct {
	// Columns names the columns in their order when the plan fixes them.
	Columns []string
	// Columnar is set when the columnar executor ran the whole plan.
	Columnar bool
	// Warnings tells the caller what the query could not know to be an
	// error, such as a field that none of the indexes it reads holds.
	Warnings []string

	total   int
	partial bool
	rows    []map[string]event.Value
	table   *exec.Result
	schema  ir.Schema
}

// pushedSource starts the name of the source that stands for a part of a
// plan the columnar executor ran.
const pushedSource = "\x00columnar"

// ErrNoIndex reports a source that names an index there is not; a pattern
// of names that matches none is not one.
var ErrNoIndex = errors.New("no such index")

// defaultIndex is the index that is there before its first event.
const defaultIndex = "main"

// Run executes plan over tables and returns its rows.
func Run(ctx context.Context, plan *ir.Plan, tables Tables, opts Options) (*Result, error) {
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	r := &run{ctx: ctx, tables: tables, opts: opts}
	defer r.release()
	root, warnings, err := resolved(ctx, plan.Root, tables, opts)
	if err != nil {
		return nil, err
	}
	warnings = append(warnings, r.absentFields(root)...)
	if root, err = r.push(ir.Transform(ir.Transform(root, collected), premapped), true); err != nil {
		return nil, err
	}
	out := &Result{Warnings: warnings}
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
	if r.opts.rows {
		return nil, nil
	}
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
	plan, err := irplan.Plan(&ir.Plan{Root: n}, irplan.Options{Now: r.opts.Now})
	if err != nil {
		// What the planner does not take, or takes for an error, is for the
		// row operators to run or to refuse.
		return nil, nil
	}
	snap, err := r.snapshot(scan)
	if err != nil {
		return nil, err
	}
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
// until the query ends; it fails for a name of a table that is not there.
func (r *run) snapshot(scan *ir.Scan) (*colstore.Snapshot, error) {
	var snaps []*colstore.Snapshot
	release := func() {
		for _, snap := range snaps {
			snap.Release()
		}
	}
	names, named := r.sources(scan)
	for _, name := range names {
		snap, ok := r.tables.Snapshot(name)
		switch {
		case ok:
			snaps = append(snaps, snap)
		case named[name] && name != defaultIndex:
			release()

			return nil, fmt.Errorf("%w: %s", ErrNoIndex, name)
		}
	}
	all := colstore.Union(snaps...)
	release()
	r.snaps = append(r.snaps, all)

	return all, nil
}

// sources returns the names of the tables that a scan reads, and which of
// them it names as they are rather than by a pattern.
func (r *run) sources(scan *ir.Scan) ([]string, map[string]bool) {
	return sourceNames(scan.Sources, r.opts.DefaultSource, r.tables.Names)
}

// sourceNames returns the names of the tables that sources name, those of
// tables that match a pattern among them, or the default source for none;
// and which of the names sources give as they are.
func sourceNames(sources []ir.Source, defaultSource string, tables func() []string) ([]string, map[string]bool) {
	if len(sources) == 0 {
		name := defaultSource
		if name == "" {
			name = defaultIndex
		}

		return []string{name}, map[string]bool{name: true}
	}
	named := map[string]bool{}
	var out []string
	seen := map[string]bool{}
	add := func(name string) {
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	var all []string
	for _, s := range sources {
		if !s.Pattern && s.Name != "*" {
			add(s.Name)
			named[s.Name] = true

			continue
		}
		if all == nil {
			all = tables()
		}
		for _, name := range all {
			if s.Name == "*" || ir.MatchName(s.Name, name) {
				add(name)
			}
		}
	}

	return out, named
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
	plan, err := irplan.Rows(scan, irplan.Options{Now: r.opts.Now})
	if err != nil {
		return nil, err
	}
	snap, err := r.snapshot(scan)
	if err != nil {
		return nil, err
	}
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

// batchOf turns the columns of a result into a batch of the row operators,
// by the rules of rowsOf.
func batchOf(res *exec.Result, schema ir.Schema) *pipeline.Batch {
	batch := &pipeline.Batch{Columns: make(map[string][]event.Value, len(res.Columns)), Len: res.Rows}
	for c := range res.Columns {
		name := res.Columns[c].Name
		kind := kindOf(schema, name)
		vals := make([]event.Value, res.Rows)
		var present []bool
		for i := range vals {
			v := res.Value(i, c)
			switch {
			case v.Kind == exec.KindAny && res.Columns[c].Field:
				if present == nil {
					present = make([]bool, res.Rows)
					for j := 0; j < i; j++ {
						present[j] = true
					}
				}

				continue
			default:
				vals[i] = pipeline.RowValue(v, kind)
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

// describedBlocks is the most blocks of one part that describe opens to
// learn the kinds of its fields.
const describedBlocks = 16

// describe returns one row per field that the rows of a table hold, in the
// order of the fields' names: the table, the field, the type of its values
// as far as the blocks looked at tell, and its place in that order.
func (r *run) describe(src ir.Source) ([]map[string]event.Value, error) {
	snap, ok := r.tables.Snapshot(src.Name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoIndex, src.Name)
	}
	defer snap.Release()
	kinds := map[string]uint8{}
	var names []string
	var b part.Block
	for _, h := range snap.Parts() {
		paths := h.Paths()
		for _, path := range paths {
			if _, seen := kinds[path]; !seen {
				kinds[path] = 0
				names = append(names, path)
			}
		}
		// The first and the last block, then blocks spread between them.
		n := h.NumBlocks()
		for _, i := range spread(n, describedBlocks) {
			if err := h.OpenBlock(i, &b); err != nil {
				return nil, err
			}
			for _, ref := range b.Columns() {
				if ref.Count > 0 {
					kinds[paths[ref.PathID]] |= 1 << ref.Kind
				}
			}
		}
	}
	sort.Strings(names)
	rows := make([]map[string]event.Value, len(names))
	for i, name := range names {
		rows[i] = map[string]event.Value{
			"TABLE_NAME":       event.StringValue(src.Name),
			"COLUMN_NAME":      event.StringValue(name),
			"TYPE_NAME":        event.StringValue(typeName(name, kinds[name])),
			"ORDINAL_POSITION": event.IntValue(int64(i + 1)),
		}
	}

	return rows, nil
}

// spread returns up to most of the numbers below n, the first and the last
// among them.
func spread(n, most int) []int {
	if n <= most {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}

		return out
	}
	out := make([]int, most)
	for i := range out {
		out[i] = i * (n - 1) / (most - 1)
	}

	return out
}

// typeName names the type of a field that holds values of kinds, a bit per
// kind: numbers of both kinds are doubles, and any other mix is text.
func typeName(path string, kinds uint8) string {
	if path == ir.TimeColumn {
		return "timestamp"
	}
	switch kinds &^ (1 << vec.KindNull) {
	case 1 << vec.KindBool:
		return "boolean"
	case 1 << vec.KindInt:
		return "bigint"
	case 1 << vec.KindFloat, 1<<vec.KindInt | 1<<vec.KindFloat:
		return "double"
	case 1 << vec.KindVariant:
		return "array"
	}

	return "string"
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
		kind := kindOf(schema, name)
		for i := range rows {
			if v := res.Value(i, c); v.Kind != exec.KindAny || !res.Columns[c].Field {
				rows[i][name] = pipeline.RowValue(v, kind)
			}
		}
	}

	return rows
}

// kindOf returns the IR kind of the values of a column: that of the time
// for the time, and that which schema gives a column it lists.
func kindOf(schema ir.Schema, name string) ir.Kind {
	if name == ir.TimeColumn {
		return ir.KindTimestamp
	}
	if col, listed := schema.Lookup(name); listed {
		return col.Type.Kind
	}

	return ir.KindAny
}

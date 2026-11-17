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

// Result is the rows of a query.
type Result struct {
	// Columns names the columns in their order when the plan fixes them.
	Columns []string
	Rows    []map[string]event.Value
	// Columnar is set when the columnar executor ran the whole plan.
	Columnar bool
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
	root, err := r.push(plan.Root)
	if err != nil {
		return nil, err
	}
	out := &Result{}
	if schema := plan.Root.Schema(); !schema.Open {
		out.Columns = schema.Names()
	}
	if scan, ok := root.(*ir.Scan); ok && len(scan.Sources) == 1 && r.ran[scan.Sources[0].Name] != nil {
		out.Rows, out.Columnar = r.ran[scan.Sources[0].Name], true
		if out.Rows == nil {
			out.Rows = []map[string]event.Value{}
		}

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
	if out.Rows, err = pipeline.CollectAll(ctx, iter); err != nil {
		return nil, err
	}

	return out, nil
}

// run is the state of one query.
type run struct {
	ctx    context.Context
	tables Tables
	opts   Options
	// ran holds the rows of the parts of the plan that the columnar
	// executor ran, by the name of the source that stands for each.
	ran map[string][]map[string]event.Value
}

// push returns the plan under n with every part that the columnar executor
// can run, from a scan upwards, replaced by a scan of that part's rows.
func (r *run) push(n ir.Node) (ir.Node, error) {
	if rows, ok, err := r.columnar(n); err != nil {
		return nil, err
	} else if ok {
		name := pushedSource + strconv.Itoa(len(r.ran))
		if r.ran == nil {
			r.ran = map[string][]map[string]event.Value{}
		}
		if rows == nil {
			rows = []map[string]event.Value{}
		}
		r.ran[name] = rows

		return &ir.Scan{Sources: []ir.Source{{Name: name}}, Out: n.Schema()}, nil
	}
	ins := n.Inputs()
	if len(ins) == 0 {
		return n, nil
	}
	outs := make([]ir.Node, len(ins))
	for i, in := range ins {
		out, err := r.push(in)
		if err != nil {
			return nil, err
		}
		outs[i] = out
	}

	return n.WithInputs(outs), nil
}

// columnar runs the plan under n in the columnar executor and returns its
// rows; ok is false for a plan that the executor does not take.
func (r *run) columnar(n ir.Node) (rows []map[string]event.Value, ok bool, err error) {
	var scan *ir.Scan
	for at := n; scan == nil; {
		ins := at.Inputs()
		switch s, isScan := at.(*ir.Scan); {
		case isScan:
			scan = s
		case len(ins) != 1:
			return nil, false, nil
		default:
			at = ins[0]
		}
	}
	plan, err := irplan.Plan(&ir.Plan{Root: n}, irplan.Options{Now: r.opts.Now})
	if err != nil {
		// What the planner does not take, or takes for an error, is for the
		// row operators to run or to refuse.
		return nil, false, nil
	}
	snap := r.snapshot(scan)
	defer snap.Release()
	res, err := exec.Run(r.ctx, snap, plan, exec.Options{Workers: r.opts.Workers})
	switch {
	case errors.Is(err, exec.ErrUnsupported):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	}

	return rowsOf(res, n.Schema()), true, nil
}

// snapshot returns the rows of the tables that a scan reads, for the caller
// to release. A table that is not there holds no rows.
func (r *run) snapshot(scan *ir.Scan) *colstore.Snapshot {
	var snaps []*colstore.Snapshot
	for _, name := range r.sources(scan) {
		if snap, ok := r.tables.Snapshot(name); ok {
			snaps = append(snaps, snap)
		}
	}
	if len(snaps) == 1 {
		return snaps[0]
	}
	all := colstore.Union(snaps...)
	for _, snap := range snaps {
		snap.Release()
	}

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
// plan that the columnar executor ran, or the events of the scan's tables
// within its time range.
func (r *run) source(scan *ir.Scan) (pipeline.Iterator, error) {
	if len(scan.Sources) == 1 {
		if rows, ok := r.ran[scan.Sources[0].Name]; ok {
			return pipeline.NewRowScanIterator(rows, r.opts.BatchSize), nil
		}
	}
	plan, err := irplan.Plan(&ir.Plan{Root: &ir.Scan{Time: scan.Time}}, irplan.Options{Now: r.opts.Now})
	if err != nil {
		return nil, err
	}
	snap := r.snapshot(scan)
	res, err := exec.Run(r.ctx, snap, plan, exec.Options{Workers: r.opts.Workers})
	snap.Release()
	if err != nil {
		return nil, err
	}
	rows := rowsOf(res, ir.Schema{Open: true})
	if scan.Descending {
		sort.SliceStable(rows, func(i, j int) bool {
			return rows[i][ir.TimeColumn].AsTimestamp().After(rows[j][ir.TimeColumn].AsTimestamp())
		})
	}

	return pipeline.NewRowScanIterator(rows, r.opts.BatchSize), nil
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
		for _, path := range h.Part.Paths() {
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

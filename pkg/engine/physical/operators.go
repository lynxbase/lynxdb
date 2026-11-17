package physical

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// aggregateNames maps IR aggregates onto the pipeline's aggregate functions.
var aggregateNames = map[string]string{
	"count": "count", "sum": "sum", "avg": "avg", "min": "min", "max": "max",
	"var_samp": "var", "var_pop": "varp", "stddev_samp": "stdev", "stddev_pop": "stdevp",
	"distinct_count": "dc", "distinct_count_approx": "dc", "percentile": "perc",
	"earliest": "earliest", "latest": "latest", "first": "first", "last": "last",
	"list": "list", "values": "values", "take": "list",
}

func (b *builder) aggFuncs(aggs []ir.NamedAgg) ([]pipeline.AggFunc, []ir.Assign, error) {
	out := make([]pipeline.AggFunc, len(aggs))
	var post []ir.Assign
	for i, a := range aggs {
		name, ok := aggregateNames[a.Agg.Func.Name]
		if !ok {
			return nil, nil, unsupported("aggregate %s", a.Agg.Func.Name)
		}
		if a.Agg.Distinct {
			return nil, nil, unsupported("DISTINCT aggregates")
		}
		f := pipeline.AggFunc{Name: name, Alias: a.Name}
		if len(a.Agg.Args) > 0 {
			arg := a.Agg.Args[0]
			if fr, ok := arg.(*ir.FieldRef); ok {
				f.Field = fr.Name
			} else {
				prog, err := b.compile(arg)
				if err != nil {
					return nil, nil, err
				}
				f.Program = prog
			}
		}
		switch a.Agg.Func.Name {
		case "percentile":
			lit := a.Agg.Args[1].(*ir.Literal)
			p, _ := lit.V.(float64)
			f.Quantile = p / 100
		case "earliest", "latest":
			if len(a.Agg.Args) > 1 {
				if fr, ok := a.Agg.Args[1].(*ir.FieldRef); !ok || fr.Name != ir.TimeColumn {
					return nil, nil, unsupported("%s ordered by a column other than %s", a.Agg.Func.Name, ir.TimeColumn)
				}
			}
		case "take":
			n := a.Agg.Args[1].(*ir.Literal).V.(int64)
			ref := &ir.FieldRef{Name: a.Name, T: ir.ArrayOf(ir.Any)}
			post = append(post, ir.Assign{Name: a.Name, Expr: &ir.Call{Func: ir.MustFunc("mvindex"),
				Args: []ir.Expr{ref, ir.IntLit(0), ir.IntLit(n - 1)}, T: ir.Any}})
		}
		if a.Agg.Filter != nil {
			prog, err := b.compile(a.Agg.Filter)
			if err != nil {
				return nil, nil, err
			}
			f.CondProgram = prog
		}
		out[i] = f
	}
	return out, post, nil
}

func (b *builder) aggregate(x *ir.Aggregate) (pipeline.Iterator, error) {
	if x.Phase != ir.AggComplete {
		return nil, unsupported("a split aggregate phase outside distributed execution")
	}
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	var temps []ir.Assign
	groupBy := make([]string, len(x.Groups))
	var renames []ir.Rename
	var nullChecks []ir.Expr
	for i, g := range x.Groups {
		if fr, ok := g.Expr.(*ir.FieldRef); ok && fr.Name == g.Name {
			groupBy[i] = g.Name
		} else if field, span, ok := fixedTimeBucket(g.Expr); ok {
			t := b.temp()
			child = pipeline.NewBinIterator(child, field, t, span)
			groupBy[i] = t
			renames = append(renames, ir.Rename{From: t, To: g.Name})
		} else {
			t := b.temp()
			temps = append(temps, ir.Assign{Name: t, Expr: g.Expr})
			groupBy[i] = t
			renames = append(renames, ir.Rename{From: t, To: g.Name})
		}
		if x.DropNullGroups {
			nullChecks = append(nullChecks, &ir.Unary{Op: ir.OpNot, X: &ir.Call{Func: ir.MustFunc("is_null"),
				Args: []ir.Expr{&ir.FieldRef{Name: groupBy[i], T: ir.Any}}, T: ir.Bool}, T: ir.Bool})
		}
	}
	var it pipeline.Iterator = child
	if len(temps) > 0 {
		if it, err = b.compute(it, temps); err != nil {
			return nil, err
		}
	}
	if len(nullChecks) > 0 {
		prog, err := b.compile(ir.And(nullChecks...))
		if err != nil {
			return nil, err
		}
		it = pipeline.NewFilterIterator(it, prog)
	}
	aggs, post, err := b.aggFuncs(x.Aggs)
	if err != nil {
		return nil, err
	}
	acct := b.account("Aggregate", pipeline.ReservationAggregate)
	if b.opts.SpillManager != nil {
		it = pipeline.NewAggregateIteratorWithSpill(it, aggs, groupBy, acct, b.opts.SpillManager)
	} else {
		it = pipeline.NewAggregateIterator(it, aggs, groupBy, acct)
	}
	if len(renames) > 0 {
		it = newRenameIterator(it, renames)
	}
	if arrays := arrayAggregates(x.Aggs); len(arrays) > 0 {
		it = &arrayAggIterator{Iterator: it, columns: arrays}
	}
	if len(post) > 0 {
		return b.compute(it, post)
	}
	return it, nil
}

// arrayAggregates maps the output columns of list, values and take to
// whether their elements are sorted and distinct.
func arrayAggregates(aggs []ir.NamedAgg) map[string]bool {
	out := map[string]bool{}
	for _, a := range aggs {
		switch a.Agg.Func.Name {
		case "values":
			out[a.Name] = true
		case "list", "take":
			out[a.Name] = false
		}
	}
	return out
}

// arrayAggIterator turns the pipeline's joined string form of list and
// values results into arrays of strings.
type arrayAggIterator struct {
	pipeline.Iterator
	columns map[string]bool
}

func (a *arrayAggIterator) Next(ctx context.Context) (*pipeline.Batch, error) {
	batch, err := a.Iterator.Next(ctx)
	if err != nil || batch == nil {
		return batch, err
	}
	for name, sorted := range a.columns {
		col := batch.Columns[name]
		for i := range col {
			if col[i].Type() != event.FieldTypeString {
				continue
			}
			var parts []string
			if s := col[i].AsString(); s != "" {
				parts = strings.Split(s, "|||")
			}
			if sorted {
				sort.Strings(parts)
			}
			elems := make([]event.Value, len(parts))
			for j, p := range parts {
				elems[j] = event.StringValue(p)
			}
			col[i] = event.ArrayValue(elems)
		}
	}
	return batch, nil
}

var bucketUnits = map[string]time.Duration{
	"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour,
}

// fixedTimeBucket recognizes time_bucket over a timestamp column with a
// fixed-length unit, which the pipeline's bin operator computes directly.
func fixedTimeBucket(e ir.Expr) (string, time.Duration, bool) {
	c, ok := e.(*ir.Call)
	if !ok || c.Func.Name != "time_bucket" {
		return "", 0, false
	}
	f, ok := c.Args[0].(*ir.FieldRef)
	if !ok || f.T.Kind != ir.KindTimestamp {
		return "", 0, false
	}
	n, ok1 := c.Args[1].(*ir.Literal).V.(int64)
	unit, ok2 := c.Args[2].(*ir.Literal).V.(string)
	d, ok3 := bucketUnits[unit]
	if !ok1 || !ok2 || !ok3 || n <= 0 {
		return "", 0, false
	}
	return f.Name, time.Duration(n) * d, true
}

func (b *builder) window(x *ir.Window) (pipeline.Iterator, error) {
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	if x.DropNullPartitions && len(x.PartitionBy) > 0 {
		return nil, unsupported("bucket_nullable=false for eventstats and streamstats")
	}
	if x.Frame.Span != nil || x.Frame.ResetBefore != nil || x.Frame.ResetAfter != nil {
		return nil, unsupported("streamstats reset_before, reset_after and time windows")
	}
	trend := 0
	for _, f := range x.Funcs {
		if f.Agg.Func.Kind == ir.WindowFunc {
			trend++
		}
	}
	if trend > 0 {
		if trend != len(x.Funcs) || len(x.PartitionBy) > 0 || x.Frame.Kind != ir.FrameRunning {
			return nil, unsupported("moving averages mixed with other window functions")
		}
		return b.movingAverages(child, x)
	}
	var temps []ir.Assign
	groupBy := make([]string, len(x.PartitionBy))
	for i, p := range x.PartitionBy {
		groupBy[i] = b.fieldOrTemp(p.Expr, &temps)
	}
	it := child
	if len(temps) > 0 {
		if it, err = b.compute(it, temps); err != nil {
			return nil, err
		}
	}
	aggs, post, err := b.aggFuncs(x.Funcs)
	if err != nil {
		return nil, err
	}
	if x.Frame.Kind == ir.FramePartition {
		it = pipeline.NewEventStatsIteratorWithBudget(it, aggs, groupBy, b.batchSize(), b.account("EventStats", pipeline.ReservationEventStats))
	} else {
		it = pipeline.NewStreamStatsIteratorWithDuration(it, aggs, groupBy, x.Frame.Rows, 0, !x.Frame.ExcludeCurrent)
	}
	it = dropTemps(it, temps)
	if len(post) > 0 {
		return b.compute(it, post)
	}
	return it, nil
}

func (b *builder) dedup(x *ir.Dedup) (pipeline.Iterator, error) {
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	progs := make([]*vm.Program, len(x.Keys))
	for i, k := range x.Keys {
		if progs[i], err = b.compile(k); err != nil {
			return nil, err
		}
	}
	return newDedupIterator(child, progs, x.Count, x.KeepNull, x.Consecutive, b.batchSize()), nil
}

func (b *builder) join(x *ir.Join) (pipeline.Iterator, error) {
	left, err := b.node(x.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.node(x.Right)
	if err != nil {
		return nil, err
	}
	j := &joinIterator{left: left, right: right, spec: x, batch: b.batchSize(), acct: b.account("Join", pipeline.ReservationJoin)}
	for _, k := range x.LeftKeys {
		p, err := b.compile(k)
		if err != nil {
			return nil, err
		}
		j.leftKeys = append(j.leftKeys, p)
	}
	for _, k := range x.RightKeys {
		p, err := b.compile(k)
		if err != nil {
			return nil, err
		}
		j.rightKeys = append(j.rightKeys, p)
	}
	if x.Cond != nil {
		if j.cond, err = b.compile(x.Cond); err != nil {
			return nil, err
		}
	}
	return j, nil
}

func (b *builder) extract(x *ir.Extract) (pipeline.Iterator, error) {
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	src, err := b.compile(x.Source)
	if err != nil {
		return nil, err
	}
	switch x.Method {
	case ir.ExtractRegex:
		re, err := regexp.Compile(x.Pattern)
		if err != nil {
			return nil, err
		}
		return newMapIterator(child, b.batchSize(), regexExtractor(src, re, x)), nil
	case ir.ExtractJSON:
		return newMapIterator(child, b.batchSize(), jsonExtractor(src, x)), nil
	}
	return nil, unsupported("extract method %s", x.Method)
}

func regexExtractor(src *vm.Program, re *regexp.Regexp, x *ir.Extract) func(map[string]event.Value) []map[string]event.Value {
	groups := map[string]int{}
	for i, n := range re.SubexpNames() {
		if n != "" {
			groups[n] = i
		}
	}
	var machine vm.VM
	noMatch := event.NullValue()
	if x.NoMatchEmpty {
		noMatch = event.StringValue("")
	}
	return func(row map[string]event.Value) []map[string]event.Value {
		v, err := machine.Execute(src, row)
		text, ok := "", false
		if err == nil && !v.IsNull() {
			text, ok = v.String(), true
			if v.Type() == event.FieldTypeString {
				text = v.AsString()
			}
		}
		if x.MaxMatches > 1 {
			var all [][]string
			if ok {
				all = re.FindAllStringSubmatch(text, x.MaxMatches)
			}
			for _, c := range x.Outputs {
				if len(all) == 0 {
					row[c.Name] = noMatch
					continue
				}
				vals := make([]event.Value, len(all))
				for i, m := range all {
					vals[i] = event.StringValue(m[groups[c.Name]])
				}
				row[c.Name] = event.ArrayValue(vals)
			}
			return []map[string]event.Value{row}
		}
		var m []string
		if ok {
			m = re.FindStringSubmatch(text)
		}
		for _, c := range x.Outputs {
			if m == nil {
				row[c.Name] = noMatch
				continue
			}
			row[c.Name] = event.StringValue(m[groups[c.Name]])
		}
		return []map[string]event.Value{row}
	}
}

func jsonExtractor(src *vm.Program, x *ir.Extract) func(map[string]event.Value) []map[string]event.Value {
	var machine vm.VM
	return func(row map[string]event.Value) []map[string]event.Value {
		v, err := machine.Execute(src, row)
		if err != nil || v.Type() != event.FieldTypeString {
			return []map[string]event.Value{row}
		}
		var obj map[string]any
		dec := json.NewDecoder(strings.NewReader(v.AsString()))
		dec.UseNumber()
		if dec.Decode(&obj) != nil {
			return []map[string]event.Value{row}
		}
		for k, val := range obj {
			row[k] = jsonToValue(val)
		}
		return []map[string]event.Value{row}
	}
}

func jsonToValue(x any) event.Value {
	switch t := x.(type) {
	case nil:
		return event.NullValue()
	case bool:
		return event.BoolValue(t)
	case string:
		return event.StringValue(t)
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return event.IntValue(n)
		}
		f, _ := t.Float64()
		return event.FloatValue(f)
	case []any:
		out := make([]event.Value, len(t))
		for i, e := range t {
			out[i] = jsonToValue(e)
		}
		return event.ArrayValue(out)
	case map[string]any:
		out := make(map[string]event.Value, len(t))
		for k, e := range t {
			out[k] = jsonToValue(e)
		}
		return event.ObjectValue(out)
	}
	return event.StringValue(fmt.Sprint(x))
}

func (b *builder) patterns(x *ir.Patterns) (pipeline.Iterator, error) {
	if x.Method != ir.PatternSimple {
		return nil, unsupported("the brain pattern method")
	}
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	src, err := b.compile(x.Source)
	if err != nil {
		return nil, err
	}
	re, err := regexp.Compile(x.Regex)
	if err != nil {
		return nil, err
	}
	var machine vm.VM
	label := func(row map[string]event.Value) (string, string, bool) {
		v, err := machine.Execute(src, row)
		if err != nil || v.IsNull() {
			return "", "", false
		}
		text := v.String()
		if v.Type() == event.FieldTypeString {
			text = v.AsString()
		}
		return re.ReplaceAllString(text, "<*>"), text, true
	}
	if !x.Aggregate {
		return newMapIterator(child, b.batchSize(), func(row map[string]event.Value) []map[string]event.Value {
			if p, _, ok := label(row); ok {
				row[x.Output] = event.StringValue(p)
			} else {
				row[x.Output] = event.NullValue()
			}
			return []map[string]event.Value{row}
		}), nil
	}
	groupProgs := make([]*vm.Program, len(x.GroupBy))
	groupNames := make([]string, len(x.GroupBy))
	for i, g := range x.GroupBy {
		if groupProgs[i], err = b.compile(g); err != nil {
			return nil, err
		}
		if fr, ok := g.(*ir.FieldRef); ok {
			groupNames[i] = fr.Name
		}
	}
	return newBlockingIterator(child, b.batchSize(), func(rows []map[string]event.Value) []map[string]event.Value {
		type agg struct {
			row     map[string]event.Value
			count   int64
			samples []event.Value
		}
		index := map[string]*agg{}
		var order []string
		for _, r := range rows {
			p, text, ok := label(r)
			if !ok {
				continue
			}
			key := p
			groupVals := make([]event.Value, len(groupProgs))
			for i, gp := range groupProgs {
				gv, _ := machine.Execute(gp, r)
				groupVals[i] = gv
				key += "\x00" + gv.String()
			}
			a, ok := index[key]
			if !ok {
				out := map[string]event.Value{x.Output: event.StringValue(p)}
				for i, n := range groupNames {
					if n != "" {
						out[n] = groupVals[i]
					}
				}
				a = &agg{row: out}
				index[key] = a
				order = append(order, key)
			}
			a.count++
			if len(a.samples) < x.MaxSamples {
				a.samples = append(a.samples, event.StringValue(text))
			}
		}
		out := make([]map[string]event.Value, 0, len(order))
		for _, k := range order {
			a := index[k]
			a.row[ir.PatternCountColumn] = event.IntValue(a.count)
			a.row[ir.PatternSamplesColumn] = event.ArrayValue(a.samples)
			out = append(out, a.row)
		}
		return out
	}), nil
}

package physical

import (
	"regexp"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irplan"
	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// aggFuncs prepares the aggregates of rows.
func (b *builder) aggFuncs(aggs []ir.NamedAgg) ([]pipeline.AggFunc, error) {
	out := make([]pipeline.AggFunc, len(aggs))
	for i, a := range aggs {
		call := a.Agg
		fn, ok := irplan.AggFunc(call.Func.Name)
		if !ok {
			return nil, unsupported("aggregate %s", call.Func.Name)
		}
		f := pipeline.AggFunc{Alias: a.Name, Agg: exec.RowAgg{Func: fn}, Kind: call.Type().Kind}
		args := call.Args
		switch call.Func.Name {
		case "percentile":
			lit, _ := args[1].(*ir.Literal)
			switch p := lit.V.(type) {
			case int64:
				f.Agg.P = float64(p)
			case float64:
				f.Agg.P = p
			}
			args = args[:1]
		case "take":
			lit, _ := args[1].(*ir.Literal)
			n, _ := lit.V.(int64)
			f.Agg.Size = int(n)
			args = args[:1]
		case "earliest", "latest":
			if len(args) > 1 {
				if fr, ok := args[1].(*ir.FieldRef); !ok || fr.Name != ir.TimeColumn {
					return nil, unsupported("%s ordered by a column other than %s", call.Func.Name, ir.TimeColumn)
				}
				args = args[:1]
			}
		}
		if len(args) == 0 {
			f.Agg.Rows = true
		} else {
			e, err := b.compile(args[0])
			if err != nil {
				return nil, err
			}
			f.Arg = e
		}
		if call.Filter != nil {
			cond, err := b.cond(call.Filter)
			if err != nil {
				return nil, err
			}
			f.Cond = cond
			if f.Agg.Rows {
				// A count of the rows where the condition holds counts a
				// value in each of them.
				f.Agg.Rows, f.Arg = false, nil
				if f.Arg, err = b.compile(ir.BoolLit(true)); err != nil {
					return nil, err
				}
			}
		}
		out[i] = f
	}
	return out, nil
}

func (b *builder) aggregate(x *ir.Aggregate) (pipeline.Iterator, error) {
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	var temps []ir.Assign
	groupBy := make([]string, len(x.Groups))
	kinds := make([]ir.Kind, len(x.Groups))
	var renames []ir.Rename
	var nullChecks []ir.Expr
	for i, g := range x.Groups {
		kinds[i] = g.Expr.Type().Kind
		if fr, ok := g.Expr.(*ir.FieldRef); ok && fr.Name == g.Name {
			groupBy[i] = g.Name
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
		cond, err := b.cond(ir.And(nullChecks...))
		if err != nil {
			return nil, err
		}
		it = pipeline.NewFilterIterator(it, cond)
	}
	aggs, err := b.aggFuncs(x.Aggs)
	if err != nil {
		return nil, err
	}
	if it, err = pipeline.NewAggregateIterator(it, aggs, groupBy, kinds); err != nil {
		return nil, err
	}
	if len(renames) > 0 {
		it = newRenameIterator(it, renames)
	}
	return it, nil
}

func (b *builder) window(x *ir.Window) (pipeline.Iterator, error) {
	child, err := b.input(x)
	if err != nil {
		return nil, err
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
	aggs, err := b.aggFuncs(x.Funcs)
	if err != nil {
		return nil, err
	}
	if x.Frame.Kind == ir.FramePartition {
		it, err = pipeline.NewEventStatsIterator(it, aggs, groupBy, b.batchSize())
	} else {
		it, err = pipeline.NewStreamStatsIterator(it, aggs, groupBy, x.Frame.Rows, !x.Frame.ExcludeCurrent)
	}
	if err != nil {
		return nil, err
	}
	return dropTemps(it, temps), nil
}

func (b *builder) dedup(x *ir.Dedup) (pipeline.Iterator, error) {
	child, err := b.input(x)
	if err != nil {
		return nil, err
	}
	keys := make([]*pipeline.Expr, len(x.Keys))
	for i, k := range x.Keys {
		if keys[i], err = b.compile(k); err != nil {
			return nil, err
		}
	}
	return newDedupIterator(child, keys, x.Count, x.KeepNull, x.Consecutive, b.batchSize()), nil
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
	j := &joinIterator{left: left, right: right, spec: x, batch: b.batchSize()}
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
		if j.cond, err = b.cond(x.Cond); err != nil {
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
	re, err := regexp.Compile(x.Pattern)
	if err != nil {
		return nil, err
	}
	return newMapIterator(child, b.batchSize(), regexExtractor(src, re, x)), nil
}

func regexExtractor(src *pipeline.Expr, re *regexp.Regexp, x *ir.Extract) func(map[string]event.Value) []map[string]event.Value {
	groups := map[string]int{}
	for i, n := range re.SubexpNames() {
		if n != "" {
			groups[n] = i
		}
	}
	noMatch := event.NullValue()
	if x.NoMatchEmpty {
		noMatch = event.StringValue("")
	}
	return func(row map[string]event.Value) []map[string]event.Value {
		text, ok := textOf(src, row)
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

func (b *builder) patterns(x *ir.Patterns) (pipeline.Iterator, error) {
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
	label := func(row map[string]event.Value) (string, string, bool) {
		text, ok := textOf(src, row)
		if !ok {
			return "", "", false
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
	groupExprs := make([]*pipeline.Expr, len(x.GroupBy))
	groupNames := make([]string, len(x.GroupBy))
	for i, g := range x.GroupBy {
		if groupExprs[i], err = b.compile(g); err != nil {
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
			key := []byte(p)
			groupVals := make([]event.Value, len(groupExprs))
			for i, ge := range groupExprs {
				key = exec.AppendKey(key, ge.Exec(r))
				groupVals[i], _ = ge.Value(r)
			}
			a, ok := index[string(key)]
			if !ok {
				out := map[string]event.Value{x.Output: event.StringValue(p)}
				for i, n := range groupNames {
					if n != "" {
						out[n] = groupVals[i]
					}
				}
				a = &agg{row: out}
				index[string(key)] = a
				order = append(order, string(key))
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

// textOf returns the text of the value of src for row, false for a null or
// a field the row does not hold.
func textOf(src *pipeline.Expr, row map[string]event.Value) (string, bool) {
	v, ok := src.Value(row)
	if !ok || v.IsNull() {
		return "", false
	}
	if v.Type() == event.FieldTypeString {
		return v.AsString(), true
	}
	return v.String(), true
}

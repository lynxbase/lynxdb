package physical

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// mapIterator streams rows through f, which returns zero or more rows per
// input row.
type mapIterator struct {
	child pipeline.Iterator
	batch int
	f     func(map[string]event.Value) []map[string]event.Value
}

func newMapIterator(child pipeline.Iterator, batch int, f func(map[string]event.Value) []map[string]event.Value) *mapIterator {
	return &mapIterator{child: child, batch: batch, f: f}
}

func (m *mapIterator) Init(ctx context.Context) error { return m.child.Init(ctx) }
func (m *mapIterator) Close() error                   { return m.child.Close() }
func (m *mapIterator) Schema() []pipeline.FieldInfo   { return nil }

func (m *mapIterator) Next(ctx context.Context) (*pipeline.Batch, error) {
	for {
		in, err := m.child.Next(ctx)
		if err != nil || in == nil {
			return nil, err
		}
		var out []map[string]event.Value
		for i := 0; i < in.Len; i++ {
			out = append(out, m.f(in.Row(i))...)
		}
		if len(out) > 0 {
			return pipeline.BatchFromRows(out), nil
		}
	}
}

// blockingIterator collects its whole input and emits f's result.
type blockingIterator struct {
	child pipeline.Iterator
	batch int
	f     func([]map[string]event.Value) []map[string]event.Value
	out   []map[string]event.Value
	off   int
	done  bool
}

func newBlockingIterator(child pipeline.Iterator, batch int, f func([]map[string]event.Value) []map[string]event.Value) *blockingIterator {
	return &blockingIterator{child: child, batch: batch, f: f}
}

func (b *blockingIterator) Init(ctx context.Context) error { return b.child.Init(ctx) }
func (b *blockingIterator) Close() error                   { return b.child.Close() }
func (b *blockingIterator) Schema() []pipeline.FieldInfo   { return nil }

func (b *blockingIterator) Next(ctx context.Context) (*pipeline.Batch, error) {
	if !b.done {
		var rows []map[string]event.Value
		for {
			in, err := b.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			if in == nil {
				break
			}
			for i := 0; i < in.Len; i++ {
				rows = append(rows, in.Row(i))
			}
		}
		b.out = b.f(rows)
		b.done = true
	}
	if b.off >= len(b.out) {
		return nil, nil
	}
	end := min(b.off+b.batch, len(b.out))
	batch := pipeline.BatchFromRows(b.out[b.off:end])
	b.off = end
	return batch, nil
}

func sortByTimeDesc(rows []map[string]event.Value) []map[string]event.Value {
	sort.SliceStable(rows, func(i, j int) bool {
		return vm.CompareValues(rows[i][ir.TimeColumn], rows[j][ir.TimeColumn]) > 0
	})
	return rows
}

func newRenameIterator(child pipeline.Iterator, renames []ir.Rename) pipeline.Iterator {
	return newMapIterator(child, 0, func(row map[string]event.Value) []map[string]event.Value {
		for _, rn := range renames {
			if !ir.IsNamePattern(rn.From) {
				if v, ok := row[rn.From]; ok {
					delete(row, rn.From)
					row[rn.To] = v
				}
				continue
			}
			moved := map[string]event.Value{}
			for k, v := range row {
				if to, ok := ir.RenameTarget(rn, k); ok {
					moved[to] = v
					delete(row, k)
				}
			}
			for k, v := range moved {
				row[k] = v
			}
		}
		return []map[string]event.Value{row}
	})
}

// zipIterator pairs rows of two inputs by position.
type zipIterator struct {
	left, right pipeline.Iterator
	override    bool
	batch       int
	rightRows   []map[string]event.Value
	loaded      bool
	pos         int
	leftDone    bool
}

func newZipIterator(left, right pipeline.Iterator, override bool, batch int) *zipIterator {
	return &zipIterator{left: left, right: right, override: override, batch: batch}
}

func (z *zipIterator) Init(ctx context.Context) error {
	if err := z.left.Init(ctx); err != nil {
		return err
	}
	return z.right.Init(ctx)
}

func (z *zipIterator) Close() error {
	err := z.left.Close()
	if e := z.right.Close(); err == nil {
		err = e
	}
	return err
}

func (z *zipIterator) Schema() []pipeline.FieldInfo { return nil }

func (z *zipIterator) Next(ctx context.Context) (*pipeline.Batch, error) {
	if !z.loaded {
		rows, err := pipeline.CollectAll(ctx, z.right)
		if err != nil {
			return nil, err
		}
		z.rightRows, z.loaded = rows, true
	}
	if !z.leftDone {
		in, err := z.left.Next(ctx)
		if err != nil {
			return nil, err
		}
		if in != nil {
			out := make([]map[string]event.Value, in.Len)
			for i := 0; i < in.Len; i++ {
				row := in.Row(i)
				if z.pos < len(z.rightRows) {
					for k, v := range z.rightRows[z.pos] {
						if _, exists := row[k]; !exists || z.override {
							row[k] = v
						}
					}
				}
				z.pos++
				out[i] = row
			}
			return pipeline.BatchFromRows(out), nil
		}
		z.leftDone = true
	}
	if z.pos >= len(z.rightRows) {
		return nil, nil
	}
	end := min(z.pos+z.batch, len(z.rightRows))
	b := pipeline.BatchFromRows(z.rightRows[z.pos:end])
	z.pos = end
	return b, nil
}

// keyOf evaluates programs into a grouping key; null reports a NULL part.
func keyOf(machine *vm.VM, progs []*vm.Program, row map[string]event.Value) (string, bool) {
	var b strings.Builder
	for _, p := range progs {
		v, err := machine.Execute(p, row)
		if err != nil || v.IsNull() {
			return "", true
		}
		b.WriteString(v.Type().String())
		b.WriteByte(':')
		b.WriteString(v.String())
		b.WriteByte(0)
	}
	return b.String(), false
}

func newDedupIterator(child pipeline.Iterator, keys []*vm.Program, count int64, keepNull, consecutive bool, batch int) pipeline.Iterator {
	var machine vm.VM
	seen := map[string]int64{}
	last := ""
	run := int64(0)
	return newMapIterator(child, batch, func(row map[string]event.Value) []map[string]event.Value {
		k, null := keyOf(&machine, keys, row)
		if null {
			if keepNull {
				return []map[string]event.Value{row}
			}
			return nil
		}
		if consecutive {
			if k == last {
				run++
			} else {
				last, run = k, 1
			}
			if run > count {
				return nil
			}
			return []map[string]event.Value{row}
		}
		seen[k]++
		if seen[k] > count {
			return nil
		}
		return []map[string]event.Value{row}
	})
}

// joinIterator is a hash join: it builds a table of the right input keyed
// by the right key expressions and probes it with each left row.
type joinIterator struct {
	left, right         pipeline.Iterator
	spec                *ir.Join
	leftKeys, rightKeys []*vm.Program
	cond                *vm.Program
	batch               int

	machine  vm.VM
	table    map[string][]int
	rows     []map[string]event.Value
	matched  []bool
	built    bool
	tailDone bool
}

func (j *joinIterator) Init(ctx context.Context) error {
	if err := j.left.Init(ctx); err != nil {
		return err
	}
	return j.right.Init(ctx)
}

func (j *joinIterator) Close() error {
	err := j.left.Close()
	if e := j.right.Close(); err == nil {
		err = e
	}
	return err
}

func (j *joinIterator) Schema() []pipeline.FieldInfo { return nil }

func (j *joinIterator) build(ctx context.Context) error {
	rows, err := pipeline.CollectAll(ctx, j.right)
	if err != nil {
		return err
	}
	j.rows = rows
	j.matched = make([]bool, len(rows))
	j.table = make(map[string][]int, len(rows))
	for i, r := range rows {
		k, null := keyOf(&j.machine, j.rightKeys, r)
		if null {
			continue
		}
		j.table[k] = append(j.table[k], i)
	}
	j.built = true
	return nil
}

func (j *joinIterator) combine(l, r map[string]event.Value) map[string]event.Value {
	out := make(map[string]event.Value, len(l)+len(r))
	for k, v := range l {
		out[k] = v
	}
	for k, v := range r {
		if _, dup := out[k]; dup && j.spec.Collision == ir.CollisionLeft {
			continue
		}
		out[k] = v
	}
	if q := j.spec.LeftQualifier; q != "" {
		for k, v := range l {
			out[q+"."+k] = v
		}
	}
	if q := j.spec.RightQualifier; q != "" {
		for k, v := range r {
			out[q+"."+k] = v
		}
	}
	return out
}

func (j *joinIterator) condTrue(row map[string]event.Value) bool {
	if j.cond == nil {
		return true
	}
	v, err := j.machine.Execute(j.cond, row)
	return err == nil && v.Type() == event.FieldTypeBool && v.AsBool()
}

func (j *joinIterator) candidates(l map[string]event.Value) []int {
	if len(j.leftKeys) == 0 {
		all := make([]int, len(j.rows))
		for i := range all {
			all[i] = i
		}
		return all
	}
	k, null := keyOf(&j.machine, j.leftKeys, l)
	if null {
		return nil
	}
	return j.table[k]
}

func (j *joinIterator) Next(ctx context.Context) (*pipeline.Batch, error) {
	if !j.built {
		if err := j.build(ctx); err != nil {
			return nil, err
		}
	}
	kind := j.spec.Kind
	for {
		in, err := j.left.Next(ctx)
		if err != nil {
			return nil, err
		}
		if in == nil {
			break
		}
		var out []map[string]event.Value
		for i := 0; i < in.Len; i++ {
			l := in.Row(i)
			hits := int64(0)
			for _, ri := range j.candidates(l) {
				combined := j.combine(l, j.rows[ri])
				if !j.condTrue(combined) {
					continue
				}
				j.matched[ri] = true
				hits++
				switch kind {
				case ir.JoinSemi, ir.JoinAnti:
				default:
					out = append(out, combined)
				}
				if kind == ir.JoinSemi || kind == ir.JoinAnti || (j.spec.MaxMatches > 0 && hits >= j.spec.MaxMatches) {
					break
				}
			}
			switch {
			case kind == ir.JoinSemi && hits > 0, kind == ir.JoinAnti && hits == 0:
				out = append(out, l)
			case hits == 0 && (kind == ir.JoinLeft || kind == ir.JoinFull):
				out = append(out, j.combine(l, nil))
			}
		}
		if len(out) > 0 {
			return pipeline.BatchFromRows(out), nil
		}
	}
	if !j.tailDone && (kind == ir.JoinRight || kind == ir.JoinFull) {
		j.tailDone = true
		var out []map[string]event.Value
		for i, r := range j.rows {
			if !j.matched[i] {
				out = append(out, j.combine(nil, r))
			}
		}
		if len(out) > 0 {
			return pipeline.BatchFromRows(out), nil
		}
	}
	return nil, nil
}

func unnestRow(x *ir.Unnest) func(map[string]event.Value) []map[string]event.Value {
	target := x.Field
	if x.As != "" {
		target = x.As
	}
	return func(row map[string]event.Value) []map[string]event.Value {
		v := row[x.Field]
		if v.Type() != event.FieldTypeArray {
			if x.As != "" && x.As != x.Field {
				row[target] = v
			}
			return []map[string]event.Value{row}
		}
		elems := v.AsArray()
		if len(elems) == 0 {
			row[target] = event.NullValue()
			return []map[string]event.Value{row}
		}
		if x.Limit > 0 && len(elems) > x.Limit {
			elems = elems[:x.Limit]
		}
		out := make([]map[string]event.Value, len(elems))
		for i, e := range elems {
			r := make(map[string]event.Value, len(row))
			for k, val := range row {
				r[k] = val
			}
			r[target] = e
			out[i] = r
		}
		return out
	}
}

func flattenRow(x *ir.FlattenObject) func(map[string]event.Value) []map[string]event.Value {
	return func(row map[string]event.Value) []map[string]event.Value {
		v, ok := row[x.Field]
		if !ok || v.Type() != event.FieldTypeObject {
			return []map[string]event.Value{row}
		}
		obj := v.AsObject()
		delete(row, x.Field)
		if len(x.Aliases) > 0 {
			keys := make([]string, 0, len(obj))
			for k := range obj {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for i, k := range keys {
				if i < len(x.Aliases) {
					row[x.Aliases[i]] = obj[k]
				}
			}
			return []map[string]event.Value{row}
		}
		var walk func(prefix string, o map[string]event.Value)
		walk = func(prefix string, o map[string]event.Value) {
			for k, val := range o {
				name := k
				if prefix != "" {
					name = prefix + "." + k
				}
				if val.Type() == event.FieldTypeObject {
					walk(name, val.AsObject())
					continue
				}
				row[name] = val
			}
		}
		walk("", obj)
		return []map[string]event.Value{row}
	}
}

func columnsOf(rows []map[string]event.Value) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out
}

func transposeRows(x *ir.Transpose) func([]map[string]event.Value) []map[string]event.Value {
	return func(rows []map[string]event.Value) []map[string]event.Value {
		if len(rows) > x.MaxRows {
			rows = rows[:x.MaxRows]
		}
		cols := columnsOf(rows)
		out := make([]map[string]event.Value, 0, len(cols))
		for _, c := range cols {
			r := map[string]event.Value{x.HeaderColumn: event.StringValue(c)}
			for i, in := range rows {
				v, ok := in[c]
				if !ok {
					v = event.NullValue()
				}
				r[fmt.Sprintf("row %d", i+1)] = v
			}
			out = append(out, r)
		}
		return out
	}
}

func numeric(v event.Value) (float64, bool) {
	switch v.Type() {
	case event.FieldTypeInt:
		return float64(v.AsInt()), true
	case event.FieldTypeFloat:
		return v.AsFloat(), true
	}
	return 0, false
}

func totalsRows(x *ir.Totals) func([]map[string]event.Value) []map[string]event.Value {
	include := func(name string) bool {
		if len(x.Fields) == 0 {
			return !strings.HasPrefix(name, "_")
		}
		for _, f := range x.Fields {
			if ir.MatchName(f, name) {
				return true
			}
		}
		return false
	}
	return func(rows []map[string]event.Value) []map[string]event.Value {
		colSums := map[string]float64{}
		colInt := map[string]bool{}
		for _, r := range rows {
			var rowSum float64
			has := false
			for k, v := range r {
				if !include(k) || k == x.RowColumn {
					continue
				}
				f, ok := numeric(v)
				if !ok {
					continue
				}
				rowSum += f
				has = true
				if _, seen := colInt[k]; !seen {
					colInt[k] = true
				}
				colInt[k] = colInt[k] && v.Type() == event.FieldTypeInt
				colSums[k] += f
			}
			if x.RowTotals && has {
				r[x.RowColumn] = numberValue(rowSum)
			}
		}
		if x.ColumnTotals {
			total := map[string]event.Value{}
			for k, s := range colSums {
				if colInt[k] {
					total[k] = event.IntValue(int64(s))
				} else {
					total[k] = event.FloatValue(s)
				}
			}
			if x.LabelColumn != "" {
				total[x.LabelColumn] = event.StringValue(x.LabelValue)
			}
			rows = append(rows, total)
		}
		return rows
	}
}

func numberValue(f float64) event.Value {
	if f == float64(int64(f)) {
		return event.IntValue(int64(f))
	}
	return event.FloatValue(f)
}

func combineRows(field string) func([]map[string]event.Value) []map[string]event.Value {
	return func(rows []map[string]event.Value) []map[string]event.Value {
		index := map[string]int{}
		var out []map[string]event.Value
		var vals [][]event.Value
		for _, r := range rows {
			keys := make([]string, 0, len(r))
			for k := range r {
				if k != field {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			var b strings.Builder
			for _, k := range keys {
				b.WriteString(k + "=" + r[k].Type().String() + ":" + r[k].String() + "\x00")
			}
			key := b.String()
			i, ok := index[key]
			if !ok {
				i = len(out)
				index[key] = i
				out = append(out, r)
				vals = append(vals, nil)
			}
			v := r[field]
			switch v.Type() {
			case event.FieldTypeNull:
			case event.FieldTypeArray:
				vals[i] = append(vals[i], v.AsArray()...)
			default:
				vals[i] = append(vals[i], v)
			}
		}
		for i, r := range out {
			r[field] = event.ArrayValue(vals[i])
		}
		return out
	}
}

// movingAverages evaluates trendline moving averages: NULL until the frame
// holds its full row count.
func (b *builder) movingAverages(child pipeline.Iterator, x *ir.Window) (pipeline.Iterator, error) {
	type state struct {
		prog *vm.Program
		ring []float64
		name string
		wma  bool
	}
	states := make([]*state, len(x.Funcs))
	for i, f := range x.Funcs {
		p, err := b.compile(f.Agg.Args[0])
		if err != nil {
			return nil, err
		}
		states[i] = &state{prog: p, name: f.Name, wma: f.Agg.Func.Name == "weighted_moving_avg"}
	}
	n := x.Frame.Rows
	var machine vm.VM
	return newMapIterator(child, b.batchSize(), func(row map[string]event.Value) []map[string]event.Value {
		for _, s := range states {
			v, err := machine.Execute(s.prog, row)
			f, ok := numeric(v)
			if err != nil || !ok {
				row[s.name] = event.NullValue()
				continue
			}
			s.ring = append(s.ring, f)
			if len(s.ring) > n {
				s.ring = s.ring[1:]
			}
			if len(s.ring) < n {
				row[s.name] = event.NullValue()
				continue
			}
			var sum, weights float64
			for i, val := range s.ring {
				w := 1.0
				if s.wma {
					w = float64(i + 1)
				}
				sum += w * val
				weights += w
			}
			row[s.name] = event.FloatValue(sum / weights)
		}
		return []map[string]event.Value{row}
	}), nil
}

// NodeStats records an operator's runtime output.
type NodeStats struct {
	Rows    int64
	Batches int64
	Wall    time.Duration
}

type statsIterator struct {
	child pipeline.Iterator
	stats *NodeStats
}

func (s *statsIterator) Init(ctx context.Context) error { return s.child.Init(ctx) }
func (s *statsIterator) Close() error                   { return s.child.Close() }
func (s *statsIterator) Schema() []pipeline.FieldInfo   { return s.child.Schema() }

func (s *statsIterator) Next(ctx context.Context) (*pipeline.Batch, error) {
	start := time.Now()
	b, err := s.child.Next(ctx)
	s.stats.Wall += time.Since(start)
	if b != nil {
		s.stats.Rows += int64(b.Len)
		s.stats.Batches++
	}
	return b, err
}

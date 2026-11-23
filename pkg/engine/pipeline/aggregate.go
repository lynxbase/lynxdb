package pipeline

import (
	"context"
	"math"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// AggFunc is one aggregate of rows, which the columnar executor's states
// compute: Agg of the values of Arg in the rows where Cond, when set, is
// true. Kind is the IR kind of its value.
type AggFunc struct {
	Alias string
	Agg   exec.RowAgg
	Arg   *Expr
	Cond  *Expr
	Kind  ir.Kind
}

// aggregator feeds rows to the groups of an exec.Accumulator. A group is
// the rows whose values in the key columns form one key of exec.AppendKey.
type aggregator struct {
	aggs   []AggFunc
	keys   []string
	kinds  []ir.Kind
	acc    *exec.Accumulator
	args   []exec.Value
	vals   []exec.Value
	fields fields
	key    []byte
	groups map[string]int
	// order holds the key of every group in the order the groups came.
	order []string
}

func newAggregator(aggs []AggFunc, keys []string, kinds []ir.Kind) (*aggregator, error) {
	specs := make([]exec.RowAgg, len(aggs))
	for i, a := range aggs {
		specs[i] = a.Agg
	}
	acc, err := exec.NewAccumulator(specs)
	if err != nil {
		return nil, err
	}

	return &aggregator{aggs: aggs, keys: keys, kinds: kinds, acc: acc, args: make([]exec.Value, len(aggs)), groups: map[string]int{}}, nil
}

// group returns the number of the group of row, which it adds when it is
// new.
func (a *aggregator) group(row map[string]event.Value) int {
	a.fields.reset(row)
	a.key = a.key[:0]
	for _, k := range a.keys {
		a.key = exec.AppendKey(a.key, a.fields.Field(k))
	}
	g, ok := a.groups[string(a.key)]
	if !ok {
		g = len(a.order)
		a.groups[string(a.key)] = g
		a.order = append(a.order, string(a.key))
	}

	return g
}

// add adds row to group g, or for a reset group, the rows of window.
func (a *aggregator) add(g int, row map[string]event.Value) {
	for i, f := range a.aggs {
		switch {
		case f.Agg.Rows, f.Cond != nil && !f.Cond.True(row):
			a.args[i] = exec.Value{}
		default:
			a.args[i] = f.Arg.Exec(row)
		}
	}
	at := int64(math.MinInt64)
	if t, ok := row[ir.TimeColumn]; ok && t.Type() == event.FieldTypeTimestamp {
		at = t.AsTimestamp().UnixNano()
	}
	a.acc.Add(g, a.args, at)
}

// set gives row the values of the aggregates of group g.
func (a *aggregator) set(g int, row map[string]event.Value) {
	a.vals = a.acc.Result(g, a.vals[:0])
	for i, f := range a.aggs {
		row[f.Alias] = RowValue(a.vals[i], f.Kind)
	}
}

// keyRow returns a row of the values of the keys of group g.
func (a *aggregator) keyRow(g int) map[string]event.Value {
	row := make(map[string]event.Value, len(a.keys)+len(a.aggs))
	key := []byte(a.order[g])
	for i, k := range a.keys {
		var v exec.Value
		v, key = exec.KeyValue(key)
		row[k] = RowValue(v, a.kinds[i])
	}

	return row
}

// AggregateIterator groups its input by the values of key columns and
// gives one row per group: the keys and the aggregates.
type AggregateIterator struct {
	child   Iterator
	agg     *aggregator
	err     error
	emitted bool
}

// NewAggregateIterator returns the aggregation of the rows of child by the
// columns of groupBy, whose IR kinds kinds gives.
func NewAggregateIterator(child Iterator, aggs []AggFunc, groupBy []string, kinds []ir.Kind) (*AggregateIterator, error) {
	agg, err := newAggregator(aggs, groupBy, kinds)
	if err != nil {
		return nil, err
	}

	return &AggregateIterator{child: child, agg: agg}, nil
}

func (a *AggregateIterator) Init(ctx context.Context) error { return a.child.Init(ctx) }

func (a *AggregateIterator) Close() error {
	a.agg.acc.Release()

	return a.child.Close()
}

func (a *AggregateIterator) Next(ctx context.Context) (*Batch, error) {
	if a.emitted {
		return nil, nil
	}
	row := map[string]event.Value{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := a.child.Next(ctx)
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			batch.RowInto(i, row)
			a.agg.add(a.agg.group(row), row)
		}
	}
	a.emitted = true
	groups := len(a.agg.order)
	if groups == 0 && len(a.agg.keys) == 0 {
		// Without keys there is one group, also of no rows.
		a.agg.order, groups = append(a.agg.order, ""), 1
	}
	out := NewBatch(groups)
	for g := 0; g < groups; g++ {
		r := a.agg.keyRow(g)
		a.agg.set(g, r)
		out.AddRow(r)
	}

	return out, nil
}

// EventStatsIterator gives every row the aggregates of the rows of its
// group.
type EventStatsIterator struct {
	child     Iterator
	agg       *aggregator
	rows      []map[string]event.Value
	group     []int
	batchSize int
	offset    int
	emitted   bool
}

// NewEventStatsIterator returns the operator that gives each row of child
// the aggregates of the rows that share its values of groupBy.
func NewEventStatsIterator(child Iterator, aggs []AggFunc, groupBy []string, batchSize int) (*EventStatsIterator, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	agg, err := newAggregator(aggs, groupBy, nil)
	if err != nil {
		return nil, err
	}

	return &EventStatsIterator{child: child, agg: agg, batchSize: batchSize}, nil
}

func (e *EventStatsIterator) Init(ctx context.Context) error { return e.child.Init(ctx) }

func (e *EventStatsIterator) Close() error {
	e.agg.acc.Release()

	return e.child.Close()
}

func (e *EventStatsIterator) Next(ctx context.Context) (*Batch, error) {
	if !e.emitted {
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			batch, err := e.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			if batch == nil {
				break
			}
			for i := 0; i < batch.Len; i++ {
				row := batch.Row(i)
				g := e.agg.group(row)
				e.agg.add(g, row)
				e.rows, e.group = append(e.rows, row), append(e.group, g)
			}
		}
		e.emitted = true
	}
	if e.offset >= len(e.rows) {
		return nil, nil
	}
	end := min(e.offset+e.batchSize, len(e.rows))
	for i := e.offset; i < end; i++ {
		e.agg.set(e.group[i], e.rows[i])
	}
	batch := BatchFromRows(e.rows[e.offset:end])
	e.offset = end

	return batch, nil
}

// StreamStatsIterator gives every row the aggregates of the rows of its
// group that came before it, and of itself when current is set: all of
// them, or the last window of them.
type StreamStatsIterator struct {
	child   Iterator
	agg     *aggregator
	window  int
	current bool
	// recent holds, for a window, the last rows of every group.
	recent map[int][]map[string]event.Value
}

// NewStreamStatsIterator returns the operator of running aggregates over
// the rows of child by the columns of groupBy, over a window of the last
// rows of a group when window is positive.
func NewStreamStatsIterator(child Iterator, aggs []AggFunc, groupBy []string, window int, current bool) (*StreamStatsIterator, error) {
	agg, err := newAggregator(aggs, groupBy, nil)
	if err != nil {
		return nil, err
	}

	return &StreamStatsIterator{child: child, agg: agg, window: window, current: current, recent: map[int][]map[string]event.Value{}}, nil
}

func (s *StreamStatsIterator) Init(ctx context.Context) error { return s.child.Init(ctx) }

func (s *StreamStatsIterator) Close() error {
	s.agg.acc.Release()

	return s.child.Close()
}

func (s *StreamStatsIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := s.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}
	rows := make([]map[string]event.Value, batch.Len)
	for i := range rows {
		row := batch.Row(i)
		g := s.agg.group(row)
		if s.window <= 0 {
			if s.current {
				s.agg.add(g, row)
			}
			s.agg.set(g, row)
			if !s.current {
				s.agg.add(g, row)
			}
			rows[i] = row

			continue
		}
		// A window of rows is aggregated anew for every row.
		recent := s.recent[g]
		if s.current {
			if recent = append(recent, row); len(recent) > s.window {
				recent = recent[1:]
			}
		}
		s.agg.acc.Reset(g)
		for _, r := range recent {
			s.agg.add(g, r)
		}
		s.agg.set(g, row)
		if !s.current {
			if recent = append(recent, row); len(recent) > s.window {
				recent = recent[1:]
			}
		}
		s.recent[g] = recent
		rows[i] = row
	}

	return BatchFromRows(rows), nil
}

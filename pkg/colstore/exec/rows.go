package exec

import (
	"slices"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// rowScratch is the memory a worker keeps between queries for plans that
// return rows.
type rowScratch struct {
	batch []colVec
	cols  [kindSlots + 1]*vec.Column
}

func (s *rowScratch) held() int {
	n := 0
	for i := range s.batch {
		n += s.batch[i].held()
	}

	return n
}

// project computes the plan's columns for the n selected rows of the open
// block and hands them to the worker's sink.
func (w *worker) project(n int, idx []uint32) error {
	s := &w.row
	s.batch = fit(s.batch, len(w.q.rowExprs))
	for i, e := range w.q.rowExprs {
		if err := w.gather(e, n, idx, &s.batch[i]); err != nil {
			return err
		}
	}
	if w.x.emit != nil {
		return w.x.emit(s.batch, n)
	}
	if w.q.byTime < 0 && w.x.condition != "" && n < w.rows {
		// All selected times are still present here, before the heap
		// discards rows outside its bound.
		c := &s.batch[w.q.timeCol]
		if c.kind == vec.KindInt {
			if last := slices.Max(c.ints[:n]); last < w.b.MaxTime {
				w.p.Misses(w.x.condition).SetLatest(w.b.Index(), last)
			}
		}
	}
	w.sink.add(s.batch)

	return nil
}

// gather stores the value of e for each selected row in dst.
func (w *worker) gather(e *sexpr, n int, idx []uint32, dst *colVec) error {
	if e.op == opPlace {
		places := dst.bulkInts(n)
		for j := range places {
			r := j
			if idx != nil {
				r = int(idx[j])
			}
			places[j] = w.place | int64(r)
		}

		return nil
	}
	dst.reset()
	if e.op == OpLiteral {
		for i := 0; i < n; i++ {
			dst.appendValue(e.lit)
		}

		return nil
	}
	if e.op != OpColumn && !e.ints {
		o, err := w.operand(e, n, idx)
		if err != nil {
			return err
		}
		for j := 0; j < n; j++ {
			r := j
			if idx != nil && !o.dense {
				r = int(idx[j])
			}
			dst.appendValue(o.at(r))
		}

		return nil
	}
	if e.op != OpColumn {
		vals, valid, err := w.evalInt(e, n, idx, 0)
		if err != nil {
			return err
		}
		copy(dst.bulkInts(n), vals)
		for i := range valid {
			if valid[i] == 0 {
				dst.setNull(i)
			}
		}

		return nil
	}
	cols := &w.row.cols
	for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
		cols[kind] = nil
		if e.kind != KindAny && e.kind != kind {
			continue
		}
		c, err := w.column(e.path, kind)
		if err != nil {
			return err
		}
		cols[kind] = c
	}
	for j := 0; j < n; j++ {
		r := j
		if idx != nil {
			r = int(idx[j])
		}
		dst.appendValue(rowValue(cols, r))
	}

	return nil
}

// rowValue returns the value that the columns of one path hold in row r,
// the lowest kind for a row that holds several.
func rowValue(cols *[kindSlots + 1]*vec.Column, r int) Value {
	for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
		c := cols[kind]
		if c == nil || !c.IsValid(r) {
			continue
		}
		switch kind {
		case vec.KindNull:
			return Value{Kind: kind}
		case vec.KindBool:
			return Value{Kind: kind, Bool: c.Bool(r)}
		case vec.KindInt:
			return Value{Kind: kind, Int: c.Int(r)}
		case vec.KindFloat:
			return Value{Kind: kind, Float: c.Float(r)}
		}

		return Value{Kind: kind, Bytes: c.Bytes(r)}
	}

	return Value{}
}

package exec

import (
	"cmp"
	"slices"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// placed is a row of the result and the place of the table row whose value
// it shows.
type placed struct {
	loc int64
	row int32
}

// resolver is what one goroutine keeps while it reads located rows.
type resolver struct {
	b   part.Block
	col vec.Column
	// bufs hold the bytes of the values read, in chunks that stay where
	// they are; cur is the chunk being filled.
	bufs [][]byte
	cur  int
	// cells and spans are what the resolver read of whole rows.
	cells []cell
	spans []fieldSpan
	// rows holds the rows of the picks of one block.
	rows []uint32
}

// decode reads the column values at the rows that survived selection.
func (r *resolver) decode(ref *part.ColumnRef, picks []placed) error {
	r.rows = r.rows[:0]
	for _, pk := range picks {
		r.rows = append(r.rows, uint32(pk.loc&0xFFFF))
	}

	return r.b.DecodeRows(ref, r.rows, &r.col)
}

// keep copies v into memory that stays where it is.
func (r *resolver) keep(v []byte) []byte {
	for ; r.cur < len(r.bufs); r.cur++ {
		if b := r.bufs[r.cur]; cap(b)-len(b) >= len(v) {
			b = append(b, v...)
			r.bufs[r.cur] = b

			return b[len(b)-len(v) : len(b) : len(b)]
		}
	}
	r.bufs = append(r.bufs, append(make([]byte, 0, max(64<<10, len(v))), v...))

	return r.bufs[r.cur][:len(v):len(v)]
}

// resolveScratch is the memory that reading located rows keeps between
// queries.
type resolveScratch struct {
	rs     []resolver
	picks  []placed
	starts []int
	vals   []Value
	spans  []fieldSpan
	cells  []cell
}

var resolvePool mem.Pool[resolveScratch]

// read stores in vals the values that column e holds in the rows of picks,
// all of which are in one block.
func (r *resolver) read(parts []*colstore.Handle, q *query, e *sexpr, picks []placed, vals []Value) error {
	block := picks[0].loc >> 16
	p := parts[block>>28].Part
	if err := p.OpenBlock(int(block&(1<<28-1)), &r.b); err != nil {
		return err
	}
	pathID, ok := p.PathID(q.paths[e.path])
	// A row that holds values of several kinds gives the lowest.
	for kind := vec.KindVariant; ok && kind >= vec.KindNull; kind-- {
		ref, has := r.b.Column(pathID, kind)
		// A field read whatever its kind also gives the nulls it holds.
		if wanted := e.kinds&kindBit(kind) != 0 || (kind == vec.KindNull && e.kind == KindAny); !wanted || !has || ref.Count == 0 {
			continue
		}
		var err error
		if kind == vec.KindNull {
			err = r.b.DecodePresence(ref, &r.col)
		} else {
			err = r.decode(ref, picks)
		}
		if err != nil {
			return err
		}
		for _, pk := range picks {
			row := int(pk.loc & 0xFFFF)
			if !r.col.IsValid(row) {
				continue
			}
			v := Value{Kind: kind}
			switch kind {
			case vec.KindNull:
			case vec.KindBool:
				v.Bool = r.col.Bool(row)
			case vec.KindInt:
				v.Int = r.col.Int(row)
			case vec.KindFloat:
				v.Float = r.col.Float(row)
			default:
				v.Bytes = r.keep(r.col.Bytes(row))
			}
			vals[pk.row] = v
		}
	}

	return nil
}

// resolve replaces, in the n rows of cols, the places that picks left in
// the columns located names by the values the rows at those places hold. It
// opens each block that holds such a row once per column, on every worker,
// so its cost follows the rows it is given.
func resolve(snap *colstore.Snapshot, q *query, located []*sexpr, cols []colVec, n, workers int) error {
	if located == nil || n == 0 {
		return nil
	}
	parts := snap.Parts()
	s := resolvePool.Get()
	defer resolvePool.Put(s)
	s.rs, s.vals = fit(s.rs, max(workers, 1)), fit(s.vals, n)
	rs, picks, starts, vals := s.rs, s.picks, s.starts, s.vals
	defer func() {
		// The values point into the buffers, which serve the next query.
		clear(vals)
		s.picks, s.starts = picks, starts
	}()
	for c, e := range located {
		if e == nil {
			continue
		}
		picks, starts = picks[:0], starts[:0]
		for i := range rs {
			for j := range rs[i].bufs {
				rs[i].bufs[j] = rs[i].bufs[j][:0]
			}
			rs[i].cur = 0
		}
		for r := 0; r < n; r++ {
			if v := cols[c].value(r); v.Kind == vec.KindInt {
				picks = append(picks, placed{v.Int, int32(r)})
			}
		}
		slices.SortFunc(picks, func(a, b placed) int { return cmp.Compare(a.loc, b.loc) })
		for i := range picks {
			if i == 0 || picks[i].loc>>16 != picks[i-1].loc>>16 {
				starts = append(starts, i)
			}
		}
		starts = append(starts, len(picks))
		clear(vals)
		var failed atomic.Pointer[error]
		err := forEach(len(starts)-1, len(rs), func(slot, i int) {
			if err := rs[slot].read(parts, q, e, picks[starts[i]:starts[i+1]], vals); err != nil {
				failed.CompareAndSwap(nil, &err)
			}
		})
		if e := failed.Load(); e != nil {
			err = *e
		}
		if err != nil {
			return err
		}
		total := 0
		for i := range vals {
			total += len(vals[i].Bytes)
		}
		cols[c].reset()
		cols[c].reserve(n, total)
		for _, v := range vals {
			cols[c].appendValue(v)
		}
	}

	return nil
}

// uses reports whether e has a node of op.
func uses(e *Expr, op Op) bool {
	if e == nil {
		return false
	}
	for _, a := range e.Args {
		if uses(a, op) {
			return true
		}
	}

	return e.Op == op
}

// reads reports whether e refers to a column of names that locates a row.
func reads(e *Expr, names []string, located []*sexpr) bool {
	if e == nil {
		return false
	}
	if e.Op == OpColumn {
		for i, name := range names {
			if name == e.Path && located[i] != nil {
				return true
			}
		}
	}
	for _, a := range e.Args {
		if reads(a, names, located) {
			return true
		}
	}

	return false
}

// finish turns what the sinks hold into the columns of the result.
func (x *execution) finish(snap *colstore.Snapshot, sinks []*sink) ([]Column, int, error) {
	q := x.q
	if !q.early {
		out, n := gather(q, sinks)
		if err := resolve(snap, q, q.final, out, n, len(x.workers)); err != nil {
			return nil, 0, err
		}

		return x.shown(snap, out, n)
	}
	// The sinks hold every group as the aggregate left it: the located
	// values are read first, and what the plan does above the aggregate
	// runs over all groups at once.
	raw := make([]colVec, len(q.agg.names))
	if len(sinks) == 1 {
		raw = sinks[0].cols
	} else {
		reserveAll(raw, sinks)
		for _, s := range sinks {
			for c := range raw {
				raw[c].appendAll(&s.cols[c])
			}
		}
	}
	n := raw[0].len()
	if err := resolve(snap, q, q.located, raw, n, len(x.workers)); err != nil {
		return nil, 0, err
	}
	var having havingEval
	post := postEval{rows: x.rows}
	out := raw
	if q.having != nil && n > 0 {
		out = having.run(q, out, n)
		n = out[0].len()
	}
	if q.post != nil {
		out = post.run(q, out, n)
	}
	var last sink
	last.initFinal(q)
	if n > 0 {
		last.add(out)
	}
	out, n = gather(q, []*sink{&last})

	return x.shown(snap, out, n)
}

// shown turns the n rows of out into the columns of the result: the places
// of a plan that asks for the rest of the fields become those fields, and
// the hidden columns go.
func (x *execution) shown(snap *colstore.Snapshot, out []colVec, n int) ([]Column, int, error) {
	q := x.q
	names, named := q.names, len(q.names)
	if q.rest {
		named--
		others, cols, err := rest(snap, q, &out[named], n, len(x.workers))
		if err != nil {
			return nil, 0, err
		}
		names, out = append(names[:named:named], others...), append(out[:named:named], cols...)
	}
	shown := make([]Column, 0, len(out))
	for c, name := range names {
		if name == placeName || slices.Contains(q.hide, name) {
			continue
		}
		col := out[c].column(name)
		// The fields that the plan does not name come as they are stored.
		col.Field = c >= named || (c < len(q.fields) && q.fields[c])
		shown = append(shown, col)
	}

	return shown, n, nil
}

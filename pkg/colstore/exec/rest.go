package exec

import (
	"cmp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// cell is the value that a row of the result holds in one field.
type cell struct {
	row int32
	v   Value
}

// fieldSpan is the cells that the rows of one block hold in one field, in
// the order of the rows' places: cells from to to of a resolver.
type fieldSpan struct {
	field, block, slot int32
	from, to           int32
}

// fieldNames numbers the fields that the rows of a result hold, as the
// goroutines that read the rows meet them.
type fieldNames struct {
	mu     sync.Mutex
	ids    map[string]int32
	names  []string
	hidden []string
	// under, when set, names the objects whose members alone are shown.
	under []string
}

// inside reports whether path is a member of the object at one of objects.
func inside(path string, objects []string) bool {
	for _, o := range objects {
		if len(path) > len(o) && path[len(o)] == '.' && strings.HasPrefix(path, o) {
			return true
		}
	}

	return false
}

// id returns the number of the field at path, or -1 for one that the plan
// hides: a hidden path, or a member of one.
func (f *fieldNames) id(path string) int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id, ok := f.ids[path]; ok {
		return id
	}
	id := int32(len(f.names))
	switch {
	case f.under != nil:
		// A member that the plan names is a column of its own already.
		if !inside(path, f.under) || slices.Contains(f.hidden, path) {
			id = -1
		}
	default:
		for _, h := range f.hidden {
			if path == h || (strings.HasPrefix(path, h) && path[len(h)] == '.') {
				id = -1
			}
		}
	}
	if id >= 0 {
		f.names = append(f.names, path)
	}
	f.ids[path] = id

	return id
}

// fields adds to the resolver's cells the values that the rows of picks,
// all of which are in block, hold in every field that is not hidden, one
// span per field.
func (r *resolver) fields(parts []*colstore.Handle, names *fieldNames, block int32, picks []placed) error {
	loc := picks[0].loc >> 16
	p := parts[loc>>28].Part
	if err := p.OpenBlock(int(loc&(1<<28-1)), &r.b); err != nil {
		return err
	}
	paths := p.Paths()
	refs := r.b.Columns()
	for i := range refs {
		ref := &refs[i]
		if ref.Count == 0 || ref.Kind < vec.KindNull || ref.Kind > vec.KindVariant {
			continue
		}
		if names.under != nil && !inside(paths[ref.PathID], names.under) {
			continue
		}
		// Most fields of a wide table are in few rows: which rows hold the
		// field costs less to read than its values.
		if err := r.b.DecodePresence(ref, &r.col); err != nil {
			return err
		}
		held := false
		for _, pk := range picks {
			held = held || r.col.IsValid(int(pk.loc&0xFFFF))
		}
		if !held {
			continue
		}
		id := names.id(paths[ref.PathID])
		if id < 0 {
			continue
		}
		if ref.Kind != vec.KindNull {
			if err := r.b.Decode(ref, &r.col); err != nil {
				return err
			}
		}
		from := len(r.cells)
		for _, pk := range picks {
			row := int(pk.loc & 0xFFFF)
			if !r.col.IsValid(row) {
				continue
			}
			v := Value{Kind: ref.Kind}
			switch ref.Kind {
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
			r.cells = append(r.cells, cell{pk.row, v})
		}
		r.spans = append(r.spans, fieldSpan{field: id, block: block, from: int32(from), to: int32(len(r.cells))})
	}

	return nil
}

// keptCells bounds the cells that a resolver keeps for the next query.
const keptCells = 1 << 18

// rest reads the fields of the n rows whose places at holds and returns
// them as one column per field, in the order of the fields' names. It opens
// each block that holds such a row once, on every worker.
func rest(snap *colstore.Snapshot, q *query, at *colVec, n, workers int) ([]string, []colVec, error) {
	if n == 0 {
		return nil, nil, nil
	}
	parts := snap.Parts()
	picks := make([]placed, 0, n)
	for r := 0; r < n; r++ {
		if v := at.value(r); v.Kind == vec.KindInt {
			picks = append(picks, placed{v.Int, int32(r)})
		}
	}
	if len(picks) == 0 {
		return nil, nil, nil
	}
	slices.SortFunc(picks, func(a, b placed) int { return cmp.Compare(a.loc, b.loc) })
	var starts []int
	for i := range picks {
		if i == 0 || picks[i].loc>>16 != picks[i-1].loc>>16 {
			starts = append(starts, i)
		}
	}
	starts = append(starts, len(picks))
	blocks := len(starts) - 1
	names := &fieldNames{ids: map[string]int32{}, hidden: q.hidden, under: q.under}
	// The blocks and the columns decoded from them serve the next query.
	scratch := resolvePool.Get()
	defer resolvePool.Put(scratch)
	scratch.rs = fit(scratch.rs, max(1, min(workers, blocks)))
	rs := scratch.rs
	for i := range rs {
		for j := range rs[i].bufs {
			rs[i].bufs[j] = rs[i].bufs[j][:0]
		}
		rs[i].cur, rs[i].cells, rs[i].spans = 0, rs[i].cells[:0], rs[i].spans[:0]
	}
	defer func() {
		for i := range rs {
			if cap(rs[i].cells) > keptCells {
				rs[i].cells = nil
			}
		}
	}()
	var failed atomic.Pointer[error]
	err := forEach(blocks, len(rs), func(slot, i int) {
		if err := rs[slot].fields(parts, names, int32(i), picks[starts[i]:starts[i+1]]); err != nil {
			failed.CompareAndSwap(nil, &err)
		}
	})
	if e := failed.Load(); e != nil {
		err = *e
	}
	if err != nil {
		return nil, nil, err
	}
	// The fields in the order of their names, whatever goroutine met which
	// first.
	order := make([]int32, len(names.names))
	for i := range order {
		order[i] = int32(i)
	}
	slices.SortFunc(order, func(a, b int32) int { return strings.Compare(names.names[a], names.names[b]) })
	column := make([]int32, len(order))
	sorted := make([]string, len(order))
	for c, id := range order {
		column[id], sorted[c] = int32(c), names.names[id]
	}
	// The blocks in the order of the first row of the result that each
	// holds: a result in the order of the scan then has the cells of a
	// field in the order of its rows.
	first := make([]int32, blocks)
	rank := make([]int32, blocks)
	for i := range first {
		first[i], rank[i] = picks[starts[i]].row, int32(i)
		for _, pk := range picks[starts[i]+1 : starts[i+1]] {
			first[i] = min(first[i], pk.row)
		}
	}
	slices.SortFunc(rank, func(a, b int32) int { return cmp.Compare(first[a], first[b]) })
	for r, i := range rank {
		first[i] = int32(r)
	}
	spans := scratch.spans[:0]
	for slot := range rs {
		for _, sp := range rs[slot].spans {
			sp.field, sp.block, sp.slot = column[sp.field], first[sp.block], int32(slot)
			spans = append(spans, sp)
		}
	}
	scratch.spans = spans
	slices.SortFunc(spans, func(a, b fieldSpan) int {
		return cmp.Or(cmp.Compare(a.field, b.field), cmp.Compare(a.block, b.block), cmp.Compare(a.from, b.from))
	})
	cols := make([]colVec, len(order))
	for i := 0; i < len(spans); {
		end := i + 1
		for end < len(spans) && spans[end].field == spans[i].field {
			end++
		}
		scratch.cells = fill(&cols[spans[i].field], rs, spans[i:end], n, scratch.cells)
		i = end
	}
	if cap(scratch.cells) > keptCells {
		scratch.cells = nil
	}
	for c := range cols {
		for cols[c].len() < n {
			cols[c].appendNull()
		}
	}

	return sorted, cols, nil
}

// fill makes c, which is empty, the column of n rows that hold the values
// of the cells in spans and nothing where no cell is. It returns tmp, which
// it uses for cells that are not in the order of their rows.
func fill(c *colVec, rs []resolver, spans []fieldSpan, n int, tmp []cell) []cell {
	cells, bytes := 0, 0
	for _, sp := range spans {
		cells += int(sp.to - sp.from)
	}
	if first := rs[spans[0].slot].cells[spans[0].from]; first.v.Kind == vec.KindString || first.v.Kind == vec.KindVariant {
		for _, sp := range spans {
			for _, cl := range rs[sp.slot].cells[sp.from:sp.to] {
				bytes += len(cl.v.Bytes)
			}
		}
	}
	c.reserve(n, bytes)
	last, ascending := int32(-1), true
	for _, sp := range spans {
		for _, cl := range rs[sp.slot].cells[sp.from:sp.to] {
			if ascending = cl.row > last; !ascending {
				break
			}
			for c.len() < int(cl.row) {
				c.appendNull()
			}
			c.appendValue(cl.v)
			last = cl.row
		}
		if !ascending {
			break
		}
	}
	if ascending {
		return tmp
	}
	// The result is not in the order of the scan, or a row holds values of
	// several kinds in the field, of which it gives the lowest.
	c.reset()
	tmp = slices.Grow(tmp[:0], cells)
	for _, sp := range spans {
		tmp = append(tmp, rs[sp.slot].cells[sp.from:sp.to]...)
	}
	slices.SortFunc(tmp, func(a, b cell) int { return cmp.Or(cmp.Compare(a.row, b.row), cmp.Compare(a.v.Kind, b.v.Kind)) })
	for _, cl := range tmp {
		if c.len() > int(cl.row) {
			continue
		}
		for c.len() < int(cl.row) {
			c.appendNull()
		}
		c.appendValue(cl.v)
	}

	return tmp
}

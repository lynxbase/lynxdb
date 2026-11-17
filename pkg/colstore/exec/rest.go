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
	field int32
	row   int32
	v     Value
}

// fieldNames numbers the fields that the rows of a result hold, as the
// goroutines that read the rows meet them.
type fieldNames struct {
	mu     sync.Mutex
	ids    map[string]int32
	names  []string
	hidden []string
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
	for _, h := range f.hidden {
		if path == h || (strings.HasPrefix(path, h) && path[len(h)] == '.') {
			id = -1
		}
	}
	if id >= 0 {
		f.names = append(f.names, path)
	}
	f.ids[path] = id

	return id
}

// fields appends to cells the values that the rows of picks, all of which
// are in one block, hold in every field that is not hidden.
func (r *resolver) fields(parts []*colstore.Handle, names *fieldNames, picks []placed, cells []cell) ([]cell, error) {
	block := picks[0].loc >> 16
	p := parts[block>>28].Part
	if err := p.OpenBlock(int(block&(1<<28-1)), &r.b); err != nil {
		return cells, err
	}
	paths := p.Paths()
	refs := r.b.Columns()
	for i := range refs {
		ref := &refs[i]
		if ref.Count == 0 || ref.Kind < vec.KindNull || ref.Kind > vec.KindVariant {
			continue
		}
		// Most fields of a wide table are in few rows: which rows hold the
		// field costs less to read than its values.
		if err := r.b.DecodePresence(ref, &r.col); err != nil {
			return cells, err
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
				return cells, err
			}
		}
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
			cells = append(cells, cell{id, pk.row, v})
		}
	}

	return cells, nil
}

// rest reads the fields of the n rows whose places at holds and returns
// them as one column per field, in the order of the fields' names. It opens
// each block that holds such a row once, on every worker.
func rest(snap *colstore.Snapshot, q *query, at *colVec, n, workers int) ([]string, []colVec, error) {
	if n == 0 {
		return nil, nil, nil
	}
	parts := snap.Parts()
	picks := make([]placed, n)
	for r := range picks {
		picks[r] = placed{at.value(r).Int, int32(r)}
	}
	slices.SortFunc(picks, func(a, b placed) int { return cmp.Compare(a.loc, b.loc) })
	var starts []int
	for i := range picks {
		if i == 0 || picks[i].loc>>16 != picks[i-1].loc>>16 {
			starts = append(starts, i)
		}
	}
	starts = append(starts, n)
	names := &fieldNames{ids: map[string]int32{}, hidden: q.hidden}
	rs := make([]resolver, max(1, min(workers, len(starts)-1)))
	found := make([][]cell, len(rs))
	var failed atomic.Pointer[error]
	err := forEach(len(starts)-1, len(rs), func(slot, i int) {
		var err error
		if found[slot], err = rs[slot].fields(parts, names, picks[starts[i]:starts[i+1]], found[slot]); err != nil {
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
	var cells []cell
	for _, f := range found {
		cells = append(cells, f...)
	}
	// A row that holds values of several kinds in a field gives the lowest.
	slices.SortFunc(cells, func(a, b cell) int {
		return cmp.Or(cmp.Compare(column[a.field], column[b.field]), cmp.Compare(a.row, b.row), cmp.Compare(a.v.Kind, b.v.Kind))
	})
	cols := make([]colVec, len(order))
	for i := 0; i < len(cells); {
		c := &cols[column[cells[i].field]]
		field := cells[i].field
		for ; i < len(cells) && cells[i].field == field; i++ {
			row := int(cells[i].row)
			if c.len() > row {
				continue
			}
			for c.len() < row {
				c.appendNull()
			}
			if cells[i].v.Kind == vec.KindNull {
				c.appendPresentNull()
			} else {
				c.appendValue(cells[i].v)
			}
		}
	}
	for c := range cols {
		for cols[c].len() < n {
			cols[c].appendNull()
		}
	}

	return sorted, cols, nil
}

package pipeline

// Row-level streaming iterators (filter, project) used by the physical
// planner, plus batch/row collection helpers.

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

type FilterIterator struct {
	child     Iterator
	predicate *vm.Program
	vmInst    vm.VM
}

func (f *FilterIterator) Init(ctx context.Context) error { return f.child.Init(ctx) }
func (f *FilterIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := f.child.Next(ctx)
		if batch == nil || err != nil {
			return nil, err
		}
		mask := make([]bool, batch.Len)
		matchCount := 0
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			if f.predicate != nil {
				result, _ := f.vmInst.Execute(f.predicate, row)
				if result.Type() == event.FieldTypeBool && result.AsBool() {
					mask[i] = true
					matchCount++
				}
			}
		}
		if matchCount == 0 {
			continue
		}
		if matchCount == batch.Len {
			return batch, nil
		}
		return compactBatch(batch, mask, matchCount), nil
	}
}
func (f *FilterIterator) Close() error { return f.child.Close() }

func NewFilterIterator(child Iterator, predicate *vm.Program) *FilterIterator {
	return &FilterIterator{child: child, predicate: predicate}
}

type ProjectIterator struct {
	child  Iterator
	fields []string
	remove bool
}

func (p *ProjectIterator) Init(ctx context.Context) error { return p.child.Init(ctx) }
func (p *ProjectIterator) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	batch, err := p.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}
	if p.remove {
		return p.applyRemove(batch), nil
	}
	return p.applyKeep(batch), nil
}
func (p *ProjectIterator) Close() error { return p.child.Close() }

func (p *ProjectIterator) applyKeep(batch *Batch) *Batch {
	out := &Batch{Columns: make(map[string][]event.Value, len(p.fields)), Len: batch.Len}
	for _, f := range p.fields {
		if strings.ContainsAny(f, "*?") {
			for col, vals := range batch.Columns {
				if matched, _ := path.Match(f, col); matched {
					out.Columns[col] = vals
					copyPresenceColumn(out, batch, col)
				}
			}
		} else {
			// A field that is an object has no column of its own: its
			// members are columns named after it.
			members := false
			for col, vals := range batch.Columns {
				if len(col) > len(f) && col[len(f)] == '.' && strings.HasPrefix(col, f) {
					out.Columns[col] = vals
					copyPresenceColumn(out, batch, col)
					members = true
				}
			}
			if vals, ok := batch.Columns[f]; ok {
				out.Columns[f] = vals
				copyPresenceColumn(out, batch, f)
			} else if !members {
				nulls := make([]event.Value, batch.Len)
				for i := range nulls {
					nulls[i] = event.NullValue()
				}
				out.Columns[f] = nulls
			}
		}
	}
	return out
}

func (p *ProjectIterator) applyRemove(batch *Batch) *Batch {
	out := &Batch{Columns: make(map[string][]event.Value, len(batch.Columns)), Len: batch.Len}
	for col, vals := range batch.Columns {
		drop := false
		for _, f := range p.fields {
			if strings.ContainsAny(f, "*?") {
				if matched, _ := path.Match(f, col); matched {
					drop = true
					break
				}
			} else if col == f || (len(col) > len(f) && col[len(f)] == '.' && strings.HasPrefix(col, f)) {
				// The members of a dropped object go with it.
				drop = true
				break
			}
		}
		if !drop {
			out.Columns[col] = vals
			copyPresenceColumn(out, batch, col)
		}
	}
	return out
}

func NewProjectIterator(child Iterator, fields []string, remove bool) *ProjectIterator {
	return &ProjectIterator{child: child, fields: fields, remove: remove}
}

func compactBatch(batch *Batch, mask []bool, matchCount int) *Batch {
	result := &Batch{Columns: make(map[string][]event.Value, len(batch.Columns)), Len: matchCount}
	for k, col := range batch.Columns {
		out := make([]event.Value, 0, matchCount)
		var outPres []bool
		if _, ok := batch.Present[k]; ok {
			outPres = make([]bool, 0, matchCount)
		}
		for i, v := range col {
			if i < batch.Len && mask[i] {
				out = append(out, v)
				if outPres != nil {
					outPres = append(outPres, batch.IsPresent(k, i))
				}
			}
		}
		result.Columns[k] = out
		if outPres != nil {
			setPresenceIfSparse(result, k, outPres)
		}
	}
	return result
}

func CollectAll(ctx context.Context, iter Iterator) (rows []map[string]event.Value, err error) {
	if err := iter.Init(ctx); err != nil {
		return nil, err
	}
	defer func() {
		if e := iter.Close(); e != nil {
			err = errors.Join(err, e)
		}
	}()
	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			rows = append(rows, batch.Row(i))
		}
	}
	return rows, nil
}

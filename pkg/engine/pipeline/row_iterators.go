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
	evalCount int64
	passCount int64
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
			f.evalCount++
			if f.predicate != nil {
				result, _ := f.vmInst.Execute(f.predicate, row)
				if result.Type() == event.FieldTypeBool && result.AsBool() {
					mask[i] = true
					matchCount++
					f.passCount++
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
func (f *FilterIterator) Close() error        { return f.child.Close() }
func (f *FilterIterator) Schema() []FieldInfo { return f.child.Schema() }
func (f *FilterIterator) VMStats() (int64, int64) {
	return f.evalCount, f.passCount
}

func NewFilterIterator(child Iterator, predicate *vm.Program) *FilterIterator {
	return &FilterIterator{child: child, predicate: predicate}
}

type ProjectIterator struct {
	child   Iterator
	fields  []string
	remove  bool
	hasGlob bool
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
func (p *ProjectIterator) Schema() []FieldInfo {
	if p.hasGlob {
		return nil
	}
	if p.remove {
		return p.child.Schema()
	}
	schema := make([]FieldInfo, len(p.fields))
	for i, f := range p.fields {
		schema[i] = FieldInfo{Name: f}
	}
	return schema
}

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
			if vals, ok := batch.Columns[f]; ok {
				out.Columns[f] = vals
				copyPresenceColumn(out, batch, f)
			} else {
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
			} else if col == f {
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
	hasGlob := false
	for _, f := range fields {
		if strings.ContainsAny(f, "*?") {
			hasGlob = true
			break
		}
	}
	return &ProjectIterator{child: child, fields: fields, remove: remove, hasGlob: hasGlob}
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

type CollectOptions struct {
	OnBatch func(int, []map[string]event.Value)
}

func CollectAll(ctx context.Context, iter Iterator, opts ...CollectOptions) (rows []map[string]event.Value, err error) {
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
		if len(opts) > 0 && opts[0].OnBatch != nil {
			opts[0].OnBatch(len(rows), rows)
		}
	}
	return rows, nil
}

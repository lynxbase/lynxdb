package pipeline

// Row-level streaming iterators (filter, project, tail) used by the physical
// planner, plus batch/row collection helpers and the IndexStore abstractions
// used by the server execution paths.

import (
	"context"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
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

// TailIterator keeps the last N rows from a child using a fixed-capacity ring
// buffer. Memory usage is bounded to O(count) regardless of input size. The
// ring buffer pattern mirrors pkg/usecases/tail.go catchupRing.
type TailIterator struct {
	child    Iterator
	count    int
	ring     []map[string]event.Value // fixed-capacity ring buffer
	ringPos  int                      // next write position when ring is full
	ringFull bool                     // true once the ring has wrapped
	consumed bool
	emitted  bool
	acct     memgov.MemoryAccount
	tracked  int64 // bytes currently tracked in the account
}

func (t *TailIterator) Init(ctx context.Context) error { return t.child.Init(ctx) }
func (t *TailIterator) Next(ctx context.Context) (*Batch, error) {
	if !t.consumed {
		// Consume all rows from child, keeping the last t.count in a ring buffer.
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			batch, err := t.child.Next(ctx)
			if err != nil {
				return nil, err
			}
			if batch == nil {
				break
			}
			for i := 0; i < batch.Len; i++ {
				row := batch.Row(i)
				rowBytes := EstimateRowBytes(row)

				if !t.ringFull {
					// Ring not yet full -- grow into capacity.
					if t.acct != nil {
						if err := t.acct.Grow(rowBytes); err != nil {
							return nil, err
						}
						t.tracked += rowBytes
					}
					t.ring = append(t.ring, row)
					if len(t.ring) == t.count {
						t.ringFull = true
						t.ringPos = 0
					}
				} else {
					// Ring full -- evict oldest, shrink its estimate, grow new.
					evicted := t.ring[t.ringPos]
					evictedBytes := EstimateRowBytes(evicted)
					if t.acct != nil {
						t.acct.Shrink(evictedBytes)
						t.tracked -= evictedBytes
						if err := t.acct.Grow(rowBytes); err != nil {
							return nil, err
						}
						t.tracked += rowBytes
					}
					t.ring[t.ringPos] = row
					t.ringPos = (t.ringPos + 1) % t.count
				}
			}
		}
		t.consumed = true
	}
	if t.emitted || len(t.ring) == 0 {
		return nil, nil
	}
	t.emitted = true

	// Linearize ring to chronological order.
	var rows []map[string]event.Value
	if !t.ringFull {
		rows = t.ring
	} else {
		rows = make([]map[string]event.Value, t.count)
		copy(rows, t.ring[t.ringPos:])
		copy(rows[t.count-t.ringPos:], t.ring[:t.ringPos])
	}
	return RowsToBatch(rows), nil
}

func (t *TailIterator) Close() error {
	if t.acct != nil {
		if t.tracked > 0 {
			t.acct.Shrink(t.tracked)
			t.tracked = 0
		}
		t.acct.Close()
	}
	return t.child.Close()
}
func (t *TailIterator) Schema() []FieldInfo { return t.child.Schema() }

func NewTailIterator(child Iterator, count, _ int) *TailIterator {
	if count <= 0 {
		count = 10
	}
	return &TailIterator{
		child: child,
		count: count,
		ring:  make([]map[string]event.Value, 0, count),
		acct:  memgov.NopAccount(),
	}
}

// NewTailIteratorWithBudget creates a TailIterator that tracks memory usage
// via the provided account. The account is Closed when the iterator is closed.
func NewTailIteratorWithBudget(child Iterator, count, batchSize int, acct memgov.MemoryAccount) *TailIterator {
	if count <= 0 {
		count = 10
	}
	return &TailIterator{
		child: child,
		count: count,
		ring:  make([]map[string]event.Value, 0, count),
		acct:  memgov.EnsureAccount(acct),
	}
}

// RowsToBatch converts row maps to a columnar batch.
func RowsToBatch(rows []map[string]event.Value) *Batch {
	if len(rows) == 0 {
		return nil
	}
	cols := make(map[string][]event.Value)
	// Collect all column names.
	colNames := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			colNames[k] = struct{}{}
		}
	}
	for name := range colNames {
		col := make([]event.Value, len(rows))
		for i, row := range rows {
			if v, ok := row[name]; ok {
				col[i] = v
			} else {
				col[i] = event.NullValue()
			}
		}
		cols[name] = col
	}
	return &Batch{Columns: cols, Len: len(rows)}
}

func parseDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	if len(s) > 1 && s[len(s)-1] == 'd' {
		n := 0
		for _, c := range s[:len(s)-1] {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}
		return time.Duration(n) * 24 * time.Hour
	}
	return 0
}

func copyBSIMetadataFilter(dst, src *Batch, mask []bool) {
	if dst == nil || src == nil || len(mask) == 0 {
		return
	}
	var indices []int
	for i, keep := range mask {
		if keep {
			indices = append(indices, i)
		}
	}
	copyBSIMetadataPermutation(dst, src, indices)
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
	copyBSIMetadataFilter(result, batch, mask)
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

func CollectAllWithBudget(ctx context.Context, iter Iterator, acct memgov.MemoryAccount) (rows []map[string]event.Value, err error) {
	acct = memgov.EnsureAccount(acct)
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
			row := batch.Row(i)
			if e := acct.Grow(EstimateRowBytes(row)); e != nil {
				return nil, e
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func growRowsEstimate(acct memgov.MemoryAccount, rows []map[string]event.Value) error {
	for _, row := range rows {
		if err := acct.Grow(EstimateRowBytes(row)); err != nil {
			return err
		}
	}
	return nil
}

type IndexStore interface {
	MaterializeEvents(ctx context.Context, index string) ([]*event.Event, error)
}
type ServerIndexStore struct{ Events map[string][]*event.Event }

func (s *ServerIndexStore) MaterializeEvents(_ context.Context, index string) ([]*event.Event, error) {
	return s.Events[index], nil
}

type ColumnarBatchStore struct{ Batches map[string][]*Batch }

func (s *ColumnarBatchStore) MaterializeEvents(_ context.Context, _ string) ([]*event.Event, error) {
	return nil, nil
}

// BuildResult bundles the root iterator of a built pipeline with its
// optional memory coordinator and governor budget.
type BuildResult struct {
	Iterator    Iterator
	Coordinator *MemoryCoordinator
	GovBudget   *memgov.BudgetAdapter
}

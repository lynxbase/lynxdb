package pipeline

import "context"

// LimitIterator implements HEAD/TAIL with early termination.
// After collecting N rows, it stops calling child.Next() entirely.
type LimitIterator struct {
	child     Iterator
	limit     int
	collected int
}

// NewLimitIterator creates a limit operator that stops after n rows.
func NewLimitIterator(child Iterator, n int) *LimitIterator {
	return &LimitIterator{child: child, limit: n}
}

func (l *LimitIterator) Init(ctx context.Context) error {
	return l.child.Init(ctx)
}

func (l *LimitIterator) Next(ctx context.Context) (*Batch, error) {
	if l.collected >= l.limit {
		return nil, nil // early termination
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	batch, err := l.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	remaining := l.limit - l.collected
	if batch.Len <= remaining {
		l.collected += batch.Len

		return batch, nil
	}
	// Truncate batch to remaining
	result := batch.Slice(0, remaining)
	l.collected += result.Len

	return result, nil
}

func (l *LimitIterator) Close() error {
	return l.child.Close()
}

// OffsetIterator skips the first N rows from its child.
type OffsetIterator struct {
	child   Iterator
	offset  int
	skipped int
}

// NewOffsetIterator creates an operator that drops the first n rows.
func NewOffsetIterator(child Iterator, n int) *OffsetIterator {
	return &OffsetIterator{child: child, offset: n}
}

func (o *OffsetIterator) Init(ctx context.Context) error {
	return o.child.Init(ctx)
}

func (o *OffsetIterator) Next(ctx context.Context) (*Batch, error) {
	for o.skipped < o.offset {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		batch, err := o.child.Next(ctx)
		if batch == nil || err != nil {
			return nil, err
		}

		remaining := o.offset - o.skipped
		if batch.Len <= remaining {
			o.skipped += batch.Len
			continue
		}

		o.skipped = o.offset
		return batch.Slice(remaining, batch.Len), nil
	}

	return o.child.Next(ctx)
}

func (o *OffsetIterator) Close() error {
	return o.child.Close()
}

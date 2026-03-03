package pipeline

import "context"

// UnionIterator concatenates output from multiple child iterators.
type UnionIterator struct {
	children []Iterator
	current  int
}

// NewUnionIterator creates an APPEND/MULTISEARCH concat operator.
func NewUnionIterator(children []Iterator) *UnionIterator {
	return &UnionIterator{children: children}
}

func (u *UnionIterator) Init(ctx context.Context) error {
	for _, c := range u.children {
		if err := c.Init(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (u *UnionIterator) Next(ctx context.Context) (*Batch, error) {
	for u.current < len(u.children) {
		batch, err := u.children[u.current].Next(ctx)
		if err != nil {
			return nil, err
		}
		if batch != nil {
			return batch, nil
		}
		u.current++
	}

	return nil, nil
}

func (u *UnionIterator) Close() error {
	for _, c := range u.children {
		c.Close()
	}

	return nil
}

func (u *UnionIterator) Schema() []FieldInfo { return nil }

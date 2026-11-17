package pipeline

import (
	"context"
	"errors"
	"testing"
)

func TestCollectAllReturnsCloseError(t *testing.T) {
	wantErr := errors.New("close failed")
	_, err := CollectAll(context.Background(), &closeErrorIterator{closeErr: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("CollectAll error = %v, want close error", err)
	}
}

type closeErrorIterator struct {
	closeErr error
}

func (c *closeErrorIterator) Init(context.Context) error { return nil }

func (c *closeErrorIterator) Next(context.Context) (*Batch, error) { return nil, nil }

func (c *closeErrorIterator) Close() error { return c.closeErr }

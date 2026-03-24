package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TeeIterator implements the tee pipeline operator — a side-effect passthrough
// that writes each batch to a destination file, then yields the batch unchanged.
type TeeIterator struct {
	child  Iterator
	dest   string
	format string
	writer *os.File
	enc    *json.Encoder
}

func NewTeeIterator(child Iterator, dest string, format string) *TeeIterator {
	return &TeeIterator{child: child, dest: dest, format: format}
}

func (t *TeeIterator) Init(ctx context.Context) error {
	f, err := os.Create(t.dest)
	if err != nil {
		return fmt.Errorf("tee: cannot create %s: %w", t.dest, err)
	}

	t.writer = f
	t.enc = json.NewEncoder(f)

	return t.child.Init(ctx)
}

func (t *TeeIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := t.child.Next(ctx)
	if batch != nil && t.writer != nil {
		for i := 0; i < batch.Len; i++ {
			_ = t.enc.Encode(teeToMap(batch.Row(i)))
		}
	}

	return batch, err
}

func (t *TeeIterator) Close() error {
	if t.writer != nil {
		_ = t.writer.Close()
	}

	return t.child.Close()
}

func (t *TeeIterator) Schema() []FieldInfo { return t.child.Schema() }

func (t *TeeIterator) Child() Iterator { return t.child }

// teeToMap converts a pipeline row (map[string]event.Value) to a JSON-friendly map.
func teeToMap(row map[string]event.Value) map[string]interface{} {
	out := make(map[string]interface{}, len(row))
	for k, v := range row {
		out[k] = v.Interface()
	}

	return out
}

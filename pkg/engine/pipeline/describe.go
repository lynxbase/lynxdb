package pipeline

import (
	"context"
	"fmt"
	"os"
)

// DescribeIterator is a logging passthrough: reads batches from child, prints
// schema info to stderr on first batch, then yields batches unchanged.
// Unlike GlimpseIterator (blocking), DescribeIterator is non-destructive.
type DescribeIterator struct {
	child   Iterator
	printed bool
}

// NewDescribeIterator creates a describe operator that prints schema on first batch.
func NewDescribeIterator(child Iterator) *DescribeIterator {
	return &DescribeIterator{child: child}
}

func (d *DescribeIterator) Init(ctx context.Context) error {
	return d.child.Init(ctx)
}

func (d *DescribeIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := d.child.Next(ctx)
	if batch != nil && !d.printed && batch.Len > 0 {
		d.printed = true
		d.printSchema(batch)
	}

	return batch, err
}

func (d *DescribeIterator) Close() error {
	return d.child.Close()
}

func (d *DescribeIterator) Schema() []FieldInfo {
	return d.child.Schema()
}

func (d *DescribeIterator) printSchema(batch *Batch) {
	fmt.Fprintln(os.Stderr, "--- describe ---")
	for _, name := range batch.ColumnNames() {
		vals := batch.Columns[name]
		if len(vals) == 0 {
			continue
		}
		typ := vals[0].Type().String()
		sample := vals[0].String()
		if len(sample) > 60 {
			sample = sample[:60] + "..."
		}
		fmt.Fprintf(os.Stderr, "  %-30s %-10s %s\n", name, typ, sample)
	}
	fmt.Fprintln(os.Stderr, "----------------")
}

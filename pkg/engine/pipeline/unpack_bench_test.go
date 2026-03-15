package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// BenchmarkUnpackIterator_Prefix benchmarks the unpack iterator with prefix
// key caching. Each batch has 1024 rows with JSON data producing ~4 fields each.
func BenchmarkUnpackIterator_Prefix(b *testing.B) {
	const batchSize = 1024
	rows := make([]event.Value, batchSize)
	for i := range rows {
		rows[i] = event.StringValue(fmt.Sprintf(
			`{"level":"error","status":%d,"service":"api-gw","host":"web-%02d"}`,
			200+i%5*100, i%10,
		))
	}

	parser, _ := unpack.NewParser("json")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clone batch columns to prevent mutation from accumulating.
		cloned := &Batch{
			Columns: map[string][]event.Value{"_raw": rows},
			Len:     batchSize,
		}
		iter := NewUnpackIterator(
			&staticIterator{batches: []*Batch{cloned}},
			parser, "_raw", nil, "j.", false,
		)
		_ = iter.Init(ctx)
		result, _ := iter.Next(ctx)
		if result == nil {
			b.Fatal("expected non-nil batch")
		}
	}
}

// BenchmarkUnpackIterator_NoPrefix benchmarks the unpack iterator without prefix
// (no prefix cache needed).
func BenchmarkUnpackIterator_NoPrefix(b *testing.B) {
	const batchSize = 1024
	rows := make([]event.Value, batchSize)
	for i := range rows {
		rows[i] = event.StringValue(fmt.Sprintf(
			`{"level":"error","status":%d,"service":"api-gw","host":"web-%02d"}`,
			200+i%5*100, i%10,
		))
	}

	parser, _ := unpack.NewParser("json")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cloned := &Batch{
			Columns: map[string][]event.Value{"_raw": rows},
			Len:     batchSize,
		}
		iter := NewUnpackIterator(
			&staticIterator{batches: []*Batch{cloned}},
			parser, "_raw", nil, "", false,
		)
		_ = iter.Init(ctx)
		result, _ := iter.Next(ctx)
		if result == nil {
			b.Fatal("expected non-nil batch")
		}
	}
}

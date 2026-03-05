package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// makeCompoundBenchEvents creates N events with status and level fields
// for benchmarking compound filter expressions.
func makeCompoundBenchEvents(n int) []*event.Event {
	events := make([]*event.Event, n)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	for i := range events {
		events[i] = &event.Event{
			Time:   base.Add(time.Duration(i) * time.Second),
			Raw:    "log line",
			Source: "bench",
			Fields: map[string]event.Value{
				"status": event.IntValue(int64(200 + (i%5)*100)),
				"level":  event.StringValue(levels[i%4]),
			},
		}
	}
	return events
}

// BenchmarkVecCompound_AND benchmarks vectorized compound AND filter.
func BenchmarkVecCompound_AND(b *testing.B) {
	events := makeCompoundBenchEvents(1024)
	// status >= 500 AND level = "ERROR"
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "level"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "ERROR"},
		},
	}

	prog := &vm.Program{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		filter := NewFilterIteratorWithExpr(scan, prog, expr)
		_ = filter.Init(ctx)
		batch, _ := filter.Next(ctx)
		if batch == nil {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkVecCompound_OR benchmarks vectorized compound OR filter.
func BenchmarkVecCompound_OR(b *testing.B) {
	events := makeCompoundBenchEvents(1024)
	// status = 200 OR status = 500
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "200"},
		},
		Op: "or",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
	}

	prog := &vm.Program{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		filter := NewFilterIteratorWithExpr(scan, prog, expr)
		_ = filter.Init(ctx)
		batch, _ := filter.Next(ctx)
		if batch == nil {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkVecIN benchmarks vectorized IN hashset lookup.
func BenchmarkVecIN(b *testing.B) {
	events := makeCompoundBenchEvents(1024)
	expr := &spl2.InExpr{
		Field: &spl2.FieldExpr{Name: "status"},
		Values: []spl2.Expr{
			&spl2.LiteralExpr{Value: "200"},
			&spl2.LiteralExpr{Value: "400"},
			&spl2.LiteralExpr{Value: "500"},
		},
	}

	prog := &vm.Program{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		filter := NewFilterIteratorWithExpr(scan, prog, expr)
		_ = filter.Init(ctx)
		batch, _ := filter.Next(ctx)
		if batch == nil {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkVecLIKE benchmarks vectorized LIKE prefix filter.
func BenchmarkVecLIKE(b *testing.B) {
	events := make([]*event.Event, 1024)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range events {
		var uri string
		if i%5 == 0 {
			uri = "/api/users/123"
		} else {
			uri = "/web/index.html"
		}
		events[i] = &event.Event{
			Time: base.Add(time.Duration(i) * time.Second),
			Raw:  "request",
			Fields: map[string]event.Value{
				"uri": event.StringValue(uri),
			},
		}
	}

	expr := &spl2.CompareExpr{
		Left:  &spl2.FieldExpr{Name: "uri"},
		Op:    "like",
		Right: &spl2.LiteralExpr{Value: "/api/%"},
	}

	prog := &vm.Program{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		filter := NewFilterIteratorWithExpr(scan, prog, expr)
		_ = filter.Init(ctx)
		batch, _ := filter.Next(ctx)
		if batch == nil {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkVecNullCheck benchmarks vectorized isnull check.
func BenchmarkVecNullCheck(b *testing.B) {
	events := make([]*event.Event, 1024)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range events {
		fields := map[string]event.Value{}
		if i%3 != 0 {
			fields["host"] = event.StringValue("web-01")
		}
		events[i] = &event.Event{
			Time:   base.Add(time.Duration(i) * time.Second),
			Raw:    "test",
			Fields: fields,
		}
	}

	expr := &spl2.FuncCallExpr{
		Name: "isnotnull",
		Args: []spl2.Expr{&spl2.FieldExpr{Name: "host"}},
	}

	prog := &vm.Program{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		filter := NewFilterIteratorWithExpr(scan, prog, expr)
		_ = filter.Init(ctx)
		batch, _ := filter.Next(ctx)
		if batch == nil {
			b.Fatal("expected results")
		}
	}
}

// BenchmarkVecRange benchmarks vectorized BETWEEN (fused range).
func BenchmarkVecRange(b *testing.B) {
	events := makeCompoundBenchEvents(1024)
	// status >= 300 AND status <= 500 → fused range
	expr := &spl2.BinaryExpr{
		Left: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    ">=",
			Right: &spl2.LiteralExpr{Value: "300"},
		},
		Op: "and",
		Right: &spl2.CompareExpr{
			Left:  &spl2.FieldExpr{Name: "status"},
			Op:    "<=",
			Right: &spl2.LiteralExpr{Value: "500"},
		},
	}

	prog := &vm.Program{}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		filter := NewFilterIteratorWithExpr(scan, prog, expr)
		_ = filter.Init(ctx)
		batch, _ := filter.Next(ctx)
		if batch == nil {
			b.Fatal("expected results")
		}
	}
}
